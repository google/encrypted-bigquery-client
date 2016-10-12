#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.

"""Contains generally applicable crypto methods.

  AesCbc: class for encrypting and decrypting in cbc mode with pkcs padding.
  GetRandBytes: function that returns random bytes.
  PRF: a pseudorandom function.
"""



import hashlib
import hmac
import os
import platform
import sys

from Crypto.Cipher import AES


DEFAULT_PRF_OUTPUT_LEN = 16
_f_urandom_fh = None
_f_urandom_func = None
_F_URANDOM_BUFLEN = 8192


def GetRandBytes(size, _open=open):  # pylint: disable=invalid-name
  """Returns size number of bytes."""
  global _f_urandom_fh
  global _f_urandom_func

  if size <= 0:
    raise ValueError('Size has to be positive.')

  if _f_urandom_func is not None:
    return _f_urandom_func(size)

  if platform.uname()[0] == 'Linux':
    _f_urandom_fh = _open('/dev/urandom', 'rb', _F_URANDOM_BUFLEN)
    _f_urandom_func = _f_urandom_fh.read  # same args and return
  else:
    _f_urandom_func = os.urandom

  return GetRandBytes(size)


def PRF(key, input_str, output_len=DEFAULT_PRF_OUTPUT_LEN, hashfunc='sha1'):
  """Creates pseudorandom output based on key and specified input.

  The hmac is used to create a prf that accepts input of varying size and
  outputs pseudorandom str of varying size.

  Args:
    key: used to create pseudorandom output.
    input_str: a str that will be interpreted as raw bytes, mapped to a
               pseudorandom output.
    output_len: the length of the output requested in bytes - default is 16, and
                has to be less than 100,000,000.
    hashfunc: the underlying hash func used - default is 'sha1'. Other hashes
              guaranteed to work are md5, sha224, sha256, sha384, and sha512
              although more may work on a specific platform.

  Returns:
    a pseudorandom str of output_len consisting of the concatenation of
    16 bytes of hmac(0, input), 16 bytes of hmac(1, input),....,.

  Raises:
    ValueError: When key or output_len is empty, or input_str is not str.
  """
  if len(key) < 1:
    raise ValueError('Key to PRF has to be a byte or larger.')
  if output_len < 1:
    raise ValueError('Prf output length has to be a byte or larger.')
  if not isinstance(input_str, str):
    raise ValueError('Expected str type for input_str, but got: %s'
                     % type(input_str))
  try:
    hasher = hashlib.new(hashfunc)
  except ValueError:
    raise  # something like "unsupported hash type {hashfunc}"
  hasher = getattr(hashlib, hashfunc)
  count = 0
  output = []
  for _ in xrange(output_len / 16):
    output.append(hmac.new(key, IntToFixedSizeString(count) + input_str,
                           hasher).digest()[:16])
    count += 1
  # if output_len is not a multiple of 16 then add the last partial block.
  if output_len % 16 != 0:
    output.append(hmac.new(
        key, IntToFixedSizeString(count) + input_str,
        hasher).digest()[:output_len % 16])
  return ''.join(output)


def IntToFixedSizeString(value):
  """Converts int to a fixed 8 character str."""
  if value >= 0 and value < 100000000:
    return '%8s' % value
  raise ValueError('value needs to be a positive integer less than '
                   '100000000')


class PRG(object):
  """Deterministically creates a psuedorandom stream based on a seed."""

  def __init__(self, seed):
    """PRG is initialized with a seed str containing at least 16 bytes."""
    if not isinstance(seed, str):
      raise ValueError('Expected str type for seed, but got: %s' % type(seed))
    if len(seed) < 16:
      raise ValueError('Size of seed has to be at least 16 characters.')
    self.__seed = seed
    self.__count = 0

  def GetNextBytes(self, n):
    """Returns next n bytes in pseudorandom stream created from seed."""
    first_block = self.__count / DEFAULT_PRF_OUTPUT_LEN
    last_block = (self.__count + n) / DEFAULT_PRF_OUTPUT_LEN
    buf = ''.join(PRF(self.__seed, str(k))
                  for k in xrange(first_block, last_block + 1))
    # Now adjust the prefix and suffix to get to the byte level.
    val = buf[self.__count % DEFAULT_PRF_OUTPUT_LEN :
              -(DEFAULT_PRF_OUTPUT_LEN - ((self.__count + n) %
                                          DEFAULT_PRF_OUTPUT_LEN))]
    self.__count += n
    return val


def PrintBytes(data):
  """Print hex values for a str."""
  for c in data:
    sys.stdout.write(r'\x' + c.encode('hex'))
  sys.stdout.write('\n')


class AesCbc(object):
  """Class for AES encryption using CBC/PKCS5Padding."""

  VALID_AES_KEY_LENGTHS = (16, 24, 32)
  AES_BLOCK_LEN = 16
  AES_IV_LEN = AES_BLOCK_LEN

  def __init__(self, key):
    """AesCbc is initialized with key of valid length."""
    if not isinstance(key, str):
      raise ValueError('Expected str type for key, but got: %s' % type(key))
    if len(key) not in AesCbc.VALID_AES_KEY_LENGTHS:
      raise ValueError('Incorrect sized AES key: %d' % len(key))
    self.__key = key

  def Encrypt(self, plaintext, iv=None):
    """Encrypts with AES/CBC/PKCS5PADDING mode.

    Aes encrypts plaintext using cbc mode with pkcs5 padding. If no iv is
    provided then uses a random iv.

    Args:
      plaintext: data to be encrypted, cannot be empty.
      iv: iv to use in cbc mode, if not provided then a random iv is selected.

    Returns:
      A ciphertext str with iv prepended if random, and no
      iv prepended if it was given - in this case the caller can decide to
      prepend it or not if the iv will be known (e.g. fixed for
      deterministic encryptions) at decryption time.

    Raises:
      ValueError: When plaintext is empty or of wrong type, or iv is of wrong
      length.
    """
    if not isinstance(plaintext, str):
      raise ValueError('Expected str type for plaintext, but got: %s' %
                       type(plaintext))
    if not plaintext:
      raise ValueError('input plaintext cannot be empty.')
    if iv is not None and len(iv) is not AesCbc.AES_IV_LEN:
      raise ValueError('Supplied iv size is incorrect: %d' % len(iv))

    if iv is None:
      iv_for_encrypt = GetRandBytes(AesCbc.AES_IV_LEN)
    else:
      iv_for_encrypt = iv
    # pkcs5 padding repeats the padded byte length value in each padded byte
    # till block is full. If plaintext is multiple of a block then a full block
    # of padding is added with the value 16 repeated in each byte.
    pkcs5_padding_len = (
        AesCbc.AES_BLOCK_LEN - (len(plaintext) % AesCbc.AES_BLOCK_LEN))
    pkcs5_padding = pkcs5_padding_len * chr(pkcs5_padding_len)

    cipher = AES.new(self.__key, AES.MODE_CBC, iv_for_encrypt)
    ciphertext = cipher.encrypt(plaintext + pkcs5_padding)
    if iv is None:
      return iv_for_encrypt + ciphertext
    else:
      return ciphertext

  def Decrypt(self, ciphertext, iv=None):
    """Decrypts with AES/CBC/PKCS5PADDING mode.

    Aes decrypts ciphertext using cbc mode with pkcs5 padding.

    Args:
      ciphertext: data to be decrypted, cannot be empty.
      iv: iv to use in cbc mode, if not provided then assume iv is prepended to
          the ciphertext.

    Returns:
      A plaintext str.

    Raises:
      ValueError: When iv and ciphertext is not of right type or length.
    """
    if not isinstance(ciphertext, str):
      raise ValueError('Expected str type for ciphertext, but got: %s' %
                       type(ciphertext))
    if not ciphertext:
      raise ValueError('ciphertext input cannot be empty.')
    if len(ciphertext) % AesCbc.AES_BLOCK_LEN != 0:
      raise ValueError('ciphertext input must be a multiple of block length: %d'
                       % len(ciphertext))
    if iv is not None and len(iv) is not AesCbc.AES_IV_LEN:
      raise ValueError('Supplied iv size is incorrect: %d' % len(iv))

    if iv is None:
      iv_for_decrypt = ciphertext[:AesCbc.AES_IV_LEN]
      ciphertext_for_decrypt = ciphertext[AesCbc.AES_IV_LEN:]
    else:
      iv_for_decrypt = iv
      ciphertext_for_decrypt = ciphertext

    cipher = AES.new(self.__key, AES.MODE_CBC, iv_for_decrypt)
    plaintext_with_pad = cipher.decrypt(ciphertext_for_decrypt)

    pkcs5_padding_len = ord(plaintext_with_pad[-1])
    if pkcs5_padding_len > AesCbc.AES_BLOCK_LEN:
      raise ValueError('Incorrect ciphertext padding in last block: %s'
                       % plaintext_with_pad[-1 * AesCbc.AES_BLOCK_LEN:])

    pkcs5_padding = pkcs5_padding_len * chr(pkcs5_padding_len)
    if plaintext_with_pad[-1 * pkcs5_padding_len:] != pkcs5_padding:
      raise ValueError('Incorrect ciphertext padding: %s'
                       % plaintext_with_pad[-1 * pkcs5_padding_len:])
    return plaintext_with_pad[:-pkcs5_padding_len]
