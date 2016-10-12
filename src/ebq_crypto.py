#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.

"""Contains crypto methods for ebq tool purposes."""



import base64
import hashlib
import random
import sys
from unicodedata import category

import common_crypto as ccrypto
import number
import paillier as pcrypto


# Dict which maps all unicode punctuation symbols to None.
_PUNCTUATION_DICT = {}


# TODO(user): This way of punctuation may not be exactly what we want, for
# example '$' is overlooked.
def _GetPunctuationDict():
  if not _PUNCTUATION_DICT:
    # Need to be careful since sys.maxunicode may depend upon if UCS-2 or UCS-4
    # is used.
    for k in xrange(sys.maxunicode):
      if category(unichr(k)).startswith('P'):
        _PUNCTUATION_DICT[k] = None
  return _PUNCTUATION_DICT


def CleanUnicodeString(s, separator=None):
  """Return list of words after lowering case and may be removing punctuations.

  Args:
    s: a unicode string.
    separator: default is None which implies separator of whitespaces and
      punctuation is removed.

  Returns:
    Lowered case string after removing runs of separators.

  Raises:
    ValueError: when s is not a unicode string.
  """
  if not isinstance(s, unicode):
    raise ValueError('Expected unicode string type data but got: %s' % type(s))
  words = s.lower().split(separator)
  if separator is None:
    # Remove punctuation
    words = [w.translate(_GetPunctuationDict()) for w in words]
  words = [w for w in words if w]
  return words


def GeneratePseudonymCipherKey(key, identifier):
  return ccrypto.PRF(key, 'pseudonym_' + str(identifier))


def GenerateProbabilisticCipherKey(key, identifier):
  return ccrypto.PRF(key, 'probabilistic_' + str(identifier))


def GenerateHomomorphicCipherKey(key, identifier):
  return ccrypto.PRF(key, 'homomorphic_' + str(identifier))


def GenerateStringHashKey(key, identifier):
  return ccrypto.PRF(key, 'stringhash_' + str(identifier))


class _Cipher(object):
  """Class encapsulating ciphers for encrypting and decrypting values."""

  def Encrypt(self, unused_plaintext):
    raise ValueError('Not implemented yet.')

  def Decrypt(self, unused_plaintext):
    raise ValueError('Not implemented yet.')


class ProbabilisticCipher(_Cipher):
  """Class for probabilistic encryption of unicode or any bytes str."""

  def __init__(self, key):
    """Cipher is initialized with a key of valid Aes key lengths."""
    super(ProbabilisticCipher, self).__init__()
    self._cipher = ccrypto.AesCbc(key)

  def Encrypt(self, plaintext):
    """Encrypts plaintext and returns base64 str.

    Args:
      plaintext: a unicode or str. If the plaintext is unicode it will
        be encoded as utf-8 prior to encryption. If the plaintext is str
        it will be encrypted as-is.

    Returns:
      base64-wrapped version of encryption of plaintext,
      potentially after encoding unicode characters in utf-8.

    Raises:
      ValueError: when plaintext is empty or not a proper type.
    """
    if isinstance(plaintext, unicode):
      plaintext = plaintext.encode('utf-8')

    if not isinstance(plaintext, str):
      raise ValueError('Expected str or unicode type plaintext but got: %s' %
                       type(plaintext))
    if not plaintext:
      raise ValueError('Input plaintext cannot be empty.')
    return base64.b64encode(self._cipher.Encrypt(plaintext))

  def Decrypt(self, ciphertext, raw=False):
    """Decrypts base64 ciphertext and returns a unicode or str plaintext.

    Args:
      ciphertext: str, base64 encoding of a string.
      raw: bool, default False, return raw bytes, not a unicode string.

    Returns:
      decrypted unicode string.

    Raises:
      ValueError: when ciphertext is not a str.
    """
    if not isinstance(ciphertext, str):
      raise ValueError('Expected type data str but got: %s' % type(ciphertext))
    raw_plaintext = self._cipher.Decrypt(base64.b64decode(ciphertext))
    if not raw:
      return raw_plaintext.decode('utf-8')
    else:
      return raw_plaintext


class PseudonymCipher(_Cipher):
  """Class for Pseudonym encryption of unicode or any bytes str."""

  def __init__(self, key):
    """Cipher is initialized with a key of valid Aes key lengths."""
    super(PseudonymCipher, self).__init__()
    self._cipher = ccrypto.AesCbc(key)

  # TODO(user): Add a tweak based on field name, so pseudonyms are different
  # for the same value if they are in different fields. An easy solution is to
  # use the field name as iv if its as long enough or use hash of it.
  def Encrypt(self, plaintext):
    """Encrypts plaintext and returns base64 str.

    Args:
      plaintext: a unicode or str. If the plaintext is unicode it will
        be encoded as utf-8 prior to encryption. If the plaintext is str
        it will be encrypted as-is.

    Returns:
      base64-wrapped version of encryption of plaintext,
      potentially after encoding unicode characters in utf-8.

    Raises:
      ValueError: when plaintext is empty or not a proper type.
    """
    if isinstance(plaintext, unicode):
      plaintext = plaintext.encode('utf-8')

    if not isinstance(plaintext, str):
      raise ValueError('Expected str or unicode type plaintext but got: %s' %
                       type(plaintext))
    if not plaintext:
      raise ValueError('Input plaintext cannot be empty.')
    return base64.b64encode(
        self._cipher.Encrypt(plaintext, iv=16 * '\x00'))

  def Decrypt(self, ciphertext, raw=False):
    """Decrypts base64 ciphertext and returns a unicode or str plaintext.

    Args:
      ciphertext: str, base64 encoding of a string.
      raw: bool, default False, return raw bytes, not a unicode string.

    Returns:
      decrypted unicode string.

    Raises:
      ValueError: when ciphertext is not a str.
    """
    if not isinstance(ciphertext, str):
      raise ValueError('Expected type data str but got: %s' % type(ciphertext))
    raw_plaintext = self._cipher.Decrypt(base64.b64decode(ciphertext),
                                         iv=16 * '\x00')
    if not raw:
      return raw_plaintext.decode('utf-8')
    else:
      return raw_plaintext


class HomomorphicIntCipher(_Cipher):
  """Class for homomorphically encrypting and adding ints and float."""

  def __init__(self, key):
    super(HomomorphicIntCipher, self).__init__()
    self._paillier = pcrypto.Paillier(key)
    # Store raw binary nsquare as a string using '\x' syntax which can be passed
    # in a query.
    nsquare_bytes = number.LongToBytes(self._paillier.nsquare)
    self.nsquare = '\\x' + '\\x'.join(x.encode('hex') for x in nsquare_bytes)

  def Encrypt(self, plaintext):
    """Encrypts int64 and returns a base64 encoding of ciphertext as bytes.

    Args:
      plaintext: either a int or long to be encrypted

    Returns:
      encrypted plaintext, converted to bytes, and base64 encoded.

    Raises:
      ValueError: when plaintext is neither an int, nor a long.
    """
    if not isinstance(plaintext, int) and not isinstance(plaintext, long):
      raise ValueError('Expected int or long type data but got: %s' %
                       type(plaintext))
    return base64.b64encode(
        number.LongToBytes(self._paillier.EncryptInt64(plaintext)))

  def Decrypt(self, ciphertext):
    """Takes ciphertext and decrypts to long.

    Args:
      ciphertext: a string which is an encrypted int that is converted to
        bytes and base64 encoded.

    Returns:
      decrypted int64.

    Raises:
      ValueError: when ciphertext is not a string.
    """
    if not isinstance(ciphertext, str):
      raise ValueError('Expected type data str but got: %s' % type(ciphertext))
    return self._paillier.DecryptInt64(
        number.BytesToLong(base64.b64decode(ciphertext)))


class HomomorphicFloatCipher(_Cipher):
  """Class for homomorphic encrypting and adding floats."""

  def __init__(self, key):
    super(HomomorphicFloatCipher, self).__init__()
    self._paillier = pcrypto.Paillier(key)
    # Store raw binary nsquare as a string using '\x' syntax which can be passed
    # in a query.
    nsquare_bytes = number.LongToBytes(self._paillier.nsquare)
    self.nsquare = '\\x' + '\\x'.join(x.encode('hex') for x in nsquare_bytes)

  def Encrypt(self, plaintext):
    """Encrypts float and returns a base64 encoding of ciphertext as bytes.

    Args:
      plaintext: float

    Returns:
      encrypted plaintext, converted to bytes, and base64 encoded.

    Raises:
      ValueError: when plaintext is not a float.
    """
    if not isinstance(plaintext, float):
      raise ValueError('Expected float type data but got: %s' %
                       type(plaintext))
    return base64.b64encode(
        number.LongToBytes(self._paillier.EncryptFloat(plaintext)))

  def Decrypt(self, ciphertext):
    """Takes ciphertext and decrypts to a float.

    Args:
      ciphertext: a string which is an encrypted float, converted to bytes,
        and base64 encoded.

    Returns:
      decrypted float.

    Raises:
      ValueError: when ciphertext is not a string.
    """
    if not isinstance(ciphertext, str):
      raise ValueError('Expected type data str but got: %s' % type(ciphertext))
    return self._paillier.DecryptFloat(
        number.BytesToLong(base64.b64decode(ciphertext)))


class StringHash(object):
  """Key hashes of word sequences."""

  def __init__(self, key, output_len=8, hashfunc='sha1'):
    """Cipher is initialized with a key for keyed hashing purposes."""
    self._key = key
    self._output_len = output_len
    self._hashfunc = hashfunc

  def GetStringKeyHash(self, field_name, data, output_len=None, hashfunc=None):
    """Calculates a keyed hash of a string.

    Args:
      field_name: unicode string that is used as part of hash calculation - i.e.
        an 8 byte length of field_name and field_name is prepended to data
        before hashing.
      data: unicode string.
      output_len: in bytes, if not set then use value from constructor.
      hashfunc: if not set then use value from constructor.

    Returns:
      base64 encoding of keyed hash of utf8 encoded data.

    Raises:
      ValueError: when data or fieldname is not a unicode string or when
        fieldname is an empty string.
    """

    if not isinstance(data, unicode):
      raise ValueError('SubstringHash methods only works with data '
                       'input type unicode, given: %s' % type(data))
    if not isinstance(field_name, unicode):
      raise ValueError('fieldname input type should be unicode, given: %s' %
                       type(field_name))
    if not field_name:
      raise ValueError('field_name cannot be empty.')
    output_digest_len = output_len or self._output_len
    digest_hashfunc = hashfunc or self._hashfunc
    # data --> len, field_name, data
    extended_data = ccrypto.IntToFixedSizeString(len(field_name))
    extended_data += field_name + data
    utf8_data = extended_data.encode('utf-8')
    raw_hash = ccrypto.PRF(self._key, utf8_data,
                           output_digest_len, digest_hashfunc)
    return base64.b64encode(raw_hash)

  def GetHashesForWordSubsequencesWithIv(
      self, field_name, data, max_sequence_len=5, random_permute='True',
      separator=None, output_len=None, hashfunc=None,
      rand_gen=ccrypto.GetRandBytes):
    """Returns hashes of all word subsequences of a max length in data.

    First splits data using separator into words and creates all word
    subsequences using all starting points and end points up to max_sequence_len
    apart.  Creates keyed hash of the utf8 encoding of each word sequence, then
    sha1 hashes the keyed hash with a prepended IV and return space separated
    base64 encoded hashes.

    Args:
      field_name: unicode string that is used as part of hash calculation.
      data: unicode string.
      max_sequence_len: maximum subsequence length in words to be hashed.
      random_permute: if true then permute the ordering of the hashes, default
        is true.
      separator: used to split string, default is white space.
      output_len: in bytes, if not set then use value from constructor.
      hashfunc: if not set then use value from constructor.
      rand_gen: a random generator function that takes an int argument for size
        and returns that many random bytes, used to create IV.

    Returns:
      digest of all word sub sequences of max_sequence_len is returned as a
      space separated string; also the ordering of the hashes in the string
      is randomized by default.

    Raises:
      ValueError: when data is not a unicode string.
    """
    if not isinstance(data, unicode):
      raise ValueError('SubstringHash methods only works with data '
                       'input type %s, given: %s' % (type(u''), type(data)))
    # TODO(user): currently, an empty string (e.g. '; ,.') results in no
    # hashes and just an iv. In a way this is correct, since no hashes would be
    # matched with this entry, however, consider if we want to have an entry to
    # indicate an empty record.
    words = CleanUnicodeString(data, separator)
    hashes = []
    iv = base64.b64encode(rand_gen(16))
    separator = separator or u' '
    output_digest_len = output_len or self._output_len
    for i in xrange(len(words)):
      for j in xrange(max_sequence_len):
        if i + j < len(words):
          subsequence = separator.join(words[i:i+j+1])
          keyed_hash = self.GetStringKeyHash(field_name, subsequence,
                                             output_digest_len, hashfunc)
          # pylint: disable=too-many-function-args
          hash_of_iv_and_keyed_hash = (
              hashlib.sha1(iv + keyed_hash).digest()[:output_digest_len])
          hashes.append(base64.b64encode(hash_of_iv_and_keyed_hash))
    # TODO(user): use a better random generator to shuffle, it accepts one.
    if random_permute:
      random.shuffle(hashes)
    return ' '.join([iv] + hashes)
