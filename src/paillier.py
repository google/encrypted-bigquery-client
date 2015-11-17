#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.

"""Paillier encryption to perform homomorphic addition on encrypted data."""



import ctypes
import ctypes.util
import math
import platform
import struct

import logging
from google.apputils import resources

import common_crypto as ccrypto
import number

 # number of int64s that can be packed into a single Paillier payload.
PACKING_LIMIT = 7
# 96 bits are used to represent signed int64s to detect overflows and
# additional 32 bits is to separate distinct int64s.
PACKING_BIT_SIZE = 96 + 32
N_LENGTH = 1024  # bits
HALF_N_LENGTH = N_LENGTH / 2  # bits
MAX_INT64 = (2 ** 63) -1
MIN_INT64 = -(2 ** 63)
_ONES_33 = long(33*'1', 2)
_ONES_63 = long(63*'1', 2)
_ONES_64 = long(64*'1', 2)
_ONES_96 = long(96*'1', 2)
_ONES_832 = long(832*'1', 2)
# Bit positions of various sections in expanded float representation created
# from an IEEE float value; (assumes starting bit is numbered as 1).
FLOAT_MSB = N_LENGTH - 1  # 1023 (here & below comments, N_lENGTH assumed 1024).
MAX_ADDS = 32  # (bits) i.e. if < 2^32 adds occur than overflow can be detected
FLOAT_NAN_LSB = FLOAT_MSB - MAX_ADDS  # 991
FLOAT_PLUSINF_LSB = FLOAT_NAN_LSB - MAX_ADDS  # 959
FLOAT_MINUSINF_LSB = FLOAT_PLUSINF_LSB - MAX_ADDS  # 927
FLOAT_CARRYOVER_LSB = FLOAT_MINUSINF_LSB - MAX_ADDS  # 895
FLOAT_SIGN_HIGH_LSB = FLOAT_CARRYOVER_LSB - MAX_ADDS  # 863
FLOAT_SIGN_LOW_LSB = FLOAT_SIGN_HIGH_LSB - MAX_ADDS  # 831
EXPLICIT_MANTISSA_BITS = 52
MANTISSA_BITS = 53
EXPONENT_BITS = 11
EXPONENT_BIAS = (2 ** (EXPONENT_BITS - 1)) - 1  # 1023 for 11 bit exponent
FLOAT_MANTISSA_LSB = FLOAT_SIGN_LOW_LSB - MANTISSA_BITS  # 778
FLOAT_MANTISSA_ZERO = FLOAT_MANTISSA_LSB / 2  # 389
_ONES_CARRYOVER_LSB = long('1' * FLOAT_CARRYOVER_LSB, 2)
_ONES_FLOAT_SIGN_LOW_LSB = long('1' * FLOAT_SIGN_LOW_LSB, 2)

# -- openssl function args and return types
_FOUND_SSL = False
_TESTED_SSL = False
try:
  if platform.system() == 'Windows':
    ssl_libpath = ctypes.util.find_library('libeay32')
  else:
    ssl_libpath = ctypes.util.find_library('ssl')
  if ssl_libpath:
    ssl = ctypes.cdll.LoadLibrary(ssl_libpath)
    _FOUND_SSL = True
  else:
    logging.info('Could not find open ssl library; paillier encryption '
                 'during load will be slower')
except (OSError, IOError):
  logging.info('Could not find open ssl library; paillier encryption '
               'during load will be slower')
if _FOUND_SSL:
  ssl.BN_new.restype = ctypes.c_void_p
  ssl.BN_new.argtypes = []
  ssl.BN_free.argtypes = [ctypes.c_void_p]
  ssl.BN_num_bits.restype = ctypes.c_int
  ssl.BN_num_bits.argtypes = [ctypes.c_void_p]
  ssl.BN_bin2bn.restype = ctypes.c_void_p
  ssl.BN_bin2bn.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_void_p]
  ssl.BN_bn2bin.restype = ctypes.c_int
  ssl.BN_bn2bin.argtypes = [ctypes.c_void_p, ctypes.c_void_p]
  ssl.BN_CTX_new.restype = ctypes.c_void_p
  ssl.BN_CTX_new.argtypes = []
  ssl.BN_CTX_free.restype = ctypes.c_int
  ssl.BN_CTX_free.argtypes = [ctypes.c_void_p]
  ssl.BN_mod_exp.restype = ctypes.c_int
  ssl.BN_mod_exp.argtypes = [ctypes.c_void_p, ctypes.c_void_p, ctypes.c_void_p,
                             ctypes.c_void_p, ctypes.c_void_p]


class Paillier(object):
  """Class for paillier encryption/decryption and homomorphic addition.

  Also includes methods to encrypt/decrypt signed int64 and float numbers.
  """

  def __init__(self, seed=None, g=None, n=None, Lambda=None, mu=None):
    """Intialize Paillier object with seed.

    Args:
      seed: str used to generate key - derives paillier parameters including
        g, n, lambda, and mu; if set to None then paillier parameters have to be
        explicitly provided which is useful in testing.
      g: Long integer, has to be provided if seed is None.
      n: Long integer, has to be provided if seed is None.
      Lambda: Long integer, has to be provided if seed is None.
      mu: Long integer, has to be provided if seed is None.

    Raises:
      ValueError: When seed is None yet one of the g, n, Lambda, mu parameters
        is not provided.
    """
    if seed is None:
      # initialization of these values directly is useful for testing purposes.
      if not (g and n and Lambda and mu):
        raise ValueError('If seed is set to none then g, n, Lambda and mu'
                         ' need to be provided.')
      self.n = n
      self.nsquare = n * n
      self.g = g
      self.__lambda = Lambda
      self.__mu = mu
      return
    if not isinstance(seed, str):
      raise ValueError('Expected string type data for seed, but got: %s' %
                       type(seed))
    if not seed:
      raise ValueError('Provided seed cannot be empty')
    prg = ccrypto.PRG(seed)
    n_len = 0
    while n_len != N_LENGTH:
      p = number.GetPrime(HALF_N_LENGTH, prg.GetNextBytes)
      q = number.GetPrime(HALF_N_LENGTH, prg.GetNextBytes)
      self.n = p * q
      n_len = len(bin(self.n)) - 2  # take 2 out for '0b' prefix
    self.nsquare = self.n * self.n
    # Simpler paillier variant with g=n+1 results in lamda equal to phi
    # and mu is phi inverse mod n.
    self.g = self.n + 1
    phi_n = (p-1) * (q-1)
    self.__lambda = phi_n
    self.__mu = number.Inverse(phi_n, self.n)

  def Encrypt(self, plaintext, r_value=None):
    """Paillier encryption of plaintext.

    Args:
      plaintext: an integer or long to be paillier encrypted.
      r_value: random value used in encryption, in default case (i.e. r_value
        is None) the value is supplied by the method.

    Returns:
      a long, representing paillier encryption of plaintext.

    Raises:
      ValueError: if plaintext is neither int nor long.
    """

    if not isinstance(plaintext, int) and not isinstance(plaintext, long):
      raise ValueError('Expected int or long type plaintext but got: %s' %
                       type(plaintext))
    r = r_value or self._GetRandomFromZNStar(N_LENGTH, self.n)
    return (ModExp(self.g, plaintext, self.nsquare) *
            ModExp(r, self.n, self.nsquare)) % self.nsquare

  def Decrypt(self, ciphertext):
    """Paillier decryption of ciphertext.

    Args:
      ciphertext: a long that is to be paillier decrypted.

    Returns:
      a long, representing paillier decryption of ciphertext.

    Raises:
      ValueError: if ciphertext is neither int nor long.
    """
    if not isinstance(ciphertext, int) and not isinstance(ciphertext, long):
      raise ValueError('Expected int or long type ciphertext but got: %s' %
                       type(ciphertext))
    u = ModExp(ciphertext, self.__lambda, self.nsquare)
    l_of_u = (u - 1) // self.n
    return (l_of_u * self.__mu) % self.n

  # TODO(user): use a pluggable random generator here and other places to test
  # more of the code base.
  def _GetRandomFromZNStar(self, n_length, n):
    while True:
      r = number.GetRandomNBitNumber(n_length)
      # check relative prime
      if r < n and number.GCD(r, n) == 1:
        break
    return r

  def Add(self, ciphertext1, ciphertext2):
    """returns E(m1 + m2) given E(m1) and E(m2).

    Args:
      ciphertext1: a long whose paillier decryption is to be added.
      ciphertext2: a long whose paillier decryption is to be added.

    Returns:
      a long as the modular product of the two ciphertexts which is equal to
        E(m1 + m2).

    Raises:
      ValueError: if either ciphertext is neither int nor long.
    """
    for c in (ciphertext1, ciphertext2):
      if not isinstance(c, int) and not isinstance(c, long):
        raise ValueError('Expected int or long type for %s but got %s' %
                         (c, type(c)))
    return ciphertext1 * ciphertext2 % self.nsquare

  def Affine(self, ciphertext, a=1, b=0):
    """Returns E(a*m + b) given E(m), a and b."""
    # This works for raw paillier payload but may not for int64/float payload.
    # First multiply ciphertext with a
    a_mult_ciphertext = pow(ciphertext, a, self.nsquare)
    # Add b to it.
    return a_mult_ciphertext * pow(self.g, b, self.nsquare) % self.nsquare

  def EncryptInt64(self, plaintext, r_value=None):
    """Paillier encryption of an Int64 plaintext.

    Paillier homomorphic addition only directly adds positive values, however,
    we would like to add both positive and negative values (i.e. int64 is
    signed). To achieve this, we will represent negative values with twos
    complement representation. Also, in order to detect overflow after adding
    multiple values, the 64 sign bit is extended (or replicated) all the way to
    the 96th bit and bits above 96 are all zeroes.

    Args:
      plaintext: a 64 bit int or long to be encrypted with values from -2^63
        to 2^63 - 1.
      r_value: random value used in encryption, in default case (i.e None) the
        value is supplied by the method.

    Returns:
      a long, representing paillier encryption of an int64 plaintext.

    Raises:
      ValueError: if not an int nor long, or less than MIN_INT64 or more than
        MAX_INT64.
    """
    if not isinstance(plaintext, int) and not isinstance(plaintext, long):
      raise ValueError('Expected int or long plaintext but got: %s' %
                       type(plaintext))
    if plaintext < MIN_INT64 or plaintext > MAX_INT64:
      raise ValueError('Int64 values need to be between %d and %d but got %d'
                       % (MIN_INT64, MAX_INT64, plaintext))
    plaintext = self._Extend64bitTo96bitTwosComplement(plaintext)
    return self.Encrypt(plaintext, r_value=r_value)

  def EncryptMultipleInt64s(self, numberlist, r_value=None):
    """Paillier encryption of  multiple 64 bit integers into a single payload.

    Args:
      numberlist: a list of 64 bit integers or long to be encrypted with values
        from -2^63 to 2^63 - 1. Number of elements in the list is limited to
        PACKING_LIMIT.
      r_value: random value used in encryption, in default case (i.e None) the
        value is supplied by the method.

    Returns:
      a long, representing paillier encryption of a list of int64s.

    Raises:
      ValueError: if int64List contains more than PACKING_LIMIT integers,
      any member of a list is not an int nor long, or less than MIN_INT64 or
      more than MAX_INT64.
    """
    plaintext = 0
    number_counter = 0
    if len(numberlist) > PACKING_LIMIT:
      raise ValueError('The number of entries in the input list cannot be'
                       + ' more than %d' % (PACKING_LIMIT))
    for entry in numberlist:
      if not isinstance(entry, int) and not isinstance(entry, long):
        raise ValueError('Expected int or long but got: %s' % type(number))
      if entry < MIN_INT64 or entry > MAX_INT64:
        raise ValueError('Int64 values need to be between %d and %d but got %d'
                         % (MIN_INT64, MAX_INT64, entry))
      entry = self._Extend64bitTo96bitTwosComplement(entry)
      if number_counter > 0:
        plaintext <<= PACKING_BIT_SIZE
      plaintext += entry
      number_counter += 1
    return self.Encrypt(plaintext, r_value=r_value)

  def DecryptMultipleInt64s(self, ciphertext):
    """Paillier decryption of ciphertext into multiple int64 values.

    Args:
      ciphertext: a long that is to be paillier decrypted into multiple int64s.

    Returns:
      a list of longs, representing paillier decryption of ciphertext

    Raises:
      ValueError: if either cipher is neither int nor long.
      OverflowError: if overflow is detected in the decrypted int.
    """
    if not isinstance(ciphertext, int) and not isinstance(ciphertext, long):
      raise ValueError('Expected int or long type ciphertext but got: %s' %
                       type(ciphertext))
    plaintext = self.Decrypt(ciphertext)
    decrypted_numbers = []
    for unused_i in range(PACKING_LIMIT):
      entry = plaintext & _ONES_96
      plaintext >>= PACKING_BIT_SIZE
      decrypted_numbers.insert(0, self._Unwrap96bitTo64bit(entry))
    return decrypted_numbers

  def _Unwrap96bitTo64bit(self, plaintext):
    valuebits1to63 = plaintext & _ONES_63  # lsb is numbered as bit 1 here.
    signbits64to96 = (plaintext & 0xffffffff8000000000000000L) >> 63
    if not (signbits64to96 == 0 or signbits64to96 == _ONES_33):
      raise OverflowError('Overflow detected in decrypted int')
    if signbits64to96 == 0:
      return  valuebits1to63
    # negative number case
    # - first find the positive value of the number by taking the 2s complement
    #   of the 96 bit (likely greater) integer.
    positive_96bit_value = (plaintext ^ _ONES_96) + 1L
    # - final value will mostly be a 63 bit number or smaller except if -2^63
    #   which gives 64 bit value 2^63.
    positive_64bit_value = positive_96bit_value & _ONES_64
    return -1 * positive_64bit_value

  def _Extend64bitTo96bitTwosComplement(self, num):
    if num >= 0:
      # positive number is extended by just adding zeroes.
      return num
    # negative number, make 96 bit 2s complement
    return (abs(num) ^ _ONES_96) + 1L

  def DecryptInt64(self, ciphertext):
    """Paillier decryption of ciphertext into a int64 value.

    Args:
      ciphertext: a long that is to be paillier decrypted into int64.

    Returns:
      a long, representing paillier decryption of ciphertext into an int64 value

    Raises:
      ValueError: if either ciphertext is neither int nor long.
      OverflowError: if overflow is detected in the decrypted int.
    """
    if not isinstance(ciphertext, int) and not isinstance(ciphertext, long):
      raise ValueError('Expected int or long type ciphertext but got: %s' %
                       type(ciphertext))
    plaintext = self.Decrypt(ciphertext)
    return self._Unwrap96bitTo64bit(plaintext)

  def EncryptFloat(self, plaintext, r_value=None):
    """Encrypt float (IEEE754 binary64bit) values with limited exponents.

    Paillier homomorphic addition only directly adds positive binary values,
    however, we would like to add both positive and negative float values
    of different magnitutes. To achieve this, we will:
    - represent the mantissa and exponent as one long binary value. This means
      that with 1024 bit n in paillier, the maximum exponent value is 389 bits.
    - represent negative values with twos complement representation.
    - Nan, +inf, -inf are each indicated by values in there own 32 bit region,
      so that when one of them is added, the appropriate region would be
      incremented and we would know this in the final aggregated value, assuming
      less than 2^32 values were aggregated.
    - We limit the number of numbers that can be added to be less than 2^32
      otherwise we would not be able to detect overflows properly, etc.
    - Also, in order to detect overflow after adding multiple values,
      the 64 sign bit is extended (or replicated) for an additional 64 bits.
      This allows us to detect if an overflow happened and knowing whether the
      most significant 32 bits out of 64 is zeroes or ones, we would know if the
      result should be a +inf or -inf.

    Args:
      plaintext: a float to be paillier encrypted, Supported float values
        have exponent <= 389.
      r_value: random value used in encryption, in default case (i.e None) the
        value is supplied by the method.

    Returns:
      a long, representing paillier encryption of a float plaintext.

    Raises:
      ValueError: if plaintext is not a float.
    """
    if not isinstance(plaintext, float):
      raise ValueError('Expected float plaintext but got: %s' % type(plaintext))

    input_as_long = struct.unpack('Q', struct.pack('d', plaintext))[0]
    mantissa = (input_as_long & 0xfffffffffffff) | 0x10000000000000
    exponent = ((input_as_long >> 52) & 0x7ff) - EXPONENT_BIAS
    sign = input_as_long >> (EXPLICIT_MANTISSA_BITS + EXPONENT_BITS)
    if IsNan(plaintext):
      # Put a 1 in the 32 bit nan indicator field.
      plaintext = 0x00000001 << FLOAT_NAN_LSB  # << 991
    elif IsInfPlus(plaintext):
      # Put a 1 in the 32 bit plus inf indicator field.
      plaintext = 0x00000001 << FLOAT_PLUSINF_LSB  # << 959
    elif IsInfMinus(plaintext):
      # Put a 1 in the 32 bit minus inf indicator field.
      plaintext = 0x00000001 << FLOAT_MINUSINF_LSB  # << 927
    elif exponent == 0 and mantissa == 0:  # explicit 0
      plaintext = 0
    elif exponent > FLOAT_MANTISSA_ZERO:  # > 389
      # Can't represent such large numbers
      raise ValueError('Floats with exponents larger than 389 are currently '
                       'not suppported.')
    elif exponent < -FLOAT_MANTISSA_ZERO - EXPLICIT_MANTISSA_BITS:  # < -389 -52
      # too small, set to zero
      plaintext = 0
    else:  # representable numbers with -441 <= exponent <= 389.
      # Place 53 bit mantissa (1 + 52 explicit bit mantissa in 831 bit payload
      # and shift according to exponent.
      # - first put 53 bit mantissa on the left most side of payload
      plaintext = mantissa << FLOAT_MANTISSA_LSB  # << 778
      # - second shift right as needed.
      plaintext >>= (FLOAT_MANTISSA_ZERO - exponent)  # >>= (389 - exponent)
      # Find 2s complement if number is negative
      if sign == 1:  # neg number
        # make 895 bit (831 + 64 extended sign bits) 2s complement
        plaintext = (plaintext  ^ _ONES_CARRYOVER_LSB) + 1L
    return self.Encrypt(plaintext, r_value=r_value)

  def DecryptFloat(self, ciphertext):
    """Paillier decryption of ciphertext into a IEEE754 binary64 float value.

    Args:
      ciphertext: a long that is to be paillier decrypted into a float.

    Returns:
      a float representing paillier decryption of ciphertext into an float value

    Raises:
      ValueError: if nan, +inf or -inf is not set correctly in decrypted value.
    """
    original_plaintext = self.Decrypt(ciphertext)
    plaintext = original_plaintext
    mantissa_and_exponent = plaintext & _ONES_FLOAT_SIGN_LOW_LSB
    plaintext >>= FLOAT_SIGN_LOW_LSB  # >>= 831
    sign_low32 = plaintext & 0xffffffff
    plaintext >>= 32
    sign_high32 = plaintext & 0xffffffff
    plaintext >>= 32
    # carry_over32 = plaintext & 0xffffffff
    plaintext >>= 32
    minus_inf32 = plaintext & 0xffffffff
    plaintext >>= 32
    plus_inf32 = plaintext & 0xffffffff
    plaintext >>= 32
    nan_32 = plaintext & 0xffffffff
    if nan_32 > 0:
      return float('nan')
    # adding a +inf and -inf should return a nan
    if plus_inf32 > 0 and minus_inf32 > 0:
      return float('nan')
    if plus_inf32 > 0:
      return float('inf')
    if minus_inf32 > 0:
      return float('-inf')
    if sign_high32 == 0 and sign_low32 > 0:
      # This indicates that positive overflow has happened, mimic ieee float
      # behaviour and return +inf.
      return float('inf')
    if sign_high32 == 0xffffffff and sign_low32 < 0xffffffff:
      # This indicates that negative overflow has happened, mimic ieee float
      # behaviour and return -inf.
      return float('-inf')
    if sign_high32 == 0 and sign_low32 == 0:
      # positive finite number.
      if mantissa_and_exponent == 0L:
        return float(0)
      size = len(bin(mantissa_and_exponent)) - 2  # -2 to remove prepended 0b
      if size >= MANTISSA_BITS:
        # take the first 53 bits and remove the leading 1 bit i.e 52 bits.
        new_mantissa = ((mantissa_and_exponent >> (size - MANTISSA_BITS))
                        & 0xfffffffffffff)
      else:
        # take all the bits and shift left to make it a normal number,
        # the exponent also gets updated appropriately.
        new_mantissa = ((mantissa_and_exponent << (MANTISSA_BITS - size))
                        & 0xfffffffffffff)
      new_exponent = ((size - MANTISSA_BITS) - FLOAT_MANTISSA_ZERO +
                      EXPONENT_BIAS)
      new_value = (new_exponent << EXPLICIT_MANTISSA_BITS) | new_mantissa
      return struct.unpack('d', struct.pack('Q', new_value))[0]
    if sign_high32 == 0xffffffff and sign_low32 == 0xffffffff:
      # negative finite number.
      # - first find the positive value of the number by taking the 2s
      # complement of the 895 bit integer.
      num = original_plaintext & _ONES_CARRYOVER_LSB
      positive_895bit_value = (num ^ _ONES_CARRYOVER_LSB) + 1L
      # - final value will mostly be a 831 bit number or smaller except if
      # 831 bits are all zero which represents -2^831 and gives a 2's complement
      # positive value of 2^831, we detect this case and return -inf.
      positive_832bit_value = positive_895bit_value & _ONES_832
      if positive_832bit_value >> FLOAT_SIGN_LOW_LSB:  # >> 831:
        return float('-inf')
      size = len(bin(positive_832bit_value)) - 2
      if size >= MANTISSA_BITS:
        # take the first 53 bits and remove the leading 1 bit.
        new_mantissa = ((positive_832bit_value >> (size - MANTISSA_BITS))
                        & 0xfffffffffffff)
      else:
        # take all the bits and shift left to make it a normal number,
        # the exponent also gets updated appropriately.
        new_mantissa = ((positive_832bit_value << (MANTISSA_BITS - size))
                        & 0xfffffffffffff)
      new_exponent = ((size - MANTISSA_BITS) - FLOAT_MANTISSA_ZERO +
                      EXPONENT_BIAS)
      new_value = ((new_exponent << EXPLICIT_MANTISSA_BITS) | new_mantissa |
                   (1 << (EXPLICIT_MANTISSA_BITS + EXPONENT_BITS)))
      return struct.unpack('d', struct.pack('Q', new_value))[0]
    raise ValueError('Got an unusual decrypted value either nan, inf or sign '
                     'bits aren\'t set correctly: %s' % hex(original_plaintext))


def IsNan(x):
  return math.isnan(x)


def IsInfPlus(x):
  return math.isinf(x) and x > 0


def IsInfMinus(x):
  return math.isinf(x) and x < 0


def _NumBytesBn(bn):
  """Returns the number of bytes in the Bignum."""
  if not _FOUND_SSL:
    raise RuntimeError('Cannot evaluate _NumBytesBn because ssl library was '
                       'not found')
  size_in_bits = ssl.BN_num_bits(bn)
  return int(math.ceil(size_in_bits / 8.0))


def ModExp(a, b, c):
  """Uses openssl, if available, to do a^b mod c where a,b,c are longs."""
  if not _FOUND_SSL:
    return pow(a, b, c)
  # convert arbitrary long args to bytes
  bytes_a = number.LongToBytes(a)
  bytes_b = number.LongToBytes(b)
  bytes_c = number.LongToBytes(c)

  # convert bytes to (pointer to) Bignums.
  bn_a = ssl.BN_bin2bn(bytes_a, len(bytes_a), 0)
  bn_b = ssl.BN_bin2bn(bytes_b, len(bytes_b), 0)
  bn_c = ssl.BN_bin2bn(bytes_c, len(bytes_c), 0)
  bn_result = ssl.BN_new()
  ctx = ssl.BN_CTX_new()

  # exponentiate and convert result to long
  ssl.BN_mod_exp(bn_result, bn_a, bn_b, bn_c, ctx)
  num_bytes_in_result = _NumBytesBn(bn_result)
  bytes_result = ctypes.create_string_buffer(num_bytes_in_result)
  ssl.BN_bn2bin(bn_result, bytes_result)
  long_result = number.BytesToLong(bytes_result.raw)

  # clean up
  ssl.BN_CTX_free(ctx)
  ssl.BN_free(bn_a)
  ssl.BN_free(bn_b)
  ssl.BN_free(bn_c)
  ssl.BN_free(bn_result)

  return long_result


def TestSslRegression():
  """Test openssl BN functions ctypes setup for regressions."""
  if not _FOUND_SSL:
    return
  a = 13237154333272387305  # random
  b = 14222796656191241573  # random
  c = 14335739297692523692  # random
  expect_m = 10659231545499717801  # pow(a, b, c)
  m = ModExp(a, b, c)
  assert m == expect_m, 'TestSslRegression: unexpected ModExp result'


if not _TESTED_SSL:
  TestSslRegression()
  _TESTED_SSL = True
