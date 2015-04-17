#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Contains number theory functions."""



import math
import struct

import common_crypto as ccrypto


def GCD(arg0, arg1):
  """Computes the greatest common divisor of given inputs."""
  # Euclidean algorithm is used to compute GCD.
  if (not (isinstance(arg0, int) or isinstance(arg0, long) or
           isinstance(arg1, int) or isinstance(arg1, long))):
    raise ValueError('Inputs should be in integer type.')
  if arg0 == 0 and arg1 == 0:
    raise ValueError('At least one input should be nonzero.')
  elif arg0 == 0:
    return arg1
  elif arg1 == 0:
    return arg0
  while arg1 != 0:
    arg0, arg1 = arg1, arg0 % arg1
  return math.fabs(arg0)


def _ExtendedGCD(arg0, arg1):
  # Given integers arg0, arg1; d = GCD(arg0, arg1) is the least positive
  # integer that can be represented as arg0 * x + arg1 * y.
  # Computes d, x and y according to the given inputs.
  d, t = abs(arg0), abs(arg1)
  x, y, r, s = 1, 0, 0, 1
  # invariants: ax + by = d, ar + bs = t
  while t > 0:
    q = d // t
    d, x, y, r, s, t = t, r, s, x - q * r, y - q * s, d - q * t
  return d, x, y


def Inverse(element, modulus):
  """Computes modular inverse of the given element."""
  # If element has an inverse, then element * x  + N * y = 1 and
  # x is the inverse of the element.
  d, x, _ = _ExtendedGCD(element, modulus)
  if element < 0:
    x = -1 * x
  if d != 1:
    return None  # modular inverse does not exist
  else:
    return x % modulus


def GetRandomNBitOddNumber(bit_length, rand_func=None):
  return GetRandomNBitNumber(bit_length, rand_func) | 1


def GetRandomNBitNumber(bit_length, rand_func=None):
  """Returns random n bit number with nth bit always 1."""
  return GetRandomNumber(bit_length, rand_func) | (2 ** (bit_length - 1))


def GetRandomNumber(bit_length, rand_func=None):
  """Returns a random number between 0 and 2^(bit_length) - 1, inclusive."""
  if rand_func is None:
    rand_func = ccrypto.GetRandBytes
  (byte_length, remainder) = divmod(bit_length, 8)
  if remainder:
    byte_length += 1
  mask = (1 << bit_length) - 1
  random_number = BytesToLong(rand_func(byte_length))
  return random_number & mask


def GetPrime(bit_length, rand_func=None):
  """Generates N bit random prime number."""
  number = GetRandomNBitOddNumber(bit_length, rand_func)
  while not IsPrime(number):
    number += 2
    if number >= 2 ** bit_length:
      number = GetRandomNBitOddNumber(bit_length, rand_func)
  return number


def IsPrime(number, error_probability=1e-6):
  """Identifies whether the given number is prime or not."""

  # Rabin Miller primality test is used for the identification. It identifies
  # a composite number as a prime number with probability 'error_probability'.
  rounds = int(math.ceil(-math.log(error_probability)/math.log(4)))
  return _RabinMillerTest(number, rounds)


def LongToBytes(number):
  """Converts an arbitrary length number to a byte string."""
  number_32bitunit_components = []
  if number == 0:
    return struct.Struct('>I').pack(0)
  while number != 0:
    number_32bitunit_components.insert(0, number & 0xffffffff)
    number >>= 32
  converter = struct.Struct('>' + str(len(number_32bitunit_components)) + 'I')
  return converter.pack(*number_32bitunit_components)


def BytesToLong(byte_string):
  """Converts given byte string to a long."""
  result = 0
  (component_length, remainder) = divmod(len(byte_string), 4)
  if remainder:
    component_length += 1
    byte_string = byte_string.rjust(component_length * 4, '\0')
  converter = struct.Struct('>' + str(component_length) + 'I')
  unpacked_data = converter.unpack(byte_string)
  for i in range(0, component_length):
    result += unpacked_data[i] << (32 * (component_length - i - 1))
  return result


def _RabinMillerTest(number, rounds):
  """Probabilistic algorithm to identify primality of given number."""
  # Each iteration of Rabin Miller test decrease false positive probability by
  # a factor of 4.
  if number < 3 or number & 1 == 0:
    return number == 2
  s, d = 0, number - 1
  while d & 1 == 0:
    s, d = s + 1, d >> 1
  for _ in xrange(min(rounds, number - 2)):
    a = RandRange(2, number - 1)
    x = pow(a, d, number)
    if x != 1 and x + 1 != number:
      for _ in xrange(1, s):
        x = pow(x, 2, number)
        if x == 1:
          return False
        elif x == number - 1:
          a = 0
          break
      if a:
        return False
  return True


def RandRange(lower_limit, upper_limit):
  """Generates a random number in range [lower_limit, upper_limit)."""
  if lower_limit >= upper_limit:
    raise ValueError('upper_limit should be greater than lower_limit')
  width = upper_limit - lower_limit
  range_bit_length = 0
  while width != 0:
    range_bit_length += 1
    width >>= 1
  result = lower_limit + GetRandomNumber(range_bit_length)
  while result >= upper_limit:
    result = lower_limit + GetRandomNumber(range_bit_length)
  return result
