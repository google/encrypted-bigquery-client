#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for number theory functions."""



import struct

from google.apputils import app
from google.apputils import basetest as googletest

import number


class NumberTheoryTest(googletest.TestCase):

  def testGCD(self):
    self.assertEqual(5, number.GCD(10, 5))
    self.assertEqual(5, number.GCD(5, 10))
    self.assertEqual(5, number.GCD(5, -10))
    self.assertEqual(5, number.GCD(-5, 10))
    self.assertEqual(3, number.GCD(0, 3))
    self.assertEqual(3, number.GCD(3, 0))
    self.assertEqual(4, number.GCD(12340945096455436092, 52844834568339503840))
    self.assertEqual(4, number.GCD(-12340945096455436092, 52844834568339503840))
    self.assertEqual(480,
                     number.GCD(12000000000000000000, 36000000000000000480))
    self.assertRaises(ValueError, number.GCD, 0, 0)

  def testInverse(self):
    self.assertEqual(1, number.Inverse(1, 7))
    self.assertEqual(6, number.Inverse(-1, 7))
    self.assertEqual(2, number.Inverse(35, 3))
    self.assertEqual(1, number.Inverse(21, 5))
    self.assertEqual(1, number.Inverse(15, 7))
    self.assertEqual(106, number.Inverse(-986435, 129))
    self.assertEqual(359611612, number.Inverse(2143445647, 1147483647))
    self.assertEqual(462185749, number.Inverse(854356993, 565456956))
    self.assertEqual(88974916886927528416964238,
                     number.Inverse(12340945096455436092,
                                    98764321234333546549887553))
    self.assertEqual(9789404347406018132923315,
                     number.Inverse(-12340945096455436092,
                                    98764321234333546549887553))

  def testGetRandomNBitNumber(self):
    number_count = 0
    for _ in xrange(100):
      rand = number.GetRandomNBitNumber(20)
      if rand >= 2 ** 19:
        number_count += 1
      self.assertTrue(rand < 2**20)
    self.assertEqual(100, number_count)

  def testGetRandomNBitOddNumber(self):
    for _ in xrange(10):
      rand = number.GetRandomNBitOddNumber(20)
      self.assertTrue(rand < 2**20  and rand & 1 == 1)

  def testGetPrime(self):
    for _ in xrange(10):
      prime_number = number.GetPrime(20)
      self.assertTrue(prime_number < 2**20 and number.IsPrime(prime_number))

  def testIsPrime(self):
    self.assertTrue(number.IsPrime(373))
    self.assertTrue(number.IsPrime(2963))
    self.assertTrue(number.IsPrime(3257))
    self.assertFalse(number.IsPrime(2717))
    self.assertFalse(number.IsPrime(573))
    self.assertFalse(number.IsPrime(34575443591121))
    self.assertTrue(number.IsPrime(34575443591123))
    self.assertFalse(number.IsPrime(28002134553497))
    self.assertFalse(number.IsPrime(28002134553499))
    self.assertTrue(number.IsPrime(9876432123433354673))
    self.assertFalse(number.IsPrime(98764321234333546549887547))
    self.assertTrue(number.IsPrime(98764321234333546549887553))

  def testBytesToLong(self):
    self.assertEqual(205, number.BytesToLong('\xcd'))
    self.assertEqual(64, number.BytesToLong('\x00\x00\x00\x40'))
    self.assertEqual(64, number.BytesToLong('\x40'))
    self.assertEqual(
        4294967295 + 1, number.BytesToLong('\x01\x00\x00\x00\x00'))
    self.assertEqual(
        18446744069414584320,
        number.BytesToLong('\xff\xff\xff\xff\x00\x00\x00\x00'))
    self.assertEqual(3, number.BytesToLong(struct.pack('>I', 3)))
    self.assertEqual(4294967303, number.BytesToLong(struct.pack('>2I', 1, 7)))
    self.assertEqual(2 ** 64, number.BytesToLong(struct.pack('>3I', 1, 0, 0)))
    self.assertEqual(2 ** 160 + 2 ** 128 + 2 ** 96 + 2 ** 64 + 2 ** 32 + 7,
                     number.BytesToLong(struct.pack('>6I', 1, 1, 1, 1, 1, 7)))

  def testLongToBytes(self):
    self.assertEqual(struct.pack('>I', 3), number.LongToBytes(3))
    self.assertEqual(struct.pack('>2I', 1, 7), number.LongToBytes(4294967303))
    self.assertEqual(struct.pack('>3I', 1, 0, 0), number.LongToBytes(2 ** 64))
    self.assertEqual(struct.pack('>6I', 1, 1, 1, 1, 1, 7),
                     number.LongToBytes(2 ** 160 + 2 ** 128 + 2 ** 96 + 2 ** 64
                                        + 2 ** 32 + 7))


def main(_):
  googletest.main()

if __name__ == '__main__':
  app.run()
