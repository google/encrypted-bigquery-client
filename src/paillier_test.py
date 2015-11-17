#!/usr/bin/env python
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Unitttest for pallier module."""




from google.apputils import app
import logging
from google.apputils import basetest as googletest

import paillier

_KEY1 = '0123456789abcdef'
_PAILLIER1 = paillier.Paillier(_KEY1)


class PaillierTest(googletest.TestCase):

  def testInitRegression(self):
    logging.debug('Running testInitRegression method.')
    self.assertEquals('1540446301194064289812749522269230895547727566172905280'
                      '2882838764869064995301252386388711882490727734617556022'
                      '8507897723047251274539138292573162215487167555658392777'
                      '9536049058356061700336746251128024825735763630561361861'
                      '0929443937044092979781080225804474709639217360215385238'
                      '4570204894053081599875939643724669', str(_PAILLIER1.n))
    self.assertEquals('2372974806862473835834144917018834556331926303125038409'
                      '6775811412153106981342389782378239488788122287676159276'
                      '1850386996821190664988405870356206273587971863611766912'
                      '0381062564616153635414185469585083583071508506707445513'
                      '2001434085731328421552649684032869139250221416966312123'
                      '1237596617231183994255908801100597449034150237339322467'
                      '5366569845523530728686693121097406792316975604263076712'
                      '1291441308807487923285088744975286985227822579524011930'
                      '5341838298587473815993345532544642884445694616558892648'
                      '6623908161022630486788963851298643108425369966761829383'
                      '1069852803627629124893816742168026660260233751979962059'
                      '831479159561', str(_PAILLIER1.nsquare))
    self.assertEquals(_PAILLIER1.n + 1, _PAILLIER1.g)


  def testModExp(self):
    logging.debug('Running testModExp method.')
    # this test only applies if we are using openssl.
    self.assertTrue(paillier._FOUND_SSL)
    a = 255
    b = 1500
    c = 253
    expect = 177
    m1 = paillier.ModExp(a, b, c)
    m2 = pow(a, b, c)
    self.assertEqual(m1, expect)
    self.assertEqual(m2, expect)

  def testEncryptDecrypt(self):
    logging.debug('Running testEncryptDecrypt method.')
    data = 123456789123456789123456789123456789
    ciphertext = _PAILLIER1.Encrypt(data)
    decryption = _PAILLIER1.Decrypt(ciphertext)
    self.assertEquals(data, decryption)

  def testAdd(self):
    logging.debug('Running testAdd method.')
    plaintext1 = 123456789123456789123456789123456789
    ciphertext1 = _PAILLIER1.Encrypt(plaintext1)
    plaintext2 = 111111110111111110111111110111111110
    ciphertext2 = _PAILLIER1.Encrypt(plaintext2)
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    decryption = _PAILLIER1.Decrypt(ciphertext3)
    self.assertEquals(plaintext1 + plaintext2, decryption)

  def testAddWraparound(self):
    logging.debug('Running testAddWraparound method.')
    # Check decryption works for n -1
    plaintext1 = _PAILLIER1.n - 1
    ciphertext1 = _PAILLIER1.Encrypt(plaintext1)
    self.assertEquals(plaintext1, _PAILLIER1.Decrypt(ciphertext1))
    # Check decryption wraps for n to 0
    plaintext2 = _PAILLIER1.n
    ciphertext2 = _PAILLIER1.Encrypt(plaintext2)
    self.assertEquals(0, _PAILLIER1.Decrypt(ciphertext2))
    # Check decryption wraps for n + 1 to 1
    plaintext3 = _PAILLIER1.n + 1
    ciphertext3 = _PAILLIER1.Encrypt(plaintext3)
    self.assertEquals(1, _PAILLIER1.Decrypt(ciphertext3))

  def testAffine(self):
    logging.debug('Running testAffine method.')
    plaintext1 = 123456789123456789123456789123456789
    ciphertext1 = _PAILLIER1.Encrypt(plaintext1)
    # test a = 2
    a = 2
    b = 111111110111111110111111110111111110
    ciphertext3 = _PAILLIER1.Affine(ciphertext1, a, b)
    decryption3 = _PAILLIER1.Decrypt(ciphertext3)
    self.assertEquals(a * plaintext1 + b, decryption3)
    # test a = 0
    ciphertext4 = _PAILLIER1.Affine(ciphertext1, 0, b)
    decryption4 = _PAILLIER1.Decrypt(ciphertext4)
    self.assertEquals(b, decryption4)
    # test a = 1
    ciphertext5 = _PAILLIER1.Affine(ciphertext1, 1, b)
    decryption5 = _PAILLIER1.Decrypt(ciphertext5)
    self.assertEquals(plaintext1 + b, decryption5)
    # test b = 0
    ciphertext6 = _PAILLIER1.Affine(ciphertext1, 2, 0)
    decryption6 = _PAILLIER1.Decrypt(ciphertext6)
    self.assertEquals(2 * plaintext1, decryption6)
    # test a=0, b = 0
    ciphertext7 = _PAILLIER1.Affine(ciphertext1, 0, 0)
    decryption7 = _PAILLIER1.Decrypt(ciphertext7)
    self.assertEquals(0, decryption7)

  def testEncryptDecryptRegression(self):
    logging.debug('Running testEncryptDecryptRegression method.')
    paillier1 = paillier.Paillier(None, 6497955158, 126869, 31536, 53022)
    ciphertext = paillier1.Encrypt(10100, 74384)
    self.assertEquals(848742150, ciphertext)
    decryption = paillier1.Decrypt(848742150)
    self.assertEquals(10100, decryption)

  def testGetRandomFromZNStar(self):
    logging.debug('Running testGetRandomFromZNStar method.')
    # 8 bit values not relatively prime to 143 and less than 143.
    not_relatively_prime_to_143 = [130, 132, 143]
    for _ in xrange(20):
      r = _PAILLIER1._GetRandomFromZNStar(8, 143)
      self.assertFalse(r in not_relatively_prime_to_143)

  def testEncryptInt64DecryptInt64(self):
    logging.debug('Running testEncryptInt64DecryptInt64 method.')
    # A small positive number
    ciphertext = _PAILLIER1.EncryptInt64(15)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(15, decryption)
    # A small negative number
    ciphertext = _PAILLIER1.EncryptInt64(-15)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(-15, decryption)
    # largest positive number
    ciphertext = _PAILLIER1.EncryptInt64(2**63 - 1)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(2**63 - 1, decryption)
    # largest negative number
    ciphertext = _PAILLIER1.EncryptInt64(-2**63)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(-2**63, decryption)

  def testAddWithEncryptDecryptInt64(self):
    logging.debug('Running testAddWithEncryptDecryptInt64 method.')
    # Add 1 to a small negative number
    ciphertext1 = _PAILLIER1.EncryptInt64(-15)
    ciphertext2 = _PAILLIER1.EncryptInt64(1)
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    decryption = _PAILLIER1.DecryptInt64(ciphertext3)
    self.assertEquals(-14, decryption)
    # Add 1 to a small positive number
    ciphertext1 = _PAILLIER1.EncryptInt64(15)
    ciphertext2 = _PAILLIER1.EncryptInt64(1)
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    decryption = _PAILLIER1.DecryptInt64(ciphertext3)
    self.assertEquals(16, decryption)
    # Add -1 to a small negative number
    ciphertext1 = _PAILLIER1.EncryptInt64(-15)
    ciphertext2 = _PAILLIER1.EncryptInt64(-1)
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    decryption = _PAILLIER1.DecryptInt64(ciphertext3)
    self.assertEquals(-16, decryption)

  def testMultipleAddWithEncryptDecryptInt64(self):
    logging.debug('Running testMultipleAddWithEncryptDecryptInt64 method.')
    c123456789 = _PAILLIER1.EncryptInt64(123456789L)
    c314159265359 = _PAILLIER1.EncryptInt64(314159265359L)
    c271828182846 = _PAILLIER1.EncryptInt64(271828182846L)
    c987654321neg = _PAILLIER1.EncryptInt64(-987654321L)
    c161803398874neg = _PAILLIER1.EncryptInt64(-161803398874L)
    c1414213562373095neg = _PAILLIER1.EncryptInt64(-1414213562373095L)
    # Add many positive numbers
    ciphertext = _PAILLIER1.Add(c123456789, c314159265359)
    ciphertext = _PAILLIER1.Add(ciphertext, c271828182846)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(123456789 + 314159265359 + 271828182846, decryption)
    # Add many negative numbers
    ciphertext = _PAILLIER1.Add(c987654321neg, c161803398874neg)
    ciphertext = _PAILLIER1.Add(ciphertext, c1414213562373095neg)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(-987654321 + -161803398874 + -1414213562373095,
                      decryption)
    # Add many positive and negative numbers with aggregate being positive
    ciphertext = _PAILLIER1.Add(c123456789, c314159265359)
    ciphertext = _PAILLIER1.Add(ciphertext, c271828182846)
    ciphertext = _PAILLIER1.Add(ciphertext, c987654321neg)
    ciphertext = _PAILLIER1.Add(ciphertext, c161803398874neg)
    ciphertext_3pos_2neg = ciphertext
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    sum_3pos_2neg = (123456789 + 314159265359 + 271828182846 + -987654321 +
                     -161803398874)
    self.assertEquals(sum_3pos_2neg, decryption)
    # Add many positive and negative numbers with aggregate being negative
    ciphertext = _PAILLIER1.Add(c123456789, c314159265359)
    ciphertext = _PAILLIER1.Add(ciphertext, c271828182846)
    ciphertext = _PAILLIER1.Add(ciphertext, c987654321neg)
    ciphertext = _PAILLIER1.Add(ciphertext, c161803398874neg)
    ciphertext = _PAILLIER1.Add(ciphertext, c1414213562373095neg)
    ciphertext_3pos_3neg = ciphertext
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    sum_3pos_3neg = (123456789 + 314159265359 + 271828182846 + -987654321 +
                     -161803398874 + -1414213562373095)
    self.assertEquals(sum_3pos_3neg, decryption)
    # Add many positive and negative numbers to reach 2^63 - 1.
    ciphertext = _PAILLIER1.EncryptInt64(2**63 - 1 - sum_3pos_2neg)
    ciphertext = _PAILLIER1.Add(ciphertext_3pos_2neg, ciphertext)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(2**63 -1, decryption)
    # Add many positive and negative numbers to reach -2^63 + 1.
    ciphertext = _PAILLIER1.EncryptInt64(-2**63 + 1 - sum_3pos_3neg)
    ciphertext = _PAILLIER1.Add(ciphertext_3pos_3neg, ciphertext)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(-2**63 + 1, decryption)
    # Add many positive and negative numbers to reach -2^63.
    ciphertext = _PAILLIER1.EncryptInt64(-2**63 - sum_3pos_3neg)
    ciphertext = _PAILLIER1.Add(ciphertext_3pos_3neg, ciphertext)
    decryption = _PAILLIER1.DecryptInt64(ciphertext)
    self.assertEquals(-2**63, decryption)

  def testEncryptInt64Fail(self):
    logging.debug('Running testEncryptInt64Fail method.')
    # check positive overflow
    try:
      _PAILLIER1.EncryptInt64(2**63)
      self.fail()
    except ValueError:
      pass  # success
    try:
      _PAILLIER1.EncryptInt64(2**128)
      self.fail()
    except ValueError:
      pass  # success
    # check negative overflow
    try:
      _PAILLIER1.EncryptInt64(-2**63 - 1)
      self.fail()
    except ValueError:
      pass  # success
    try:
      _PAILLIER1.EncryptInt64(-2**128)
      self.fail()
    except ValueError:
      pass  # success
    # positive overflow as a result of adding
    ciphertext1 = _PAILLIER1.EncryptInt64(2**63 - 1)
    ciphertext2 = _PAILLIER1.EncryptInt64(1)
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    try:
      _PAILLIER1.DecryptInt64(ciphertext3)
      self.fail()
    except OverflowError:
      pass  # success
    # negative overflow as a result of adding
    ciphertext1 = _PAILLIER1.EncryptInt64(-2**63)
    ciphertext2 = _PAILLIER1.EncryptInt64(-1)
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    try:
      _PAILLIER1.DecryptInt64(ciphertext3)
      self.fail()
    except OverflowError:
      pass  # success

  def testFloatBasicMethods(self):
    logging.debug('Running testFloatBasicMethods method.')
    self.assertTrue(paillier.IsNan(float('nan')))
    for f in (1e3000, 1.5e3000):
      self.assertTrue(paillier.IsInfPlus(f))
      self.assertFalse(paillier.IsInfMinus(f))
      self.assertFalse(paillier.IsNan(f))
    for f in (-1e3000, -1.5e3000):
      self.assertTrue(paillier.IsInfMinus(f))
      self.assertFalse(paillier.IsInfPlus(f))
      self.assertFalse(paillier.IsNan(f))

  def testEncryptDecryptFloatInfAndNan(self):
    logging.debug('Running testEncryptDecryptFloat method.')
    nan_value = float('nan')
    # Encrypt/decrypt a nan
    c_nan = _PAILLIER1.EncryptFloat(nan_value)
    self.assertTrue(paillier.IsNan(_PAILLIER1.DecryptFloat(c_nan)))
    # Encrypt/decrypt after adding two nan
    c_2nan = _PAILLIER1.Add(c_nan, c_nan)
    self.assertTrue(paillier.IsNan(_PAILLIER1.DecryptFloat(c_2nan)))
    # Encrypt/decrypt a plus inf
    c_plus_inf = _PAILLIER1.EncryptFloat(1e3000)
    self.assertTrue(paillier.IsInfPlus(_PAILLIER1.DecryptFloat(c_plus_inf)))
    # Encrypt/decrypt after adding two plus inf
    c_2plus_inf = _PAILLIER1.Add(c_plus_inf, c_plus_inf)
    self.assertTrue(paillier.IsInfPlus(_PAILLIER1.DecryptFloat(c_2plus_inf)))
    # Encrypt/decrypt a minus inf
    c_minus_inf = _PAILLIER1.EncryptFloat(-1e3000)
    self.assertTrue(paillier.IsInfMinus(_PAILLIER1.DecryptFloat(c_minus_inf)))
    # Encrypt/decrypt after adding two minus inf
    c_2minus_inf = _PAILLIER1.Add(c_minus_inf, c_minus_inf)
    self.assertTrue(paillier.IsInfMinus(_PAILLIER1.DecryptFloat(c_2minus_inf)))
    # Encrypt/decrypt after adding plus inf and minus inf --> nan
    c_sum_plus_minus_inf = _PAILLIER1.Add(c_plus_inf, c_minus_inf)
    self.assertTrue(
        paillier.IsNan(_PAILLIER1.DecryptFloat(c_sum_plus_minus_inf)))
    # Encrypt/decrypt after adding plus inf and nan --> nan
    c_sum_inf_nan = _PAILLIER1.Add(c_plus_inf, c_nan)
    self.assertTrue(paillier.IsNan(_PAILLIER1.DecryptFloat(c_sum_inf_nan)))
    # Encrypt/decrypt after adding normal finite num to inf and nans.
    c_1_0 = _PAILLIER1.EncryptFloat(1.0)
    c_1_0neg = _PAILLIER1.EncryptFloat(-1.0)
    c_sum_fin_nan = _PAILLIER1.Add(c_1_0, c_nan)
    self.assertTrue(paillier.IsNan(_PAILLIER1.DecryptFloat(c_sum_fin_nan)))
    c_sum_fin_nan = _PAILLIER1.Add(c_1_0neg, c_nan)
    self.assertTrue(paillier.IsNan(_PAILLIER1.DecryptFloat(c_sum_fin_nan)))
    # Encrypt/decrypt after adding normal finite num to plus inf.
    c_sum_plus_inf = _PAILLIER1.Add(c_1_0, c_plus_inf)
    self.assertTrue(
        paillier.IsInfPlus(_PAILLIER1.DecryptFloat(c_sum_plus_inf)))
    c_sum_plus_inf = _PAILLIER1.Add(c_1_0neg, c_plus_inf)
    self.assertTrue(
        paillier.IsInfPlus(_PAILLIER1.DecryptFloat(c_sum_plus_inf)))
    # Encrypt/decrypt after adding normal finite num to minus inf.
    c_sum_minus_inf = _PAILLIER1.Add(c_1_0, c_minus_inf)
    self.assertTrue(
        paillier.IsInfMinus(_PAILLIER1.DecryptFloat(c_sum_minus_inf)))
    c_sum_minus_inf = _PAILLIER1.Add(c_1_0neg, c_minus_inf)
    self.assertTrue(
        paillier.IsInfMinus(_PAILLIER1.DecryptFloat(c_sum_minus_inf)))

  def testEncryptDecryptFloatSmallNormalNumbers(self):
    logging.debug('Running testEncryptDecryptFloatSmallNormalNumbers method.')
    # small positive numbers
    c_1_0 = _PAILLIER1.EncryptFloat(1.0)
    self.assertEquals(1.0, _PAILLIER1.DecryptFloat(c_1_0))
    c_15_1234 = _PAILLIER1.EncryptFloat(15.1234)
    self.assertEquals(15.1234, _PAILLIER1.DecryptFloat(c_15_1234))
    # small negative numbers
    c_1_0neg = _PAILLIER1.EncryptFloat(-1.0)
    self.assertEquals(-1.0, _PAILLIER1.DecryptFloat(c_1_0neg))
    c_15_1234neg = _PAILLIER1.EncryptFloat(-15.1234)
    self.assertEquals(-15.1234, _PAILLIER1.DecryptFloat(c_15_1234neg))

    # numbers close to 1.0
    # smallest number > than 1.0
    data = 1.0000000000000002
    c_smallest_gt_1 = _PAILLIER1.EncryptFloat(data)
    self.assertEquals(data,
                      _PAILLIER1.DecryptFloat(c_smallest_gt_1))
    self.assertNotEquals(1.0, _PAILLIER1.DecryptFloat(c_smallest_gt_1))
    # smaller than 1.0000000000000002 is rounded to 1.0
    data = 1.0000000000000001
    self.assertEquals(
        1.0, _PAILLIER1.DecryptFloat(_PAILLIER1.EncryptFloat(data)))
    # negative number < than -1.0
    data = -1.0000000000000002
    c_smallest_lt_1neg = _PAILLIER1.EncryptFloat(data)
    self.assertEquals(data,
                      _PAILLIER1.DecryptFloat(c_smallest_lt_1neg))
    self.assertNotEquals(-1.0, _PAILLIER1.DecryptFloat(c_smallest_lt_1neg))
    # negative smaller than -1.0000000000000002 is rounded to -1.0
    self.assertEquals(-1.0,
                      _PAILLIER1.DecryptFloat(_PAILLIER1.EncryptFloat(
                          -1.0000000000000001)))

    # one third
    c_onethird = _PAILLIER1.EncryptFloat(1.0/3.0)
    self.assertEquals(1.0/3.0, _PAILLIER1.DecryptFloat(c_onethird))
    # negative one third
    c_onethird = _PAILLIER1.EncryptFloat(-1.0/3.0)
    self.assertEquals(-1.0/3.0, _PAILLIER1.DecryptFloat(c_onethird))

    # check small fractions close to 0 on the positive and negative side.
    # smallest normal exponent
    smallest_normal = 2**-389
    self.assertEquals(smallest_normal, _PAILLIER1.DecryptFloat(
        _PAILLIER1.EncryptFloat(smallest_normal)))
    # slightly larger than smallest normal exponent
    near_smallest_normal = 1.23456789 * 2**-389
    self.assertEquals(near_smallest_normal, _PAILLIER1.DecryptFloat(
        _PAILLIER1.EncryptFloat(near_smallest_normal)))
    # negative number with smallest normal exponent
    smallest_normal = -2**-389
    self.assertEquals(smallest_normal, _PAILLIER1.DecryptFloat(
        _PAILLIER1.EncryptFloat(smallest_normal)))
    # negative number that has slightly larger than the smallest exponent.
    near_smallest_normal = -1.23456789 * 2**-389
    self.assertEquals(near_smallest_normal, _PAILLIER1.DecryptFloat(
        _PAILLIER1.EncryptFloat(near_smallest_normal)))

  def testEncryptDecryptFloatSubNormalNumbers(self):
    logging.debug('Running testEncryptDecryptFloatSubNormalNumbers method.')
    # --------- positive subnormal numbers -----------
    # smallest subnormal numbers.
    smallest_subnormal = 2**-441
    c_smallest_subnormal = _PAILLIER1.EncryptFloat(smallest_subnormal)
    self.assertEquals(smallest_subnormal,
                      _PAILLIER1.DecryptFloat(c_smallest_subnormal))
    # check smaller than smallest subnormal numbers rounds to zero.
    # precision of python double is 52 bits, so use it to test 'closeness'
    self.assertEquals(0.0, _PAILLIER1.DecryptFloat(_PAILLIER1.EncryptFloat(
        2**-442)))

    # ---- negative  small subnormal numbers ------
    # smallest negative subnormal numbers.
    smallest_subnormal = -2**-441
    c_smallest_subnormal = _PAILLIER1.EncryptFloat(smallest_subnormal)
    self.assertEquals(smallest_subnormal,
                      _PAILLIER1.DecryptFloat(c_smallest_subnormal))
    # check smaller than smallest subnormal numbers rounds to zero.
    #  precision of python double is 52 bits, so use it to test 'closeness'
    self.assertEquals(0.0, _PAILLIER1.DecryptFloat(_PAILLIER1.EncryptFloat(
        -2**-442)))

  def testEncryptDecryptFloatLargeFiniteNumbers(self):
    logging.debug('Running testEncryptDecryptFloatLargeFiniteNumbers method.')
    # ------- positive large finite numbers ------
    large1 = 1.23456789123456789123456789 * 2**80
    large2 = 1.23456789123456789123456789 * 2**100
    c_large1 = _PAILLIER1.EncryptFloat(large1)
    self.assertEquals(large1, _PAILLIER1.DecryptFloat(c_large1))
    c_large2 = _PAILLIER1.EncryptFloat(large2)
    self.assertEquals(large2, _PAILLIER1.DecryptFloat(c_large2))
    # mantissa with 17 digits.
    c_mantissa17digits = _PAILLIER1.EncryptFloat(
        1234567890.0123456)
    self.assertEquals(1234567890.0123456,
                      _PAILLIER1.DecryptFloat(c_mantissa17digits))
    # mantissa with 18 digits rounds only approximately.
    c_mantissa18digits = _PAILLIER1.EncryptFloat(
        1234567890.01234568)
    decrypted = _PAILLIER1.DecryptFloat(c_mantissa18digits)
    self.assertNotEquals(1234567890.0123456, decrypted)
    self.assertTrue(abs(decrypted - 1234567890.0123456) <= .0000003)
    # absolute largest normal number allowed
    largest_normal = 1.9999999999999998 * 2**389
    c_largest_normal = _PAILLIER1.EncryptFloat(largest_normal)
    self.assertEquals(largest_normal, _PAILLIER1.DecryptFloat(c_largest_normal))
    # larger than largest normal number throws error
    try:
      beyond_largest_normal = 1.0 * 2**390
      _PAILLIER1.EncryptFloat(beyond_largest_normal)
      self.fail()
    except ValueError:
      pass  # success

    # ---- negative  large finite numbers ------
    # mantissa with 17 digits and negative.
    c_mantissa17digits_neg = _PAILLIER1.EncryptFloat(
        -1234567890.0123456)
    self.assertEquals(-1234567890.0123456,
                      _PAILLIER1.DecryptFloat(c_mantissa17digits_neg))
    # mantissa with 18 digits rounds only approximately.
    c_mantissa18digits_neg = _PAILLIER1.EncryptFloat(
        -1234567890.01234568)
    decrypted = _PAILLIER1.DecryptFloat(c_mantissa18digits_neg)
    self.assertNotEquals(-1234567890.0123456, decrypted)
    self.assertTrue(abs(decrypted - -1234567890.0123456) <= .0000003)
    # absolute largest negative normal number allowed
    largest_normal = -1.9999999999999998 * 2**389
    c_largest_normal = _PAILLIER1.EncryptFloat(largest_normal)
    self.assertEquals(largest_normal, _PAILLIER1.DecryptFloat(c_largest_normal))
    # larger magnitude than negative normal number with largest magnituede
    # throws and error
    try:
      beyond_largest_normal = -1.0 * 2**390
      _PAILLIER1.EncryptFloat(beyond_largest_normal)
      self.fail()
    except ValueError:
      pass  # success

  def testEncryptDecryptFloatAddFiniteNumbers(self):
    logging.debug('Running testEncryptDecryptFloatAddFiniteNumbers method.')
    # add a chain of small positive numbers
    c_1_0 = _PAILLIER1.EncryptFloat(1.0)
    c_2_0 = _PAILLIER1.EncryptFloat(2.0)
    c_3_0 = _PAILLIER1.Add(c_2_0, c_1_0)
    c_5_0 = _PAILLIER1.Add(c_3_0, c_2_0)
    self.assertEquals(3.0, _PAILLIER1.DecryptFloat(c_3_0))
    self.assertEquals(5.0, _PAILLIER1.DecryptFloat(c_5_0))
    # add a chain of small negative numbers
    c_2_0neg = _PAILLIER1.EncryptFloat(-2.0)
    c_4_0neg = _PAILLIER1.EncryptFloat(-4.0)
    c_6_0neg = _PAILLIER1.Add(c_2_0neg, c_4_0neg)
    self.assertEquals(-6.0, _PAILLIER1.DecryptFloat(c_6_0neg))
    # add positive and negative small numbers
    c_1_0neg = _PAILLIER1.Add(c_5_0, c_6_0neg)
    self.assertEquals(-1.0, _PAILLIER1.DecryptFloat(c_1_0neg))
    # add a large positive and negative number
    large1 = 1.999999999999999 * 2**389
    c_large1 = _PAILLIER1.EncryptFloat(large1)
    c_large2neg = _PAILLIER1.EncryptFloat(-0.999999999999999 * 2**389)
    decrypted = _PAILLIER1.DecryptFloat(_PAILLIER1.Add(c_large1, c_large2neg))
    # check close to 1.0 * 2**389
    self.assertTrue(decrypted > .9999999999999998 * 2**389)
    self.assertTrue(decrypted < 1.000000000000002 * 2**389)
    # add a very large and a relatively small number --> same large number.
    c_small1 = _PAILLIER1.EncryptFloat(.99999999999999 * 2**-430)
    decrypted = _PAILLIER1.DecryptFloat(_PAILLIER1.Add(c_large1, c_small1))
    self.assertEquals(large1, decrypted)
    # add two subnormal numbers.
    smallest_subnormal = 2**-441
    c_smallest_subnormal = _PAILLIER1.EncryptFloat(smallest_subnormal)
    c_2smallest_subnormal = _PAILLIER1.Add(c_smallest_subnormal,
                                           c_smallest_subnormal)
    self.assertEquals(2**-440, _PAILLIER1.DecryptFloat(c_2smallest_subnormal))
    # add a positive and negative subnormal numbers
    c_smallest_subnormal_neg = _PAILLIER1.EncryptFloat(-2**-441)
    self.assertEquals(smallest_subnormal, _PAILLIER1.DecryptFloat(
        _PAILLIER1.Add(c_2smallest_subnormal, c_smallest_subnormal_neg)))

  def testEncryptDecryptMultipleInt64s(self):
    plain_numbers = [56, -34, 23, 9, 15, -15, 0]
    ciphertext = _PAILLIER1.EncryptMultipleInt64s(plain_numbers)
    decryption = _PAILLIER1.DecryptMultipleInt64s(ciphertext)
    self.assertEquals(plain_numbers, decryption)
    plain_numbers = [-9, 0, 0, 0, 2**63 - 1, -2**63, 100]
    ciphertext = _PAILLIER1.EncryptMultipleInt64s(plain_numbers)
    decryption = _PAILLIER1.DecryptMultipleInt64s(ciphertext)
    self.assertEquals(plain_numbers, decryption)
    plain_numbers = [123456789L, 314159265359L, 271828182846L, -987654321L,
                     161803398874L, 1414213562373095L, 0L]
    ciphertext = _PAILLIER1.EncryptMultipleInt64s(plain_numbers)
    decryption = _PAILLIER1.DecryptMultipleInt64s(ciphertext)
    self.assertEquals(plain_numbers, decryption)

  def testAddMultipleInt64s(self):
    ciphertext1 = _PAILLIER1.EncryptMultipleInt64s([25, -25, -15, 15, -15])
    ciphertext2 = _PAILLIER1.EncryptMultipleInt64s([50, 25, 1, 1, -1])
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    decryption = _PAILLIER1.DecryptMultipleInt64s(ciphertext3)
    self.assertEquals([0, 0, 75, 0, -14, 16, -16], decryption)
    ciphers_sum = _PAILLIER1.EncryptMultipleInt64s([0])
    for _ in xrange(100):
      ciphers_sum = _PAILLIER1.Add(ciphers_sum, ciphertext1)
    decryption = _PAILLIER1.DecryptMultipleInt64s(ciphers_sum)
    self.assertEquals([0, 0, 2500, -2500, -1500, 1500, -1500], decryption)

    ciphertext1 = _PAILLIER1.EncryptMultipleInt64s(
        [123456789, -987654321, 123456789L, -2**63, 2**63 - 1, -2**63 + 1, 0])
    ciphertext2 = _PAILLIER1.EncryptMultipleInt64s(
        [314159265359, -161803398874, 314159265359L, -15, -15, -15, 0])
    ciphertext3 = _PAILLIER1.EncryptMultipleInt64s(
        [271828182846, -1414213562373095, 271828182846, 15, 15, 15, 0])
    ciphertext4 = _PAILLIER1.EncryptMultipleInt64s(
        [0, 0, -987654321, 0, 0, 0, 0])
    ciphertext5 = _PAILLIER1.EncryptMultipleInt64s(
        [0, 0, -161803398874, 0, 0, 0, 0])
    ciphers_sum = _PAILLIER1.Add(ciphertext1, ciphertext2)
    ciphers_sum = _PAILLIER1.Add(ciphers_sum, ciphertext3)
    ciphers_sum = _PAILLIER1.Add(ciphers_sum, ciphertext4)
    ciphers_sum = _PAILLIER1.Add(ciphers_sum, ciphertext5)
    decryption = _PAILLIER1.DecryptMultipleInt64s(ciphers_sum)
    positive_sum = 123456789 + 314159265359 + 271828182846
    negative_sum = -987654321 + -161803398874 + -1414213562373095
    total = 123456789 + 314159265359 + 271828182846 - 987654321 - 161803398874
    self.assertEquals([positive_sum, negative_sum, total, -2**63, 2**63 - 1,
                       -2**63 + 1, 0], decryption)

  def testEncryptDecryptMultipleInt64sFail(self):
    # check packing limit
    try:
      _PAILLIER1.EncryptMultipleInt64s([0, 0, 0, 0, 0, 0, 0, 0])
      self.fail()
    except ValueError:
      pass  # success
    # check positive overflow
    try:
      _PAILLIER1.EncryptMultipleInt64s([2**63, 0])
      self.fail()
    except ValueError:
      pass  # success
    # check negative overflow
    try:
      _PAILLIER1.EncryptMultipleInt64s([-2**63 - 1, 0])
      self.fail()
    except ValueError:
      pass  # success
    # positive overflow as a result of adding
    ciphertext1 = _PAILLIER1.EncryptMultipleInt64s([2**63 - 1, 0])
    ciphertext2 = _PAILLIER1.EncryptMultipleInt64s([1, 0])
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    try:
      _PAILLIER1.DecryptMultipleInt64s(ciphertext3)
      self.fail()
    except OverflowError:
      pass  # success
    # negative overflow as a result of adding
    ciphertext1 = _PAILLIER1.EncryptMultipleInt64s([-2**63, 0])
    ciphertext2 = _PAILLIER1.EncryptMultipleInt64s([-1, 0])
    ciphertext3 = _PAILLIER1.Add(ciphertext1, ciphertext2)
    try:
      _PAILLIER1.DecryptMultipleInt64s(ciphertext3)
      self.fail()
    except OverflowError:
      pass  # success

  def testTestSslRegression(self):
    """Test TestSslRegression() module method."""
    self.assertTrue(paillier._FOUND_SSL)
    paillier.TestSslRegression()


def main(_):
  googletest.main()


if __name__ == '__main__':
  app.run()
