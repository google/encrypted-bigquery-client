#!/usr/bin/env python
# Copyright 2012 Google Inc. All Rights Reserved.

"""Unit test for ebq_crypto module."""




from google.apputils import app
import logging
from google.apputils import basetest as googletest

import ebq_crypto as ecrypto

_KEY1 = '0123456789abcdef'
_PLAINTEXT1 = 'this is test string one'


class ProbabilisticCiphertTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    self.cipher = ecrypto.ProbabilisticCipher(_KEY1)

  def testProbabilisticEncryptDecryptUnicodeString(self):
    logging.debug('Running testProbabilisticEncryptDecryptUtf8String method.')
    # test success with different plaintexts
    for plaintext in (u'22', u'this is test string one', u'-1.3', u'5545',
                      u"""this is a longer test string that should go on for
                      more than two AES blocks or perhaps many more of them
                      also."""):
      ciphertext = self.cipher.Encrypt(plaintext)
      self.assertEqual(plaintext, self.cipher.Decrypt(ciphertext))
    # non string type should raise an error.
    try:
      self.cipher.Encrypt(22)
      self.fail()
    except ValueError:
      pass  # success

  def testDecryptWhenRaw(self):
    """Test Decrypt() in raw mode while passing invalid utf-8 bytes."""
    invalid_utf8 = '\xf0\xf0\xf0'
    ciphertext = self.cipher.Encrypt(invalid_utf8)
    plaintext = self.cipher.Decrypt(ciphertext, raw=True)
    self.assertEqual(invalid_utf8, plaintext)

  def testDecryptWhenNotRaw(self):
    """Test Decrypt() when not in raw mode, which is the default."""
    invalid_utf8 = '\xf0\xf0\xf0'
    ciphertext = self.cipher.Encrypt(invalid_utf8)
    # although these bytes were encrypted and returned as ciphertext,
    # attempting to decrypt them (works) and transform them back to
    # unicode via utf-8 decode (raw=False) should fail.
    self.assertRaises(
        UnicodeDecodeError, self.cipher.Decrypt, ciphertext, raw=False)


class PseudonymCiphertTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    self.cipher = ecrypto.PseudonymCipher(_KEY1)

  def testPseudonymEncryptDecryptUnicodeString(self):
    logging.debug('Running testPseudonymEncryptDecryptUtf8String method.')
    # test success with different plaintexts
    for plaintext in (u'22', u'this is test string one', u'-1.3', u'5545',
                      u"""this is a longer test string that should go on for
                      more than two AES blocks or perhaps many more of them
                      also."""):
      ciphertext = self.cipher.Encrypt(plaintext)
      self.assertEqual(plaintext, self.cipher.Decrypt(ciphertext))
    # non string type should raise an error.
    try:
      self.cipher.Encrypt(22)
      self.fail()
    except ValueError:
      pass  # success


def _GetRandForTesting(size):
  # return some constant of appropriate size
  return size * '1'


class HomomorphicIntCiphertTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    self.cipher = ecrypto.HomomorphicIntCipher(_KEY1)

  def testHomomorphicEncryptIntDecryptInt(self):
    logging.debug('Running testHomomorphicEncryptIntDecryptInt method.')
    # test success with different plaintexts
    for plaintext in (2, 5, 55, 333333333, 44444444444):
      ciphertext = self.cipher.Encrypt(plaintext)
      self.assertEqual(plaintext, self.cipher.Decrypt(ciphertext))
    # non int/long type should raise an error.
    try:
      self.cipher.Encrypt('22')
      self.fail()
    except ValueError:
      pass  # success
    try:
      self.cipher.Encrypt(22222222222222222222222)
      self.fail()
    except ValueError:
      pass  # success


class HomomorphicFloatCipherTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    self.cipher = ecrypto.HomomorphicFloatCipher(_KEY1)

  def testHomomorphicEncryptFloatDecryptFloat(self):
    logging.debug('Running testHomomorphicEncryptFloatDecryptFloat method.')
    # test success with different plaintexts
    for plaintext in (1.22, 0.4565, 55.45, 33.3333333, 444444444.44):
      ciphertext = self.cipher.Encrypt(plaintext)
      self.assertEqual(plaintext, self.cipher.Decrypt(ciphertext))
    # encrypting a too large float should raise an error.
    try:
      self.cipher.Encrypt(1.0*2**400)
      self.fail()
    except ValueError:
      pass  # success
    # non int/long type should raise an error.
    try:
      self.cipher.Encrypt('22')
      self.fail()
    except ValueError:
      pass  # success


class StringHashTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    self.hasher = ecrypto.StringHash(_KEY1, 8, 'sha1')
    self.fieldname = u'Description'

  def testGetStringKeyHash(self):
    logging.debug('Running testGetStringKeyHash method.')
    hash1 = self.hasher.GetStringKeyHash(self.fieldname, u'school')
    self.assertEqual(12, len(hash1))  # expanded to 8 * 4/3 due to base64
    # check 2nd call to hash gives same output
    hash2 = self.hasher.GetStringKeyHash(self.fieldname, u'school')
    self.assertEqual(hash1, hash2)
    # check different input gives a different hash
    hash3 = self.hasher.GetStringKeyHash(self.fieldname, u'not school')
    self.assertNotEqual(hash1, hash3)
    # check that hash output length can be specified on digest call
    hash4 = self.hasher.GetStringKeyHash(self.fieldname, u'school',
                                         output_len=33)
    # -- 33*4/3 rounded up to a multiple of 4 for base64 encoding
    self.assertEqual(44, len(hash4))
    # check that another hash function can be specified on digest call
    hash5 = self.hasher.GetStringKeyHash(self.fieldname, u'school',
                                         output_len=33, hashfunc='sha256')
    self.assertNotEqual(hash4, hash5)
    # check that another outputlen and hashfunc can be set through constructor
    hasher6 = ecrypto.StringHash(_KEY1, 33, 'sha256')
    self.assertEqual(hash5, hasher6.GetStringKeyHash(self.fieldname, u'school'))

  def testGetHashessForWordSubsequencesWithIv(self):
    logging.debug('Running testGetHashesForWordSubsequencesWithIv method.')
    text = u'The quick brown fox jumps over the lazy dog'
    hashes1 = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, text, random_permute=False, rand_gen=_GetRandForTesting)
    # hashes1 has 36 words because text has 9 words and there are 35 word
    # subsequences of length 5 or less, and one extra due to pre-pended iv.
    self.assertEqual(36, len(hashes1.split()))
    # For regression testing, below is the expected string.
    self.assertEqual('MTExMTExMTExMTExMTExMQ== r+LnPgD7hZQ= xSqjeTLry4M= '
                     'Nsx9q20oJFk= /xu7ZfpL2B0= +kzAzlhR4Q4= sOKsrKXhkCQ= '
                     '8qmxrO4cbSg= 0zvX/8lk2f4= htApcCWILMg= sKK2mV5HpXY= '
                     '7pCfT7322NU= j33+LJhZFug= IP1X3g/lPDU= UtP0wX/xX4E= '
                     '3a127xbQ5Hg= Kc5wG5S71ac= crqNunt/kdY= y2cx1LMP1Pk= '
                     'GU4VGtrwcmI= QdgK8S91ZIw= wr8+BHzGCzc= KAez7MjDGVo= '
                     'nzHEdXrWRPU= X/zhUoGgoss= 9vOSQpX3CZk= NpU2fSVRlKw= '
                     'FCIrv3nzunI= jrCH4Takl+I= JSs5E/K2Wr8= r+LnPgD7hZQ= '
                     'Xvb827F9rzw= htitZIrHc4w= e+6DbqjmqFU= c2xxyrQH3dU= '
                     'GYmYmk5pI1g=', hashes1)
    # check 2nd call to hash gives same output
    hashes2 = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, text, random_permute=False, rand_gen=_GetRandForTesting)
    self.assertEqual(hashes1, hashes2)
    # check different smaller input (by one word) gives a different hash
    hashes3 = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, text[3:], random_permute=False,
        rand_gen=_GetRandForTesting)
    self.assertNotEqual(hashes1, hashes3)
    self.assertEqual(31, len(hashes3.split()))  # one extra for IV
    # check that hash output length can be specified on digest call
    # - for 16 bytes length, the b64 encode turns it into 24 bytes.
    hashes4 = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, text[3:], output_len=16, random_permute=False,
        rand_gen=_GetRandForTesting)
    self.assertEqual(24, len(hashes4.split()[1]))  # skip 0th which is IV
    # check that another hash function can be specified on digest call
    hashes5 = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, text[3:], hashfunc='sha256', random_permute=False,
        rand_gen=_GetRandForTesting)
    self.assertNotEqual(hashes1, hashes5)
    # check that another max_sequence_len can be set
    hashes6 = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, text, max_sequence_len=3, random_permute=False)
    self.assertEqual(25, len(hashes6.split()))  # one extra for IV

    # check hashing of string with punctuations marks.
    # - As in hashes3 above we also skip the first word, the result should be
    #   same as hashes3.
    unclean_text = (u'The; quick,,,, BROWN''!\"#%&\'()*,/?@[]{}--___, fox ' +
                    u'jUmps. over the lazy dog...')
    hashes_unclean = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, unclean_text[3:], random_permute=False,
        rand_gen=_GetRandForTesting)
    self.assertEqual(hashes3, hashes_unclean)

    # check hashing with a different separator
    text_slash = u'http://www.google.com/johndoe-inbox'
    hashes_slash = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, text_slash, random_permute=False,
        rand_gen=_GetRandForTesting, separator='/')
    self.assertEqual(7, len(hashes_slash.split()))  # one extra for IV
    self.assertEqual('MTExMTExMTExMTExMTExMQ== FvAl46zVC9s= tyI89/YsFnI= '
                     'Xx51CZp6Nks= PkkH/6bqWnI= 32+DPg79MK8= 3lESAvKjz84=',
                     hashes_slash)

    # check empty text, results in just the iv.
    empty_text = u' ;,.'
    hashes_empty = self.hasher.GetHashesForWordSubsequencesWithIv(
        self.fieldname, empty_text, random_permute=False,
        rand_gen=_GetRandForTesting)
    # - expect base64 encoding of IV of 16 ones.
    self.assertEqual('MTExMTExMTExMTExMTExMQ==', hashes_empty)


def main(_):
  googletest.main()


if __name__ == '__main__':
  app.run()
