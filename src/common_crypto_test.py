#!/usr/bin/env python
#
# Copyright 2012 Google Inc. All Rights Reserved.

"""Unitttest for common_crypto module."""




import base64
import mox
import stubout

from google.apputils import app
import logging
from google.apputils import basetest as googletest

import common_crypto as ccrypto

_KEY1 = '0123456789abcdef'
_KEY2 = 'fedcba9876543210'
_PLAINTEXT1 = 'this is test string one'
_PLAINTEXT2 = 'this is test string two'


class CommonCryptoTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    self.mox = mox.Mox()
    self.stubs = stubout.StubOutForTesting()

  def tearDown(self):
    self.mox.UnsetStubs()
    self.stubs.UnsetAll()

  def testGetRandBytes(self):
    logging.debug('Running testGetRandBytes method.')
    rand1 = ccrypto.GetRandBytes(16)
    rand2 = ccrypto.GetRandBytes(16)
    self.assertTrue(isinstance(rand1, str))
    self.assertEqual(16, len(rand1))
    self.assertEqual(16, len(rand2))
    self.assertNotEqual(rand1, rand2)

  def testGetRandBytesWhenZero(self):
    logging.debug('Running testGetRandBytesWhenZero method.')
    self.assertRaises(ValueError, ccrypto.GetRandBytes, 0)
    self.assertRaises(ValueError, ccrypto.GetRandBytes, -10)

  def testGetRandBytesWhenPlatformLinux(self):
    logging.debug('Running testGetRandBytesWhenPlatformLinux method.')

    fmock = self.mox.CreateMockAnything()
    self.stubs.Set(ccrypto, '_f_urandom_func', None)
    self.mox.StubOutWithMock(ccrypto.platform, 'uname')
    self.mox.StubOutWithMock(ccrypto.os, 'urandom')
    ccrypto.platform.uname().AndReturn(['Linux', 'h', 'v', 'x', 'x86', 'x86'])
    fmock('/dev/urandom', 'rb', ccrypto._F_URANDOM_BUFLEN).AndReturn(fmock)
    fmock.read(16).AndReturn('a' * 16)
    fmock.read(16).AndReturn('b' * 16)

    self.mox.ReplayAll()
    b = ccrypto.GetRandBytes(16, _open=fmock)
    self.assertEqual(len(b), 16)
    b = ccrypto.GetRandBytes(16, _open=fmock)
    self.assertEqual(len(b), 16)
    self.mox.VerifyAll()

  def testGetRandBytesWhenPlatformOther(self):
    logging.debug('Running testGetRandBytesWhenPlatformOther method.')

    fmock = self.mox.CreateMockAnything()
    self.stubs.Set(ccrypto, '_f_urandom_func', None)
    self.mox.StubOutWithMock(ccrypto.platform, 'uname')
    self.mox.StubOutWithMock(ccrypto.os, 'urandom')
    ccrypto.platform.uname().AndReturn(['OFQv', 'h', 'v', 'x', 'x86', 'x86'])
    ccrypto.os.urandom(16).AndReturn('a' * 16)
    ccrypto.os.urandom(16).AndReturn('b' * 16)

    self.mox.ReplayAll()
    b = ccrypto.GetRandBytes(16, _open=fmock)
    self.assertEqual(len(b), 16)
    b = ccrypto.GetRandBytes(16, _open=fmock)
    self.assertEqual(len(b), 16)
    self.mox.VerifyAll()

  def testPRFWhenUnsupportedHash(self):
    logging.debug('Running testPRFWhenUnsupportedHash method.')
    hashfunc = 'sha999'
    # sanity check
    self.assertRaises(ValueError, ccrypto.hashlib.new, hashfunc)
    self.assertRaises(
        ValueError, ccrypto.PRF, _KEY1, _PLAINTEXT1, hashfunc=hashfunc)

  def testPRF(self):
    logging.debug('Running testPRF method.')
    output1 = ccrypto.PRF(_KEY1, _PLAINTEXT1, hashfunc='sha256')
    output1_again = ccrypto.PRF(_KEY1, _PLAINTEXT1, hashfunc='sha256')
    output2 = ccrypto.PRF(_KEY1, _PLAINTEXT2, hashfunc='sha256')
    self.assertEqual(16, len(output1))
    self.assertEqual(output1, output1_again)
    self.assertNotEqual(output1, output2)
    self.assertEqual('\xa7\x15\xd9\xbbyO@\xad\xfc\x9f\x02\xcb\x9cD\xb7\x29',
                     output1)
    output3 = ccrypto.PRF(_KEY1, _PLAINTEXT1, output_len=37, hashfunc='sha256')
    self.assertEqual(37, len(output3))
    self.assertTrue(output1 in output3)
    self.assertFalse(output1 in output3[16:])  # check prefix is not repeated.
    # test using default hash function sha1
    output4 = ccrypto.PRF(_KEY1, _PLAINTEXT1)
    self.assertEqual(16, len(output4))
    self.assertNotEqual(output1, output4)


class PRGTest(googletest.TestCase):

  def testGetNextBytes(self):
    logging.debug('Running testGetNextBytes method.')
    # For regression testing, below is the expected rand stream with seed _KEY1
    expected = ('\x79\x1a\x05\x1e\x79\xa6\x9b\x6f\x74\x13\x0f\xa3\x09\x10\xdd'
                '\xdc\x8c\xb9\xc8\xf1\xba\xc5\xbf\xca\x1e\x57\x8a\x38\x47\x2b'
                '\x76\x42\x12\x76\x81\xa9\xf1\xf0\x10\x5d\x35\xb6\xa3\x3d\xdc'
                '\xd9\xbc\x5a\x33\xc9\x3e\x98\x04\x35\x01\x48\x01\xcb\x7d\x1a'
                '\x01\x50\x59\xee\xe1\x71\x16\x83\x84\xd6\xf0\xf0\xd7')
    prg = ccrypto.PRG(_KEY1)
    val1 = prg.GetNextBytes(23)
    self.assertEqual(type(''), type(val1))
    self.assertEqual(23, len(val1))
    self.assertEqual(expected[0:23], val1)
    val2 = prg.GetNextBytes(23)
    self.assertEqual(23, len(val2))
    self.assertNotEqual(val1, val2)
    self.assertEqual(expected[23:46], val2)
    val3 = prg.GetNextBytes(5)
    self.assertEqual(5, len(val3))
    self.assertEqual(expected[46:51], val3)
    val4 = prg.GetNextBytes(6)
    self.assertEqual(6, len(val4))
    self.assertEqual(expected[51:57], val4)
    val5 = prg.GetNextBytes(16)
    self.assertEqual(16, len(val5))
    self.assertEqual(expected[57:73], val5)
    # Re-initialize PRG and see if output is same if requested in different
    # pieces.
    prg = ccrypto.PRG(_KEY1)
    val1 = prg.GetNextBytes(3)
    self.assertEqual(3, len(val1))
    self.assertEqual(expected[0:3], val1)
    val2 = prg.GetNextBytes(3)
    self.assertEqual(3, len(val2))
    self.assertEqual(expected[3:6], val2)
    val3 = prg.GetNextBytes(12)
    self.assertEqual(12, len(val3))
    self.assertEqual(expected[6:18], val3)
    prg.GetNextBytes(40)  # skip some bytes
    val4 = prg.GetNextBytes(15)
    self.assertEqual(15, len(val4))
    self.assertEqual(expected[58:73], val4)


class AesCbcTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    self.cipher = ccrypto.AesCbc(_KEY1)

  def testCbcEncryptDecryptSuccessfully(self):
    logging.debug('Running testCbcEncryptDecryptSuccessfully method.')
    # test success for 16, 16, 24, and 32 byte length keys.
    for key in (_KEY1, _KEY2, _KEY1 + '12345678', _KEY1 + _KEY2):
      self.cipher = ccrypto.AesCbc(key)
      ciphertext = self.cipher.Encrypt(_PLAINTEXT1)
      self.assertEqual(_PLAINTEXT1, self.cipher.Decrypt(ciphertext))
    # test success with different plaintexts
    self.cipher = ccrypto.AesCbc(_KEY1)
    for plaintext in ('22', _PLAINTEXT1, _PLAINTEXT2,
                      _PLAINTEXT1 + _PLAINTEXT2):
      ciphertext = self.cipher.Encrypt(plaintext)
      self.assertEqual(plaintext, self.cipher.Decrypt(ciphertext))

  def testCbcEncryptDecryptUtf8Successfully(self):
    logging.debug('Running testCbcEncryptDecryptUtf8Successfully method.')
    # test success with different plaintexts
    for plaintext in (u'22', u'this is test string one',
                      u'this is test string two'):
      ciphertext = self.cipher.Encrypt(plaintext.encode('utf-8'))
      self.assertEqual(plaintext,
                       self.cipher.Decrypt(ciphertext).decode('utf-8'))

  def testCbcEncryptDecryptWithDeterministicIV(self):
    logging.debug('Running testCbcEncryptDecryptDeterministicIV method.')
    # using a deterministic iv of 16 zero bytes.
    ciphertext1 = self.cipher.Encrypt(_PLAINTEXT1, 16 * '\x00')
    self.assertEqual('uQEjLWnHgf0BebLYkk4MG09oUmftxowRjbHrXACCqtI=',
                     base64.b64encode(ciphertext1))
    ciphertext2 = self.cipher.Encrypt(_PLAINTEXT1, '1111111111111111')
    self.assertEqual('mStZags/CYes8OFhgMf6LpTI6LlUsdnrFwZuUis/xZM=',
                     base64.b64encode(ciphertext2))
    ciphertext3 = base64.b64decode(
        'uQEjLWnHgf0BebLYkk4MG09oUmftxowRjbHrXACCqtI=')
    plaintext3 = self.cipher.Decrypt(ciphertext3, 16 * '\x00')
    self.assertEqual(_PLAINTEXT1, plaintext3)

  def testCbcEncryptDecryptFailureWithBadKey(self):
    logging.debug('Running testCbcEncryptDecryptFailureWithBadKey method.')
    # test fail for key empty, None, wrong type, non-16, 24, or 32 length
    for key in ('', None, 22, '22', '12345678901234567890'):
      self.assertRaises(ValueError, ccrypto.AesCbc, key)

  def testCbcEncryptFailureWithBadPlaintext(self):
    logging.debug('Running testCbcEncryptFailureWithBadPlaintext method.')
    # test failue with empty , None, and non-string plaintext
    for plaintext in ('', None, 22):
      self.assertRaises(ValueError, self.cipher.Encrypt, plaintext)

  def testCbcDecryptFailureWithBadCiphertext(self):
    logging.debug('Running testCbcEncryptFailureWithBadCiphertext method.')
    # test failue with empty, None, non-string ciphertext
    for ciphertext in ('', None, 22):
      self.assertRaises(ValueError, self.cipher.Decrypt, ciphertext)

    # change iv in ciphertext, results in an incorrect decryption
    ciphertext = self.cipher.Encrypt(_PLAINTEXT1)
    # add 1 to the first iv byte.
    ciphertext_changed_iv = chr((ord(ciphertext[0]) + 1) % 256) + ciphertext[1:]
    self.assertNotEqual(_PLAINTEXT1, self.cipher.Decrypt(ciphertext_changed_iv))
    # deleting a char in iv, should throw an exception.
    self.assertRaises(ValueError, self.cipher.Decrypt, ciphertext[1:])
    # deleting a char in padding, should throw an exception.
    self.assertRaises(ValueError, self.cipher.Decrypt, ciphertext[:-1])

    # changing a char (e.g. last) in padding, should throw an exception.
    ciphertext_changed_pad = ciphertext[:-1] + chr((ord(ciphertext[-1:]) + 1) %
                                                   256)
    try:
      decrypted_changed_pad = self.cipher.Decrypt(ciphertext_changed_pad)
      # Rarely (around 1 in 256 times), the padding may be accidentally
      # acceptable because AES decryption may result in last byte being x01.
      # But we will check that there are extra bytes beyond the 23rd bytes of
      # _PLAINTEXT1, and fail otherwise.
      extra = decrypted_changed_pad[len(_PLAINTEXT1):]
      if not extra:
        self.fail()
    except ValueError:
      pass  # success


def main(_):
  googletest.main()


if __name__ == '__main__':
  app.run()
