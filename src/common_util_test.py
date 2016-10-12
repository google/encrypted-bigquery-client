#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for common util functions."""



import mox
import stubout

from google.apputils import app
import gflags as flags
from google.apputils import basetest as googletest

import bigquery_client
import common_util as util
import test_util

FLAGS = flags.FLAGS


class CommonUtilityTest(googletest.TestCase):

  def testGetEntryFromSchema(self):
    simple_schema = test_util.GetCarsSchema()
    nested_schema = test_util.GetJobsSchema()
    row = util.GetEntryFromSchema('Year', simple_schema)
    self.assertEqual(row['name'], 'Year')
    self.assertEqual(row['encrypt'], 'none')
    row = util.GetEntryFromSchema('citiesLived.place', nested_schema)
    self.assertEqual(row['name'], 'place')
    self.assertEqual(row['encrypt'], 'searchwords')
    row = util.GetEntryFromSchema(
        'citiesLived.job.position', nested_schema)
    self.assertEqual(row['name'], 'position')
    self.assertEqual(row['encrypt'], 'pseudonym')
    row = util.GetEntryFromSchema(
        'citiesLived.job', nested_schema)
    self.assertEqual(row, None)
    row = util.GetEntryFromSchema(
        'citiesLived.non_existent_field', nested_schema)
    self.assertEqual(row, None)

  def testConvertFromTimestamp(self):
    """Test _ConvertFromTimestamp()."""
    t = util.time.time()
    d_utc = util.datetime.datetime.utcfromtimestamp(t)
    d_local = util.datetime.datetime.fromtimestamp(t)
    self.assertEqual(d_utc, util._ConvertFromTimestamp(t))
    self.assertEqual(d_local, util._ConvertFromTimestamp(t, utc=False))



class FieldTokenTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    self.mox = mox.Mox()
    self.stubs = stubout.StubOutForTesting()
    self.magic = 123456

  def tearDown(self):
    self.mox.UnsetStubs()
    self.stubs.UnsetAll()

  # pylint: disable=invalid-name
  def _helperTestPropertyInstance(self, name, cls=util.FieldToken):
    """Test that cls.name is a property."""
    a = getattr(cls, name, None)
    self.assertTrue(isinstance(a, property))

  # pylint: disable=invalid-name
  def _helperTestPropertyGet(self, name, cls=util.FieldToken):
    """Test cls().name == value."""
    f = cls('unrelated')
    self.assertNotEqual(getattr(f, name), self.magic)
    setattr(f, '_%s' % name, self.magic)
    self.assertEqual(getattr(f, name), self.magic)

  # pylint: disable=invalid-name
  def _helperTestPropertySet(self, name, value, cls=util.FieldToken):
    """Test cls().name = value."""
    f = cls('unrelated')
    self.assertNotEqual(value, self.magic)
    setattr(f, '_%s' % name, self.magic)
    setattr(f, name, value)
    self.assertEqual(getattr(f, '_%s' % name), value)

  def testInit(self):
    """Test __init__()."""
    f = util.FieldToken('foo')
    self.assertEqual(f, 'foo')

  def testOriginalName(self):
    """Test property original_name."""
    self._helperTestPropertyInstance('original_name')
    self._helperTestPropertyGet('original_name')
    self._helperTestPropertySet('original_name', 'column_original')

  def testAlias(self):
    """Test property alias."""
    self._helperTestPropertyInstance('alias')
    self._helperTestPropertyGet('alias')
    self._helperTestPropertySet('alias', 'alternative_name')

  def testSetAlias(self):
    """Test property alias SetAlias()."""
    f = util.FieldToken('foo')
    ret = f.SetAlias('bar')
    self.assertEqual(f._alias, 'bar')
    self.assertEqual(ret, f)


def main(_):
  googletest.main()

if __name__ == '__main__':
  app.run()
