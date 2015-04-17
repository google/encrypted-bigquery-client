#!/usr/bin/env python
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit test for load library module."""




import base64
import hashlib
import json
import os
import tempfile

from google.apputils import app
import gflags as flags
import logging
from google.apputils import basetest as googletest

import common_util as util
import ebq_crypto as ecrypto
import load_lib
import test_util


FLAGS = flags.FLAGS

_TABLE_ID = '1'

_BEFORE_MODIFY_SCHEMA = """[
  {"name": "Year", "type": "Integer"},
  {"name": "fullName", "type": "string", "mode": "nullable"},
  {"name": "month", "type": "string", "encrypt": "PSEUDONYM", "mode":
  "repeated"},
  {"name": "holidays", "type": "Record", "fields":
  [
    {"name": "name", "type": "STRING", "encrypt": "none", "mode": "required"},
    {"name": "religion", "type": "record", "fields":
    [
      {"name": "name", "type": "string"},
      {"name": "countries", "type": "string", "mode": "repeateD"}
    ]}
  ]}
]\n"""

_AFTER_MODIFY_SCHEMA = """[
  {"name": "Year", "type": "integer", "mode": "required", "encrypt": "none"},
  {"name": "fullName", "type": "string", "mode": "nullable", "encrypt": "none"},
  {"name": "month", "type": "string", "encrypt": "pseudonym", "mode":
  "repeated"},
  {"name": "holidays", "type": "record", "mode": "required", "fields":
  [
    {"name": "name", "type": "string", "encrypt": "none", "mode": "required"},
    {"name": "religion", "type": "record", "mode": "required", "fields":
    [
      {"name": "name", "type": "string", "mode": "required", "encrypt": "none"},
      {"name": "countries", "type": "string", "mode": "repeated", "encrypt":
      "none"}
    ]}
  ]}
]\n"""

_CARS_REWRITTEN_SCHEMA = """[
  {
    "type": "integer",
    "name": "Year",
    "mode": "required"
  },
  {
    "type": "string",
    "name": "p698000442118338_PSEUDONYM_Make",
    "mode": "required"
  },
  {
    "type": "string",
    "name": "p698000442118338_SEARCHWORDS_Model",
    "mode": "required"
  },
  {
    "type": "string",
    "name": "p698000442118338_PROBABILISTIC_Model",
    "mode": "required"
  },
  {
    "type": "string",
    "name": "p698000442118338_SEARCHWORDS_Description",
    "mode": "nullable"
  },
  {
    "type": "string",
    "name": "p698000442118338_SEARCHWORDS_Website",
    "mode": "nullable"
  },
  {
    "type": "string",
    "name": "p698000442118338_PROBABILISTIC_Price",
    "mode": "required"
  },
  {
    "type": "string",
    "name": "p698000442118338_HOMOMORPHIC_INT_Invoice_Price",
    "mode": "required"
  },
  {
    "type": "string",
    "name": "p698000442118338_HOMOMORPHIC_FLOAT_Holdback_Percentage",
    "mode": "required"
  }
]\n"""

_JOBS_REWRITTEN_SCHEMA = """[
  {
    "name": "kind",
    "type": "string",
    "mode": "required"
  },
  {
    "name": "p698000442118338_PSEUDONYM_fullName",
    "type": "string",
    "mode": "required"
  },
  {
    "name": "p698000442118338_HOMOMORPHIC_INT_age",
    "type": "string",
    "mode": "required"
  },
  {
    "name": "p698000442118338_SEARCHWORDS_gender",
    "type": "string",
    "mode": "nullable"
  },
  {
    "name": "p698000442118338_PROBABILISTIC_gender",
    "type": "string",
    "mode": "nullable"
  },
  {
    "name": "citiesLived",
    "type": "record",
    "mode": "repeated",
    "fields": [
      {
        "name": "p698000442118338_SEARCHWORDS_place",
        "type": "string",
        "mode": "required"
      },
      {
        "name": "p698000442118338_HOMOMORPHIC_FLOAT_numberOfYears",
        "type": "string",
        "mode": "required"
      },
      {
        "name": "job",
        "type": "record",
        "mode": "repeated",
        "fields": [
          {
            "name": "p698000442118338_PSEUDONYM_position",
            "type": "string",
            "mode": "required"
          },
          {
            "name": "p698000442118338_PROBABILISTIC_yearsPositionHeld",
            "type": "string",
            "mode": "required"
          },
          {
            "name": "p698000442118338_SEARCHWORDS_manager",
            "type": "string",
            "mode": "repeated"
          },
          {
            "name": "jobRank",
            "type": "integer",
            "mode": "nullable"
          }
        ]
      }
    ]
  }
]\n"""

_PLACES_REWRITTEN_SCHEMA = """[
  {
    "name": "kind",
    "type": "string",
    "mode": "required"
  },
  {
    "name": "p698000442118338_PSEUDONYM_fullName",
    "type": "string",
    "mode": "required"
  },
  {
    "name": "p698000442118338_HOMOMORPHIC_INT_age",
    "type": "string",
    "mode": "required"
  },
  {
    "name": "p698000442118338_SEARCHWORDS_gender",
    "type": "string",
    "mode": "nullable"
  },
  {
    "name": "p698000442118338_PROBABILISTIC_gender",
    "type": "string",
    "mode": "nullable"
  },
  {
    "name": "citiesLived",
    "type": "record",
    "mode": "repeated",
    "fields": [
      {
        "name": "p698000442118338_SEARCHWORDS_place",
        "type": "string",
        "mode": "required"
      },
      {
        "name": "p698000442118338_HOMOMORPHIC_FLOAT_numberOfYears",
        "type": "string",
        "mode": "required"
      }
    ]
  },
  {
    "name": "spouse",
    "type": "record",
    "mode": "required",
    "fields": [
      {
        "name": "p698000442118338_PSEUDONYM_spouseName",
        "type": "string",
        "mode": "required"
      },
      {
        "name": "p698000442118338_HOMOMORPHIC_FLOAT_yearsMarried",
        "type": "string",
        "mode": "required"
      },
      {
        "name": "spouseAge",
        "type": "integer",
        "mode": "required"
      }
    ]
  }
]\n"""


_MASTER_KEY = 'yUotEBhjyCDzQEAxgb0/BA=='




class LoadLibraryTest(googletest.TestCase):

  def setUp(self):
    """Run once for each test in the class."""
    os.environ['TMPDIR'] = FLAGS.test_tmpdir
    self.dirname = tempfile.mkdtemp()

  def _WriteTempCarsCsvFile(self):
    output = test_util.GetCarsCsv()
    tmpfile = '%s/temp_cars.csv' % self.dirname
    with open(tmpfile, 'wt') as f:
      f.write(output)
    return tmpfile

  def _WriteTempJobsJsonFile(self):
    output = test_util.GetJobsJson()
    tmpfile = '%s/temp_jobs.json' % self.dirname
    with open(tmpfile, 'wt') as f:
      f.write(output)
    return tmpfile

  def _WriteTempPlacesJsonFile(self):
    output = test_util.GetPlacesJson()
    tmpfile = '%s/temp_places.json' % self.dirname
    with open(tmpfile, 'wt') as f:
      f.write(output)
    return tmpfile

  def testCreateAndStoreMasterKeyFile(self):
    infile = os.path.join(self.dirname, 'created_master_key_file')
    self.assertFalse(os.path.exists(infile))
    load_lib._CreateAndStoreMasterKeyFile(infile)
    self.assertTrue(os.path.exists(infile))
    f = open(infile, 'rt')
    self.assertEquals(16, len(base64.b64decode(f.read())))
    f.close()

  def testReadMasterKeyFile(self):
    infile = os.path.join(self.dirname, 'created_master_key_file')
    self.assertFalse(os.path.exists(infile))
    load_lib._CreateAndStoreMasterKeyFile(infile)
    self.assertTrue(os.path.exists(infile))
    f = open(infile, 'rt')
    master_key = base64.b64decode(f.read())
    self.assertEquals(master_key, load_lib.ReadMasterKeyFile(infile))
    f.close()

  def testModifyFields(self):
    read_schema = json.loads(_BEFORE_MODIFY_SCHEMA)
    load_lib._ModifyFields(read_schema)
    expected_schema = json.loads(_AFTER_MODIFY_SCHEMA)
    self.assertEquals(read_schema, expected_schema)

  def testReadandValidateSchemaFromFile(self):
    infile = os.path.join(self.dirname, 'test_schema_file')
    f = open(infile, 'wt')
    f.write(test_util.GetCarsSchemaString())
    f.close()
    read_schema = load_lib.ReadSchemaFile(infile)
    load_lib._ValidateExtendedSchema(read_schema)
    expected_schema = json.loads(test_util.GetCarsSchemaString())
    self.assertEquals(expected_schema, read_schema)
    # append some non-json text and check failure.
    f = open(infile, 'at')
    f.write('bogus')
    f.close()
    try:
      load_lib.ReadSchemaFile(infile)
      self.fail()
    except ValueError:
      pass  # success

  def testReadandValidateNestedSchemaFromFile(self):
    infile = os.path.join(self.dirname, 'test_nested_schema_file')
    f = open(infile, 'wt')
    f.write(test_util.GetPlacesSchemaString())
    f.close()
    read_schema = load_lib.ReadSchemaFile(infile)
    load_lib._ValidateExtendedSchema(read_schema)
    expected_schema = json.loads(test_util.GetPlacesSchemaString())
    self.assertEquals(expected_schema, read_schema)
    # append some non-json text and check failure.
    f = open(infile, 'at')
    f.write('bogus')
    f.close()
    try:
      load_lib.ReadSchemaFile(infile)
      self.fail()
    except ValueError:
      pass  # success

  def testReadandValidateMultipleNestedSchemaFromFile(self):
    infile = os.path.join(self.dirname, 'test_multiple_nested_schema_file')
    f = open(infile, 'wt')
    f.write(test_util.GetJobsSchemaString())
    f.close()
    read_schema = load_lib.ReadSchemaFile(infile)
    load_lib._ValidateExtendedSchema(read_schema)
    expected_schema = json.loads(test_util.GetJobsSchemaString())
    self.assertEquals(expected_schema, read_schema)
    # append some non-json text and check failure.
    f = open(infile, 'at')
    f.write('bogus')
    f.close()
    try:
      load_lib.ReadSchemaFile(infile)
      self.fail()
    except ValueError:
      pass  # success

  def testRewriteNonNestedSchemaAsJsonFile(self):
    schema = json.loads(test_util.GetCarsSchemaString())
    new_schema = load_lib.RewriteSchema(schema)
    expected_schema = json.loads(_CARS_REWRITTEN_SCHEMA)
    self.assertEquals(expected_schema, new_schema)

  def testRewriteNestedSchemaAsJsonFile(self):
    schema = json.loads(test_util.GetPlacesSchemaString())
    new_schema = load_lib.RewriteSchema(schema)
    expected_schema = json.loads(_PLACES_REWRITTEN_SCHEMA)
    self.assertEquals(expected_schema, new_schema)

  def testRewriteMultipleNestedSchemaAsJsonFile(self):
    schema = json.loads(test_util.GetJobsSchemaString())
    new_schema = load_lib.RewriteSchema(schema)
    expected_schema = json.loads(_JOBS_REWRITTEN_SCHEMA)
    self.assertEquals(expected_schema, new_schema)

  def testValidateCsvDataFile(self):
    schema = json.loads(test_util.GetCarsSchemaString())
    infile = self._WriteTempCarsCsvFile()
    load_lib._ValidateCsvDataFile(schema, infile)

  def testValidateJsonDataFile(self):
    schema = json.loads(test_util.GetPlacesSchemaString())
    infile = self._WriteTempPlacesJsonFile()
    load_lib._ValidateJsonDataFile(schema, infile)

  def testValidateComplexJsonDataFile(self):
    schema = json.loads(test_util.GetJobsSchemaString())
    infile = self._WriteTempJobsJsonFile()
    load_lib._ValidateJsonDataFile(schema, infile)

  def testConvertCsvDataFile(self):
    schema = json.loads(test_util.GetCarsSchemaString())
    infile = self._WriteTempCarsCsvFile()
    outfile = os.path.join(self.dirname, 'cars.enc_data')
    master_key = base64.b64decode(_MASTER_KEY)
    string_hasher = ecrypto.StringHash(
        ecrypto.GenerateStringHashKey(master_key, _TABLE_ID))
    pseudonym_cipher = ecrypto.PseudonymCipher(
        ecrypto.GeneratePseudonymCipherKey(master_key, _TABLE_ID))
    load_lib.ConvertCsvDataFile(schema, master_key, _TABLE_ID, infile, outfile)
    # validate new data file against new rewritten schema.
    new_schema = json.loads(_CARS_REWRITTEN_SCHEMA)
    load_lib._ValidateCsvDataFile(new_schema, outfile)
    # Sanity check one row entries. Entries for semantic encrypted fields cannot
    # be checked because the values are randomized.
    fout = open(outfile, 'rt')
    row0 = fout.readline()
    self.assertTrue('1997' in row0)
    self.assertTrue(pseudonym_cipher.Encrypt(unicode('Ford')) in row0)
    # Get iv and hash for Model searchwords field whose value is 'E350'
    (model_iv, model_hash) = row0.split(',')[2].split(' ')
    # Calculate expected key hash value for 'E350'
    expected_model_key_hash = string_hasher.GetStringKeyHash(
        util.SEARCHWORDS_PREFIX + u'Model', u'E350'.lower())
    # Calculate outer sha1 using model_iv and expected key hash.
    expected_model_hash = base64.b64encode(hashlib.sha1(
        model_iv + expected_model_key_hash).digest()[:8])
    self.assertEquals(expected_model_hash, model_hash)
    fout.close()

  def testConvertJsonDataFile(self):
    schema = json.loads(test_util.GetPlacesSchemaString())
    infile = self._WriteTempPlacesJsonFile()
    outfile = os.path.join(self.dirname, 'places.enc_data')
    master_key = base64.b64decode(_MASTER_KEY)
    string_hasher = ecrypto.StringHash(
        ecrypto.GenerateStringHashKey(master_key, _TABLE_ID))
    load_lib.ConvertJsonDataFile(schema, master_key, _TABLE_ID, infile, outfile)
    # validate new data file against new rewritten schema.
    new_schema = json.loads(_PLACES_REWRITTEN_SCHEMA)
    load_lib._ValidateJsonDataFile(new_schema, outfile)
    fout = open(outfile, 'rt')
    for line in fout:
      data = json.loads(line)
      break
    self.assertEqual(data['kind'], 'person')
    self.assertTrue(util.SEARCHWORDS_PREFIX + u'gender' in data)
    (model_iv, model_hash) = data[util.SEARCHWORDS_PREFIX +
                                  u'gender'].split(' ')
    expected_model_key_hash = string_hasher.GetStringKeyHash(
        util.SEARCHWORDS_PREFIX + u'gender', u'Male'.lower())
    expected_model_hash = base64.b64encode(hashlib.sha1(
        model_iv + expected_model_key_hash).digest()[:8])
    self.assertEquals(expected_model_hash, model_hash)
    self.assertTrue(util.SEARCHWORDS_PREFIX + u'place' in
                    data['citiesLived'][0])
    (model_iv, model_hash) = data[
        'citiesLived'][0][util.SEARCHWORDS_PREFIX + u'place'].split(' ')
    expected_model_key_hash = string_hasher.GetStringKeyHash(
        util.SEARCHWORDS_PREFIX + u'place', u'Seattle'.lower())
    expected_model_hash = base64.b64encode(hashlib.sha1(
        model_iv + expected_model_key_hash).digest()[:8])
    self.assertEquals(expected_model_hash, model_hash)
    self.assertEquals(data['spouse']['spouseAge'], 23)
    fout.close()

  def testConvertComplexJsonDataFile(self):
    schema = json.loads(test_util.GetJobsSchemaString())
    infile = self._WriteTempJobsJsonFile()
    outfile = os.path.join(self.dirname, 'jobs.enc_data')
    master_key = base64.b64decode(_MASTER_KEY)
    string_hasher = ecrypto.StringHash(
        ecrypto.GenerateStringHashKey(master_key, _TABLE_ID))
    load_lib.ConvertJsonDataFile(schema, master_key, _TABLE_ID, infile, outfile)
    # validate new data file against new rewritten schema.
    new_schema = json.loads(_JOBS_REWRITTEN_SCHEMA)
    load_lib._ValidateJsonDataFile(new_schema, outfile)
    fout = open(outfile, 'rt')
    for line in fout:
      data = json.loads(line)
      break
    self.assertEqual(data['kind'], 'person')
    self.assertTrue(util.SEARCHWORDS_PREFIX + u'gender' in data)
    (model_iv, model_hash) = data[util.SEARCHWORDS_PREFIX +
                                  u'gender'].split(' ')
    expected_model_key_hash = string_hasher.GetStringKeyHash(
        util.SEARCHWORDS_PREFIX + u'gender', u'Male'.lower())
    expected_model_hash = base64.b64encode(hashlib.sha1(
        model_iv + expected_model_key_hash).digest()[:8])
    self.assertEquals(expected_model_hash, model_hash)
    self.assertTrue(util.SEARCHWORDS_PREFIX + u'place' in
                    data['citiesLived'][0])
    (model_iv, model_hash) = data[
        'citiesLived'][0][util.SEARCHWORDS_PREFIX + u'place'].split(' ')
    expected_model_key_hash = string_hasher.GetStringKeyHash(
        util.SEARCHWORDS_PREFIX + u'place', u'Seattle'.lower())
    expected_model_hash = base64.b64encode(hashlib.sha1(
        model_iv + expected_model_key_hash).digest()[:8])
    self.assertEquals(expected_model_hash, model_hash)
    self.assertEquals(data['citiesLived'][0]['job'][0]['jobRank'], 1)
    self.assertEquals(data['citiesLived'][1]['job'], [])
    self.assertEquals(
        len(data['citiesLived'][0]['job'][0][util.SEARCHWORDS_PREFIX +
                                             u'manager']), 3)
    self.assertEquals(
        len(data['citiesLived'][0]['job'][0][util.SEARCHWORDS_PREFIX +
                                             u'manager'][0].split(' ')), 4)
    fout.close()


def main(_):
  googletest.main()


if __name__ == '__main__':
  app.run()
