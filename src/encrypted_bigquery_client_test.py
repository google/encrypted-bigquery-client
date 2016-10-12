#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for Encrypted Bigquery Client module."""



from copy import deepcopy
import random

import mox
import stubout

from google.apputils import app
import gflags as flags
from google.apputils import basetest as googletest

import bigquery_client
import common_util as util
import ebq_crypto as ecrypto
import encrypted_bigquery_client
import test_util

FLAGS = flags.FLAGS


# TODO(user): Need to add unit tests for _DecryptRows.
class EncryptedBigqueryClientTest(googletest.TestCase):

  def setUp(self):
    self.mox = mox.Mox()
    self.stubs = stubout.StubOutForTesting()

  def tearDown(self):
    self.mox.UnsetStubs()
    self.stubs.UnsetAll()

  def _EncryptTable(self, cipher, table, column_index):
    rewritten_table = deepcopy(table)
    for i in range(len(table)):
      rewritten_table[i][column_index] = cipher.Encrypt(table[i][column_index])
    return rewritten_table

  def testComputeRowsEvaluate1(self):
    # Query is 'SELECT 1 + 1, 1 * 1'
    # Testing no queried values.
    stack = [[1, 1, util.OperatorToken('+', 2)],
             [1, 1, util.OperatorToken('*', 2)]]
    query = {}
    real_result = [['2', '1']]
    result = encrypted_bigquery_client._ComputeRows(stack, query)
    self.assertEqual(result, real_result)

  def testComputeRowsEvaluate2(self):
    # Query is 'SELECT 1 + a, 1 * b, "hello"'
    # There are two rows of values for a and b (shown in query).
    # Result becomes as below:
    # 1 + a | 1 * b | "hello"
    #   2       3     "hello"
    #   4       5     "hello"
    stack = [[1, util.FieldToken('a'), util.OperatorToken('+', 2)],
             [1, util.FieldToken('b'), util.OperatorToken('*', 2)],
             [util.StringLiteralToken('"hello"')]]
    query = {'a': [1, 3], 'b': [3, 5]}
    real_result = [['2', '3', 'hello'], ['4', '5', 'hello']]
    result = encrypted_bigquery_client._ComputeRows(stack, query)
    self.assertEqual(result, real_result)

  def testComputeRowsWithTableNoManifest(self):
    """Test _ComputeRows() with a query that returns simple row values."""
    # Query is
    # SELECT
    # column AS HP11223344,
    # COUNT(column) AS cnt_column
    # FROM table
    # GROUP BY HP11223344
    # HAVING (cnt_column > 1)
    prefix = util.PSEUDONYM_PREFIX
    stack = [
        [util.PseudonymToken('column').SetAlias('HP1122')],
        [util.AggregationQueryToken('COUNT(column)').SetAlias('cnt_column')],
    ]
    # This is just example data. The values have no meaning to the test.
    queried_values = {
        'cnt_column': [
            2L, 6L, 6L, 4L, 2L, 5L, 2L, 7L, 6L, 2L, 3L, 2L, 2L, 4L, 4L, 4L,
            7L, 5L, 4L, 3L, 2L, 5L, 2L, 4L, 3L],
        '%scolumn' % prefix: [
            '"90283714"', '"17007482"', '"40222489"', '"81558712"',
            '"13579536"', '"50946068"', '"82310121"', '"25566093"',
            '"27641327"', '"74384378"', '"58169219"', '"42514380"',
            '"53429045"', '"23220692"', '"28206670"', '"85131464"',
            '"52975787"', '"39194916"', '"96266483"', '"88770661"',
            '"16849538"', '"90717726"', '"49895676"', '"23519752"',
            '"19332978"'],
        'COUNT(%scolumn)' % prefix: [],
        'HP1122': [],
    }
    real_result = []
    for i in xrange(len(queried_values['cnt_column'])):
      a = queried_values['%scolumn' % prefix][i]
      b = queried_values['cnt_column'][i]
      real_result.append([a, str(b)])

    self.mox.ReplayAll()
    result = encrypted_bigquery_client._ComputeRows(
        stack, queried_values, manifest=None)
    self.mox.VerifyAll()

    self.assertEqual(result, real_result)

  def testComputeRowsWithTableAndManifest(self):
    """Test _ComputeRows() with a query that returns simple row values."""
    # Query is
    # SELECT
    # column AS HP11223344,
    # COUNT(column) AS cnt_column
    # FROM table
    # GROUP BY HP11223344
    # HAVING (cnt_column > 1)
    mock_manifest = self.mox.CreateMockAnything()
    prefix = util.PSEUDONYM_PREFIX
    stack = [
        [util.PseudonymToken('column').SetAlias('HP1122')],
        [util.AggregationQueryToken('COUNT(column)').SetAlias('cnt_column')],
    ]
    # This is just example data. The values have no meaning to the test.
    queried_values = {
        'cnt_column': [
            2L, 6L, 6L, 4L, 2L, 5L, 2L, 7L, 6L, 2L, 3L, 2L, 2L, 4L, 4L, 4L,
            7L, 5L, 4L, 3L, 2L, 5L, 2L, 4L, 3L],
        '%scolumn' % prefix: [
            '"90283714"', '"17007482"', '"40222489"', '"81558712"',
            '"13579536"', '"50946068"', '"82310121"', '"25566093"',
            '"27641327"', '"74384378"', '"58169219"', '"42514380"',
            '"53429045"', '"23220692"', '"28206670"', '"85131464"',
            '"52975787"', '"39194916"', '"96266483"', '"88770661"',
            '"16849538"', '"90717726"', '"49895676"', '"23519752"',
            '"19332978"'],
        'COUNT(%scolumn)' % prefix: [],
        'HP1122': [],
    }
    real_result = []
    for i in xrange(len(queried_values['cnt_column'])):
      a = queried_values['%scolumn' % prefix][i]
      b = queried_values['cnt_column'][i]
      real_result.append([a, str(b)])

    num_rows = len(queried_values['cnt_column'])
    mock_manifest.statistics = mock_manifest
    mock_manifest.get(mock_manifest.RECORDS_WRITTEN, 0).AndReturn(num_rows)

    self.mox.ReplayAll()
    result = encrypted_bigquery_client._ComputeRows(
        stack, queried_values, manifest=mock_manifest)
    self.mox.VerifyAll()

    self.assertEqual(result, real_result)

  def testComputeRowsWithTableEmptyResult(self):
    """Test _ComputeRows() with a query that returns empty (zero) rows."""
    # Query is
    # SELECT
    # column AS HP11223344,
    # COUNT(column) AS cnt_column
    # FROM table
    # GROUP BY HP11223344
    # HAVING (cnt_column > 1)
    prefix = util.PSEUDONYM_PREFIX
    stack = [
        [util.PseudonymToken('column').SetAlias('HP1122')],
        [util.AggregationQueryToken('COUNT(column)').SetAlias('cnt_column')],
    ]
    queried_values = {
        'cnt_column': [],
        '%scolumn' % prefix: [],
        'COUNT(%scolumn)' % prefix: [],
        'HP1122': [],
    }
    real_result = []
    result = encrypted_bigquery_client._ComputeRows(stack, queried_values)
    self.assertEqual(result, real_result)

  def testDecryptValues(self):
    """Test _DecryptValues()."""
    cars_schema = test_util.GetCarsSchema()
    jobs_schema = test_util.GetJobsSchema()
    master_key = test_util.GetMasterKey()
    field = '%sInvoice_Price' % util.HOMOMORPHIC_INT_PREFIX
    table = [[1], [2], [3]]
    cipher = ecrypto.HomomorphicIntCipher(master_key)
    ciphers = {util.HOMOMORPHIC_INT_PREFIX: cipher}
    table = self._EncryptTable(cipher, table, 0)
    table.append([None])
    column = encrypted_bigquery_client._DecryptValues(
        field, table, 0, ciphers, cars_schema,
        util.HOMOMORPHIC_INT_PREFIX)
    self.assertEqual(column, [1, 2, 3, util.LiteralToken('null', None)])
    field = 'citiesLived.job.%sposition' % util.PSEUDONYM_PREFIX
    table = [[0, unicode('Hello')], [1, unicode('My')], [-1, unicode('job')]]
    cipher = ecrypto.PseudonymCipher(master_key)
    ciphers = {util.PSEUDONYM_PREFIX: cipher}
    table = self._EncryptTable(cipher, table, 1)
    table.insert(1, [100, None])
    column = encrypted_bigquery_client._DecryptValues(
        field, table, 1, ciphers, jobs_schema,
        util.PSEUDONYM_PREFIX)
    self.assertEqual(column,
                     [util.StringLiteralToken('"Hello"'),
                      util.LiteralToken('null', None),
                      util.StringLiteralToken('"My"'),
                      util.StringLiteralToken('"job"')])
    field = '%snonexistent_field' % util.HOMOMORPHIC_FLOAT_PREFIX
    self.assertRaises(ValueError,
                      encrypted_bigquery_client._DecryptValues,
                      field, table, 1, ciphers, cars_schema,
                      util.HOMOMORPHIC_FLOAT_PREFIX)

  def testGetUnencryptedValues(self):
    table = [[1], [2], [3], [None]]
    column = encrypted_bigquery_client._GetUnencryptedValuesWithType(
        table, 0, 'integer')
    self.assertEqual(column, [1, 2, 3, util.LiteralToken('null', None)])
    table = [[1, 'Hello'], [2, None], [None, 'Bye']]
    column = encrypted_bigquery_client._GetUnencryptedValuesWithType(
        table, 1, 'string')
    self.assertEqual(column,
                     [util.StringLiteralToken('"Hello"'),
                      util.LiteralToken('null', None),
                      util.StringLiteralToken('"Bye"')])
    self.assertRaises(ValueError,
                      encrypted_bigquery_client._GetUnencryptedValuesWithType,
                      table, 1, None)

  def testGetTimestampValues(self):
    int_str = '1396368000'
    float_str = '1396368000.0'
    sn_str = '1.396368E9'
    ts_str = '"2014-04-01 16:00:00 UTC"'
    table = [[int_str], [float_str], [sn_str], [None]]
    column = encrypted_bigquery_client._GetTimestampValues(table, 0)
    self.assertEqual(
        column,
        [ts_str, ts_str, ts_str, util.LiteralToken('null', None)])

  def testDecryptGroupConcatValues(self):
    cars_schema = test_util.GetCarsSchema()
    jobs_schema = test_util.GetJobsSchema()
    master_key = test_util.GetMasterKey()
    query = 'GROUP_CONCAT(%sModel)' % util.PROBABILISTIC_PREFIX
    cipher = ecrypto.ProbabilisticCipher(master_key)
    ciphers = {util.PROBABILISTIC_PREFIX: cipher}
    unencrypted_values = (
        [['A', 'B', 'C', 'D'], ['1', '2', '3', '4'], ['Hello', 'Bye']])
    table = []
    for values in unencrypted_values:
      encrypted_values = []
      for token in values:
        encrypted_values.append(cipher.Encrypt(unicode(token)))
      table.append([','.join(encrypted_values), random.random()])
    table.insert(0, [None, None])
    column = encrypted_bigquery_client._DecryptGroupConcatValues(
        query, table, 0, ciphers, cars_schema, util.PROBABILISTIC_PREFIX)
    self.assertEqual(column,
                     [util.LiteralToken('null', None),
                      util.StringLiteralToken('"A,B,C,D"'),
                      util.StringLiteralToken('"1,2,3,4"'),
                      util.StringLiteralToken('"Hello,Bye"')])
    query = ('GROUP_CONCAT(citiesLived.job.%sposition) within citiesLived.job'
             % util.PSEUDONYM_PREFIX)
    cipher = ecrypto.PseudonymCipher(master_key)
    ciphers = {util.PSEUDONYM_PREFIX: cipher}
    table = []
    for values in unencrypted_values:
      encrypted_values = []
      for token in values:
        encrypted_values.append(cipher.Encrypt(unicode(token)))
      table.append([','.join(encrypted_values)])
    column = encrypted_bigquery_client._DecryptGroupConcatValues(
        query, table, 0, ciphers, jobs_schema, util.PSEUDONYM_PREFIX)
    self.assertEqual(column,
                     [util.StringLiteralToken('"A,B,C,D"'),
                      util.StringLiteralToken('"1,2,3,4"'),
                      util.StringLiteralToken('"Hello,Bye"')])
    query = '%sModel' % util.PROBABILISTIC_PREFIX
    self.assertRaises(ValueError,
                      encrypted_bigquery_client._DecryptGroupConcatValues,
                      query, table, 0, ciphers, cars_schema,
                      util.PROBABILISTIC_PREFIX)
    query = ('GROUP_CONCAT(citiesLived.%snumberOfYears) within citiesLived'
             % util.HOMOMORPHIC_FLOAT_PREFIX)
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      encrypted_bigquery_client._DecryptGroupConcatValues,
                      query, table, 0, ciphers, jobs_schema,
                      util.HOMOMORPHIC_FLOAT_PREFIX)

  def testCreateTable(self):
    """Test CreateTable()."""

    ebc_module = encrypted_bigquery_client
    ebc_cls = ebc_module.EncryptedBigqueryClient

    class SimpleTestEBC(ebc_cls):
      """Class with simpler __init__, rather than lots of mox."""

      def __init__(self, **kwds):
        """Intentionally do not call parent __init__()."""
        self.master_key_filename = 'master_key_filename'

    mock_create_table = self.mox.CreateMockAnything()
    mock_cipher = self.mox.CreateMockAnything()

    in_reference = 'reference'
    in_ignore_existing = False
    in_schema = '/tmp/path/somewhere/dne/schemafile.schema'
    in_description = 'description'
    in_friendly_name = 'fn'
    in_expiration = 1000

    schema = {'rewrite': False, 'read': True}
    ue_ie = u'\u0454'
    ue_ie_utf8 = ue_ie.encode('utf-8')
    assert ue_ie is not ue_ie_utf8
    json_schema = 'json schema'
    zlib_schema = 'zlib schema'
    master_key = 'master_key'
    hashed_key = 'GR0xdEWYxzkd1FA+J50A3bWMq4c='  # b64(sha1(master_key))
    encrypted_schema = 'EEE'
    encrypted_schema_b64 = 'RUVF'  # b64(encrypted_schema)
    new_schema = schema.copy()
    new_schema.update(rewrite=True)

    new_description = 'new_description'

    ebc = SimpleTestEBC(test='1')
    self.mox.StubOutWithMock(ebc, '_CheckKeyfileFlag')
    self.mox.StubOutWithMock(ebc_module.load_lib, 'ReadSchemaFile')
    self.mox.StubOutWithMock(ebc_module.load_lib, 'ReadMasterKeyFile')
    self.mox.StubOutWithMock(ebc_module.ecrypto, 'ProbabilisticCipher')
    self.mox.StubOutWithMock(ebc_module.json, 'dumps')
    self.mox.StubOutWithMock(ebc_module.zlib, 'compress')
    self.mox.StubOutWithMock(ebc_module.util, 'ConstructTableDescription')
    self.mox.StubOutWithMock(ebc_module.load_lib, 'RewriteSchema')
    self.stubs.Set(
        ebc_module.bigquery_client.BigqueryClient, 'CreateTable',
        mock_create_table)

    ebc._CheckKeyfileFlag()
    ebc_module.load_lib.ReadSchemaFile(in_schema).AndReturn(schema)
    ebc_module.load_lib.ReadMasterKeyFile(
        'master_key_filename', True).AndReturn(master_key)
    ebc_module.ecrypto.ProbabilisticCipher(master_key).AndReturn(mock_cipher)
    # note ue_ie vs ue_ie_utf8 for encode(utf-8) test
    ebc_module.json.dumps(schema).AndReturn(json_schema + ue_ie)
    ebc_module.zlib.compress(
        json_schema + ue_ie_utf8).AndReturn(zlib_schema)
    mock_cipher.Encrypt(zlib_schema).AndReturn(encrypted_schema)
    ebc_module.util.ConstructTableDescription(
        in_description, hashed_key, ebc_module.util.EBQ_TABLE_VERSION,
        encrypted_schema_b64).AndReturn(new_description)
    ebc_module.load_lib.RewriteSchema(schema).AndReturn(new_schema)
    # and here comes the super() call...
    mock_create_table(
        in_reference, in_ignore_existing, new_schema, new_description,
        in_friendly_name, in_expiration).AndReturn(None)

    self.mox.ReplayAll()
    # CreateTable() takes a schema filename on disk for in_schema, but
    # the superclass method takes a schema structure e.g. a dict.
    ebc.CreateTable(
        in_reference, ignore_existing=in_ignore_existing, schema=in_schema,
        description=in_description, friendly_name=in_friendly_name,
        expiration=in_expiration)
    self.mox.VerifyAll()


class EncryptedTablePrinterTest(googletest.TestCase):
  """Test the EncryptedTablePrinter class."""

  def setUp(self):
    self.mox = mox.Mox()
    self.stubs = stubout.StubOutForTesting()

  def tearDown(self):
    self.mox.UnsetStubs()
    self.stubs.UnsetAll()

  def testInitWithManifest(self):
    """Test __init__() with manifest kwarg."""
    manifest = 'zmanifestz'
    try:
      _ = encrypted_bigquery_client.EncryptedTablePrinter(manifest=manifest)
      self.fail('EncryptedTablePrinter should raise ValueError')
    except ValueError, e:
      self.assertEqual(e.args[0], 'Cannot print table without master_key.')
    except:  # pylint: disable=bare-except
      self.fail('EncryptedTablePrinter should raise specific ValueError')


def main(_):
  googletest.main()

if __name__ == '__main__':
  app.run()
