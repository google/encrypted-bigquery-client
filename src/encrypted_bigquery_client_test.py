#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for Encrypted Bigquery Client module."""



from copy import deepcopy
import random

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

  def _EncryptTable(self, cipher, table, column_index):
    rewritten_table = deepcopy(table)
    for i in range(len(table)):
      rewritten_table[i][column_index] = cipher.Encrypt(table[i][column_index])
    return rewritten_table

  def testComputeRows(self):
    # Query is 'SELECT 1 + 1, 1 * 1'
    # Testing no queried values.
    stack = [[1, 1, util.OperatorToken('+', 2)],
             [1, 1, util.OperatorToken('*', 2)]]
    query = {}
    real_result = [['2', '1']]
    result = encrypted_bigquery_client._ComputeRows(stack, query)
    self.assertEqual(result, real_result)
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

  def testDecryptValues(self):
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


def main(_):
  googletest.main()

if __name__ == '__main__':
  app.run()
