#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for query library module."""




import mox
import stubout
from google.apputils import app
import gflags as flags
import logging
from google.apputils import basetest as googletest

import bigquery_client
import common_util as util
import query_lib
import query_parser as parser
import test_util

FLAGS = flags.FLAGS

_TABLE_ID = '1'
_TEST_NSQUARE = '0'




class QueryLibraryTest(googletest.TestCase):

  def testExtractEncryptedQueries(self):
    # Original query is 'SELECT (111 + a_1) * a, TRUE OR False, null, PI(),
    # FUNC_PI, a1'
    # Query sent to server becomes 'SELECT a, a_1, FUNC_PI, a1'
    stacks = [[util.FieldToken('a'), 111, util.FieldToken('a_1'),
               util.OperatorToken('+', 2), util.OperatorToken('*', 2)],
              ['TRUE', 'False', util.OperatorToken('OR', 2)],
              ['null'], [util.BuiltInFunctionToken('PI')],
              [util.FieldToken('FUNC_PI')], [util.FieldToken('a1')],
              [util.FieldToken('a.b')],
              [util.UnencryptedQueryToken('%s0_'
                                          % util.UNENCRYPTED_ALIAS_PREFIX)]]
    query_list = query_lib._ExtractFieldQueries(stacks, strize=True)
    expect_query_list = set(
        ['a', 'a_1', 'FUNC_PI', 'a1',
         'a.b AS a' + util.PERIOD_REPLACEMENT + 'b'])
    self.assertEqual(expect_query_list, query_list)

  def testRewriteEncryptedFields(self):
    queries = [util.FieldToken('Year'), util.FieldToken('Model'),
               util.FieldToken('Make'), util.FieldToken('Invoice_Price'),
               util.FieldToken('Price'), util.FieldToken('Website'),
               util.FieldToken('Description')]
    rewritten_queries = [util.FieldToken('Year'),
                         util.ProbabilisticToken('Model'),
                         util.PseudonymToken('Make'),
                         util.HomomorphicIntToken('Invoice_Price'),
                         util.ProbabilisticToken('Price'),
                         util.SearchwordsToken('Website'),
                         util.SearchwordsToken('Description')]
    test_schema = test_util.GetCarsSchema()
    new_queries = query_lib._RewriteEncryptedFields([queries], test_schema)
    self.assertEqual(new_queries, [rewritten_queries])

  def testRewriteAggregations(self):
    stack = [util.CountStarToken(),
             util.AggregationFunctionToken('COUNT', 1)]
    rewritten_stack = ['COUNT(*)']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.ProbabilisticToken('Price'),
             util.AggregationFunctionToken('COUNT', 1)]
    rewritten_stack = ['COUNT(' + util.PROBABILISTIC_PREFIX + 'Price)']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.ProbabilisticToken('Price'), 4,
             util.AggregationFunctionToken('COUNT', 2)]
    rewritten_stack = ['COUNT(' + util.PROBABILISTIC_PREFIX + 'Price, 4)']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.FieldToken('Year'), 5,
             util.AggregationFunctionToken('DISTINCTCOUNT', 2),
             util.FieldToken('Year'), util.AggregationFunctionToken('COUNT', 1),
             util.OperatorToken('+', 2)]
    rewritten_stack = ['COUNT(DISTINCT Year, 5)', 'COUNT(Year)', '+']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [0, util.BuiltInFunctionToken('cos'),
             util.AggregationFunctionToken('COUNT', 1)]
    rewritten_stack = ['COUNT(1.0)']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.StringLiteralToken('"Hello"'), 2,
             util.BuiltInFunctionToken('left'), util.StringLiteralToken('"y"'),
             util.BuiltInFunctionToken('concat'),
             util.AggregationFunctionToken('GROUP_CONCAT', 1)]
    rewritten_stack = ['GROUP_CONCAT("Hey")']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.FieldToken('Year'), util.FieldToken('Year'),
             util.OperatorToken('*', 2),
             util.AggregationFunctionToken('SUM', 1)]
    rewritten_stack = ['SUM((Year * Year))']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.HomomorphicIntToken('Invoice_Price'),
             util.AggregationFunctionToken('SUM', 1)]
    rewritten_stack = [
        0.0, 'COUNT(' + util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price)',
        '*', 1.0, 'TO_BASE64(BYTES(PAILLIER_SUM(FROM_BASE64(' +
        util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price), \'0\')))', '*', '+']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.HomomorphicFloatToken('Holdback_Percentage'),
             util.AggregationFunctionToken('AVG', 1)]
    rewritten_stack = [
        0.0, 'COUNT(' + util.HOMOMORPHIC_FLOAT_PREFIX +
        'Holdback_Percentage)', '*', 1.0,
        'TO_BASE64(BYTES(PAILLIER_SUM(FROM_BASE64(' +
        util.HOMOMORPHIC_FLOAT_PREFIX + 'Holdback_Percentage), \'0\')))',
        '*', '+', 'COUNT(' + util.HOMOMORPHIC_FLOAT_PREFIX +
        'Holdback_Percentage)', '/']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.HomomorphicIntToken('Invoice_Price'), 2,
             util.OperatorToken('+', 2), 5,
             util.OperatorToken('*', 2),
             util.AggregationFunctionToken('SUM', 1)]
    rewritten_stack = [
        0.0, 'COUNT(' + util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price)',
        '*', 5.0, 'TO_BASE64(BYTES(PAILLIER_SUM(FROM_BASE64(' +
        util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price), \'0\')))', '*', '+',
        0.0, 'COUNT(' + util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price)',
        '*', 1.0, 'SUM((2 * 5))', '*', '+', '+']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.PseudonymToken('Make'), 2,
             util.AggregationFunctionToken('DISTINCTCOUNT', 2)]
    rewritten_stack = ['COUNT(DISTINCT ' + util.PSEUDONYM_PREFIX +
                       'Make, 2)']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.FieldToken('Year'), util.AggregationFunctionToken('TOP', 1)]
    rewritten_stack = ['TOP(Year)']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.PseudonymToken('Make'), 5, 1,
             util.AggregationFunctionToken('TOP', 3)]
    rewritten_stack = ['TOP(' + util.PSEUDONYM_PREFIX + 'Make, 5, 1)']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.FieldToken('Year'), util.BuiltInFunctionToken('cos'),
             util.HomomorphicIntToken('Invoice_Price'),
             util.OperatorToken('+', 2),
             util.AggregationFunctionToken('SUM', 1)]
    rewritten_stack = [
        0.0, 'COUNT(' + util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price)',
        '*', 1.0, 'SUM(cos(Year))', '*', '+', 0.0,
        'COUNT(' + util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price)', '*',
        1.0, 'TO_BASE64(BYTES(PAILLIER_SUM(FROM_BASE64(' +
        util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price),'
        ' \'0\')))', '*', '+', '+']
    self.assertEqual(query_lib._RewriteAggregations(
        [stack], _TEST_NSQUARE), [rewritten_stack])
    stack = [util.ProbabilisticToken('Model'),
             util.AggregationFunctionToken('DISTINCTCOUNT', 1)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        query_lib._RewriteAggregations, [stack], _TEST_NSQUARE)
    stack = [util.ProbabilisticToken('Price'),
             util.AggregationFunctionToken('SUM', 1)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        query_lib._RewriteAggregations, [stack], _TEST_NSQUARE)
    stack = [util.HomomorphicIntToken('Invoice_Price'),
             util.HomomorphicFloatToken('Holdback_Percentage'),
             util.OperatorToken('*', 2),
             util.AggregationFunctionToken('SUM', 1)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        query_lib._RewriteAggregations, [stack], _TEST_NSQUARE)
    stack = [util.HomomorphicFloatToken('Holdback_Percentage'),
             util.BuiltInFunctionToken('cos'),
             util.AggregationFunctionToken('SUM', 1)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        query_lib._RewriteAggregations, [stack], _TEST_NSQUARE)
    stack = [util.HomomorphicIntToken('Invoice_Price'),
             util.AggregationFunctionToken('TOP', 1)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        query_lib._RewriteAggregations, [stack], _TEST_NSQUARE)
    stack = [util.FieldToken('Year'),
             util.AggregationFunctionToken('SUM', 1),
             util.AggregationFunctionToken('SUM', 1)]
    rewritten_stack = ['SUM(SUM(Year))']
    self.assertEqual(query_lib._RewriteAggregations([stack], _TEST_NSQUARE),
                     [rewritten_stack])
    stack = [util.HomomorphicIntToken('Invoice_Price'),
             util.AggregationFunctionToken('SUM', 1),
             util.AggregationFunctionToken('SUM', 1)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        query_lib._RewriteAggregations, [stack], _TEST_NSQUARE)
    stack = [util.FieldToken('Year'),
             util.AggregationFunctionToken('GROUP_CONCAT', 1),
             util.AggregationFunctionToken('GROUP_CONCAT', 1)]
    rewritten_stack = ['GROUP_CONCAT(GROUP_CONCAT(Year))']
    self.assertEqual(query_lib._RewriteAggregations([stack], _TEST_NSQUARE),
                     [rewritten_stack])
    stack = [util.PseudonymToken('Make'),
             util.AggregationFunctionToken('GROUP_CONCAT', 1),
             util.AggregationFunctionToken('GROUP_CONCAT', 1)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        query_lib._RewriteAggregations, [stack], _TEST_NSQUARE)

  def testReplaceAliasWhenNested(self):
    # Query is 'SELECT a + b as a, a + b as b'
    stacks = [[util.FieldToken('a'), util.FieldToken('b'),
               util.OperatorToken('+', 2)],
              [util.FieldToken('a'), util.FieldToken('b'),
               util.OperatorToken('+', 2)]]
    alias = {0: 'a', 1: 'b'}
    new_stack = query_lib._ReplaceAlias(stacks, alias)
    real_stack = [['a', 'b', '+'], ['a', 'b', '+', 'b', '+']]
    self.assertEqual(new_stack, real_stack)

  def testAsConstructColumnNames(self):
    alias = {0: 'a'}
    columns = [[util.FieldToken('b')], [1, 2, util.OperatorToken('+', 2)]]
    as_clause = query_lib._AsClause(alias)
    self.assertEqual(as_clause.ConstructColumnNames(columns),
                     [{'name': 'a'}, {'name': '(1 + 2)'}])

  def testWhereRewrite(self):
    schema = test_util.GetCarsSchema()
    master_key = test_util.GetMasterKey()
    as_clause = query_lib._AsClause({})
    stack = [util.FieldToken('Make'), util.StringLiteralToken('"Hello"'),
             util.OperatorToken('==', 2)]
    where_clause = query_lib._WhereClause(stack, as_clause=as_clause,
                                          schema=schema, nsquare=_TEST_NSQUARE,
                                          master_key=master_key,
                                          table_id=_TABLE_ID)
    self.assertEqual(where_clause.Rewrite(),
                     'WHERE (%sMake == "HS57DHbh2KlkqNJREmu1wQ==")'
                     % util.PSEUDONYM_PREFIX)
    stack = [util.FieldToken('Model'), util.StringLiteralToken('"A"'),
             util.OperatorToken('contains', 2)]
    where_clause = query_lib._WhereClause(stack, as_clause=as_clause,
                                          schema=schema, nsquare=_TEST_NSQUARE,
                                          master_key=master_key,
                                          table_id=_TABLE_ID)
    self.assertEqual(where_clause.Rewrite(),
                     'WHERE (%sModel contains to_base64(left(bytes(sha1(concat(left('
                     '%sModel, 24), \'yB9HY2qv+DI=\'))), 8)))'
                     % (util.SEARCHWORDS_PREFIX,
                        util.SEARCHWORDS_PREFIX))
    stack = []
    where_clause = query_lib._WhereClause(stack, as_clause=as_clause,
                                          schema=schema, nsquare=_TEST_NSQUARE,
                                          master_key=master_key,
                                          table_id=_TABLE_ID)
    self.assertEqual(where_clause.Rewrite(), '')

  def testWhereRewriteWithRelated(self):
    """Test WHERE when pseudonym value exists in two different tables."""
    schema = test_util.GetCarsSchema()

    # add 'related' field just for this test
    for field in schema:
      if field['name'] == 'Make':
        field['related'] = 'cars_name'
        break

    # this value determined by running the test, not by manual calc
    ciphertext = 'sspWKAH/NKuUyX8ji1mmSw=='

    # test 1, use table_id
    table_id = _TABLE_ID
    master_key = test_util.GetMasterKey()
    as_clause = query_lib._AsClause({})
    stack = [util.FieldToken('Make'), util.StringLiteralToken('"Hello"'),
             util.OperatorToken('==', 2)]
    where_clause_1 = query_lib._WhereClause(
        stack, as_clause=as_clause, schema=schema, nsquare=_TEST_NSQUARE,
        master_key=master_key, table_id=table_id)
    rewritten_sql_1 = where_clause_1.Rewrite()
    self.assertEqual(rewritten_sql_1, 'WHERE (%sMake == "%s")' % (
        util.PSEUDONYM_PREFIX, ciphertext))

    # test 2, change table_id, query should be same as test #1
    table_id = _TABLE_ID + '_other'
    master_key = test_util.GetMasterKey()
    as_clause = query_lib._AsClause({})
    stack = [util.FieldToken('Make'), util.StringLiteralToken('"Hello"'),
             util.OperatorToken('==', 2)]
    where_clause_2 = query_lib._WhereClause(
        stack, as_clause=as_clause, schema=schema, nsquare=_TEST_NSQUARE,
        master_key=master_key, table_id=table_id)
    rewritten_sql_2 = where_clause_2.Rewrite()
    self.assertEqual(rewritten_sql_2, 'WHERE (%sMake == "%s")' % (
        util.PSEUDONYM_PREFIX, ciphertext))

    # verify different tables were used
    self.assertNotEqual(where_clause_1.table_id, where_clause_2.table_id)
    # and verify that same WHERE query="literal" was generated
    self.assertEqual(rewritten_sql_1, rewritten_sql_2)

  def testHavingRewrite(self):
    schema = test_util.GetCarsSchema()
    master_key = test_util.GetMasterKey()
    as_clause = query_lib._AsClause({})
    stack = [util.FieldToken('SUM(Year)'), 1, util.OperatorToken('<', 2)]
    having_clause = query_lib._HavingClause(stack, as_clause=as_clause,
                                            schema=schema,
                                            nsquare=_TEST_NSQUARE,
                                            master_key=master_key,
                                            table_id=_TABLE_ID)
    self.assertEqual(having_clause.Rewrite(), 'HAVING (SUM(Year) < 1)')
    stack = [1000,
             util.AggregationQueryToken(
                 'TO_BASE64(BYTES(PAILLIER_SUM(FROM_BASE64(' +
                 util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price), \'0\')))'),
             util.OperatorToken('==', 2)]
    having_clause = query_lib._HavingClause(stack, as_clause=as_clause,
                                            schema=schema,
                                            nsquare=_TEST_NSQUARE,
                                            master_key=master_key,
                                            table_id=_TABLE_ID)
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError, having_clause.Rewrite)
    stack = [util.FieldToken('GROUP_CONCAT(' + util.PSEUDONYM_PREFIX +
                             'Model)'),
             util.BuiltInFunctionToken('len'), 5, util.OperatorToken('>', 2)]
    having_clause = query_lib._HavingClause(stack, as_clause=as_clause,
                                            schema=schema,
                                            nsquare=_TEST_NSQUARE,
                                            master_key=master_key,
                                            table_id=_TABLE_ID)
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError, having_clause.Rewrite)
    stack = []
    having_clause = query_lib._HavingClause(stack, as_clause=as_clause,
                                            schema=schema,
                                            nsquare=_TEST_NSQUARE,
                                            master_key=master_key,
                                            table_id=_TABLE_ID)
    self.assertEqual(having_clause.Rewrite(), '')

  def testGroupByRewrite(self):
    test_schema = test_util.GetCarsSchema()
    as_clause = query_lib._AsClause({})
    within_clause = query_lib._WithinClause({})
    select_clause = query_lib._SelectClause([['1']], as_clause=as_clause,
                                            within_clause=within_clause,
                                            schema=test_schema,
                                            nsquare=_TEST_NSQUARE)
    select_clause.Rewrite()
    fields = [util.FieldToken('Price')]
    clause = query_lib._GroupByClause(fields, schema=test_schema,
                                      nsquare=_TEST_NSQUARE,
                                      select_clause=select_clause)
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      clause.Rewrite)
    fields = [util.FieldToken('Invoice_Price'), util.FieldToken('Make')]
    clause = query_lib._GroupByClause(fields, schema=test_schema,
                                      nsquare=_TEST_NSQUARE,
                                      select_clause=select_clause)
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      clause.Rewrite)
    fields = [util.FieldToken('Make')]
    clause = query_lib._GroupByClause(fields, schema=test_schema,
                                      nsquare=_TEST_NSQUARE,
                                      select_clause=select_clause)
    self.assertEqual(clause.Rewrite(),
                     'GROUP BY %sMake' % util.PSEUDONYM_PREFIX)
    fields = [util.FieldToken('Year')]
    clause = query_lib._GroupByClause(fields, schema=test_schema,
                                      nsquare=_TEST_NSQUARE,
                                      select_clause=select_clause)
    self.assertEqual(clause.Rewrite(), 'GROUP BY Year')
    fields = []
    clause = query_lib._GroupByClause(fields, schema=test_schema,
                                      nsquare=_TEST_NSQUARE,
                                      select_clause=select_clause)
    self.assertEqual(clause.Rewrite(), '')

  def testOrderBySortTable(self):
    column_names = []
    order_list = ['hello']
    clause = query_lib._OrderByClause(order_list)
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      clause.SortTable, column_names, None)
    column_names = [{'name': 'a'}, {'name': 'b'}, {'name': 'c'}]
    order_list = ['c asc', 'b desc', 'a']
    table_values = [[1, 2, 'hey'], [1, 3, 'hey'], [2, 3, 'hello']]
    real_table = [[2, 3, 'hello'], [1, 3, 'hey'], [1, 2, 'hey']]
    clause = query_lib._OrderByClause(order_list)
    table = clause.SortTable(column_names, table_values)
    self.assertEqual(table, real_table)

  def testLimitRewrite(self):
    clause = query_lib._LimitClause([5])
    self.assertEqual(clause.Rewrite(), 'LIMIT 5')
    clause = query_lib._LimitClause([])
    self.assertEqual(clause.Rewrite(), '')

  def testExtractUnencryptedQueries(self):
    stacks = [[util.FieldToken('Year')], [1],
              [util.FieldToken('Year'), 1, util.OperatorToken('+', 2)],
              [util.ProbabilisticToken('Price')],
              [util.FieldToken('GROUP_CONCAT(%sModel)'
                               % util.PSEUDONYM_PREFIX)],
              [util.FieldToken('SUM(Year + 1)')]]
    unencrypted_expression_list = [
        'Year AS ' + util.UNENCRYPTED_ALIAS_PREFIX + '0_',
        '1 AS ' + util.UNENCRYPTED_ALIAS_PREFIX + '1_',
        '(Year + 1) AS ' + util.UNENCRYPTED_ALIAS_PREFIX + '2_',
        'SUM(Year + 1) AS ' + util.UNENCRYPTED_ALIAS_PREFIX + '3_']
    self.assertEqual(query_lib._ExtractUnencryptedQueries(stacks, {}),
                     unencrypted_expression_list)
    stacks = [[util.FieldToken('Year')], [1],
              [util.FieldToken('Year'), 1, util.OperatorToken('+', 2)],
              [util.ProbabilisticToken('Price')],
              [util.FieldToken('GROUP_CONCAT(%sModel)'
                               % util.PSEUDONYM_PREFIX)],
              [util.FieldToken('SUM(Year + 1)')]]
    within = {4: 'w1', 5: 'w2'}
    unencrypted_expression_list = [
        'Year AS ' + util.UNENCRYPTED_ALIAS_PREFIX + '0_',
        '1 AS ' + util.UNENCRYPTED_ALIAS_PREFIX + '1_',
        '(Year + 1) AS ' + util.UNENCRYPTED_ALIAS_PREFIX + '2_',
        'SUM(Year + 1) WITHIN w2 AS ' + util.UNENCRYPTED_ALIAS_PREFIX +
        '3_']
    self.assertEqual(query_lib._ExtractUnencryptedQueries(stacks, within),
                     unencrypted_expression_list)

  def testRewriteQueryWhenGroupBy(self):
    master_key = test_util.GetMasterKey()
    schema = test_util.GetCarsSchema()
    query = (
        'SELECT Year from test_dataset.cars WHERE Year > 1990 GROUP BY Year '
        'ORDER BY Year LIMIT 2')
    clauses = parser.ParseQuery(query)
    rewritten_query = (
        'SELECT Year AS %s0_ FROM test_dataset.cars WHERE (Year > 1990) '
        'GROUP BY %s0_ LIMIT 2' % (
            util.UNENCRYPTED_ALIAS_PREFIX, util.UNENCRYPTED_ALIAS_PREFIX))
    self.assertEqual(
        query_lib.RewriteQuery(clauses, schema, master_key, _TABLE_ID)[0],
        rewritten_query)

  def testRewriteQueryWhenSumYear(self):
    master_key = test_util.GetMasterKey()
    schema = test_util.GetCarsSchema()
    query = 'SELECT SUM(Year) from test_dataset.cars having SUM(Year) > 7000'
    clauses = parser.ParseQuery(query)
    rewritten_query = (
        'SELECT SUM(Year) AS %s0_ FROM test_dataset.cars '
        'HAVING (SUM(Year) > 7000)' % util.UNENCRYPTED_ALIAS_PREFIX)
    self.assertEqual(
        query_lib.RewriteQuery(clauses, schema, master_key, _TABLE_ID)[0],
        rewritten_query)

  def testRewriteQueryWhenLocalEvaluate(self):
    master_key = test_util.GetMasterKey()
    schema = test_util.GetCarsSchema()
    query = 'SELECT Price + 1 from test_dataset.cars'
    clauses = parser.ParseQuery(query)
    expect_rewritten_query = (
        'SELECT %sPrice FROM test_dataset.cars' % util.PROBABILISTIC_PREFIX)
    self.assertEqual(
        expect_rewritten_query,
        query_lib.RewriteQuery(clauses, schema, master_key, _TABLE_ID)[0])

  def testRewriteQueryWhenMakeAlias(self):
    master_key = test_util.GetMasterKey()
    schema = test_util.GetCarsSchema()
    query = 'SELECT Make AS alias_make FROM test_dataset.cars'
    expect_rewritten_query = (
        'SELECT %sMake AS alias_make FROM test_dataset.cars' % (
            util.PSEUDONYM_PREFIX))
    clauses = parser.ParseQuery(query)
    self.assertEqual(clauses['AS'].get(0, None), 'alias_make')
    self.assertEqual(
        expect_rewritten_query,
        query_lib.RewriteQuery(clauses, schema, master_key, _TABLE_ID)[0])

  def testRewriteQueryWhenCountMakeAlias(self):
    master_key = test_util.GetMasterKey()
    schema = test_util.GetCarsSchema()
    query = 'SELECT COUNT(Make) AS cnt_make FROM test_dataset.cars'
    rewritten_query = (
        'SELECT COUNT(%sMake) AS cnt_make FROM test_dataset.cars' % (
            util.PSEUDONYM_PREFIX))
    clauses = parser.ParseQuery(query)
    self.assertEqual(clauses['AS'], {0: 'cnt_make'})
    self.assertEqual(
        query_lib.RewriteQuery(clauses, schema, master_key, _TABLE_ID)[0],
        rewritten_query)

  def testRewriteQueryWhen(self):
    master_key = test_util.GetMasterKey()
    schema = test_util.GetCarsSchema()
    query = ('SELECT SUM(Invoice_Price), GROUP_CONCAT(Make) '
             'FROM test_dataset.cars')
    clauses = parser.ParseQuery(query)
    rewritten_query = (
        'SELECT COUNT(%sInvoice_Price), %s%sInvoice_Price), '
        '\'\\x44\\x08\\xb5\\xaa\\xcc\\x3f\\xf6\\xb3\\x36\\xe4'
        '\\xb2\\xec\\xc7\\x75\\x1f\\xb2\\xdb\\xf3\\x3a\\x54\\xa1'
        '\\x86\\xf3\\x66\\xcc\\xcb\\x49\\xc1\\x41\\xd2\\x05\\xe2'
        '\\x8a\\x07\\xf2\\xe8\\x00\\x09\\x2e\\x6e\\x41\\x32\\x6c'
        '\\xe8\\xa9\\x07\\x62\\x5c\\x94\\x7d\\x00\\x0e\\x5d\\x8d'
        '\\xd0\\x1e\\x44\\x6d\\xe6\\x6a\\x2d\\x38\\x5e\\x53\\xfd'
        '\\xbc\\x47\\x6a\\xdc\\xd7\\x35\\x09\\xa3\\x1d\\xdf\\x98'
        '\\x17\\x6d\\x65\\xa1\\x7f\\xdd\\x6c\\x0e\\x26\\x06\\xc9'
        '\\x6f\\x87\\x4d\\x0e\\x60\\x90\\x8f\\xe5\\x39\\xf6\\xfc'
        '\\xd7\\x5a\\xea\\xd9\\x6d\\x44\\x51\\x23\\xee\\xaa\\xff'
        '\\xd3\\xa5\\xae\\xa7\\x66\\xfd\\x5e\\xa9\\x16\\x4e\\x60'
        '\\x86\\x83\\x44\\x83\\xb5\\x8c\\xdb\\x7f\\x06\\x8d\\x44'
        '\\x10\\xa2\\x47\\x99\\x35\\xf0\\xe5\\x7d\\x1d\\x19\\x91'
        '\\xc2\\x13\\x9e\\x18\\xdf\\x60\\xb1\\xca\\xf0\\xe9\\xe0'
        '\\x9e\\xaa\\xb2\\x92\\x9f\\xac\\xfb\\x3a\\x18\\xc8\\xf4'
        '\\xfe\\xb9\\x98\\xee\\x8a\\xcb\\x84\\x8e\\xc1\\x54\\xf2'
        '\\x55\\x71\\xdc\\x0b\\xd2\\x86\\x4c\\xbc\\xc3\\x47\\x96'
        '\\x1d\\x83\\xac\\x10\\x36\\x2c\\x81\\xd3\\x39\\x1e\\x64'
        '\\x51\\xe2\\xd7\\x35\\x1b\\x54\\xb8\\xbe\\x2b\\x42\\xea'
        '\\x51\\x58\\x1a\\x36\\xbe\\x45\\xe2\\xd1\\xd0\\x15\\x8f'
        '\\xa4\\xa7\\xb4\\x34\\x19\\xa1\\x4d\\xd0\\x14\\x77\\x9d'
        '\\xd8\\xab\\xc7\\xda\\x6f\\x15\\xae\\x42\\x12\\xfd\\x5c'
        '\\x4d\\x6a\\x41\\xfb\\x06\\x6a\\x1c\\xf4\\x54\\x59\\xfe'
        '\\xb1\\xc3\\xec\\x11\'))), GROUP_CONCAT(%sMake) '
        'FROM test_dataset.cars' % (
            util.HOMOMORPHIC_INT_PREFIX,
            util.PAILLIER_SUM_PREFIX,
            util.HOMOMORPHIC_INT_PREFIX,
            util.PSEUDONYM_PREFIX))
    self.assertEqual(
        query_lib.RewriteQuery(clauses, schema, master_key, _TABLE_ID)[0],
        rewritten_query)

  def testRewriteJoin(self):
    master_key = test_util.GetMasterKey()
    schema = test_util.GetCarsSchema()
    query = (
        'SELECT Year '
        'FROM test_dataset.cars '
        'JOIN avg_yearly_car_costs ON '
        'avg_yearly_car_costs.year = test_dataset.cars.Year '
        'JOIN reliability_data ON '
        'reliability_data.make = test_dataset.cars.Make'
        )
    clauses = parser.ParseQuery(query)
    rewritten_query = (
        'SELECT Year AS p698000442118338_ue0_ '
        'FROM test_dataset.cars '
        'JOIN avg_yearly_car_costs ON '
        '(avg_yearly_car_costs.year = test_dataset.cars.Year) '
        'JOIN reliability_data ON '
        '(reliability_data.make = test_dataset.cars.Make)'
        )
    self.assertEqual(
        query_lib.RewriteQuery(clauses, schema, master_key, _TABLE_ID)[0],
        rewritten_query)


class QueryManifestTest(googletest.TestCase):
  """Test the QueryManifest class."""

  def setUp(self):
    """Run once for each test in the class."""
    self.mox = mox.Mox()
    self.stubs = stubout.StubOutForTesting()

  def tearDown(self):
    self.mox.UnsetStubs()
    self.stubs.UnsetAll()

  def testGenerate(self):
    """Test QueryManifest.Generate()."""
    m = query_lib.QueryManifest.Generate()
    self.assertTrue(isinstance(getattr(m, 'manifest', None), dict))

  def testSetRawColumnAlias(self):
    """Test QueryManifest._SetRawColumnAlias()."""
    m = query_lib.QueryManifest.Generate()
    self.assertEqual(m.manifest['columns'], {})
    self.assertEqual(m.manifest['column_aliases'], {})
    m._SetRawColumnAlias('c', 'a')
    self.assertEqual(m.manifest['columns'], {'c': 'a'})
    self.assertEqual(m.manifest['column_aliases'], {'a': 'c'})

  def testGetRawColumnName(self):
    """Test QueryManifest._GetRawColumnName()."""
    column_name = 'column_name'
    column_alias = 'derp alias'

    m = query_lib.QueryManifest.Generate()
    self.mox.StubOutWithMock(m, 'manifest', True)
    m.manifest.__getitem__('column_aliases').AndReturn(m.manifest)
    m.manifest.get(column_alias, None).AndReturn(column_name)

    self.mox.ReplayAll()
    self.assertEqual(column_name, m._GetRawColumnName(column_alias))
    self.mox.VerifyAll()

  def testGenerateColumnAlias(self):
    """Test QueryManifest.GenerateColumnAlias()."""
    m = query_lib.QueryManifest.Generate()
    self.mox.StubOutWithMock(m, 'base_hasher')
    m.base_hasher.copy().AndReturn(m.base_hasher)
    m.base_hasher.update('column_name')
    m.base_hasher.hexdigest().AndReturn('hexdigest')
    expect_alias = '%s%s' % (m.HASH_PREFIX, 'hexdigest')

    self.mox.ReplayAll()
    alias = m.GenerateColumnAlias('column_name')
    self.assertEqual(alias, expect_alias)
    self.mox.VerifyAll()

  def testGetColumnAliasForNameWhenNew(self):
    """Test QueryManifest.GetColumnAliasForName()."""
    column_name = 'column_name'
    column_alias = 'derp alias'

    m = query_lib.QueryManifest.Generate()

    self.mox.StubOutWithMock(m, 'GenerateColumnAlias')
    self.mox.StubOutWithMock(m, '_SetRawColumnAlias')
    m.GenerateColumnAlias(column_name).AndReturn(column_alias)
    m._SetRawColumnAlias(column_name, column_alias)

    self.mox.ReplayAll()
    self.assertEqual(column_alias, m.GetColumnAliasForName(column_name))
    self.mox.VerifyAll()

  def testGetColumnAliasForNameWhenNewNoGenerate(self):
    """Test QueryManifest.GetColumnAliasForName()."""
    column_name = 'column_name'
    column_alias = None

    m = query_lib.QueryManifest.Generate()

    self.mox.StubOutWithMock(m, 'GenerateColumnAlias')
    self.mox.StubOutWithMock(m, '_SetRawColumnAlias')

    self.mox.ReplayAll()
    self.assertEqual(
        column_alias, m.GetColumnAliasForName(column_name, generate=False))
    self.mox.VerifyAll()

  def testGetColumnAliasForName(self):
    """Test QueryManifest.GetColumnAliasForName()."""
    column_name = 'column_name'
    column_alias = 'derp alias'

    m = query_lib.QueryManifest.Generate()
    m.manifest['columns'][column_name] = column_alias

    self.mox.ReplayAll()
    self.assertEqual(column_alias, m.GetColumnAliasForName(column_name))
    self.mox.VerifyAll()

  def testGetColumnNameForAlias(self):
    """Test QueryManifest.GetColumnNameForAlias()."""
    column_name = 'column_name'
    column_alias = 'derp alias'

    m = query_lib.QueryManifest.Generate()
    self.mox.StubOutWithMock(m, '_GetRawColumnName', True)
    m._GetRawColumnName(column_alias).AndReturn(column_name)

    self.mox.ReplayAll()
    self.assertEqual(column_name, m.GetColumnNameForAlias(column_alias))
    self.mox.VerifyAll()

  def testExtractFieldQueriesWithManifest(self):
    """Test _ExtractFieldQueries() with usage of QueryManifest class."""

    class TestFieldToken(query_lib.util.FieldToken):

      def SetOriginalName(self, v):
        self.original_name = v
        return self

      def SetAlias(self, v):
        self.alias = v
        return self

    mock_manifest = self.mox.CreateMockAnything()

    def sca(column_name, alias=None, extras=None):
      """Returns a SQL column AS alias str clause while setting mocks."""
      if alias is not None:
        output = TestFieldToken(column_name).SetAlias(alias)
      else:
        if extras is None:
          mock_manifest.GetColumnAliasForName(column_name).AndReturn(
              'gcafn_%s' % column_name)
          output = TestFieldToken(column_name).SetAlias(
              'gcafn_%s' % column_name)
        else:
          mock_manifest.GetColumnAliasForName(
              column_name, extras=extras).AndReturn(
                  'gcafn_e_%s' % extras[0])
          output = TestFieldToken(column_name).SetAlias(
              'gcafn_e_%s' % extras[0])
      return str(output)

    # some repeated strings.
    static_alias0_str = 'column_alias9'
    static_orginal_name0_str = 'column.subcolumn.subcolumn2'

    # build the input to _ExtractFieldQueries().
    stacks = [
        [TestFieldToken('column').SetAlias(static_alias0_str)],
        [TestFieldToken('column.subcolumn')],
        [TestFieldToken(
            'column.subcolumn.DISTINCT_STRING_subcolumn2').SetOriginalName(
                static_orginal_name0_str)],
    ]

    # build the expected output from _ExtractFieldQueries().
    # this helper function sca() also wires up mock calls.
    expected_queries = set()
    expected_queries.add(sca('column', alias=static_alias0_str))
    expected_queries.add(sca('column.subcolumn'))
    expected_queries.add(sca(
        'column.subcolumn.DISTINCT_STRING_subcolumn2',
        extras=[static_orginal_name0_str]))

    alias = {0: static_alias0_str}

    self.mox.ReplayAll()
    queries = query_lib._ExtractFieldQueries(
        stacks, manifest=mock_manifest, strize=True, alias=alias)
    self.assertEqual(expected_queries, queries)
    self.mox.VerifyAll()

  def testStatisticsGet(self):
    """Test statistics property."""
    m = query_lib.QueryManifest.Generate()
    self.assertEqual(id(m.statistics), id(m.manifest['statistics']))
    m.statistics['foo'] = 'bar'
    self.assertEqual(m.manifest['statistics']['foo'], 'bar')


def main(_):
  googletest.main()

if __name__ == '__main__':
  app.run()
