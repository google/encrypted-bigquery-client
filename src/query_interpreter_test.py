#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for ebq interpreter."""



import math

from google.apputils import app
import gflags as flags
import logging
from google.apputils import basetest as googletest

import bigquery_client
import common_util as util
import query_interpreter as interpreter
import test_util

FLAGS = flags.FLAGS

_TABLE_ID = '1'




class QueryInterpreterTest(googletest.TestCase):

  def testSimpleExpression(self):
    stack = [1, 2, util.OperatorToken('+', 2)]
    self.assertEqual(interpreter.ToInfix(list(stack)), '(1 + 2)')
    self.assertEqual(interpreter.Evaluate(stack), 3)

  def testUnary(self):
    stack = [1, 2, util.OperatorToken('~', 1), util.OperatorToken('<', 2),
             util.OperatorToken('not', 1)]
    self.assertEqual(interpreter.ToInfix(list(stack)), 'not (1 < ~ 2)')
    self.assertEqual(interpreter.Evaluate(stack), True)

  def testBinary(self):
    stack = [1, 2, util.OperatorToken('+', 2), 3, util.OperatorToken('*', 2), 9,
             util.OperatorToken('/', 2), 2, util.OperatorToken('-', 2)]
    self.assertEqual(interpreter.ToInfix(list(stack)),
                     '((((1 + 2) * 3) / 9) - 2)')
    self.assertEqual(interpreter.Evaluate(stack), -1)

  def testNoArgumentFunction(self):
    stack = [util.BuiltInFunctionToken('pi')]
    self.assertEqual(interpreter.ToInfix(list(stack)), 'pi()')
    self.assertEqual(interpreter.Evaluate(stack), math.pi)

  def testOneArgumentFunction(self):
    stack = [0, util.BuiltInFunctionToken('cos'),
             util.BuiltInFunctionToken('ln'), util.BuiltInFunctionToken('sqrt')]
    self.assertEqual(interpreter.ToInfix(list(stack)), 'sqrt(ln(cos(0)))')
    self.assertEqual(interpreter.Evaluate(stack), 0)

  def testMultipleArgumentFunction(self):
    stack = ['True', 3, 0, util.BuiltInFunctionToken('if'), 2, 1,
             util.BuiltInFunctionToken('pow'), util.BuiltInFunctionToken('pow')]
    self.assertEqual(interpreter.ToInfix(list(stack)),
                     'pow(if(True, 3, 0), pow(2, 1))')
    self.assertEqual(interpreter.Evaluate(stack), 9)

  def testString(self):
    stack = [util.StringLiteralToken('"TESTING IS FUN."'), 4,
             util.BuiltInFunctionToken('left')]
    self.assertEqual(interpreter.ToInfix(list(stack)),
                     'left("TESTING IS FUN.", 4)')
    self.assertEqual(interpreter.Evaluate(stack), 'TEST')

  def testBooleanLiterals(self):
    stack = [util.LiteralToken('True', True), util.LiteralToken('False', False),
             util.OperatorToken('or', 2), 1, 2,
             util.OperatorToken('=', 2), util.OperatorToken('or', 2)]
    self.assertEqual(interpreter.ToInfix(list(stack)),
                     '((True or False) or (1 = 2))')
    self.assertEqual(interpreter.Evaluate(stack), True)

  def testCountStar(self):
    stack = [util.CountStarToken()]
    self.assertEqual(interpreter.ToInfix(list(stack)), '*')

  def testNull(self):
    stack = [util.LiteralToken('null', None)]
    self.assertEqual(interpreter.ToInfix(list(stack)), 'null')
    self.assertEqual(interpreter.Evaluate(stack), None)

  def testRewrittenAggregation(self):
    stack = [util.FieldToken('Year'),
             util.AggregationFunctionToken('DISTINCTCOUNT', 1)]
    self.assertEqual(interpreter.ToInfix(list(stack)),
                     'COUNT(DISTINCT Year)')

  def testTooManyArgumentsFunction(self):
    stack = [1, 2, 3, util.BuiltInFunctionToken('COS')]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.ToInfix, list(stack))
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.Evaluate, stack)

  def testNotEnoughArgumentsFunction(self):
    stack = [1, util.BuiltInFunctionToken('pow')]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.ToInfix, list(stack))
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.Evaluate, stack)

  def testNonexistentFunction(self):
    stack = [1, util.BuiltInFunctionToken('hi')]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.ToInfix, list(stack))
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.Evaluate, stack)

  def testSimpleWhere(self):
    schema = test_util.GetCarsSchema()
    key = test_util.GetMasterKey()
    stack = [1, 2, util.OperatorToken('>', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(1 > 2)')
    stack = [1, 2, util.OperatorToken('=', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(1 = 2)')
    stack = [util.FieldToken('PI()'), 1, util.OperatorToken('>', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(PI() > 1)')
    stack = [1, util.OperatorToken('>', 2)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    stack = [util.FieldToken('Year'), 2000, util.OperatorToken('<', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(Year < 2000)')

  def testFailOperationsOnEncryptions(self):
    schema = test_util.GetCarsSchema()
    key = test_util.GetMasterKey()
    stack = [util.PseudonymToken('Year'), 1, util.OperatorToken('+', 2), 2000,
             util.OperatorToken('>=', 2)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    stack = [util.ProbabilisticToken('Model'), 2,
             util.BuiltInFunctionToken('left')]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    stack = [util.HomomorphicIntToken('Invoice_Price'),
             util.BuiltInFunctionToken('is_nan')]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)

  def testEncryptedEquality(self):
    schema = test_util.GetCarsSchema()
    key = test_util.GetMasterKey()
    stack = [util.FieldToken('Year'), 1, util.OperatorToken('+', 2), 2000,
             util.OperatorToken('=', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '((Year + 1) = 2000)')
    stack = [util.FieldToken('Year'), util.PseudonymToken('Make'),
             util.OperatorToken('=', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(Year = ' + util.PSEUDONYM_PREFIX + 'Make)')
    stack = [util.PseudonymToken('Make'), util.StringLiteralToken('"Hello"'),
             util.OperatorToken('==', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(' + util.PSEUDONYM_PREFIX +
        'Make == "HS57DHbh2KlkqNJREmu1wQ==")')
    stack = [util.StringLiteralToken('"Hello"'), util.PseudonymToken('Make'),
             util.OperatorToken('==', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '("HS57DHbh2KlkqNJREmu1wQ==" == ' + util.PSEUDONYM_PREFIX +
        'Make)')
    stack = [util.StringLiteralToken('"Hello"'), util.PseudonymToken('Make'),
             util.OperatorToken('!=', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '("HS57DHbh2KlkqNJREmu1wQ==" != ' + util.PSEUDONYM_PREFIX +
        'Make)')
    stack = [util.PseudonymToken('Make'),
             util.PseudonymToken('Make2'), util.OperatorToken('==', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(' + util.PSEUDONYM_PREFIX + 'Make == ' +
        util.PSEUDONYM_PREFIX + 'Make2)')
    stack = [util.HomomorphicIntToken('Invoice_Price'), 2,
             util.OperatorToken('==', 2)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    stack = [util.PseudonymToken('Make'),
             util.ProbabilisticToken('Price'), util.OperatorToken('=', 2)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    schema = test_util.GetPlacesSchema()
    stack = [util.PseudonymToken('spouse.spouseName'),
             util.StringLiteralToken('"Hello"'), util.OperatorToken('=', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(spouse.' + util.PSEUDONYM_PREFIX +
        'spouseName = "HS57DHbh2KlkqNJREmu1wQ==")')

  def testEncryptedContains(self):
    schema = test_util.GetCarsSchema()
    key = test_util.GetMasterKey()
    stack = [util.FieldToken('Year'), util.BuiltInFunctionToken('string'),
             util.StringLiteralToken('"1"'),
             util.OperatorToken('CONTAINS', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(string(Year) contains "1")')
    stack = [util.SearchwordsToken('Model'), util.StringLiteralToken('"A"'),
             util.OperatorToken('contains', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(' + util.SEARCHWORDS_PREFIX + 'Model contains '
        'to_base64(left(sha1(concat(left(' + util.SEARCHWORDS_PREFIX +
        'Model, 24), \'yB9HY2qv+DI=\')), 8)))')
    stack = [util.SearchwordsToken('Model'), util.FieldToken('Year'),
             util.OperatorToken('contains', 2)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    stack = [util.PseudonymToken('Make'), 'A',
             util.OperatorToken('contains', 2)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    stack = [util.SearchwordsToken('Model'),
             util.SearchwordsToken('Model'), util.OperatorToken('contains', 2)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    stack = ['Hello', util.SearchwordsToken('Model'),
             util.OperatorToken('contains', 2)]
    self.assertRaises(
        bigquery_client.BigqueryInvalidQueryError,
        interpreter.RewriteSelectionCriteria, stack, schema, key, _TABLE_ID)
    stack = [util.SearchwordsToken('Model'), util.StringLiteralToken('"A"'),
             util.OperatorToken('contains', 2), util.OperatorToken('not', 1)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        'not (' + util.SEARCHWORDS_PREFIX + 'Model contains '
        'to_base64(left(sha1(concat(left(' + util.SEARCHWORDS_PREFIX +
        'Model, 24), \'yB9HY2qv+DI=\')), 8)))')
    schema = test_util.GetPlacesSchema()
    stack = [util.SearchwordsToken('citiesLived.place'),
             util.StringLiteralToken('"A"'), util.OperatorToken('contains', 2)]
    self.assertEqual(
        interpreter.RewriteSelectionCriteria(stack, schema, key, _TABLE_ID),
        '(citiesLived.' + util.SEARCHWORDS_PREFIX + 'place contains '
        'to_base64(left(sha1(concat(left(citiesLived.' +
        util.SEARCHWORDS_PREFIX + 'place, 24), \'cBKPKGiY2cg=\')), 8)))')

  def testGetSingleValue(self):
    stack = [1, 1, 1, util.OperatorToken('+', 2)]
    start, postfix = interpreter.GetSingleValue(stack)
    self.assertEqual(start, 1)
    self.assertEqual(postfix, [1, 1, util.OperatorToken('+', 2)])
    stack = [1, util.OperatorToken('+', 2)]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.GetSingleValue, stack)

  def testExpandExpression(self):
    stack = [util.FieldToken('x'), util.FieldToken('y'),
             util.OperatorToken('+', 2), util.FieldToken('x'),
             util.OperatorToken('+', 2), 2, util.FieldToken('z'),
             util.OperatorToken('*', 2), util.OperatorToken('-', 2),
             5, util.OperatorToken('-', 2), 3, util.OperatorToken('+', 2)]
    list_fields, constant = interpreter._ExpandExpression(stack)
    self.assertEqual(list_fields, [[2.0, util.FieldToken('x')],
                                   [1.0, util.FieldToken('y')],
                                   [-2.0, util.FieldToken('z')]])
    self.assertEqual(constant, -2.0)
    stack = [util.FieldToken('x'), 4, util.OperatorToken('+', 2), 6,
             util.OperatorToken('*', 2), 2, util.OperatorToken('/', 2)]
    list_fields, constant = interpreter._ExpandExpression(stack)
    self.assertEqual(list_fields, [[3.0, util.FieldToken('x')]])
    self.assertEqual(constant, 12.0)
    stack = [util.FieldToken('x'), 1, util.OperatorToken('+', 2),
             util.FieldToken('y'), util.OperatorToken('*', 2)]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter._ExpandExpression, stack)
    stack = [util.FieldToken('x'), util.FieldToken('y'),
             util.OperatorToken('/', 2)]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter._ExpandExpression, stack)

  def testCheckValidSumAverageArgument(self):
    stack = [util.FieldToken('Year'), util.FieldToken('Year'),
             util.OperatorToken('*', 2),
             util.HomomorphicIntToken('Invoice_Price'),
             util.OperatorToken('+', 2)]
    expected_stack = [[['Year', 'Year', '*'],
                       [util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price']],
                      True, True]
    self.assertEqual(interpreter.CheckValidSumAverageArgument(stack),
                     expected_stack)
    stack = [2, util.FieldToken('Year'), util.FieldToken('Year'),
             util.OperatorToken('*', 2),
             util.HomomorphicIntToken('Invoice_Price'),
             util.OperatorToken('+', 2), util.OperatorToken('*', 2)]
    expected_stack = [[[2, 'Year', 'Year', '*', '*'],
                       [2, util.HOMOMORPHIC_INT_PREFIX + 'Invoice_Price',
                        '*']], True, True]
    self.assertEqual(interpreter.CheckValidSumAverageArgument(stack),
                     expected_stack)
    stack = [util.ProbabilisticToken('Price')]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.CheckValidSumAverageArgument, stack)
    stack = [util.HomomorphicIntToken('Invoice_Price'),
             util.HomomorphicFloatToken('Holdback_Percentage'),
             util.OperatorToken('*', 2)]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.CheckValidSumAverageArgument, stack)
    stack = [util.HomomorphicIntToken('Invoice_Price'),
             util.FieldToken('Year'), util.OperatorToken('*', 2)]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.CheckValidSumAverageArgument, stack)
    stack = [util.HomomorphicIntToken('Invoice_Price'),
             util.FieldToken('Year'), util.OperatorToken('/', 2)]
    self.assertRaises(bigquery_client.BigqueryInvalidQueryError,
                      interpreter.CheckValidSumAverageArgument, stack)

  def testToBase64(self):
    """Test built-in function TO_BASE64(string)."""
    stack = [util.StringLiteralToken('"hello test"'),
             util.BuiltInFunctionToken('to_base64')]
    self.assertEqual(interpreter.Evaluate(stack), 'aGVsbG8gdGVzdA==')

  def testFromBase64(self):
    """Don't implement FROM_BASE64() until BigQuery has BYTES type."""
    base64_str = 'aGVsbG8gdGVzdA=='  # "hello test"
    stack = [util.StringLiteralToken('"%s"' % base64_str),
             util.BuiltInFunctionToken('from_base64')]
    self.assertEqual(
        interpreter.Evaluate(stack), 'FROM_BASE64("%s")' % base64_str)


def main(_):
  googletest.main()

if __name__ == '__main__':
  app.run()
