#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for query parser module."""





from pyparsing import ParseException

import logging
from google.apputils import basetest as googletest
import query_parser as parser




class QueryParserTest(googletest.TestCase):

  def _RunMathQuery(self, expression, stack):
    test_stack = []
    parser._MathParser(test_stack).parseString(expression)
    self.assertEqual(stack, test_stack)

  def testNumber(self):
    logging.debug('Running testNumber method.')
    expression = '1234567890'
    self._RunMathQuery(expression, [1234567890])

  def testDecimalExponentNumber(self):
    logging.debug('Running testDecimalExponentNumber method.')
    expression = '1.23e45'
    self._RunMathQuery(expression, [1.23E45])

  def testComplexNumbers(self):
    logging.debug('Running testComplexNumbers method.')
    expression = '.9'
    self._RunMathQuery(expression, [.9])
    expression = '.8E5'
    self._RunMathQuery(expression, [.8E5])
    expression = '1.'
    self._RunMathQuery(expression, [1])
    expression = '-.8E-50'
    self._RunMathQuery(expression, [.8E-50, -1, '*'])
    expression = '+1'
    self._RunMathQuery(expression, [1])
    expression = '+1E+501'
    self._RunMathQuery(expression, [1E+501])

  def testQuotedString(self):
    logging.debug('Running testQuotedString method.')
    expression = '"hello, my name is \'test\'"'
    self._RunMathQuery(expression, ['"hello, my name is \'test\'"'])

  def testLabel(self):
    logging.debug('Running testLabel method.')
    expression = 'a'
    self._RunMathQuery(expression, ['a'])

  def testComplexLabels(self):
    logging.debug('Running testComplexLabels method.')
    expression = 'a_123'
    self._RunMathQuery(expression, ['a_123'])
    expression = 'a_1.b2'
    self._RunMathQuery(expression, ['a_1.b2'])

  def testUnary(self):
    logging.debug('Running testUnary method.')
    expression = '-~1'
    self._RunMathQuery(expression, [1, '~', -1, '*'])

  def testBinary(self):
    logging.debug('Running testBinary method.')
    expression = '1 + 2 + 3'
    self._RunMathQuery(expression, [1, 2, '+', 3, '+'])

  def testNoArgumentFunction(self):
    logging.debug('Running testNoArgumentFunction method.')
    expression = 'PI()'
    self._RunMathQuery(expression, ['pi'])

  def testOneArgumentFunction(self):
    logging.debug('Running testOneArgumentFunction method.')
    expression = 'cos(0)'
    self._RunMathQuery(expression, [0, 'cos'])

  def testMultipleArgumentFunction(self):
    logging.debug('Running testMultipleArgumentFunction method.')
    expression = 'right("hahaha", 2)'
    self._RunMathQuery(expression, ['"hahaha"', 2, 'right'])

  def testSumFunction(self):
    logging.debug('Running testSumFunction method.')
    expression = 'SUM(Year)'
    self._RunMathQuery(expression, ['Year', 'SUM'])

  def testAverageFunction(self):
    logging.debug('Running testAverageFunction method.')
    expression = 'AVG(Year * 2 + 1)'
    self._RunMathQuery(expression, ['Year', 2, '*', 1, '+', 'AVG'])

  def testTopFunction(self):
    logging.debug('Running testTopFunction method.')
    expression = 'TOP(Year)'
    self._RunMathQuery(expression, ['Year', 'TOP'])

  def testComplexTopFunction(self):
    logging.debug('Running testComplexTopFunction method.')
    expression = 'TOP(Year, 10, 1)'
    self._RunMathQuery(expression, ['Year', 10, 1, 'TOP'])

  def testCountFunction(self):
    logging.debug('Running testCountFunction method.')
    expression = 'COUNT(Year)'
    self._RunMathQuery(expression, ['Year', 'COUNT'])

  def testComplexCountFunction(self):
    logging.debug('Running testComplexCountFunction method.')
    expression = 'COUNT(distinct Year, 5)'
    self._RunMathQuery(expression, ['Year', 5, 'DISTINCTCOUNT'])

  def testCountStarFunction(self):
    logging.debug('Running testCountStarFunction method.')
    expression = 'COUNT(*)'
    self._RunMathQuery(expression, ['*', 'COUNT'])

  def testGroupConcatFunction(self):
    logging.debug('Running testGroupConcatFunction method.')
    expression = 'GROUP_CONCAT(Year)'
    self._RunMathQuery(expression, ['Year', 'GROUP_CONCAT'])

  def testNestedFunction(self):
    logging.debug('Running testNestedFunction method.')
    expression = 'sin(pow(tan(1), int(left("1 is cool", int(cos(0))))))'
    self._RunMathQuery(expression,
                       [1, 'tan', '"1 is cool"',
                        0, 'cos', 'int', 'left', 'int', 'pow', 'sin'])

  def testParentheses(self):
    logging.debug('Running testParentheses method.')
    expression = '(cos((-((2 * (1 - 3))))))'
    self._RunMathQuery(expression,
                       [2, 1, 3, '-', '*', -1, '*', 'cos'])

  def testOrderOfOperations(self):
    logging.debug('Running testOrderOfOperations method.')
    expression = """a and 1 & sin(1) | 2 or left("haha", 2) == "ha" ^ 100.0 ==
                    1E2 != 2 < ~5 >= b << 2 + 5 - -5 * 0"""
    self._RunMathQuery(expression,
                       ['a', 1, 1, 'sin', '&', 2, '|', 'and', '"haha"', 2,
                        'left', '"ha"', '==', 100.0, 1E2, '==', 2, 5, '~', '<',
                        'b', 2, 5, '+', 5, -1, '*', 0, '*', '-', '<<', '>=',
                        '!=', '^', 'or'])

  def testTopThreeArguments(self):
    logging.debug('Running testTopThreeArguments method.')
    expression = 'TOP(Year, 1, 1)'
    self._RunMathQuery(expression, ['Year', 1, 1, 'TOP'])

  def testAggregationFunctionArgument(self):
    logging.debug('Running testAggregationFunctionArgument method.')
    expression = 'TOP(sin(Year), 1, 1)'
    self._RunMathQuery(expression, ['Year', 'sin', 1, 1, 'TOP'])

  def testQuantiles(self):
    logging.debug('Running testQuantiles method.')
    expression = 'QUANTILES(2 + Year)'
    self._RunMathQuery(expression, [2, 'Year', '+', 'QUANTILES'])
    expression = 'QUANTILES(Year + 2, 1)'
    self._RunMathQuery(expression, ['Year', 2, '+', 1, 'QUANTILES'])

  def testStddev(self):
    logging.debug('Running testStddev method.')
    expression = 'STDDEV(20 * Year)'
    self._RunMathQuery(expression, [20, 'Year', '*', 'STDDEV'])

  def testVariance(self):
    logging.debug('Running testVariance method.')
    expression = 'variance(label)'
    self._RunMathQuery(expression, ['label', 'VARIANCE'])

  def testLast(self):
    logging.debug('Running testLast method.')
    expression = 'Last(label / label2)'
    self._RunMathQuery(expression, ['label', 'label2', '/', 'LAST'])

  def testNth(self):
    logging.debug('Running testNth method.')
    expression = 'nth(1, label)'
    self._RunMathQuery(expression, [1, 'label', 'NTH'])

  # pylint: disable=dangerous-default-value
  def _RunQuery(self, query, select_arg=[], from_arg=[], join_arg=[],
                where_arg=[], having_arg=[], group_by_arg=[], order_by_arg=[],
                limit_arg=[], as_arg={}, within_arg={}):
    """Run the actual test."""
    clauses = {
        'SELECT': select_arg,
        'AS': as_arg,
        'WITHIN': within_arg,
        'FROM': from_arg,
        'JOIN': join_arg,
        'WHERE': where_arg,
        'HAVING': having_arg,
        'GROUP BY': group_by_arg,
        'ORDER BY': order_by_arg,
        'LIMIT': limit_arg,
    }
    real_clauses = parser.ParseQuery(query)
    self.assertEqual(clauses, real_clauses)

  def testMath(self):
    logging.debug('Running testMath method.')
    ebq_query = 'SELECT 1 + 4 / 2 * 3 - 5'
    self._RunQuery(ebq_query,
                   select_arg=[[1, 4, 2, '/', 3, '*', '+', 5, '-']])

  def testMultipleMath(self):
    logging.debug('Running testMultipleMath method.')
    ebq_query = 'Select a, 1 + 1, cos(1), "hello"'
    self._RunQuery(ebq_query,
                   select_arg=[['a'], [1, 1, '+'], [1, 'cos'], ['"hello"']])

  def testFromClause(self):
    logging.debug('Running testFromClause method.')
    ebq_query = 'SELECT 1 FROM table'
    self._RunQuery(ebq_query, select_arg=[[1]], from_arg=['table'])

  def testWhereClause(self):
    logging.debug('Running testWhereClause method.')
    ebq_query = 'SELECT a where a < 1'
    self._RunQuery(ebq_query, select_arg=[['a']], where_arg=['a', 1, '<'])

  def testHavingClause(self):
    logging.debug('Running testHavingClause method.')
    ebq_query = 'SELECT a having a < 1'
    self._RunQuery(ebq_query, select_arg=[['a']], having_arg=['a', 1, '<'])

  def testGroupOrderClause(self):
    logging.debug('Running testGroupOrderClause method.')
    ebq_query = 'SELECT a group by a order by a'
    self._RunQuery(ebq_query, select_arg=[['a']], group_by_arg=['a'],
                   order_by_arg=['a'])

  def testLimitClause(self):
    logging.debug('Running testLimitClause method.')
    ebq_query = 'select a limit 10'
    self._RunQuery(ebq_query, select_arg=[['a']], limit_arg=[10])

  def testSimpleCommands(self):
    logging.debug('Running testCommands method.')
    ebq_query = 'select 1 From table lIMit 5'
    self._RunQuery(ebq_query, select_arg=[[1]], from_arg=['table'],
                   limit_arg=[5])

  def testUnimplementedFunctions(self):
    logging.debug('Running an unimplemented function.')
    ebq_query = 'select from_base64("aGVsbG8gdGVzdA==")'
    self._RunQuery(ebq_query,
                   select_arg=[['"aGVsbG8gdGVzdA=="', 'from_base64']])

  def testAllCommands(self):
    logging.debug('Running testAllCommands method.')
    ebq_query = """select a, b, c, d from table where a < b
                     group by d, c, b, a
                     having c == d
                     order by a, b, c, d limit 4"""
    self._RunQuery(ebq_query, select_arg=[['a'], ['b'], ['c'], ['d']],
                   from_arg=['table'], where_arg=['a', 'b', '<'],
                   having_arg=['c', 'd', '=='],
                   group_by_arg=['d', 'c', 'b', 'a'],
                   order_by_arg=['a', 'b', 'c', 'd'], limit_arg=[4])

  def testSomeCommands(self):
    logging.debug('Running testSomeCommands method.')
    ebq_query = 'select a, b, c from Cars where b + 1 < a order by a, b, c'
    self._RunQuery(ebq_query, select_arg=[['a'], ['b'], ['c']],
                   from_arg=['Cars'], where_arg=['b', 1, '+', 'a', '<'],
                   order_by_arg=['a', 'b', 'c'])

  def testComplexOrderBy(self):
    logging.debug('Running testComplexOrderBy method.')
    ebq_query = 'select a, b, c, d order by a Asc, b, c deSC, d Desc'
    self._RunQuery(ebq_query, select_arg=[['a'], ['b'], ['c'], ['d']],
                   order_by_arg=['a ASC', 'b', 'c DESC', 'd DESC'])

  def testAlias(self):
    logging.debug('Running testAlias method.')
    ebq_query = """Select 1 as a, 2, 3 as b, 4, 5, 6 as c from table where a
                     contains b order by a, b, c"""
    self._RunQuery(ebq_query,
                   select_arg=[[1], [2], [3], [4], [5], [6]],
                   from_arg=['table'], where_arg=['a', 'b', 'contains'],
                   order_by_arg=['a', 'b', 'c'],
                   as_arg={0: 'a', 2: 'b', 5: 'c'})

  def testAliasWithoutAs1(self):
    logging.debug('Running testAliasWithoutAs1 method.')
    ebq_query = 'SELECT 1 a from table'
    self._RunQuery(ebq_query, select_arg=[[1]], from_arg=['table'],
                   as_arg={0: 'a'})

  def testAliasWithoutAs2(self):
    logging.debug('Running testAliasWithoutAs1 method.')
    ebq_query = 'SELECT 1 as a, 2 b, 3, 4 c, 5 as e from table'
    self._RunQuery(ebq_query, select_arg=[[1], [2], [3], [4], [5]],
                   from_arg=['table'], as_arg={0: 'a', 1: 'b', 3: 'c', 4: 'e'})

  def testAliasAggregation(self):
    logging.debug('Running testAliasAggregation method.')
    ebq_query = 'SELECT COUNT(*) as a, a * 2 from table'
    self._RunQuery(ebq_query,
                   select_arg=[['*', 'COUNT'], ['a', 2, '*']],
                   from_arg=['table'],
                   as_arg={0: 'a'})

  def testFlatten(self):
    logging.debug('Running testFlatten method.')
    ebq_query = 'SELECT a from (FLATTEN(table1, field1))'
    self._RunQuery(ebq_query, select_arg=[['a']],
                   from_arg=['(FLATTEN(table1,field1))'])

  def testMultipleFlatten(self):
    logging.debug('Running testMultipleFlatten method.')
    ebq_query = ('SELECT a from (FLATTEN(table1, field1)) '
                 '(flatten(table2, field2))')
    self._RunQuery(ebq_query, select_arg=[['a']],
                   from_arg=[
                       '(FLATTEN(table1,field1))(FLATTEN(table2,field2))'])

  def testWithin1(self):
    logging.debug('Running testWithin1 method.')
    ebq_query = 'SELECT SUM(a.b) within a from table'
    self._RunQuery(ebq_query,
                   select_arg=[['a.b', 'SUM']],
                   from_arg=['table'], within_arg={0: 'a'})

  def testWithin2(self):
    logging.debug('Running testWithin2 method.')
    ebq_query = 'SELECT SUM(a.b) within a as c from table'
    self._RunQuery(ebq_query,
                   select_arg=[['a.b', 'SUM']],
                   from_arg=['table'],
                   as_arg={0: 'c'},
                   within_arg={0: 'a'})

  def testWithin3(self):
    logging.debug('Running testWithin3 method.')
    ebq_query = ('SELECT a, SUM(a.b) within a, COUNT(c.d.h) within c.d as e, '
                 'f as g from table')
    self._RunQuery(ebq_query,
                   select_arg=[['a'], ['a.b', 'SUM'], ['c.d.h', 'COUNT'],
                               ['f']],
                   from_arg=['table'],
                   as_arg={2: 'e', 3: 'g'},
                   within_arg={1: 'a', 2: 'c.d'})

  def _CheckParseFail(self, command):
    self.assertRaises(ParseException, parser.ParseQuery, command)

  def testBadSelect1(self):
    logging.debug('Running testBadSelect1 method.')
    ebq_query = 'SELECT 1,'
    self._CheckParseFail(ebq_query)

  def testBadSelect2(self):
    logging.debug('Running testBadSelect2 method.')
    ebq_query = 'SELECT'
    self._CheckParseFail(ebq_query)

  def testBadSelect3(self):
    logging.debug('Running testBadSelect3 method.')
    ebq_query = 'SELECTa'
    self._CheckParseFail(ebq_query)

  def testBadFrom1(self):
    logging.debug('Running testBadFrom1 method.')
    ebq_query = 'SELECT a, b from table1 table2'
    self._CheckParseFail(ebq_query)

  def testBadFrom2(self):
    logging.debug('Running testBadFrom2 method.')
    ebq_query = 'SELECT a, b from table1,'
    self._CheckParseFail(ebq_query)

  def testBadFrom3(self):
    logging.debug('Running testBadFrom3 method.')
    ebq_query = 'SELECT a, b from '
    self._CheckParseFail(ebq_query)

  def testBadWhereHaving1(self):
    logging.debug('Running testBadWhereHaving1 method.')
    ebq_query = 'SELECT a, b from table wherea <b'
    self._CheckParseFail(ebq_query)

  def testBadWhereHaving2(self):
    logging.debug('Running testBadWhereHaving2 method.')
    ebq_query = 'SELECT a, b from table where '
    self._CheckParseFail(ebq_query)

  def testBadWhereHaving3(self):
    logging.debug('Running testBadWhereHaving3 method.')
    ebq_query = 'SELECT a, b from table where a < b,'
    self._CheckParseFail(ebq_query)

  def testBadWhereHaving4(self):
    logging.debug('Running testBadWhereHaving4 method.')
    ebq_query = 'SELECT a, b from table where b > a having'
    self._CheckParseFail(ebq_query)

  def testBadWhereHaving5(self):
    logging.debug('Running testBadWhereHaving5 method.')
    ebq_query = 'SELECT a, b from table havinga'
    self._CheckParseFail(ebq_query)

  def testBadWhereHaving6(self):
    logging.debug('Running testBadWhereHaving6 method.')
    ebq_query = 'SELECT a, b from table where having a < b'
    self._CheckParseFail(ebq_query)

  def testBadOrderGroup1(self):
    logging.debug('Running testBadOrderGroup1 method.')
    ebq_query = 'SELECT a, b from table order by'
    self._CheckParseFail(ebq_query)

  def testBadOrderGroup2(self):
    logging.debug('Running testBadOrderGroup2 method.')
    ebq_query = 'SELECT a, b from table order by a,'
    self._CheckParseFail(ebq_query)

  def testBadOrderGroup3(self):
    logging.debug('Running testBadOrderGroup3 method.')
    ebq_query = 'SELECT a, b from table order by a group by a, b'
    self._CheckParseFail(ebq_query)

  def testBadOrderGroup04(self):
    logging.debug('Running testBadOrderGroup04 method.')
    ebq_query = 'SELECT a, b from table order bya group by a, b'
    self._CheckParseFail(ebq_query)

  def testBadOrderGroup5(self):
    logging.debug('Running testBadOrderGroup5 method.')
    ebq_query = 'SELECT a, b from table order by a group by a,'
    self._CheckParseFail(ebq_query)

  def testBadLimit1(self):
    logging.debug('Running testBadLimit1 method.')
    ebq_query = 'select a from table limit'
    self._CheckParseFail(ebq_query)

  def testBadLimit2(self):
    logging.debug('Running testBadLimit2 method.')
    ebq_query = 'select a from table limit a'
    self._CheckParseFail(ebq_query)

  def testBadLimit3(self):
    logging.debug('Running testBadLimit3 method.')
    ebq_query = 'select a from table limit1'
    self._CheckParseFail(ebq_query)

  def testBadOrdering1(self):
    logging.debug('Running testBadOrdering1 method.')
    ebq_query = 'select a, b where a < b from table'
    self._CheckParseFail(ebq_query)

  def testBadOrdering2(self):
    logging.debug('Running testBadOrdering2 method.')
    ebq_query = 'select a from table having a < b where a < b'
    self._CheckParseFail(ebq_query)

  def testBadOrdering3(self):
    logging.debug('Running testBadOrdering3 method.')
    ebq_query = 'select a from table having a < b group by a, b'
    self._CheckParseFail(ebq_query)

  def testBadOrdering4(self):
    logging.debug('Running testBadOrdering4 method.')
    ebq_query = 'select a from table limit 5 having a < b'
    self._CheckParseFail(ebq_query)

  def testAllStatementOrdering(self):
    logging.debug('Running testAllStatementOrdering method.')
    ebq_query = (
        'select a, b, count(b) as cnt '
        'from t '
        'join t2 on t2.id=t.id '
        'join t3 on t3.id=t.id and t3.id!=t2.blockid '
        'where a > 1 '
        'group by b '
        'having cnt > 2 '
        'order by cnt '
        'limit 10')
    self._RunQuery(
        ebq_query,
        select_arg=[['a'], ['b'], ['b', 'COUNT']],
        as_arg={2: 'cnt'},
        from_arg=['t'],
        join_arg=[
            ['t2', 't2.id', 't.id', '='],
            ['t3', 't3.id', 't.id', '=', 't3.id', 't2.blockid', '!=', 'and'],
        ],
        where_arg=['a', 1, '>'],
        group_by_arg=['b'],
        having_arg=['cnt', 2, '>'],
        order_by_arg=['cnt'],
        limit_arg=[10])

  def testAggregationFunctionWithAlias(self):
    ebq_query = 'select count(column) as cnt_column from foo'
    self._RunQuery(
        ebq_query,
        select_arg=[['column', 'COUNT']],
        as_arg={0: 'cnt_column'},
        from_arg=['foo']
        )

  def testMultipleTablesFail(self):
    logging.debug('Running testMultipleTablesFail method.')
    ebq_query = 'select a from table1, table2'
    self._CheckParseFail(ebq_query)


if __name__ == '__main__':
  googletest.main()
