#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Contains parser to handle queries for enqcuirer.

The main purpose of this separate parser is to break down queries properly so
that they can be modified before being sent to BigQuery.
"""



import pyparsing as pp

import bigquery_client
import common_util as util


def _MathParser(math_stack):
  """Defines the entire math expression for BigQuery queries.

  Converts the expression into postfix notation. The stack is reversed
  (i.e. the last element acts the top of the stack).

  Actions do not occur unless parseString is called on the BNF returned.
  The actions will modify the original list that was passed when the BNF
  was generated.

  The <math_stack> will return the single expression converted to postfix.

  Arguments:
    math_stack: Returns postfix notation of one math expression.

  Returns:
    A BNF of an math/string expression.
  """

  def PushAggregation(tokens):
    """Pushes aggregation functions onto the stack.

    When the aggregation is pushed, the name is rewritten. The label is
    prepended with AGGREGATION_ to signal that an aggregation is occurring.
    Following this prefix is an integer, which represents the number of comma
    separated arguments that were provided. Finally, the name of the function
    is appended to the label. For most functions, the aggregation name is
    simply appended. However, there are special exceptions for COUNT.
    A normal count function is rewritten as AGGREGATION_i_COUNT. However,
    a count with the distinct keyword is rewritten to
    AGGREGATION_i_DISTINCTCOUNT.

    Args:
      tokens: The function name and arguments in a list object.
    """
    function_name = tokens[0]
    # Rename count with distinct keyword as distinctcount.
    if function_name == 'COUNT':
      if 'DISTINCT' in list(tokens):
        function_name = 'DISTINCTCOUNT'
    # Assume all aggregation functions have at least one argument.
    # If a function n commas, then it has n + 1 arguments.
    num_args = 1
    for token in tokens:
      if token == ',':
        num_args += 1
    math_stack.append(util.AggregationFunctionToken(function_name, num_args))

  def PushFunction(tokens):
    """Push a function token onto the stack.

    Args:
      tokens: list of all tokens, tokens[0] is the function name str.
    """
    math_stack.append(util.BuiltInFunctionToken(tokens[0]))

  def PushSingleToken(tokens):
    """Push the topmost token onto the stack."""
    if util.IsFloat(tokens[0]):
      try:
        token = int(tokens[0])
      except ValueError:
        token = float(tokens[0])
    elif tokens[0].startswith('\'') or tokens[0].startswith('"'):
      token = util.StringLiteralToken(tokens[0])
    elif tokens[0].lower() in util.BIGQUERY_CONSTANTS:
      token = util.LiteralToken(tokens[0].lower(),
                                util.BIGQUERY_CONSTANTS[tokens[0].lower()])
    else:
      token = util.FieldToken(tokens[0])
    math_stack.append(token)

  def PushCountStar(tokens):
    if tokens[0] != '*':
      raise ValueError('Not a count star argument.')
    math_stack.append(util.CountStarToken())

  def PushUnaryOperators(tokens):
    # The list must be reversed since unary operations are unwrapped in the
    # other direction. An example is ~-1. The negation occurs before the bit
    # inversion.
    for i in reversed(range(0, len(tokens))):
      if tokens[i] == '-':
        math_stack.append(int('-1'))
        math_stack.append(util.OperatorToken('*', 2))
      elif tokens[i] == '~':
        math_stack.append(util.OperatorToken('~', 1))
      elif tokens[i].lower() == 'not':
        math_stack.append(util.OperatorToken('not', 1))

  def PushBinaryOperator(tokens):
    math_stack.append(util.OperatorToken(tokens[0], 2))

  # Miscellaneous symbols and keywords.
  comma = pp.Literal(',')
  decimal = pp.Literal('.')
  exponent_literal = pp.CaselessLiteral('E')
  lp = pp.Literal('(')
  rp = pp.Literal(')')
  count_star = pp.Literal('*')
  distinct_keyword = pp.CaselessKeyword('DISTINCT')

  # Any non-space containing sequence of characters that must begin with
  # an alphabetical character and contain alphanumeric characters
  # and underscores (i.e. function or variable names).
  label = pp.Word(pp.alphas, pp.alphas + pp.nums + '_' + '.')

  # A single/double quote surrounded string.
  string = pp.quotedString

  # Various number representations.
  integer = pp.Word(pp.nums)
  decimal_type1 = pp.Combine(integer + decimal + pp.Optional(integer))
  decimal_type2 = pp.Combine(decimal + integer)
  real = decimal_type1 | decimal_type2
  exponent = exponent_literal + pp.Word('+-' + pp.nums, pp.nums)
  number_without_exponent = real | integer
  number = pp.Combine(number_without_exponent + pp.Optional(exponent))
  integer_argument = pp.Word(pp.nums)
  integer_argument.setParseAction(PushSingleToken)

  # Forward declaration for recusive grammar. We assume that full_expression can
  # represent any expression that is valid.
  full_expression = pp.Forward()

  # Aggregation function definitions.
  avg_function = pp.CaselessKeyword('AVG') + lp + full_expression + rp
  count_star.setParseAction(PushCountStar)
  count_argument = ((pp.Optional(distinct_keyword) + full_expression) |
                    count_star)
  count_function = (pp.CaselessKeyword('COUNT') + lp +
                    count_argument + pp.Optional(comma + integer_argument) + rp)
  quantiles_function = (pp.CaselessKeyword('QUANTILES') + lp + full_expression +
                        pp.Optional(comma + integer_argument) + rp)
  stddev_function = pp.CaselessKeyword('STDDEV') + lp + full_expression + rp
  variance_function = pp.CaselessKeyword('VARIANCE') + lp + full_expression + rp
  last_function = pp.CaselessKeyword('LAST') + lp + full_expression + rp
  max_function = pp.CaselessKeyword('MAX') + lp + full_expression + rp
  min_function = pp.CaselessKeyword('MIN') + lp + full_expression + rp
  nth_function = (pp.CaselessKeyword('NTH') + lp + integer_argument + comma +
                  full_expression + rp)
  group_concat_function = (pp.CaselessKeyword('GROUP_CONCAT') + lp +
                           full_expression + rp)
  sum_function = pp.CaselessKeyword('SUM') + lp + full_expression + rp
  top_function = (pp.CaselessKeyword('TOP') + lp + full_expression +
                  pp.Optional(comma + integer_argument +
                              pp.Optional(comma + integer_argument)) + rp)
  aggregate_functions = (avg_function | count_function | quantiles_function |
                         stddev_function | variance_function | last_function |
                         max_function | min_function | nth_function |
                         group_concat_function | sum_function | top_function)
  aggregate_functions.setParseAction(PushAggregation)

  functions_arguments = pp.Optional(full_expression +
                                    pp.ZeroOrMore(comma.suppress() +
                                                  full_expression))
  functions = label + lp + functions_arguments + rp
  functions.setParseAction(PushFunction)

  literals = number | string | label
  literals.setParseAction(PushSingleToken)

  # Any expression that can be modified by an unary operator.
  # We include strings (even though they can't be modified by any unary
  # operator) since atoms do not necessitate modification by unary operators.
  # These errors will be caught by the interpreter.
  atom = ((lp + full_expression + rp) |
          aggregate_functions |
          functions |
          literals)

  unary_operators = (pp.CaselessLiteral('+') |
                     pp.CaselessLiteral('-') |
                     pp.CaselessLiteral('~') |
                     pp.CaselessKeyword('not'))
  # Take all unary operators preceding atom (possibly many).
  current_expression = (pp.ZeroOrMore(unary_operators) +
                        atom.suppress())
  current_expression.setParseAction(PushUnaryOperators)

  # All operators in same set have same precedence. Precedence is top to bottom.
  binary_operators = [
      (pp.CaselessLiteral('*') | pp.CaselessLiteral('/') |
       pp.CaselessLiteral('%')),
      pp.CaselessLiteral('+') | pp.CaselessLiteral('-'),
      pp.CaselessLiteral('>>') | pp.CaselessLiteral('<<'),
      (pp.CaselessLiteral('<=') | pp.CaselessLiteral('>=') |
       pp.CaselessLiteral('<') | pp.CaselessLiteral('>')),
      (pp.CaselessLiteral('==') | pp.CaselessLiteral('=') |
       pp.CaselessLiteral('!=')),
      pp.CaselessKeyword('is') | pp.CaselessKeyword('contains'),
      pp.CaselessLiteral('&'),
      pp.CaselessLiteral('^'),
      pp.CaselessLiteral('|'),
      pp.CaselessKeyword('and'),
      pp.CaselessKeyword('or'),
  ]

  # Take the operator set of the most precedence that has not been parsed.
  # Find and collapse all operators of the set. Thus, order of operations
  # is not broken. Equivalent to recursive descent parsing.
  # Below code is equivalent to:
  # expression = expression + pp.ZeroOrMore(op_level1 + expression)
  # expression = expression + pp.ZeroOrMore(op_level2 + expression)
  # ...
  for operator_set in binary_operators:
    # Represents _i-1 ai part of expression that is added to current expression.
    operator_expression = operator_set + current_expression
    # Push only the operator, both atoms will have already been pushed.
    operator_expression.setParseAction(PushBinaryOperator)
    # pylint: disable=g-no-augmented-assignment
    current_expression = (current_expression +
                          pp.ZeroOrMore(operator_expression))

  # pylint: disable=pointless-statement
  full_expression << current_expression
  return full_expression


def _EBQParser(clauses):
  """Defines the entire EBQ query.

  Actions only occur when parseString is called on the BNF returned.
  All actions will modify the original dictionary passed in as the argument
  when the BNF was generated.

  The dictionary will map clause names to their respective argument. Below
  each clause's argument arrangement is explained:

  SELECT:
  List of postfix expressions (one for each comma separated expression).
  Each postfix expression is also a stack represented using a list.
  a, b + 1 --> [['a'], ['b', '1', '+']]

  FROM:
  List of table names.
  table1, table2, ... --> [table1, table2, ...]

  WITHIN:
  Dictionary mapping expression indices to within modifiers.
  expr0 within a, expr1, expr2 within b --> {0: 'a', 2: 'b'}

  AS:
  Dictionary mapping expression indices to aliases.
  expr0 as a, expr1, expr as b --> {0: 'a', 2: 'b'}

  WHERE:
  List containing single postfix expression.
  WHERE a < 1 --> ['a', '1', '<']

  HAVING:
  List containing single postfix expression.
  HAVING SUM(Year) < 1 --> ['Year', 'AGGREGATION_1_SUM', '1', '<']

  GROUP BY:
  List of fields.
  GROUP BY f1, f2, ... --> [f1, f2, ...]

  ORDER BY:
  List of fields.
  ORDER BY f1, f2, ... --> [f1, f2, ...]

  LIMIT:
  List of a single integer.
  LIMIT n --> [n]

  Arguments:
    clauses: Dictionary containing clause name to arguments. Originally, all
      arguments have initial, empty values.

  Returns:
    A BNF of a EBQ query.
  """

  def AddAll(tokens):
    temp_stack.append(''.join(tokens))

  def AddArgument(tokens):
    clauses[tokens[0]].extend(temp_stack)
    temp_stack[:] = []

  def AddSelectArgument():
    clauses['SELECT'].append(list(temp_stack))
    temp_stack[:] = []

  def AddLabel(tokens):
    temp_stack.append(tokens[0])

  def AddAlias(tokens):
    clauses['AS'][len(clauses['SELECT'])] = tokens[0]

  def AddInteger(tokens):
    temp_stack.append(int(tokens[0]))

  def AddLast(tokens):
    temp_stack[len(temp_stack) - 1] += ' ' + tokens[0]

  def AddWithin(tokens):
    clauses['WITHIN'][len(clauses['SELECT'])] = tokens[0]

  def AddJoinArgument(tokens):
    clauses[tokens[0]].append(list(temp_stack))
    temp_stack[:] = []

  temp_stack = []

  as_kw = pp.CaselessKeyword('AS')
  select_kw = pp.CaselessKeyword('SELECT')
  within_kw = pp.CaselessKeyword('WITHIN')
  flatten_kw = pp.CaselessKeyword('FLATTEN')
  from_kw = pp.CaselessKeyword('FROM')
  join_kw = pp.CaselessKeyword('JOIN')
  join_on_kw = pp.CaselessKeyword('ON')
  where_kw = pp.CaselessKeyword('WHERE')
  having_kw = pp.CaselessKeyword('HAVING')
  order_kw = pp.CaselessKeyword('ORDER BY')
  asc_kw = pp.CaselessKeyword('ASC')
  desc_kw = pp.CaselessKeyword('DESC')
  group_kw = pp.CaselessKeyword('GROUP BY')
  limit_kw = pp.CaselessKeyword('LIMIT')

  push_label = pp.Word(
      pp.alphas, pp.alphas + pp.nums + '_' + '.').setParseAction(AddLabel)
  pos_int = pp.Word(pp.nums).setParseAction(AddInteger)
  order_label = (push_label +
                 pp.Optional((asc_kw | desc_kw).setParseAction(AddLast)))
  label = pp.Word(pp.alphas, pp.alphas + pp.nums + '_' + '.')
  alias_label = pp.Word(
      pp.alphas, pp.alphas + pp.nums + '_' + '.').setParseAction(AddAlias)
  within_label = pp.Word(
      pp.alphas, pp.alphas + pp.nums + '_' + '.').setParseAction(AddWithin)

  math_expr = _MathParser(temp_stack)
  within_expr = math_expr + pp.Optional(within_kw + within_label)
  alias_expr = within_expr + pp.Optional((
      (as_kw + alias_label) |
      (~from_kw + ~where_kw + ~group_kw + ~having_kw + ~order_kw + ~limit_kw +
       alias_label)))
  select_expr = (
      (select_kw + alias_expr).setParseAction(AddSelectArgument) +
      pp.ZeroOrMore((pp.Literal(',') + alias_expr).setParseAction(
          AddSelectArgument)))
  flatten_expr = (pp.OneOrMore(pp.Literal('(') + flatten_kw + pp.Literal('(') +
                               label + pp.Literal(',') + label +
                               pp.Literal(')') + pp.Literal(
                                   ')'))).setParseAction(AddAll)
  from_expr = select_expr + pp.Optional((
      from_kw + (flatten_expr | push_label)).setParseAction(AddArgument))
  join_expr = from_expr + pp.ZeroOrMore((
      join_kw + push_label + join_on_kw +
      _MathParser(temp_stack)).setParseAction(AddJoinArgument))
  where_expr = join_expr + pp.Optional((
      where_kw + _MathParser(temp_stack)).setParseAction(AddArgument))
  group_expr = where_expr + pp.Optional((
      group_kw + push_label + pp.ZeroOrMore(
          pp.Literal(',') + push_label)).setParseAction(AddArgument))
  having_expr = group_expr + pp.Optional((
      having_kw + _MathParser(temp_stack)).setParseAction(AddArgument))
  order_expr = having_expr + pp.Optional((
      order_kw + order_label + pp.ZeroOrMore(
          pp.Literal(',') + order_label)).setParseAction(AddArgument))
  limit_expr = order_expr + pp.Optional((
      limit_kw + pos_int).setParseAction(AddArgument))
  entire_expr = limit_expr + pp.StringEnd()

  return entire_expr


def ParseQuery(query):
  """Parses the entire query.

  Arguments:
    query: The command the user sent that needs to be parsed.

  Returns:
    Dictionary mapping clause names to their arguments.

  Raises:
    bigquery_client.BigqueryInvalidQueryError: When invalid query is given.
  """
  clause_arguments = {
      'SELECT': [],
      'AS': {},
      'WITHIN': {},
      'FROM': [],
      'JOIN': [],
      'WHERE': [],
      'GROUP BY': [],
      'HAVING': [],
      'ORDER BY': [],
      'LIMIT': [],
  }
  try:
    _EBQParser(clause_arguments).parseString(query)
  except ValueError as e:
    raise bigquery_client.BigqueryInvalidQueryError(e, None, None, None)
  return clause_arguments
