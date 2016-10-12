#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Functions that interpret postfix expressions into useful information."""



import math
import operator
import re


import bigquery_client
import common_util as util
import ebq_crypto as ecrypto

# =============================================================================
# = Built-in Bigquery operators.
# =============================================================================
_UNARY_OPERATORS = {
    'not': lambda a: not a,
    '~': operator.invert
    }

_BINARY_OPERATORS = {
    '+': operator.add,
    '-': operator.sub,
    '*': operator.mul,
    '/': operator.truediv,
    '%': operator.mod,
    '<': operator.lt,
    '<=': operator.le,
    '>': operator.gt,
    '>=': operator.ge,
    '=': operator.eq,
    '==': operator.eq,
    '!=': operator.ne,
    '<<': operator.lshift,
    '>>': operator.rshift,
    'and': lambda a, b: a and b,
    'or': lambda a, b: a or b,
    '&': operator.and_,
    '|': operator.or_,
    '^': operator.xor,
    'contains': lambda a, b: a in b,
    'is': lambda a, b: a is b,
    }

# =============================================================================
# = Built-in Bigquery functions.
# =============================================================================
_ZERO_ARGUMENT_FUNCTIONS = {
    'pi': math.pi,
    'current_date': util.CurrentDate(),
    'current_time': util.CurrentTime(),
    'current_timestamp': util.CurrentTimestamp(),
    'now': util.Now(),
    }

_ONE_ARGUMENT_FUNCTIONS = {
    'abs': abs,
    'acos': math.acos,
    'acosh': math.acosh,
    'asin': math.asin,
    'asinh': math.asinh,
    'atan': math.atan,
    'atanh': math.atanh,
    'ceil': math.ceil,
    'cos': math.cos,
    'cosh': math.cosh,
    'degrees': math.degrees,
    'floor': math.floor,
    'ifnull': lambda a, b: a if not (a is None) else b,
    'ln': math.log,
    'log': math.log,
    'log2': lambda a: math.log(a, 2),
    'log10': lambda a: math.log(a, 10),
    'radians': math.radians,
    'round': round,
    'sin': math.sin,
    'sinh': math.sinh,
    'sqrt': math.sqrt,
    'tan': math.tan,
    'tanh': math.tanh,
    'boolean': bool,
    'float': float,
    'hex_string': hex,
    'integer': int,
    'string': str,
    'is_inf': math.isinf,
    'is_nan': math.isnan,
    'is_explicitly_defined': lambda a: True if a else False,
    'length': len,
    'lower': lambda a: a.lower(),
    'upper': lambda a: a.upper(),
    'date': util.Date,
    'day': util.Day,
    'dayofweek': util.DayOfWeek,
    'dayofyear': util.DayOfYear,
    'format_utc_usec': util.FormatUTCUsec,
    'hour': util.Hour,
    'minute': util.Minute,
    'month': util.Month,
    'msec_to_timestamp': util.MsecToTimestamp,
    'parse_utc_usec': util.ParseUTCUsec,
    'quarter': util.Quarter,
    'sec_to_timestamp': util.SecToTimestamp,
    'second': util.Second,
    'time': util.Time,
    'timestamp': util.Timestamp,
    'timestamp_to_msec': util.TimestampToMsec,
    'timestamp_to_sec': util.TimestampToSec,
    'timestamp_to_usec': util.TimestampToUsec,
    'from_base64': lambda x: 'FROM_BASE64("%s")' % x,  # client unimplemented
    'to_base64': util.ToBase64,
    'usec_to_timestamp': util.UsecToTimestamp,
    'utc_usec_to_day': util.UTCUsecToDay,
    'utc_usec_to_hour': util.UTCUsecToHour,
    'utc_usec_to_month': util.UTCUsecToMonth,
    'utc_usec_to_week': util.UTCUsecToWeek,
    'utc_usec_to_year': util.UTCUsecToYear,
    'year': util.Year,
    'format_ip': util.FormatIP,
    'parse_ip': util.ParseIP,
    'format_packed_ip': util.FormatPackedIP,
    'parse_packed_ip': util.ParsePackedIP,
    'host': util.Host,
    'domain': util.Domain,
    'tld': util.Tld,
    }

_TWO_ARGUMENT_FUNCTIONS = {
    'pow': pow,
    'atan2': math.atan2,
    'regexp_match': re.match,
    'regexp_extract': util.RegexpExtract,
    'concat': operator.concat,
    'left': lambda a, b: a[:min(b, len(a))],
    'right': lambda a, b: a[min(b, len(a)):],
    'datediff': util.DateDiff,
    'strftime_utc_usec': util.StrfTimeUTCUsec,
    }

_THREE_ARGUMENT_FUNCTIONS = {
    'substr': lambda a, b, c: a[b:min(len(a), b+c)],
    'if': lambda a, b, c: b if a else b,
    'date_add': util.DateAdd,
    'regexp_replace': lambda a, b, c: re.sub(b, c, a),
    'rpad': util.RightPad,
    'lpad': util.LeftPad,
    }

# Functions that return type of string.
_STRING_RETURN_FUNCTIONS = [
    'hex_string',
    'string',
    'current_date',
    'current_time',
    'current_timestamp',
    'date',
    'date_add',
    'format_utc_usec',
    'msec_to_timestamp',
    'sec_to_timestamp',
    'strftime_utc_usec',
    'time',
    'timestamp',
    'usec_to_timestamp',
    'format_ip',
    'format_packed_ip',
    'parse_packed_ip',
    'regexp_match',
    'regexp_extract',
    'regexp_replace',
    'concat',
    'left',
    'lower',
    'lpad',
    'right',
    'rpad',
    'substr',
    'upper',
    'host',
    'domain',
    'tld',
    ]


def RewriteSelectionCriteria(stack, schema, master_key, table_id):
  """Rewrites selection criteria (arguments of WHERE and HAVING clause).

  Arguments:
    stack: The postfix expression that is the where/having expression.
    schema: The user defined values and encryption.
    master_key: Used to get ciphers for encryption.
    table_id: Used to generate a proper key.

  Returns:
    An infix version of the <stack>. The expression is rewritten so that it
    can be sent to the BigQuery server.

  Raises:
    bigquery_client.BigqueryInvalidQueryError: If the expression is invalid
    (such as searching non-searchable encrypted fields, etc).
  """

  pseudonym_cipher = ecrypto.PseudonymCipher(
      ecrypto.GeneratePseudonymCipherKey(master_key, table_id))
  string_hasher = ecrypto.StringHash(
      ecrypto.GenerateStringHashKey(master_key, table_id))

  pseudonym_ciphers = {}

  for field in schema:
    if (field.get('encrypt', '') in ['pseudonym'] and
        field.get('related', None) is not None):
      pseudonym_ciphers[field['related']] = ecrypto.PseudonymCipher(
          ecrypto.GeneratePseudonymCipherKey(master_key, field['related']))

  def FailIfEncrypted(tokens):
    if util.IsEncryptedExpression(tokens):
      raise bigquery_client.BigqueryInvalidQueryError(
          'Invalid where/having expression.', None, None, None)

  def FailIfDeterministic(tokens):
    if util.IsDeterministicExpression(tokens):
      raise bigquery_client.BigqueryInvalidQueryError(
          'Cannot do equality on probabilistic encryption, '
          'only pseudonym encryption.', None, None, None)

  def RewritePseudonymEncryption(token, op2=None):
    if isinstance(token, util.StringLiteralToken):
      if op2 is not None and getattr(op2, 'related', None) is not None:
        if pseudonym_ciphers.get(op2.related, None) is not None:
          return '"%s"' % pseudonym_ciphers[op2.related].Encrypt(
              unicode(token[1:-1]))
        else:
          raise bigquery_client.BigqueryInvalidQueryError(
              'Cannot process token with related attribute in schema without '
              'matching related attribute', None, None, None)
      else:
        return '"%s"' % pseudonym_cipher.Encrypt(unicode(token[1:-1]))
    else:
      return token

  def RewriteSearchwordsEncryption(field, literal):
    """Rewrites the literal such that it can be checked for containment.

    Arguments:
      field: The field which is being checked if literal is contained within.
      literal: Substring being searched for.

    Returns:
      A tuple containing both field and literal rewritten.

    Raises:
      ValueError: Try to rewrite non-searchwords encryption.
    """
    if (not isinstance(field, util.SearchwordsToken) and
        not isinstance(field, util.ProbabilisticToken)):
      raise ValueError('Invalid encryption to check containment.')
    field = field.original_name
    row = util.GetEntryFromSchema(field, schema)
    modified_field = util.SEARCHWORDS_PREFIX + row['name']
    field = field.split('.')
    field[-1] = modified_field
    modified_field = '.'.join(field)
    if 'searchwords_separator' in row:
      searchwords_separator = row['searchwords_separator']
    else:
      searchwords_separator = None
    word_list = ecrypto.CleanUnicodeString(
        unicode(literal.value), separator=searchwords_separator)
    if searchwords_separator is None:
      word_seq = ' '.join(word_list)
    else:
      word_seq = searchwords_separator.join(word_list)
    keyed_hash = (u'\'%s\''
                  % string_hasher.GetStringKeyHash(
                      modified_field.split('.')[-1], word_seq))
    modified_string = (
        u'to_base64(left(bytes(sha1(concat(left(%s, 24), %s))), 8))'
        % (modified_field, keyed_hash))
    return (modified_field, modified_string)

  def CheckSearchableField(op1):
    """Checks if the operand is a searchable encrypted field.

    Arguments:
      op1: The operand that is being checked if it is searchable.

    Returns:
      True iff op1 is searchable.
    """
    if isinstance(op1, util.SearchwordsToken):
      return True
    elif not isinstance(op1, util.ProbabilisticToken):
      return False
    op1 = op1.original_name
    row = util.GetEntryFromSchema(op1, schema)
    if row['encrypt'] in ['probabilistic_searchwords', 'searchwords']:
      return True
    else:
      return False
    return False

  def RewriteContainsOrFail(op1, op2):
    """Tries to rewrite a contains expression.

    Arguments:
      op1: The first operand of the contains binary operator.
      op2: The second operand of the contians binary operator.

    Returns:
      The rewritten versions of both operands.

    Raises:
      bigquery_client.BigqueryInvalidQueryError: If the contains expressions
      is invalid.
    """
    if not isinstance(op1, util.EncryptedToken):
      return (op1, op2)
    if not CheckSearchableField(op1):
      raise bigquery_client.BigqueryInvalidQueryError(
          'Cannot do contains on an encrypted field that is not searchable.',
          None, None, None)
    elif not isinstance(op2, util.StringLiteralToken):
      raise bigquery_client.BigqueryInvalidQueryError(
          'The substring to be checked must be a literal.', None, None, None)
    return RewriteSearchwordsEncryption(op1, op2)

  def CheckAndRewriteStack(postfix):
    if not postfix:
      raise bigquery_client.BigqueryInvalidQueryError(
          'Not enough arguments.', None, None, None)
    top = postfix.pop()
    if isinstance(top, util.OperatorToken):
      args = []
      for unused_i in range(top.num_args):
        args.append(CheckAndRewriteStack(postfix))
      args.reverse()
      if top.num_args == 1:
        return '%s %s' % (str(top), args[0])
      elif str(top) in ['=', '==', '!=']:
        FailIfDeterministic(args)
        if (isinstance(args[0], util.PseudonymToken) or
            isinstance(args[1], util.PseudonymToken)):
          args[0] = RewritePseudonymEncryption(args[0], args[1])
          args[1] = RewritePseudonymEncryption(args[1], args[0])
      elif str(top) == 'contains':
        FailIfEncrypted([args[1]])
        args[0], args[1] = RewriteContainsOrFail(args[0], args[1])
      else:
        FailIfEncrypted(args)
      return '(%s %s %s)' % (args[0], str(top), args[1])
    elif isinstance(top, util.BuiltInFunctionToken):
      func_name = str(top)
      if func_name in _ZERO_ARGUMENT_FUNCTIONS:
        return '%s()' % func_name
      elif func_name in _ONE_ARGUMENT_FUNCTIONS:
        op = CheckAndRewriteStack(postfix)
        FailIfEncrypted([op])
        return '%s(%s)' % (func_name, op)
      elif func_name in _TWO_ARGUMENT_FUNCTIONS:
        op2 = CheckAndRewriteStack(postfix)
        op1 = CheckAndRewriteStack(postfix)
        FailIfEncrypted([op1, op2])
        return '%s(%s, %s)' % (func_name, op1, op2)
      elif func_name in _THREE_ARGUMENT_FUNCTIONS:
        op3 = CheckAndRewriteStack(postfix)
        op2 = CheckAndRewriteStack(postfix)
        op1 = CheckAndRewriteStack(postfix)
        FailIfEncrypted([op1, op2, op3])
        return '%s(%s, %s, %s)' % (func_name, op1, op2, op3)
      else:
        raise bigquery_client.BigqueryInvalidQueryError(
            '%s function does not exist.' % func_name, None, None, None)
    elif not isinstance(top, basestring):
      return str(top)
    else:
      return top

  temp_stack = list(stack)
  new_expression = CheckAndRewriteStack(temp_stack)
  if temp_stack:
    raise bigquery_client.BigqueryInvalidQueryError(
        'Too many arguments.', None, None, None)
  return new_expression


def Evaluate(stack):
  """Evaluates the postfix stack to find the result of the expression.

  The <stack> is the expression to be resolved in postfix notation.

  Arguments:
    stack: Postfix notation expression whose result is wanted. The <stack>
    will be modified as elements are popped off.

  Raises:
    ValueError: Too many arguments provided for functions/operators.

  Returns:
    The resulting value after resolving the postfix expression.
  """

  def Resolve(stack):
    """Resolves the postfix stack and evaluates the expression into one value.

    The <stack> is the expression in postfix notation.

    Arguments:
      stack: Postfix notation to be resolved.

    Raises:
      ValueError: If an invalid function name is given or not enough arguments
      are provided for an operator/function.

    Returns:
      The resolution of the postfix notation.
    """
    if not stack:
      raise bigquery_client.BigqueryInvalidQueryError(
          'Not enough arguments.', None, None, None)
    top = stack.pop()
    if isinstance(top, util.OperatorToken):
      args = []
      for unused_i in range(top.num_args):
        args.append(Resolve(stack))
      args.reverse()
      if None in args:
        return None
      if top.num_args == 1:
        return _UNARY_OPERATORS[str(top)](*args)
      else:
        try:
          return _BINARY_OPERATORS[top](*args)
        except ZeroDivisionError:
          raise bigquery_client.BigqueryInvalidQueryError(
              'Division by zero.', None, None, None)
    elif isinstance(top, util.BuiltInFunctionToken):
      func_name = str(top)
      if func_name in _ZERO_ARGUMENT_FUNCTIONS:
        result = _ZERO_ARGUMENT_FUNCTIONS[func_name]
      elif func_name in _ONE_ARGUMENT_FUNCTIONS:
        op = Resolve(stack)
        if op is None:
          result = None
        result = _ONE_ARGUMENT_FUNCTIONS[func_name](op)
      elif func_name in _TWO_ARGUMENT_FUNCTIONS:
        op2 = Resolve(stack)
        op1 = Resolve(stack)
        if op1 is None or op2 is None:
          result = None
        result = _TWO_ARGUMENT_FUNCTIONS[func_name](op1, op2)
      elif func_name in _THREE_ARGUMENT_FUNCTIONS:
        op3 = Resolve(stack)
        op2 = Resolve(stack)
        op1 = Resolve(stack)
        if op1 is None or op2 is None or op3 is None:
          result = None
        result = _THREE_ARGUMENT_FUNCTIONS[func_name](op1, op2, op3)
      else:
        raise bigquery_client.BigqueryInvalidQueryError(
            'No function ' + func_name + ' exists.', None, None, None)
      return result
    elif isinstance(top, util.FieldToken):
      raise bigquery_client.BigqueryInvalidQueryError(
          '%s does not exist as a column.' % str(top), None, None, None)
    elif isinstance(top, util.LiteralToken):
      return top.value
    else:
      return top

  result = Resolve(stack)
  if stack:
    raise bigquery_client.BigqueryInvalidQueryError(
        'Invaid number of arguments.', None, None, None)
  return result


def _ConvertStack(postfix):
  """Convert postfix stack to infix string.

  Arguments:
    postfix: A stack in postfix notation. The postfix stack will be modified
    as elements are being popped from the top.

  Raises:
    ValueError: There are not enough arguments for functions/operators.

  Returns:
    A string of the infix represetation of the stack.
  """
  if not postfix:
    raise bigquery_client.BigqueryInvalidQueryError(
        'Not enough arguments.', None, None, None)
  top = postfix.pop()
  if isinstance(top, util.OperatorToken):
    args = []
    for unused_i in range(top.num_args):
      args.append(_ConvertStack(postfix))
    args.reverse()
    if top.num_args == 1:
      return '%s %s' % (str(top), args[0])
    else:
      return '(%s %s %s)' % (args[0], str(top), args[1])
  elif isinstance(top, util.BuiltInFunctionToken):
    func_name = str(top)
    if func_name in _ZERO_ARGUMENT_FUNCTIONS:
      return '%s()' % func_name
    elif func_name in _ONE_ARGUMENT_FUNCTIONS:
      op = _ConvertStack(postfix)
      return '%s(%s)' % (func_name, op)
    elif func_name in _TWO_ARGUMENT_FUNCTIONS:
      op2 = _ConvertStack(postfix)
      op1 = _ConvertStack(postfix)
      return '%s(%s, %s)' % (top, op1, op2)
    elif func_name in _THREE_ARGUMENT_FUNCTIONS:
      op3 = _ConvertStack(postfix)
      op2 = _ConvertStack(postfix)
      op1 = _ConvertStack(postfix)
      return '%s(%s, %s, %s)' % (top, op1, op2, op3)
    else:
      raise bigquery_client.BigqueryInvalidQueryError(
          'Function %s does not exist.' % str(top), None, None, None)
  elif isinstance(top, util.AggregationFunctionToken):
    num_args = top.num_args
    func_name = str(top)
    ops = []
    for unused_i in range(int(num_args)):
      ops.append(_ConvertStack(postfix))
    ops.reverse()
    if func_name == 'DISTINCTCOUNT':
      func_name = 'COUNT'
      ops[0] = 'DISTINCT ' + ops[0]
    ops = [str(op) for op in ops]
    return func_name + '(' + ', '.join(ops) + ')'
  elif not isinstance(top, basestring):
    return str(top)
  else:
    return top


def ToInfix(stack):
  """Converts a postfix notation stack into an infix string.

  Arguments:
    stack: Postfix notation that is being converted. <stack> is going to be
    modified as elements are being popped off.

  Raises:
    ValueError: Too many arguments for functions/operators in the stack.

  Returns:
    String of expression in infix notation.
  """
  infix = _ConvertStack(stack)
  if stack:
    raise bigquery_client.BigqueryInvalidQueryError(
        'Invalid number of arguments.', None, None, None)
  return infix


def CheckValidSumAverageArgument(stack):
  """Checks if stack is a proper argument for SUM/AVG.

  This recursive algorithm performs tainting. It uses a special structure to
  store data which is as follows:

  s = [list of postfix expressions, taint1, taint2]

  The list of postfix expressiions all added together is equivalent to the
  expanded version of <stack>.
  taint1 represents whether s contains an encrypted field.
  taint2 represents whether s contains any field. taint1 is true iff s contains
  any field (encrypted or unencrypted).

  This algorithm fails if any encrypted field is multipled/divided by any other
  field (either encrypted or unencrypted).

  Arguments:
    stack: The postfix expression that is being checked if valid for SUM/AVG
    argument.

  Returns:
    A tuple containing a list of postfix expressions, and two types of taints.
    Representing whether a field is in s and an encrypted field is in s.

  Raises:
    bigquery_client.BigqueryInvalidQueryError: Thrown iff <stack> is not a valid
    linear expression (or one we cannot compute) that can be a SUM/AVG argument.
  """
  top = stack.pop()
  if ((isinstance(top, util.OperatorToken) and top.num_args == 1) or
      isinstance(top, util.BuiltInFunctionToken) or
      isinstance(top, util.AggregationFunctionToken) or
      isinstance(top, util.LiteralToken)):
    raise bigquery_client.BigqueryInvalidQueryError(
        'Invalid SUM arguments. %s is not supported' %top, None, None, None)
  elif top in ['+', '-']:
    op2 = CheckValidSumAverageArgument(stack)
    op1 = CheckValidSumAverageArgument(stack)
    list_fields = list(op1[0])
    if top == '-':
      for i in range(len(op2[0])):
        op2[0][i].extend([-1, util.OperatorToken('*', 2)])
    for i in range(len(op2[0])):
      list_fields.append(op2[0][i])
    return [list_fields, op1[1] or op2[1], op1[2] or op2[2]]
  elif top == '*':
    op2 = CheckValidSumAverageArgument(stack)
    op1 = CheckValidSumAverageArgument(stack)
    if (op1[1] and (op2[1] or op2[2])) or (op2[1] and (op1[1] or op1[2])):
      raise bigquery_client.BigqueryInvalidQueryError(
          'Invalid AVG/SUM argument. An encrypted field is multipled by another'
          ' field.', None, None, None)
    list_fields = []
    for field1 in op1[0]:
      for field2 in op2[0]:
        value = list(field1)
        value.extend(field2)
        value.append(util.OperatorToken('*', 2))
        list_fields.append(value)
    return [list_fields, op1[1] or op2[1], op1[2] or op2[2]]
  elif top == '/':
    op2 = CheckValidSumAverageArgument(stack)
    op1 = CheckValidSumAverageArgument(stack)
    if op2[1] or (op1[1] and op2[2]):
      raise bigquery_client.BigqueryInvalidQueryError(
          'Division by/of an encrypted field: not a linear function.', None,
          None, None)
    append_divisor = []
    for field in op2[0]:
      append_divisor.extend(field)
    for i in range(len(op2[0]) - 1):
      append_divisor.append(util.OperatorToken('+', 2))
    append_divisor.append(util.OperatorToken('/', 2))
    list_fields = list(op1[0])
    for i in xrange(len(list_fields)):
      list_fields[i].extend(append_divisor)
    return [list_fields, op1[1], op1[2] or op2[2]]
  else:
    if (isinstance(top, util.PseudonymToken) or
        isinstance(top, util.SearchwordsToken) or
        isinstance(top, util.ProbabilisticToken)):
      raise bigquery_client.BigqueryInvalidQueryError(
          'Cannot do SUM/AVG on non-homomorphic encryption.', None, None, None)
    is_encrypted = (
        isinstance(top, util.HomomorphicIntToken) or
        isinstance(top, util.HomomorphicFloatToken))
    return [[[top]], is_encrypted, not util.IsFloat(top)]


def ExpandExpression(stack):
  """Convert stack into the form of factor * field + constant.

  Arguments:
    stack: The postfix expression wished to be expanded.

  Returns:
    The infix string of stack iff stack is unencrypted. Otherwise, expands
    stack into the form of factor * field + constant.
  """

  def IsEncrypted(tokens):
    for token in tokens:
      if util.IsEncrypted(token):
        return True
    return False

  if not IsEncrypted(stack):
    return [[[1.0, ToInfix(stack)]], 0.0]
  return _ExpandExpression(stack)


def _ExpandExpression(stack):
  """Expands the postfix versions of stack into an expression.

  This whole recursive function depends on a very complex data structure that
  is used to represent any linear expression. The data structure is as follows:

  [list of pairs, constant]

  For example the polynomial, ax + by + cz + d (a, b, c, d are integers and
  x, y, z are fields) is represented by the data structure as follows:

  [[[a, x], [b, y], [c, z]], d]

  The list of pairs represents variables and their constant factor. The first
  element of the pair will be the constant while the second element will
  be the actual field. The constant is just the constant factor of the
  expression.

  Now, I will explain the definition of addition, subtraction, multiplication
  and division for this data structure.

  Addition/Subtraction:
  To perform this, the list of pairs from the first operand is taken. Then,
  we iterate through the list of pairs in the second list, we try to find
  the field of each respective pair in the list of the first operand. If it
  is found, the constant factor of that pair is updated in the first list.
  Otherwise, a new pair is appended (the constant factor is negated if
  subtraction is occurring). Finally, the constants are added/subtracted.

  Example:
  s1 = x + 2y + 3 = [[[1.0, x], [2.0, y]], 3.0]
  s2 = 1.5x + 4z + 1 = [[[1.5, x], [4.0, z]], 1.0]

  s1 + s2 = 2.5x + 2y + 4z + 4 = [[[2.5, x], [2.0, y], [4.0, z]], 4.0]

  Multiplication:
  Only a specific type of multiplication can occur. A constant multiplied by
  a linear expressions. If two linear expressions are multiplied, it is no
  longer linear, so an error is raised. So we check and assure that at least one
  list is empty. Then, we take the constant factor of the empty listed operand
  and multiply each constant factor and the constant of the other operand.

  Example of failure (multiplication of two fields):
  s1 = x + 3
  s2 = y

  Example of good calculation:
  s1 = x + 2y + 1 = [[[1.0, x], [2.0, y]], 1.0]
  s2 = 3 = [[], 3.0]

  s1 * s2 = [[[3.0, x], [6.0, y]], 3.0]

  Division:
  For division, the denominator must be a constant and not contain any fields.
  If the denominator is not a constant, an exception is raised. Otherwise, we
  take the constant and just divide each constant factor and constant in the
  numerator.

  Example of failure (denominator is not a constant):
  s1 = x + 1
  s2 = y

  Example of good calculation:
  s1 = 2x + 4y + 6 = [[[2.0, x], [4.0, y]], 6.0]
  s2 = 2 = [[], 2.0]

  s1 / s2 = [[[1.0, x], [2.0, y]], 3.0]

  Arguments:
    stack: Postfix expression that you want to expand.

  Returns:
    The above described data structure representing the expression.

  Raises:
    bigquery_client.BigqueryInvalidQueryError: If the stack is not linear or
    if there are invalid arguments.
  """

  top = stack.pop()
  if ((isinstance(top, util.OperatorToken) and top.num_args == 1) or
      isinstance(top, util.BuiltInFunctionToken) or
      isinstance(top, util.AggregationFunctionToken) or
      isinstance(top, util.LiteralToken)):
    raise bigquery_client.BigqueryInvalidQueryError(
        'Invalid SUM arguments. %s is not supported' % top, None, None, None)
  elif top in ['+', '-']:
    op2 = _ExpandExpression(stack)
    op1 = _ExpandExpression(stack)
    list_fields = list(op1[0])
    for token in op2[0]:
      found = False
      for i in range(len(list_fields)):
        if token[1] == list_fields[i][1]:
          found = True
          list_fields[i][0] = _BINARY_OPERATORS[top](list_fields[i][0],
                                                     token[0])
          break
      if not found:
        if top == '-':
          token[0] *= -1
        list_fields.append(token)
    return [list_fields, _BINARY_OPERATORS[top](op1[1], op2[1])]
  elif top == '*':
    op2 = _ExpandExpression(stack)
    op1 = _ExpandExpression(stack)
    if op1[0] and op2[0]:
      raise bigquery_client.BigqueryInvalidQueryError(
          'Not a linear function. Two fields are being multipled.', None, None,
          None)
    list_fields = list(op1[0])
    list_fields.extend(list(op2[0]))
    if not op1[0]:
      for fields in list_fields:
        fields[0] *= op1[1]
    else:
      for fields in list_fields:
        fields[0] *= op2[1]
    return [list_fields, op1[1] * op2[1]]
  elif top == '/':
    op2 = _ExpandExpression(stack)
    op1 = _ExpandExpression(stack)
    if op2[0]:
      raise bigquery_client.BigqueryInvalidQueryError(
          'Division by a label: not a linear function.', None, None, None)
    list_fields = list(op1[0])
    try:
      for fields in list_fields:
        fields[0] /= op2[1]
      return [list_fields, op1[1] / op2[1]]
    except ZeroDivisionError:
      raise bigquery_client.BigqueryInvalidQueryError(
          'Division by zero.', None, None, None)
  elif util.IsFloat(top):
    return [[], float(top)]
  else:
    if (isinstance(top, util.PseudonymToken) or
        isinstance(top, util.SearchwordsToken) or
        isinstance(top, util.ProbabilisticToken)):
      raise bigquery_client.BigqueryInvalidQueryError(
          'Cannot do SUM/AVG on non-homomorphic encryption.', None, None, None)
    return [[[1.0, top]], 0.0]


def GetSingleValue(stack):
  """Function that is used to extract the single top function argument.

  Arguments:
    stack: The stack from where the single argument is to be extracted.

  Returns:
    A tuple that contains the index of the leftmost element not extracted
    and the postfix expression in a stack of the topmost argument value.
  """
  temp_stack = list(stack)
  # This method will try and return the infix string of the topmost value.
  # However, we are more interested in the modifications to the stack. The
  # method will pop off all elements needed to extract the topmost value.
  _ConvertStack(temp_stack)
  start_idx = len(temp_stack)
  return start_idx, list(stack[start_idx:])
