#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Contains common constants and functions for ebq client tool."""



import base64
import datetime
import re
import time

import ipaddr

import bigquery_client

# This version is written into the table description.
# The ebq client compares this value to the one stored there when performing
# ebq operations.
# Incrementing it will e.g. break compatibility between old clients and
# newer datasets.
EBQ_TABLE_VERSION = '1.0'

# Adds distinction to avoid collisions between internal naming and user naming.
_DISTINCT_STRING = 'p698000442118338_'

# =============================================================================
# = EBQ prefixes.
# =============================================================================
# Rename encrypted fields based on their encryption type.
HOMOMORPHIC_FLOAT_PREFIX = _DISTINCT_STRING + 'HOMOMORPHIC_FLOAT_'
HOMOMORPHIC_INT_PREFIX = _DISTINCT_STRING + 'HOMOMORPHIC_INT_'
PROBABILISTIC_PREFIX = _DISTINCT_STRING + 'PROBABILISTIC_'
PSEUDONYM_PREFIX = _DISTINCT_STRING + 'PSEUDONYM_'
SEARCHWORDS_PREFIX = _DISTINCT_STRING + 'SEARCHWORDS_'

ENCRYPTED_FIELD_PREFIXES = [
    HOMOMORPHIC_FLOAT_PREFIX,
    HOMOMORPHIC_INT_PREFIX,
    PROBABILISTIC_PREFIX,
    PSEUDONYM_PREFIX,
    SEARCHWORDS_PREFIX,
]

# Rename aggregation and built-in function tokens with prefix.
# Allows us to distinguish field names and actual functions.
AGGREGATION_PREFIX = _DISTINCT_STRING + 'AGGREGATION_'
FUNCTION_PREFIX = _DISTINCT_STRING + 'FUNCTION_'

# BigQuery renames aggregations and expressions to f0_, f1_, ...
# Following this convention, EBQ renames unencrypted queries to ue0_, ue1_, ...
UNENCRYPTED_ALIAS_PREFIX = _DISTINCT_STRING + 'ue'

GROUP_CONCAT_PREFIX = 'GROUP_CONCAT('
PAILLIER_SUM_PREFIX = 'TO_BASE64(BYTES(PAILLIER_SUM(FROM_BASE64('

# =============================================================================
# = EBQ miscellaneous constants.
# =============================================================================
COUNT_STAR = _DISTINCT_STRING + 'COUNT_STAR'
PAILLIER_SUM_STRING = 'TO_BASE64(BYTES(PAILLIER_SUM(FROM_BASE64(%s), \'%s\')))'
# EPR stands for ebq period replacement.
PERIOD_REPLACEMENT = '_' + _DISTINCT_STRING + 'EPR_'

AGGREGATION_FUNCTIONS = [
    'AVG',
    'COUNT',
    'GROUP_CONCAT',
    'QUANTILES',
    'STDDEV',
    'VARIANCE',
    'LAST',
    'MAX',
    'MIN',
    'NTH',
    'SUM',
    'TOP',
]

BIGQUERY_KEYWORDS = [
    'not',
    'and',
    'or',
    'is',
    'contains',
]

BIGQUERY_CONSTANTS = {
    'null': None,
    'true': True,
    'false': False,
}


# =============================================================================
# = EBQ types.
# =============================================================================
class AliasToken(object):
  """Interface class to mix-in alias to any other class."""

  @property
  def alias(self):
    return getattr(self, '_alias', None)

  @alias.setter
  def alias(self, value):
    self._alias = value

  def SetAlias(self, value):
    self.alias = value
    return self

  def __str__(self):
    a = getattr(self, '_alias', None)
    if a is not None:
      return '%s AS %s' % (str.__str__(self), str(a))
    else:
      return str.__str__(self)


class CountStarToken(str):
  """Typing for count star."""

  def __new__(cls):
    return str.__new__(cls, '*')


class AggregationQueryToken(AliasToken, str):
  """Typing for aggregations that need to be queried."""
  pass


class UnencryptedQueryToken(AliasToken, str):
  """Typing for unencrypted queries that need to be queried."""
  pass


class LiteralToken(str):
  """Typing for literals."""

  def __new__(cls, str_value, real_value):
    obj = str.__new__(cls, str_value)
    obj.value = real_value
    return obj


class StringLiteralToken(LiteralToken):

  def __new__(cls, str_value):
    if str_value[0] != str_value[-1]:
      raise ValueError('Not a literal string.')
    if str_value[0] not in ['\'', '"']:
      raise ValueError('Not a literal string.')
    return super(StringLiteralToken, cls).__new__(
        cls, str_value, str_value[1:-1])


class FunctionToken(AliasToken, str):
  """Base type for functions."""

  def __new__(cls, value, num_args):
    obj = str.__new__(cls, value)
    obj.num_args = num_args
    return obj


class AggregationFunctionToken(FunctionToken):
  """Typing for aggregation functions."""


class BuiltInFunctionToken(FunctionToken):
  """Typing for built in BigQuery functions."""

  def __new__(cls, value):
    # Number of arguments not specified since already specified in
    # query_interpreter.
    return super(BuiltInFunctionToken, cls).__new__(cls, value.lower(), -1)


class OperatorToken(FunctionToken):
  """Typing for unary/binary operators."""

  def __new__(cls, value, num_args):
    return super(OperatorToken, cls).__new__(cls, value.lower(), num_args)


class FieldToken(AliasToken, str):

  def __new__(cls, value):
    return str.__new__(cls, value)

  @property
  def original_name(self):
    return getattr(self, '_original_name', None)

  @original_name.setter
  def original_name(self, value):
    self._original_name = value


class EncryptedToken(FieldToken):

  def __new__(cls, value, prefix, related=None):
    cls.original_name = value
    value = value.split('.')
    value[-1] = '%s%s' % (prefix, value[-1])
    value = '.'.join(value)
    o = super(EncryptedToken, cls).__new__(cls, value)
    o.related = related
    return o


class HomomorphicFloatToken(EncryptedToken):

  def __new__(cls, value):
    return super(HomomorphicFloatToken, cls).__new__(
        cls, value, HOMOMORPHIC_FLOAT_PREFIX)


class HomomorphicIntToken(EncryptedToken):

  def __new__(cls, value):
    return super(HomomorphicIntToken, cls).__new__(
        cls, value, HOMOMORPHIC_INT_PREFIX)


class ProbabilisticToken(EncryptedToken):

  def __new__(cls, value):
    return super(ProbabilisticToken, cls).__new__(
        cls, value, PROBABILISTIC_PREFIX)


class PseudonymToken(EncryptedToken):

  def __new__(cls, value, related=None):
    return super(PseudonymToken, cls).__new__(
        cls, value, PSEUDONYM_PREFIX, related=related)


class SearchwordsToken(EncryptedToken):

  def __new__(cls, value):
    return super(SearchwordsToken, cls).__new__(
        cls, value, SEARCHWORDS_PREFIX)


# =============================================================================
# = EBQ common utility functions.
# =============================================================================
def ConstructPaillierSumQuery(field, nsquare):
  return AggregationQueryToken(PAILLIER_SUM_STRING % (field, nsquare))


def ConstructTableDescription(description, hashed_key, table_version, schema):
  return (
      '%s||EBQ generated info, Do not remove!||'
      'Hash of master key: %s||Version: %s||Schema: %s'
      % (description, hashed_key, table_version, schema))


def GetEntryFromSchema(field_name, schema):
  """Find the correct row in the schema that defines field_name.

  Arguments:
    field_name: The name of the field whose definition is being searched in
    schema.
    schema: The user defined json which characterizes each field.

  Returns:
    Part of the schema that defines field_name or None otherwise.
  """

  def FindEntryFromSchema(field_name, schema):
    for entry in schema:
      if entry['name'] == field_name:
        return entry
    return None

  all_fields = field_name.split('.')
  for i in range(len(all_fields) - 1):
    entry = FindEntryFromSchema(all_fields[i], schema)
    if not entry or 'fields' not in entry:
      return None
    schema = entry['fields']
  entry = FindEntryFromSchema(all_fields[-1], schema)
  if not entry or 'fields' in entry:
    return None
  return entry


def GetFieldType(field, schema):
  row = GetEntryFromSchema(field, schema)
  if not row or 'type' not in row:
    return None
  return row['type']


def IsAggregationQuery(expr):
  if expr.startswith(PAILLIER_SUM_PREFIX):
    return True
  for function in AGGREGATION_FUNCTIONS:
    if expr.startswith('%s(' % function) and expr.endswith(')'):
      return True
  return False


def IsEncrypted(token):
  if isinstance(token, EncryptedToken):
    return True
  if isinstance(token, basestring):
    for prefix in ENCRYPTED_FIELD_PREFIXES:
      if token.startswith(prefix):
        return True
  return False


def IsEncryptedExpression(tokens):
  for token in tokens:
    if IsEncrypted(token):
      return True
  return False


def IsFloat(expr):
  try:
    float(expr)
    return True
  except ValueError:
    return False


def IsLabel(expr):
  """Determines if an expression needs to be sent to server to be queried.

  Arguments:
    expr: Label that could be a query.

  Returns:
    True iff expr is a alphanumeric label that is not a constant or
    function.
  """
  if not isinstance(expr, basestring):
    return False
  if not expr[0].isalpha():
    return False
  elif expr.startswith(FUNCTION_PREFIX):
    return False
  elif (expr.startswith(UNENCRYPTED_ALIAS_PREFIX) and
        expr.endswith('_')):
    return False
  elif expr.lower() in BIGQUERY_CONSTANTS:
    return False
  elif expr.lower() in BIGQUERY_KEYWORDS:
    return False
  for e in expr[1:]:
    if not e.isalpha() and not e.isdigit() and e != '_' and e != '.':
      return False
  return True


def IsDeterministic(token):
  """Determines if a field is deterministically encrypted."""
  if isinstance(token, EncryptedToken):
    return not isinstance(token, PseudonymToken)
  if isinstance(token, basestring):
    for prefix in ENCRYPTED_FIELD_PREFIXES:
      if prefix != PSEUDONYM_PREFIX and token.startswith(prefix):
        return True
  return False


def IsDeterministicExpression(tokens):
  for token in tokens:
    if IsDeterministic(token):
      return True
  return False


def ParseAggregationFunctionToken(token):
  if not token.startswith(AGGREGATION_PREFIX):
    raise ValueError('Not an aggregation function.')
  token = token.split(AGGREGATION_PREFIX)[1]
  token = token.split('_')
  return token[0], '_'.join(token[1:])


def TrimString(token):
  if isinstance(token, basestring):
    return token[1:-1]
  return token

# =============================================================================
# = EBQ common utility functions.
# =============================================================================

# The difference between UTC and PST/PDT is 7 hours or 25200 seconds.
_TIME_DIFFERENCE_UTC_PST = 25200

# The number of days cumulated for each month.
_NON_LEAP_YEAR_CDF_DAYS_BY_MONTH = [
    0,
    31,
    59,
    90,
    120,
    151,
    181,
    212,
    243,
    273,
    304,
    334,
    365,
]


# Supported date and time functions.
def _ConvertToDatetimeObject(date_string):
  try:
    return datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
  except ValueError:
    raise bigquery_client.BigqueryInvalidQueryError(
        'Not a valid timestamp object.', None, None, None)


def _ConvertFromTimestamp(timestamp, utc=True):
  try:
    if utc:
      return datetime.datetime.utcfromtimestamp(timestamp)
    else:
      return datetime.datetime.fromtimestamp(timestamp)
  except ValueError as e:
    raise bigquery_client.BigqueryInvalidQueryError(e, None, None, None)


def _NumberOfDaysSinceAd(year, month, day):
  number_of_days_without_leap_years = (
      365 * year + _NON_LEAP_YEAR_CDF_DAYS_BY_MONTH[month - 1] + day)

  if month <= 2:
    year -= 1

  number_of_leap_days = year / 4 - ((year / 100 + 1) * 3) / 4

  return number_of_days_without_leap_years + number_of_leap_days


def CurrentDate():
  return str(datetime.datetime.utcnow().date())


def CurrentTime():
  return datetime.datetime.utcnow().strftime('%H:%M:%S')


def CurrentTimestamp():
  return datetime.datetime.utcnow().strftime(
      '%Y-%m-%d %H:%M:%S')


def Date(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return str(timestamp.date())


def DateAdd(timestamp, interval, interval_units):
  timestamp = _ConvertToDatetimeObject(timestamp)
  # Python has built-in date addition of day, hour, minute and second.
  if interval_units.lower() in ['day', 'hour', 'minute', 'second']:
    args = {interval_units.lower() + 's': interval}
    return (timestamp + datetime.timedelta(**args)).strftime(
        '%Y-%m-%d %H:%M:%S')
  elif interval_units.lower() == 'year':
    try:
      return timestamp.replace(year=timestamp.year + interval).strftime(
          '%Y-%m-%d %H:%M:%S')
    except ValueError:
      # Not a leap year so the day does not exist.
      return timestamp.replace(
          day=28, month=2, year=timestamp.year + interval).strftime(
              '%Y-%m-%d %H:%M:%S')
  elif interval_units.lower() == 'month':
    try:
      new_year = timestamp.year + (timestamp.month + interval - 1) / 12
      new_month = (timestamp.month + interval - 1) % 12 + 1
      return timestamp.replace(month=new_month, year=new_year).strftime(
          '%Y-%m-%d %H:%M:%S')
    except ValueError:
      # Not a leap year so the day does not exist.
      return timestamp.replace(
          day=28, month=2, year=new_year).strftime(
              '%Y-%m-%d %H:%M:%S')
  else:
    raise bigquery_client.BigqueryInvalidQueryError(
        'Invalid interval unit type.', None, None, None)


def DateDiff(timestamp1, timestamp2):
  timestamp1 = _ConvertToDatetimeObject(timestamp1)
  timestamp2 = _ConvertToDatetimeObject(timestamp2)
  days1 = _NumberOfDaysSinceAd(timestamp1.year, timestamp1.month,
                               timestamp1.day)
  days2 = _NumberOfDaysSinceAd(timestamp2.year, timestamp2.month,
                               timestamp2.day)
  return days1 - days2


def Day(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return timestamp.day


def DayOfWeek(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  # The function isoweekday() considers Monday to be 1. Bigquery considers
  # Sunday to be 1. Shift one weekday forward to have same results.
  return timestamp.isoweekday() % 7 + 1


def DayOfYear(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return timestamp.timetuple().tm_yday


def FormatUTCUsec(unix_timestamp):
  unix_seconds = float(unix_timestamp) / 1e6
  return str(_ConvertFromTimestamp(unix_seconds))


def FromBase64(b64_data):
  return base64.b64decode(b64_data)


def Hour(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return timestamp.timetuple().tm_hour


def Minute(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return timestamp.timetuple().tm_min


def Month(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return timestamp.timetuple().tm_mon


def MsecToTimestamp(expr):
  seconds = float(expr) / 1e3
  return _ConvertFromTimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S')


def Now():
  now = datetime.datetime.now()
  epoch = datetime.datetime.utcfromtimestamp(0)
  return (now - epoch).total_seconds() * 1000000


def ParseUTCUsec(date_string):
  try:
    date = datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S')
    return (time.mktime(date.timetuple()) - _TIME_DIFFERENCE_UTC_PST) * 1000000
  except ValueError:
    pass
  try:
    date = datetime.datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S.%f')
    return (time.mktime(date.timetuple()) - _TIME_DIFFERENCE_UTC_PST) * 1000000
  except ValueError:
    raise bigquery_client.BigqueryInvalidQueryError(
        'Requires one of two following formats: \'%Y-%m-%d %H:%M:%s\' or'
        '\'%Y-%m-%d %H:%M:%S.%f\'.', None, None, None)


def Quarter(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return (timestamp.timetuple().tm_mon - 1) / 3 + 1


def SecToTimestamp(seconds):
  return _ConvertFromTimestamp(seconds).strftime(
      '%Y-%m-%d %H:%M:%S UTC')


def Second(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return timestamp.timetuple().tm_sec


def StrfTimeUTCUsec(unix_timestamp, date_format):
  unix_seconds = float(unix_timestamp) / 1000000
  return _ConvertFromTimestamp(
      unix_seconds + _TIME_DIFFERENCE_UTC_PST).strftime(
          date_format)


def Time(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return timestamp.strftime('%H:%M:%S')


def Timestamp(date_string):
  timestamp = _ConvertToDatetimeObject(date_string)
  return timestamp.strftime('%Y-%m-%d %H:%M:%S')


def TimestampToMsec(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return (time.mktime(timestamp.timetuple()) - _TIME_DIFFERENCE_UTC_PST) * 1000


def TimestampToSec(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return time.mktime(timestamp.timetuple()) - _TIME_DIFFERENCE_UTC_PST


def TimestampToUsec(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return (time.mktime(timestamp.timetuple()) -
          _TIME_DIFFERENCE_UTC_PST) * 1000000


def ToBase64(data):
  return base64.b64encode(data)


def UsecToTimestamp(expr):
  seconds = float(expr) / 1000000
  return _ConvertFromTimestamp(seconds).strftime('%Y-%m-%d %H:%M:%S')


def UTCUsecToDay(seconds):
  day_seconds = seconds % (24 * 60 * 60 * 1000000)
  return seconds - day_seconds


def UTCUsecToHour(seconds):
  hour_seconds = seconds % (60 * 60 * 1000000)
  return seconds - hour_seconds


def UTCUsecToMonth(seconds):
  seconds = float(seconds) / 1000000
  date = _ConvertFromTimestamp(seconds + _TIME_DIFFERENCE_UTC_PST)
  date = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
  return (time.mktime(date.timetuple()) - _TIME_DIFFERENCE_UTC_PST) * 1000000


def UTCUsecToWeek(seconds, day_of_week):
  """Converts UTC seconds to last <day_of_week>.

  Args:
    seconds: Date and time in UTC seconds.
    day_of_week: Last <day_of_week> to go back to. Sunday is 0.

  Returns:
    UTC seconds of last <day_of_week>.
  """
  seconds = float(seconds) / 1000000
  date = _ConvertFromTimestamp(seconds + _TIME_DIFFERENCE_UTC_PST)
  date = date.replace(hour=0, minute=0, second=0, microsecond=0)
  current_day_of_week = (date.weekday() + 1) % 7
  if current_day_of_week >= day_of_week:
    date += datetime.timedelta(days=day_of_week - current_day_of_week)
  else:
    days_back = 7 - (day_of_week - current_day_of_week)
    date += datetime.timedelta(days=-days_back)
  return (time.mktime(date.timetuple()) - _TIME_DIFFERENCE_UTC_PST) * 1000000


def UTCUsecToYear(seconds):
  seconds = float(seconds) / 1000000
  date = _ConvertFromTimestamp(seconds + _TIME_DIFFERENCE_UTC_PST + 3600)
  date = date.replace(month=1, day=1, hour=0, minute=0, second=0,
                      microsecond=0)
  return (time.mktime(date.timetuple()) -
          _TIME_DIFFERENCE_UTC_PST - 3600) * 1000000


def Year(timestamp):
  timestamp = _ConvertToDatetimeObject(timestamp)
  return timestamp.year


# Supported string and regexp functions.
def RightPad(original, max_len, padding):
  padding *= ((max_len - len(original)) / len(padding) + 1)
  return original + padding[:max_len - len(original)]


def LeftPad(original, max_len, padding):
  padding *= ((max_len - len(original)) / len(padding) + 1)
  return padding[:max_len - len(original)] + original


def RegexpExtract(string, reg_exp):
  search = re.search(reg_exp, string)
  if not search:
    raise bigquery_client.BigqueryInvalidQueryError(
        'No captured group.', None, None, None)
  return search.group(1)


# Supported IP functions.
def FormatIP(packed_ip):
  try:
    ip_address = ipaddr.IPv4Address(packed_ip)
  except ipaddr.AddressValueError as e:
    raise bigquery_client.BigqueryInvalidQueryError(e, None, None, None)
  return str(ip_address)


def ParseIP(readable_ip):
  try:
    ip_address = ipaddr.IPv4Address(readable_ip)
  except ipaddr.AddressValueError as e:
    raise bigquery_client.BigqueryInvalidQueryError(e, None, None, None)
  return int(ip_address)


def FormatPackedIP(packed_ip):
  """Formats packed binary data to a readable ip address.

  Args:
    packed_ip: The packed binary data to be converted.

  Returns:
    A readable ip address.

  Returns:
    bigquery_client.BigqueryInvalidQueryError: If the address is not valid.
  """
  packed_ip = ipaddr.Bytes(str(packed_ip))
  try:
    ip_address = ipaddr.IPv4Address(packed_ip)
    return str(ip_address)
  except ipaddr.AddressValueError as e:
    pass
  try:
    ip_address = ipaddr.IPv6Address(packed_ip)
    return str(ip_address)
  except ipaddr.AddressValueError as e:
    raise bigquery_client.BigqueryInvalidQueryError(e, None, None, None)


def ParsePackedIP(readable_ip):
  try:
    ip_address = ipaddr.IPv4Address(readable_ip)
    return str(ipaddr.v4_int_to_packed(int(ip_address)))
  except ValueError:
    pass
  try:
    ip_address = ipaddr.IPv6Address(readable_ip)
    return str(ipaddr.v6_int_to_packed(int(ip_address)))
  except ValueError:
    raise bigquery_client.BigqueryInvalidQueryError(
        'Invalid readable ip.', None, None, None)


# TODO(user): Implement all URL functions.
# Supported URL functions.
def Host(_):
  raise bigquery_client.BigqueryInvalidQueryError(
      'Not implemented yet.', None, None, None)


def Domain(_):
  raise bigquery_client.BigqueryInvalidQueryError(
      'Not implemented yet.', None, None, None)


def Tld(_):
  raise bigquery_client.BigqueryInvalidQueryError(
      'Not implemented yet.', None, None, None)
