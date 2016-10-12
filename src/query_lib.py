#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Contains utility functions that EBQ requires to edit queries."""



from copy import copy

import hashlib
import uuid

import bigquery_client
import common_util as util
import ebq_crypto as ecrypto
import query_interpreter as interpreter


class QueryManifest(object):
  """Class used to create a manifest/metadata about a query.

  Generating unique hashes as column aliases is being done only
  to generate sufficiently unique alias names. There is no attempt
  being made to obscure the column name for any privacy related reason.
  """

  HASH_PREFIX = 'HP'
  RECORDS_WRITTEN = 'recordsWritten'

  def __init__(self, uuid_cls=None, hash_cls=None):
    self.manifest = {
        'columns': {},
        'column_aliases': {},
        'statistics': {
            self.RECORDS_WRITTEN: None,
        },
    }
    self.uuid = str(uuid_cls())
    self.base_hasher = hash_cls(self.uuid)

  @classmethod
  def Generate(cls, unused_schema=None):
    """Generate a QueryManifest instance."""
    qm = cls(uuid_cls=uuid.uuid4, hash_cls=hashlib.sha256)
    return qm

  def _SetRawColumnAlias(self, column_name, column_alias):
    """Set a column name, column alias pair.

    Args:
      column_name: str, original column name.
      column_alias: str, column alias for this column name.
    """
    self.manifest['column_aliases'][column_alias] = column_name
    self.manifest['columns'][column_name] = column_alias

  def _GetRawColumnName(self, column_alias):
    """Get a column name for an alias in use.

    Args:
      column_alias: str, column alias to retrieve name for.
    Returns:
      str column_name if column_alias is defined, or None if not defined.
    """
    return self.manifest['column_aliases'].get(column_alias, None)

  def GenerateColumnAlias(self, column_name):
    """Returns a column alias for the given column name.

    Args:
      column_name: str
    Returns:
      str, column alias that is safe for SQL alias usage (see HASH_PREFIX)
    """
    hasher = self.base_hasher.copy()
    hasher.update(column_name)
    return '%s%s' % (self.HASH_PREFIX, hasher.hexdigest())

  def GetColumnAliasForName(self, column_name, extras=None, generate=True):
    """Returns the column alias for the given column name.

    Args:
      column_name: str
      extras: list, default None, if an iterable is supplied then each
        item is set as an additional column name->alias mapping, even though
        only one alias->column_name (arg1) mapping exists.
      generate: bool, default True, if true generate new aliases, otherwise
        return None when alias is not yet generated.
    Returns:
      str, column alias
    """
    column_alias = self.manifest['columns'].get(column_name, None)
    if column_alias is None:
      if generate:
        column_alias = self.GenerateColumnAlias(column_name)
        if extras is None:
          self._SetRawColumnAlias(column_name, column_alias)
        else:
          for extra_column_name in extras:
            self._SetRawColumnAlias(extra_column_name, column_alias)
          # SUBTLE: Set the column_alias->column_name last so that
          # column_alias does not map to any of the _extra_ column names,
          # but rather only the column_name.
          self._SetRawColumnAlias(column_name, column_alias)
    return column_alias

  def GetColumnNameForAlias(self, column_alias):
    """Returns the column name for the given column alias.

    Args:
      column_alias: str
    Returns:
      str, column name
    """
    return self._GetRawColumnName(column_alias)

  @property
  def statistics(self):
    return self.manifest['statistics']

  def __str__(self):
    return '[%s:%s %s]' % (self.__class__.__name__, id(self), self.manifest)


class _Clause(object):
  """Base class for rewriting queries for the server in encrypted bigquery."""
  _argument = None

  def __init__(self, argument, argument_type=None, **extra_args):
    super(_Clause, self).__init__()
    if argument_type is not None:
      if not isinstance(argument, argument_type):
        raise ValueError('Invalid argument. Expected type %s.' % argument_type)
    self._argument = argument
    for key, value in extra_args.iteritems():
      setattr(self, key, value)

  def Rewrite(self):
    return ''

  def GetOriginalArgument(self):
    return self._argument

  def _CheckNecessaryAttributes(self, attributes):
    for attribute in attributes:
      try:
        getattr(self, attribute)
      except AttributeError:
        raise ValueError('Need %s attribute to rewrite.' % attribute)


class _AsClause(_Clause):
  """Class for rewriting as clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_AsClause, self).__init__(argument, dict, **extra_args)

  def ConstructColumnNames(self, column_names):
    """Replaces original column names with their alias, if one exists."""
    rewritten_column_names = []
    for i in range(len(column_names)):
      single_column = {}
      if i in self._argument:
        single_column['name'] = self._argument[i]
      else:
        single_column['name'] = str(interpreter.ToInfix(copy(column_names[i])))
      rewritten_column_names.append(single_column)
    return rewritten_column_names


class _WithinClause(_Clause):
  """Class for rewriting within clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_WithinClause, self).__init__(argument, dict, **extra_args)


class _SelectClause(_Clause):
  """Class for rewriting select clause arguments."""
  _unencrypted_queries = None
  _encrypted_queries = None
  _aggregation_queries = None
  _table_expressions = None

  def __init__(self, argument, **extra_args):
    super(_SelectClause, self).__init__(argument, list, **extra_args)

  def Rewrite(self):
    """Rewrites select argument to send to BigQuery server.

    Returns:
      Rewritten select clause.

    Raises:
      ValueError: Invalid clause type or necessary argument not given.
    """
    if not self._argument:
      raise ValueError('Cannot have empty select clause.')
    necessary_attributes = [
        'as_clause',
        'within_clause',
        'schema',
        'nsquare',
    ]
    self._CheckNecessaryAttributes(necessary_attributes)
    if not isinstance(self.as_clause, _AsClause):
      raise ValueError('Invalid as clause.')
    if not isinstance(self.within_clause, _WithinClause):
      raise ValueError('Invalid within clause.')
    manifest = getattr(self, 'manifest', None)
    temp_argument = copy(self._argument)
    # TODO(user): A different approach to handling aliases could be
    # to add their usage into this rewriting function. It would be
    # more universal but trickier to code.
    self._table_expressions = (
        _RewritePostfixExpressions(temp_argument,
                                   self.as_clause.GetOriginalArgument(),
                                   self.schema,
                                   self.nsquare))
    self._unencrypted_queries = (
        _ExtractUnencryptedQueries(self._table_expressions,
                                   self.within_clause.GetOriginalArgument()))
    self._aggregation_queries = (
        _ExtractAggregationQueries(self._table_expressions,
                                   self.within_clause.GetOriginalArgument(),
                                   self.as_clause.GetOriginalArgument()))
    self._encrypted_queries = _ExtractFieldQueries(
        self._table_expressions, self.as_clause.GetOriginalArgument(),
        manifest)
    all_queries = copy(self._aggregation_queries)
    all_queries.extend(self._encrypted_queries)
    all_queries.extend(self._unencrypted_queries)
    return 'SELECT %s' % ', '.join(map(str, all_queries))

  def GetAggregationQueries(self):
    if self._aggregation_queries is None:
      raise ValueError('Queries have yet to be retrieved. Rewrite query first.')
    return self._aggregation_queries

  def GetEncryptedQueries(self):
    if self._encrypted_queries is None:
      raise ValueError('Queries have yet to be retrieved. Rewrite query first.')
    return self._encrypted_queries

  def GetUnencryptedQueries(self):
    if self._unencrypted_queries is None:
      raise ValueError('Queries have yet to be retrieved. Rewrite query first.')
    return self._unencrypted_queries

  def GetTableExpressions(self):
    if self._table_expressions is None:
      raise ValueError('Queries have yet to be retrieved. Rewrite query first.')
    return self._table_expressions


class _FromClause(_Clause):
  """Class for rewriting from clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_FromClause, self).__init__(argument, list, **extra_args)

  def Rewrite(self):
    if not self._argument:
      return ''
    return 'FROM %s' % ', '.join(self._argument)


class _JoinClause(_Clause):
  """Class for rewriting JOIN clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_JoinClause, self).__init__(argument, list, **extra_args)

  def Rewrite(self):
    """Rewrites where argument to send to BigQuery server.

    Returns:
      Rewritten where clause.

    Raises:
      ValueError: Invalid clause type or necessary argument not given.
    """
    if not self._argument:
      return ''

    joins = ['']
    for one_join in self._argument:
      join_expr = copy(one_join)[1:]
      # TODO(user): This must validate table names to make this
      # fully functional, and support aliased columns.
      join_expr = interpreter.RewriteSelectionCriteria(
          join_expr, self.schema, self.master_key, self.table_id)
      join_clause = '%s ON %s' % (one_join[0], join_expr)
      joins.append(join_clause)

    return ' JOIN '.join(joins)[1:]


class _HavingClause(_Clause):
  """Class for rewriting having clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_HavingClause, self).__init__(argument, list, **extra_args)

  def Rewrite(self):
    """Rewrites having argument to send to BigQuery server.

    Returns:
      Rewritten having clause.

    Raises:
      ValueError: Invalid clause type or necessary argument not given.
    """
    if not self._argument:
      return ''
    necessary_attributes = [
        'as_clause',
        'schema',
        'nsquare',
        'master_key',
        'table_id',
    ]
    self._CheckNecessaryAttributes(necessary_attributes)
    if not isinstance(self.as_clause, _AsClause):
      raise ValueError('Invalid as clause.')
    rewritten_argument = [copy(self._argument)]
    rewritten_argument = _RewritePostfixExpressions(
        rewritten_argument, self.as_clause.GetOriginalArgument(), self.schema,
        self.nsquare)[0]
    for token in rewritten_argument:
      if not isinstance(token, util.AggregationQueryToken):
        continue
      if token.startswith(util.PAILLIER_SUM_PREFIX):
        raise bigquery_client.BigqueryInvalidQueryError(
            'Cannot include SUM/AVG on homomorphic encryption in HAVING '
            'clause.', None, None, None)
      elif token.startswith(util.GROUP_CONCAT_PREFIX):
        field = token.split(util.GROUP_CONCAT_PREFIX)[1][:-1]
        if util.IsEncrypted(field):
          raise bigquery_client.BigqueryInvalidQueryError(
              'Cannot include GROUP_CONCAT on encrypted field in HAVING '
              'clause.', None, None, None)
    rewritten_argument = interpreter.RewriteSelectionCriteria(
        rewritten_argument, self.schema, self.master_key, self.table_id)
    return 'HAVING %s' % rewritten_argument


class _WhereClause(_Clause):
  """Class for rewriting where clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_WhereClause, self).__init__(argument, list, **extra_args)

  def Rewrite(self):
    """Rewrites where argument to send to BigQuery server.

    Returns:
      Rewritten where clause.

    Raises:
      ValueError: Invalid clause type or necessary argument not given.
    """
    if not self._argument:
      return ''
    necessary_attributes = [
        'as_clause',
        'schema',
        'nsquare',
        'master_key',
        'table_id',
    ]
    self._CheckNecessaryAttributes(necessary_attributes)
    if not isinstance(self.as_clause, _AsClause):
      raise ValueError('Invalid as clause.')
    rewritten_argument = [copy(self._argument)]
    rewritten_argument = _RewritePostfixExpressions(
        rewritten_argument, self.as_clause.GetOriginalArgument(), self.schema,
        self.nsquare)[0]
    rewritten_argument = interpreter.RewriteSelectionCriteria(
        rewritten_argument, self.schema, self.master_key, self.table_id)
    return 'WHERE %s' % rewritten_argument


class _GroupByClause(_Clause):
  """Class for rewriting group by clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_GroupByClause, self).__init__(argument, list, **extra_args)

  def Rewrite(self):
    """Rewrites group by argument to send to BigQuery server.

    Returns:
      Rewritten group by clause.

    Raises:
      ValueError: Invalid clause type or necessary argument not given.
    """
    if not self._argument:
      return ''
    necessary_attributes = [
        'nsquare',
        'schema',
        'select_clause',
    ]
    self._CheckNecessaryAttributes(necessary_attributes)
    if not isinstance(self.select_clause, _SelectClause):
      raise ValueError('Invalid select clause.')
    for argument in self._argument:
      row = util.GetEntryFromSchema(argument, self.schema)
      if (row['encrypt'].startswith('probabilistic') or
          row['encrypt'] == 'homomorphic' or
          row['encrypt'] == 'searchwords'):
        raise bigquery_client.BigqueryInvalidQueryError(
            'Cannot GROUP BY %s encryption.' % row['encrypt'], None, None, None)
    # Group by arguments have no alias, so an empty dictionary is adequate.
    rewritten_argument = _RewritePostfixExpressions(
        [self._argument], {}, self.schema, self.nsquare)[0]
    # Only want expressions, remove alias from expression.
    unencrypted_expression_list = []
    for query in self.select_clause.GetUnencryptedQueries():
      unencrypted_expression_list.append(' '.join(query.split(' ')[:-2]))
    for i in range(len(rewritten_argument)):
      if rewritten_argument[i] in unencrypted_expression_list:
        rewritten_argument[i] = (
            '%s%d_' % (
                util.UNENCRYPTED_ALIAS_PREFIX,
                unencrypted_expression_list.index(rewritten_argument[i])))
      else:
        manifest = getattr(self, 'manifest', None)
        if manifest is not None:
          column_alias = manifest.GetColumnAliasForName(
              rewritten_argument[i], generate=False)
        else:
          column_alias = None
        if column_alias is not None:
          rewritten_argument[i] = column_alias
        else:
          rewritten_argument[i] = rewritten_argument[i].replace(
              '.', util.PERIOD_REPLACEMENT)
    return 'GROUP BY %s' % ', '.join(rewritten_argument)


class _OrderByClause(_Clause):
  """Class for rewriting order by clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_OrderByClause, self).__init__(argument, list, **extra_args)

  def SortTable(self, column_names, table_rows):
    """Sort table based on ORDER BY arguments.

    Arguments:
      column_names: Column names of to be printed table.
      table_rows: Values of each row.

    Raises:
      bigquery_client.BigqueryInvalidQueryError: ORDER BY argument not a valid
        column name.

    Returns:
      The table sorted based on ORDER BY arguments.
    """
    # If order by clause is not part of query, just return the original table.
    if not self._argument:
      return table_rows

    # Check that each order by argument is a column in the table.
    for argument in self._argument:
      # Argument is on the form field [ASC|DESC]. Only interested in field name.
      field = argument.split(' ')[0]
      found = False
      for column_name in column_names:
        if column_name['name'] == field:
          found = True
          break
      if not found:
        raise bigquery_client.BigqueryInvalidQueryError(
            '%s appears in ORDER BY, but is not a named column in SELECT.'
            % field, None, None, None)

    # Sort based on the least important field (last specified argument) to the
    # most important argument (first specified argument). This works as long
    # as long as the sort we use is stable. Python's sort is stable:
    # http://docs.python.org/2/howto/sorting.html.

    # Stores current arrangement of tables by original index.
    # Initially, stores list from 0, 1, ... N.
    current_index_sort = list(range(len(table_rows)))

    for argument in reversed(self._argument):
      field = argument.split(' ')[0]
      # Check if we are sorting ascending or descending.
      reverse_sort = (len(argument.split(' ')) == 2 and
                      argument.split(' ')[1].lower() == 'desc')
      # Unsorted list consists of column value of each row and their index.
      unsorted_list = []
      for i in xrange(len(column_names)):
        if column_names[i]['name'] == field:
          for j in current_index_sort:
            unsorted_list.append((table_rows[j][i], j))
      current_index_sort[:] = []
      for row in sorted(unsorted_list, key=lambda v: v[0],
                        reverse=reverse_sort):
        current_index_sort.append(row[1])

    sorted_table = []
    for i in current_index_sort:
      sorted_table.append(table_rows[i])
    return sorted_table


class _LimitClause(_Clause):
  """Class for rewriting limit clause arguments."""

  def __init__(self, argument, **extra_args):
    super(_LimitClause, self).__init__(argument, list, **extra_args)

  def Rewrite(self):
    """Rewrites limit argument to send to BigQuery server.

    Returns:
      Rewritten limit clause.

    Raises:
      ValueError: Invalid clause type or necessary argument not given.
    """
    if self._argument:
      return 'LIMIT %s' % self._argument[0]
    return ''


def RewriteQuery(clauses, schema, master_key, table_id, manifest=None):
  """Rewrite original query so that it can be sent to the BigQuery server.

  Arguments:
    clauses: List of clauses and corresponding arguments.
    schema: User defined field types.
    master_key: Master key for encryption/decryption.
    table_id: Used to generate proper keys.
    manifest: optional, Used to store metadata about the query.

  Returns:
    Rewritten BigQuery-ready query.

  Raises:
    bigquery_client.BigqueryInvalidQueryError: Invalid original query.
    ValueError: Invalid clause type given.
  """
  nsquare = ecrypto.HomomorphicIntCipher(
      ecrypto.GenerateHomomorphicCipherKey(master_key, table_id)).nsquare

  as_clause = _AsClause(clauses['AS'])
  within_clause = _WithinClause(clauses['WITHIN'])
  order_by_clause = _OrderByClause(clauses['ORDER BY'])

  column_names = as_clause.ConstructColumnNames(clauses['SELECT'])

  extra_arguments = {
      'as_clause': as_clause,
      'within_clause': within_clause,
      'schema': schema,
      'master_key': master_key,
      'table_id': table_id,
      'nsquare': nsquare,
  }

  if manifest is not None:
    extra_arguments['manifest'] = manifest

  # The order that clauses are rewritten matters. That is why pair of list
  # instead of a dict is used.
  clause_factory = [
      ('SELECT', _SelectClause),
      ('FROM', _FromClause),
      ('JOIN', _JoinClause),
      ('WHERE', _WhereClause),
      ('GROUP BY', _GroupByClause),
      ('HAVING', _HavingClause),
      ('LIMIT', _LimitClause),
  ]

  rewritten_query_clauses = []

  for clause_pair in clause_factory:
    clause = clause_pair[1](clauses[clause_pair[0]], **extra_arguments)
    rewritten_clause = clause.Rewrite()
    if rewritten_clause:
      rewritten_query_clauses.append(rewritten_clause)
    if clause_pair[0] == 'SELECT':
      extra_arguments['select_clause'] = clause
      aggregation_queries = clause.GetAggregationQueries()
      encrypted_queries = clause.GetEncryptedQueries()
      unencrypted_queries = clause.GetUnencryptedQueries()
      table_expressions = clause.GetTableExpressions()

  print_arguments = {
      'master_key': master_key,
      'table_id': table_id,
      'schema': schema,
      'encrypted_queries': encrypted_queries,
      'aggregation_queries': aggregation_queries,
      'unencrypted_queries': unencrypted_queries,
      'order_by_clause': order_by_clause,
      'column_names': column_names,
      'table_expressions': table_expressions,
  }

  if manifest is not None:
    print_arguments['manifest'] = manifest

  rewritten_query = ' '.join(rewritten_query_clauses)
  return rewritten_query, print_arguments


def _ExtractAggregationQueries(stacks, within, alias):
  """Extracts all aggregations that need to be queried on the server.

  If the aggregation's expression was modified by a within clause, the within
  clause is added to each aggregation being sent to the server.

  Arguments:
    stacks: All postfix stacks with potential aggregation queries.
    within: Indicates which nodes/records to aggregate over for expressions.
    alias: Column aliases in dict form {int index: alias string}.
  Returns:
    A list of all aggregation queries that need to be sent to the server.
  """

  query_list = []
  for i in range(len(stacks)):
    for j in range(len(stacks[i])):
      if isinstance(stacks[i][j], util.AggregationQueryToken):
        if i in within:
          query = '%s WITHIN %s' % (stacks[i][j], within[i])
          stacks[i][j] = (
              util.AggregationQueryToken(
                  '%s WITHIN %s' % (stacks[i][j], within[i])))
        else:
          query = stacks[i][j]
        if i in alias:
          query.alias = alias[i]
        if query not in query_list:
          query_list.append(query)
  return query_list


def _ExtractFieldQueries(stacks, alias=None, manifest=None, strize=False):
  """Extracts all labels that need to be queried on the server.

  Each encrypted field is aliased by the field name with all its period replaced
  by a special distinct token. This is done to ensure that all periods can be
  retrieved after the query since BigQuery replaces all periods with underscores
  during the query to the server.

  Args:
    stacks: All postfix stacks with potential fields.
    alias: optional, Dictionary of aliases.
    manifest: optional, a QueryManifest instance.
    strize: optional, true if all columns should be str().

  Returns:
    A set of all queries that need to be sent to the server.
  """
  query_list = set()
  i = 0
  if alias is None:
    alias = {}
  for stack in stacks:
    for s in stack:
      if isinstance(s, util.FieldToken):
        column = s
        if alias and i in alias:
          column_alias = alias[i]
        elif manifest is not None:
          original_name = column.original_name
          if original_name is not None:
            column_alias = manifest.GetColumnAliasForName(
                column, extras=[original_name])
          else:
            column_alias = manifest.GetColumnAliasForName(column)
        else:
          column_alias = s.replace('.', util.PERIOD_REPLACEMENT)
        if column_alias != column:
          column.alias = column_alias
        if strize:
          column = str(column)
        query_list.add(column)
    i += 1
  return query_list


def _ExtractUnencryptedQueries(postfix_stacks, within):
  """Extracts expressions (not a single term) that are unencrypted.

  If the expression was modified by a within clause, then the within clause
  is prepended to the expression.

  Args:
    postfix_stacks: List of postfix expressions with a potentially unencrypted
      expression.
    within: Dictionary of index of expressions to nodes/records to aggregate
      over.

  Returns:
    List of unencrypted expressions that are not a single term.
  """

  def _IsEncryptedExpression(stack):
    for token in stack:
      if (not isinstance(token, util.FieldToken) and
          not isinstance(token, util.AggregationQueryToken)):
        continue
      if (util.HOMOMORPHIC_FLOAT_PREFIX in token or
          util.HOMOMORPHIC_INT_PREFIX in token or
          util.PSEUDONYM_PREFIX in token or
          util.PROBABILISTIC_PREFIX in token or
          util.SEARCHWORDS_PREFIX in token):
        return True
    return False

  unencrypted_expressions = []
  counter = 0
  for i in range(len(postfix_stacks)):
    if not _IsEncryptedExpression(postfix_stacks[i]):
      expression = interpreter.ToInfix(list(postfix_stacks[i]))
      if i in within:
        expression += ' WITHIN %s' % within[i]
      expression += ' AS %s%d_' % (util.UNENCRYPTED_ALIAS_PREFIX, counter)
      unencrypted_expressions.append(expression)
      postfix_stacks[i] = [
          util.UnencryptedQueryToken(
              '%s%d_' % (util.UNENCRYPTED_ALIAS_PREFIX, counter))]
      counter += 1

  return unencrypted_expressions


def _RewritePostfixExpressions(postfix_expressions, alias, schema, nsquare):
  """Rewrites a postfix expression into a more useful form.

  Three edits are done to all expressions:
  - All aliases are replaced by their full expression. This allows us to be able
    to use our aliases and not worry about the user's initial aliasing.
  - Prepend all encrypted fields with their proper prefix.
  - Replace all aggregations on encrypted fields with a corresponding query
    that will allow us to do aggregations on ciphertext.
  Note: This will fail if any improper queries are found.

  Arguments:
    postfix_expressions: List of postfix expressions to be replaced.
    alias: Dictionary that maps indices to aliases.
    schema: User defined field types and values.
    nsquare: Used during replacement of SUM/AVG on homomorphic encrypted fields.

  Returns:
    A postfix expression with no aliases, properly named encrypted fields and
    proper aggregations that work on ciphertexts.
  """
  new_expressions = copy(postfix_expressions)
  new_expressions = _ReplaceAlias(new_expressions, alias)
  new_expressions = _RewriteEncryptedFields(new_expressions, schema)
  new_expressions = _RewriteAggregations(new_expressions, nsquare)
  return new_expressions


def _ReplaceAlias(postfix_expressions, alias):
  """Removes all aliases and replaces all aliases with actual expressions.

  Arguments:
    postfix_expressions: All postfix expressions with potential aliases.
    alias: Dictionary containing all alias and their respective expressions
      index in <postfix_expressions>.

  Returns:
    A list of postfix expressions that replaces all aliases with their full
    expression.
  """
  new_postfix_expressions = []
  for i in range(len(postfix_expressions)):
    temp_stack = []
    for j in range(len(postfix_expressions[i])):
      if (isinstance(postfix_expressions[i][j], util.FieldToken) and
          str(postfix_expressions[i][j]) in alias.values()):
        alias_index = alias.keys()[
            alias.values().index(postfix_expressions[i][j])]
        if alias_index < i:
          temp_stack.extend(new_postfix_expressions[alias_index])
        else:
          temp_stack.append(postfix_expressions[i][j])
      else:
        temp_stack.append(postfix_expressions[i][j])
    new_postfix_expressions.append(temp_stack)
  return new_postfix_expressions


def _RewriteAggregations(postfix_expressions, nsquare):
  """Replaces all aggregations with expressions that can aggregate ciphertext.

  Currently, all aggregations are broken up into postfix format where arguments
  are preceded by their aggregation. Rewriting, will collapse all aggregations
  into one token. If the aggregation occurs over a ciphertext field, a check
  and possible rewriting will occur such that aggregation over ciphertext is
  possible.

  Arguments:
    postfix_expressions: List of postfix expressions that have aggregation
      functions in postfix format.
    nsquare: Used to rewrite SUM/AVG aggregations over homomorphic encryption.

  Returns:
    A list of postfix expressions where each expression's aggregation tokens
    have been collapsed into a single token, checked and assured to be allowed
    to be queried and possibly rewritten.
  """
  rewritten_expression = []
  for expression in postfix_expressions:
    temp_expression = copy(expression)
    while _CollapseAggregations(temp_expression, nsquare):
      pass
    rewritten_expression.append(temp_expression)
  return rewritten_expression


def _RewriteEncryptedFields(postfix_expressions, schema):
  """Takes all encrypted fields and prepends them with the right prefix.

  Arguments:
    postfix_expressions: An list of postfix expressions that needs to be
      rewritten.
    schema: Determines the user determined encryption types and information
      about fields.

  Returns:
    An list of postfix expression that have been rewritten with proper prefixes.

  Raises:
    bigquery_client.BigqueryInvalidQueryError: When a distinct count is
      attempted with non-determinstic fields.
  """

  def RewriteField(field):
    """Rewrite fields for real query with server."""
    if not isinstance(field, util.FieldToken):
      return field
    row = util.GetEntryFromSchema(field, schema)
    if not row:
      return field
    if row['encrypt'].startswith('probabilistic'):
      return util.ProbabilisticToken(str(field))
    elif row['encrypt'] == 'pseudonym':
      if row.get('related', None) is not None:
        return util.PseudonymToken(str(field), related=row['related'])
      else:
        return util.PseudonymToken(str(field))
    elif row['encrypt'] == 'homomorphic' and row['type'] == 'integer':
      return util.HomomorphicIntToken(str(field))
    elif row['encrypt'] == 'homomorphic' and row['type'] == 'float':
      return util.HomomorphicFloatToken(str(field))
    elif row['encrypt'] == 'searchwords':
      return util.SearchwordsToken(str(field))
    return field

  rewritten_expressions = []
  for expression in postfix_expressions:
    rewritten_expressions.append([RewriteField(token) for token in expression])
  return rewritten_expressions


def _CollapseAggregations(stack, nsquare):
  """Collapses the aggregations by combining arguments and functions.

  During collapses, checks will be done to if aggregations are done on
  encrypted fields. The following aggregations will be rewritten:

  SUM(<homomorphic field>) becomes
  TO_BASE64(PAILLIER_SUM(FROM_BASE64(<homomorphic field>), <nsquare>))

  AVG(<homomorphic field>) becomes
  TO_BASE64(PAILLIER_SUM(FROM_BASE64(<homomorphic field>), <nsquare>)) /
  COUNT(<homomorphic field>)

  Arguments:
    stack: The stack whose aggregations are to be collapsed.
    nsquare: Used for homomorphic addition.

  Returns:
    True iff an aggregation was found and collapsed. In other words, another
    potential aggregation can still exist.
  """
  for i in xrange(len(stack)):
    if isinstance(stack[i], util.AggregationFunctionToken):
      num_args = stack[i].num_args
      function_type = str(stack[i])
      postfix_exprs = []
      infix_exprs = []
      start_idx = i
      rewritten_infix_expr = None
      is_encrypted = False
      # pylint: disable=unused-variable
      for j in xrange(int(num_args)):
        start_idx, postfix_expr = interpreter.GetSingleValue(
            stack[:start_idx])
        is_encrypted = is_encrypted or util.IsEncryptedExpression(postfix_expr)
        while _CollapseFunctions(postfix_expr):
          pass
        postfix_exprs.append(postfix_expr)
        infix_exprs.append(interpreter.ToInfix(list(postfix_expr)))
      # Check for proper nested aggregations.
      # PAILLIER_SUM and GROUP_CONCAT on encrypted fields are not legal
      # arguments for an aggregation.
      for expr in postfix_exprs:
        for token in expr:
          if not isinstance(token, util.AggregationQueryToken):
            continue
          if token.startswith(util.PAILLIER_SUM_PREFIX):
            raise bigquery_client.BigqueryInvalidQueryError(
                'Cannot use SUM/AVG on homomorphic encryption as argument '
                'for another aggregation.', None, None, None)
          elif token.startswith(util.GROUP_CONCAT_PREFIX):
            fieldname = token.split(util.GROUP_CONCAT_PREFIX)[1][:-1]
            if util.IsEncrypted(fieldname):
              raise bigquery_client.BigqueryInvalidQueryError(
                  'Cannot use GROUP_CONCAT on an encrypted field as argument '
                  'for another aggregation.', None, None, None)
      infix_exprs.reverse()
      if function_type in ['COUNT', 'DISTINCTCOUNT']:
        if (function_type == 'DISTINCTCOUNT' and
            util.IsDeterministicExpression(postfix_exprs[0])):
          raise bigquery_client.BigqueryInvalidQueryError(
              'Cannot do distinct count on non-pseudonym encryption.',
              None, None, None)
        if function_type == 'DISTINCTCOUNT':
          infix_exprs[0] = 'DISTINCT ' + infix_exprs[0]
        rewritten_infix_expr = [
            util.AggregationQueryToken('COUNT(%s)' % ', '.join(infix_exprs))]
      elif function_type == 'TOP':
        if util.IsDeterministicExpression(postfix_exprs[0]):
          raise bigquery_client.BigqueryInvalidQueryError(
              'Cannot do TOP on non-deterministic encryption.',
              None, None, None)
        rewritten_infix_expr = [
            util.AggregationQueryToken('TOP(%s)' % ', '.join(infix_exprs))]
      elif function_type in ['AVG', 'SUM'] and is_encrypted:
        list_fields = interpreter.CheckValidSumAverageArgument(
            postfix_expr)[0]
        rewritten_infix_expr = []
        # The representative label is the field that is going to be used
        # to get constant values. An expression SUM(ax + b) must be rewritten as
        # a * SUM(x) + b * COUNT(x). Represetative label is x (this isn't unique
        # as many fields can be in COUNT).
        representative_label = ''
        for field in list_fields:
          for token in field:
            if util.IsLabel(token):
              representative_label = token
              break
          if representative_label:
            break
        for field in list_fields:
          expression = interpreter.ExpandExpression(field)
          queries, constant = expression[0], expression[1]
          rewritten_infix_expr.append(float(constant))
          rewritten_infix_expr.append(
              util.AggregationQueryToken('COUNT(%s)' % representative_label))
          rewritten_infix_expr.append(util.OperatorToken('*', 2))
          for query in queries:
            rewritten_infix_expr.append(float(query[0]))
            if (isinstance(query[1], util.HomomorphicFloatToken) or
                isinstance(query[1], util.HomomorphicIntToken)):
              rewritten_infix_expr.append(
                  util.ConstructPaillierSumQuery(query[1], nsquare))
            else:
              rewritten_infix_expr.append(
                  util.AggregationQueryToken('SUM(%s)' % query[1]))
            rewritten_infix_expr.append(util.OperatorToken('*', 2))
          for j in range(len(queries)):
            rewritten_infix_expr.append(util.OperatorToken('+', 2))
        for j in range(len(list_fields) - 1):
          rewritten_infix_expr.append(util.OperatorToken('+', 2))
        if function_type == 'AVG':
          rewritten_infix_expr.append(
              util.AggregationQueryToken('COUNT(%s)' % representative_label))
          rewritten_infix_expr.append(util.OperatorToken('/', 2))
      elif function_type == 'GROUP_CONCAT':
        rewritten_infix_expr = [
            util.AggregationQueryToken('GROUP_CONCAT(%s)'
                                       % ', '.join(infix_exprs))]
      elif is_encrypted:
        raise bigquery_client.BigqueryInvalidQueryError(
            'Cannot do %s aggregation on any encrypted fields.' % function_type,
            None, None, None)
      else:
        rewritten_infix_expr = [
            util.AggregationQueryToken(
                '%s(%s)' % (function_type, ', '.join(infix_exprs)))]
      stack[start_idx:i+1] = rewritten_infix_expr
      return True
  return False


def _CollapseFunctions(stack):
  """Collapses functions by evaluating them for actual values.

  Replaces a function's postfix expression with a single token. If the function
  can be evaluated (no fields included as arguments), the single token is
  the value of function's evaluation. Otherwise, the function is collapsed
  into a single token without evaluation.

  Arguments:
    stack: The stack whose functions are to be collapsed and resolved.

  Raises:
    bigquery_client.BigqueryInvalidQueryError: If a field exists inside
    the arguments of a function.

  Returns:
    True iff a function is found and collapsed. In other words, another
    potential function can still exist.
  """
  for i in xrange(len(stack)):
    if isinstance(stack[i], util.BuiltInFunctionToken):
      start_idx, postfix_expr = interpreter.GetSingleValue(stack[:i+1])
      if util.IsEncryptedExpression(postfix_expr):
        raise bigquery_client.BigqueryInvalidQueryError(
            'Invalid aggregation function argument: Cannot put an encrypted '
            'field as an argument to a built-in function.', None, None, None)
      # If the expression has no fields, we want to get the actual value.
      # But, if the field has a field, we have to get the infix string instead.
      try:
        result = interpreter.Evaluate(list(postfix_expr))
        if isinstance(result, basestring):
          result = util.StringLiteralToken('"%s"' % result)
        elif result is None:
          result = util.LiteralToken('NULL', None)
        elif str(result).lower() in ['true', 'false']:
          result = util.LiteralToken(str(result).lower(), result)
        stack[start_idx:i+1] = [result]
      except bigquery_client.BigqueryInvalidQueryError:
        result = interpreter.ToInfix(list(postfix_expr))
        stack[start_idx:i+1] = [util.FieldToken(result)]
      return True
  return False
