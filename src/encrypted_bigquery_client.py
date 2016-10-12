#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Encrypted Bigquery Client library for Python."""



import base64
import hashlib
import json
import shutil
import tempfile
import zlib


from pyparsing import ParseException

from google.apputils import app
import gflags as flags

import bigquery_client
import bq
import common_util as util
import ebq_crypto as ecrypto
import load_lib
import query_interpreter as interpreter
import query_lib
import query_parser as parser


FLAGS = flags.FLAGS


class EncryptedTablePrinter(bq.TablePrinter):
  """Class encapsulating encrypted table printing for Encrypted BigQuery."""

  def __init__(self, **kwds):
    super(EncryptedTablePrinter, self).__init__(**kwds)
    necessary_attributes = [
        'master_key',
        'table_id',
        'schema',
        'aggregation_queries',
        'encrypted_queries',
        'unencrypted_queries',
        'order_by_clause',
        'column_names',
        'table_expressions',
    ]
    for attribute in necessary_attributes:
      try:
        getattr(self, attribute)
      except AttributeError:
        raise ValueError('Cannot print table without %s.' % attribute)

  def PrintTable(self, fields, rows):
    """Decrypts table values and then prints the table.

    Arguments:
      fields: Column names for table.
      rows: Table values for each column.
    """
    manifest = getattr(self, 'manifest', None)
    decrypted_queries = _DecryptRows(
        fields, rows, self.master_key, self.table_id, self.schema,
        self.encrypted_queries, self.aggregation_queries,
        self.unencrypted_queries, manifest=manifest)
    table_values = _ComputeRows(self.table_expressions, decrypted_queries)
    if self.order_by_clause:
      table_values = self.order_by_clause.SortTable(self.column_names,
                                                    table_values)

    # pylint: disable=protected-access
    formatter = bq._GetFormatterFromFlags(secondary_format='pretty')
    formatter.AddFields(self.column_names)
    formatter.AddRows(table_values)
    formatter.Print()


class EncryptedBigqueryClient(bigquery_client.BigqueryClient):
  """Class encapsulating interaction with the Encrypted BigQuery service."""

  def __init__(self, **kwds):
    """Initializes EncryptedBigqueryClient."""
    super(EncryptedBigqueryClient, self).__init__(**kwds)
    flag_names = [
        'master_key_filename',
    ]
    for flag_name in flag_names:
      setattr(self, flag_name, getattr(FLAGS, flag_name))

  def _CheckKeyfileFlag(self):
    if not self.master_key_filename:
      raise app.UsageError(
          'Must specify a master key to encrypt/decrypt values.\n'
          'If you do not want any encryption/decryption to occur, consider\n'
          'using the Bigquery client. If you wish to generate a key_file during'
          ' a load command, just specify the path file to where the new key '
          'will be placed. This file must not exist beforehand.')

  def _CheckSchemaFlag(self):
    if not self.ebq_schema:
      raise app.UsageError(
          'Must specify a ebq_schema to find which type of encryption is\n'
          'being used for each field type. If you do not want/have any\n'
          'encryption, consider using the Bigquery client.')

  def _CheckSchemaFile(self, schema):
    if ':' in schema:
      raise app.UsageError(
          '\nMust specify an extended schema JSON file as opposed to '
          'text schema.\n ebq requires every command to provide the '
          'extended schema file.')
    if ',' in schema:
      raise app.UsageError(
          '\nMust specify a local source file, cannot upload '
          'URIs with encryption yet.')

  def _GetTableCreationTime(self, identifier):
    reference = super(EncryptedBigqueryClient, self).GetReference(identifier)
    object_info = super(EncryptedBigqueryClient, self).GetObjectInfo(reference)
    if object_info is None:
      raise bigquery_client.BigqueryNotFoundError(
          'Table %s not found.' % identifier, None, None, None)
    if 'creationTime' not in object_info:
      raise bigquery_client.BigqueryNotFoundError(
          'Could not gather creation time from table.', None, None, None)
    return object_info['creationTime']

  def _GetEBQTableInfo(self, identifier):
    reference = super(EncryptedBigqueryClient, self).GetReference(identifier)
    object_info = super(EncryptedBigqueryClient, self).GetObjectInfo(reference)
    if object_info is None:
      raise bigquery_client.BigqueryNotFoundError(
          'Table %s not found.' % identifier, None, None, None)
    if 'description' not in object_info:
      raise bigquery_client.BigqueryNotFoundError(
          'Could not get essential EBQ info from description. Only use ebq '
          'update to edit table descriptions. Using bq will cause the table '
          'to be unusable.', None, None, None)
    description = object_info['description'].split('||')
    try:
      hashed_key = description[-3].split('Hash of master key: ')[1]
      version_number = description[-2].split('Version: ')[1]
      schema = description[-1].split('Schema: ')[1]
    except Exception:
      raise bigquery_client.BigqueryNotFoundError(
          'Corrupt description containing essential EBQ info.', None, None,
          None)
    return hashed_key, version_number, schema

  def _LoadJobStatistics(self, manifest, job):
    """Load statistics from the bq job into manifest.

    Args:
      manifest: query_lib.QueryManifest instance, to load stats into
      job: dict, job stats out of BQ API
    """
    try:
      job_state = job['status']['state']
      if job_state != 'DONE':
        return

      query_plans = job['statistics']['query']['queryPlan']
      if len(query_plans) < 1:
        return

      records_written = int(query_plans[-1][manifest.RECORDS_WRITTEN])
      if records_written < 0:
        return

      manifest.statistics[manifest.RECORDS_WRITTEN] = records_written

    except KeyError:
      return
    except ValueError:
      return

  def Load(self, destination_table, source, schema=None, **kwds):
    """Encrypt the given data and then load it into BigQuery.

    The job will execute synchronously if sync=True is provided as an
    argument.

    Args:
      destination_table: TableReference to load data into.
      source: String specifying source data to load.
      schema: The schema that defines fields to be loaded.
      **kwds: Passed on to self.ExecuteJob.

    Returns:
      The resulting job info.
    """
    self._CheckKeyfileFlag()
    self._CheckSchemaFile(schema)

    # To make encrypting more secure, we use different keys for each table
    # and cipher. To generate a different key for each table, we need a distinct
    # table identifier for each table. A table name is not secure since a table
    # can be deleted and created with the same name and, thus the same key. The
    # only distinct identifier happens to be creation time. Therefore, we must
    # construct a table if it does not exist so we can use the creation time
    # to encrypt values.
    try:
      self.CreateTable(destination_table, schema=schema)
    except bigquery_client.BigqueryDuplicateError:
      pass  # Table already exists.

    temp_dir = tempfile.mkdtemp()
    orig_schema = load_lib.ReadSchemaFile(schema)
    new_schema = load_lib.RewriteSchema(orig_schema)
    new_schema_file = '%s/schema.enc_schema' % temp_dir
    # write the new schema as a json file
    with open(new_schema_file, 'wt') as f:
      json.dump(new_schema, f, indent=2)
    new_source_file = '%s/data.enc_data' % temp_dir
    # TODO(user): Put the filepath to the master key in .bigqueryrc file.
    master_key = load_lib.ReadMasterKeyFile(self.master_key_filename, True)
    table_name = str(destination_table).split(':')[-1]
    table_id = '%s_%s' % (table_name,
                          self._GetTableCreationTime(str(destination_table)))
    hashed_table_key, table_version, table_schema = self._GetEBQTableInfo(
        str(destination_table))
    hashed_master_key = hashlib.sha1(master_key)
    # pylint: disable=too-many-function-args
    hashed_master_key = base64.b64encode(hashed_master_key.digest())
    if hashed_master_key != hashed_table_key:
      raise bigquery_client.BigqueryAccessDeniedError(
          'Invalid master key for this table.', None, None, None)
    if table_version != util.EBQ_TABLE_VERSION:
      raise bigquery_client.BigqueryNotFoundError(
          'Invalid table version.', None, None, None)
    # TODO(user): Generate a different key.
    cipher = ecrypto.ProbabilisticCipher(master_key)
    table_schema = cipher.Decrypt(base64.b64decode(table_schema), raw=True)
    table_schema = zlib.decompress(table_schema)
    table_schema = table_schema.decode('utf-8')
    table_schema = json.loads(table_schema)
    if table_schema != orig_schema:
      raise bigquery_client.BigqueryAccessDeniedError(
          'Invalid schema for this table.', None, None, None)
    if kwds['source_format'] == 'NEWLINE_DELIMITED_JSON':
      load_lib.ConvertJsonDataFile(
          orig_schema, master_key, table_id, source, new_source_file)
    elif kwds['source_format'] == 'CSV' or not kwds['source_format']:
      load_lib.ConvertCsvDataFile(
          orig_schema, master_key, table_id, source, new_source_file)
    else:
      raise app.UsageError(
          'Currently, we do not allow loading from file types other than\n'
          'NEWLINE_DELIMITED_JSON and CSV.')
    job = super(EncryptedBigqueryClient, self).Load(
        destination_table, new_source_file, schema=new_schema_file, **kwds)
    try:
      shutil.rmtree(temp_dir)
    except OSError:
      raise OSError('Temp file deleted by user before termination.')
    return job

  def Query(self, query, **kwds):
    """Execute the given query, returning the created job and info for print.

    Arguments:
      query: Query to execute.
      **kwds: Passed on to BigqueryClient.ExecuteJob.

    Returns:
      The resulting job info and other info necessary for printing.
    """
    self._CheckKeyfileFlag()
    master_key = load_lib.ReadMasterKeyFile(self.master_key_filename)

    try:
      clauses = parser.ParseQuery(query)
    except ParseException as e:
      raise bigquery_client.BigqueryInvalidQueryError(e, None, None, None)
    if clauses['FROM']:
      table_id = '%s_%s' % (clauses['FROM'][0],
                            self._GetTableCreationTime(clauses['FROM'][0]))
      hashed_table_key, table_version, table_schema = self._GetEBQTableInfo(
          clauses['FROM'][0])
      hashed_master_key = hashlib.sha1(master_key)
      # pylint: disable=too-many-function-args
      hashed_master_key = base64.b64encode(hashed_master_key.digest())
      if hashed_master_key != hashed_table_key:
        raise bigquery_client.BigqueryAccessDeniedError(
            'Invalid master key for this table.', None, None, None)
      if table_version != util.EBQ_TABLE_VERSION:
        raise bigquery_client.BigqueryNotFoundError(
            'Invalid table version.', None, None, None)
      cipher = ecrypto.ProbabilisticCipher(master_key)
      orig_schema = zlib.decompress(
          cipher.Decrypt(base64.b64decode(table_schema), raw=True))
      orig_schema = json.loads(orig_schema.decode('utf-8'))
    else:
      table_id = None
      orig_schema = []

    manifest = query_lib.QueryManifest.Generate()
    rewritten_query, print_args = query_lib.RewriteQuery(clauses,
                                                         orig_schema,
                                                         master_key,
                                                         table_id,
                                                         manifest)
    job = super(EncryptedBigqueryClient, self).Query(
        rewritten_query, **kwds)
    self._LoadJobStatistics(manifest, job)

    printer = EncryptedTablePrinter(**print_args)
    bq.Factory.ClientTablePrinter.SetTablePrinter(printer)

    return job

  def UpdateTable(self, reference, schema=None,
                  description=None, friendly_name=None, expiration=None):
    """Updates a table.

    Arguments:
      reference: the DatasetReference to update.
      schema: an optional schema.
      description: an optional table description.
      friendly_name: an optional friendly name for the table.
      expiration: optional expiration time in milliseconds since the epoch.
    """
    if schema:
      self._CheckKeyfileFlag()

    if description:
      hashed_table_key, table_version, table_schema = (
          self._GetEBQTableInfo(str(reference)))
      if schema:
        master_key = load_lib.ReadMasterKeyFile(self.master_key_filename)
        # pylint: disable=too-many-function-args
        hashed_key = base64.b64encode(hashlib.sha1(master_key).digest())
        if hashed_key != hashed_table_key:
          raise bigquery_client.BigqueryAccessDeniedError(
              'Invalid master key for this table.', None, None, None)
        cipher = ecrypto.ProbabilisticCipher(master_key)
        real_schema = json.dumps(load_lib.RewriteSchema(schema))
        real_schema = str.encode('utf-8')
        table_schema = base64.b64encode(
            cipher.Encrypt(zlib.compress(real_schema)))
      description = util.ConstructTableDescription(
          description, hashed_table_key, table_version, table_schema)

    # Rewrite the schema if the schema is to be updated.
    if schema:
      schema = load_lib.RewriteSchema(schema)

    super(EncryptedBigqueryClient, self).UpdateTable(
        reference, schema, description, friendly_name, expiration)

  def CreateTable(self, reference, ignore_existing=False, schema=None,
                  description=None, friendly_name=None, expiration=None):
    """Create a table corresponding to TableReference.

    Arguments:
      reference: the TableReference to create.
      ignore_existing: (boolean, default False) If False, raise an exception if
        the dataset already exists.
      schema: An required schema (also requires a master key).
      description: an optional table description.
      friendly_name: an optional friendly name for the table.
      expiration: optional expiration time in milliseconds since the epoch.

    Raises:
      TypeError: if reference is not a TableReference.
      BigqueryDuplicateError: if reference exists and ignore_existing
        is False.
    """
    if schema is None:
      raise bigquery_client.BigqueryNotFoundError(
          'A schema must be specified when making a table.', None, None, None)
    self._CheckKeyfileFlag()
    schema = load_lib.ReadSchemaFile(schema)
    master_key = load_lib.ReadMasterKeyFile(self.master_key_filename, True)
    # pylint: disable=too-many-function-args
    hashed_key = base64.b64encode(hashlib.sha1(master_key).digest())
    cipher = ecrypto.ProbabilisticCipher(master_key)
    pretty_schema = json.dumps(schema)
    pretty_schema = pretty_schema.encode('utf-8')
    pretty_schema = zlib.compress(pretty_schema)
    encrypted_schema = base64.b64encode(cipher.Encrypt(pretty_schema))
    if description is None:
      description = ''
    new_description = util.ConstructTableDescription(
        description, hashed_key, util.EBQ_TABLE_VERSION, encrypted_schema)
    new_schema = load_lib.RewriteSchema(schema)
    super(EncryptedBigqueryClient, self).CreateTable(
        reference, ignore_existing, new_schema, new_description, friendly_name,
        expiration)


def _DecryptRows(fields, rows, master_key, table_id, schema, query_list,
                 aggregation_query_list, unencrypted_query_list,
                 manifest=None):
  """Decrypts all values in rows.

  Arguments:
    fields: Column names.
    rows: Table values.
    master_key: Key to get ciphers.
    table_id: Used to generate keys.
    schema: Represents information about fields.
    query_list: List of fields that were queried.
    aggregation_query_list: List of aggregations of fields that were queried.
    unencrypted_query_list: List of unencrypted expressions.
    manifest: optional, query_lib.QueryManifest instance.
  Returns:
    A dictionary that returns for each query, a list of decrypted values.

  Raises:
    bigquery_client.BigqueryInvalidQueryError: User trying to query for a
    SEARCHWORD encrypted field. SEARCHWORD encrypted fields cannot be decrypted.
  """
  # create ciphers for decryption
  prob_cipher = ecrypto.ProbabilisticCipher(
      ecrypto.GenerateProbabilisticCipherKey(master_key, table_id))
  pseudonym_cipher = ecrypto.PseudonymCipher(
      ecrypto.GeneratePseudonymCipherKey(master_key, table_id))
  homomorphic_int_cipher = ecrypto.HomomorphicIntCipher(
      ecrypto.GenerateHomomorphicCipherKey(master_key, table_id))
  homomorphic_float_cipher = ecrypto.HomomorphicFloatCipher(
      ecrypto.GenerateHomomorphicCipherKey(master_key, table_id))

  ciphers = {
      util.PROBABILISTIC_PREFIX: prob_cipher,
      util.PSEUDONYM_PREFIX: pseudonym_cipher,
      util.HOMOMORPHIC_INT_PREFIX: homomorphic_int_cipher,
      util.HOMOMORPHIC_FLOAT_PREFIX: homomorphic_float_cipher,
  }

  queried_values = {}
  for query in query_list:
    if len(query.split(' ')) >= 3 and query.split(' ')[-2] == 'AS':
      queried_values[' '.join(query.split(' ')[:-2])] = []
    else:
      queried_values[query] = []
  for query in aggregation_query_list:
    queried_values[query] = []
  for i in xrange(len(unencrypted_query_list)):
    queried_values['%s%d_' % (util.UNENCRYPTED_ALIAS_PREFIX, i)] = []

  # If a manifest is supplied rewrite the column names according to any
  # computed aliases that were used. Otherwise, resort to the old scheme
  # of substituting the '.' in multidimensional schemas in/out.
  if manifest is not None:
    for i in xrange(len(fields)):
      # TODO(user): This is a hash lookup on every column name.
      # The lookup is efficient and the column names are sufficiently random
      # as compared to likely human language column names such that false
      # hits should not be possible. However this may need future revision.
      n = manifest.GetColumnNameForAlias(fields[i]['name'])
      if n is not None:
        fields[i]['name'] = n
  else:
    for i in xrange(len(fields)):
      fields[i]['name'] = fields[i]['name'].replace(
          util.PERIOD_REPLACEMENT, '.')

  for i in xrange(len(fields)):
    encrypted_name = fields[i]['name'].split('.')[-1]
    if fields[i]['type'] == 'TIMESTAMP':
      queried_values[fields[i]['name']] = _GetTimestampValues(rows, i)
    elif encrypted_name.startswith(util.PROBABILISTIC_PREFIX):
      queried_values[fields[i]['name']] = (
          _DecryptValues(fields[i]['name'], rows, i, ciphers, schema,
                         util.PROBABILISTIC_PREFIX))
    elif encrypted_name.startswith(util.PSEUDONYM_PREFIX):
      queried_values[fields[i]['name']] = (
          _DecryptValues(fields[i]['name'], rows, i, ciphers, schema,
                         util.PSEUDONYM_PREFIX))
    elif encrypted_name.startswith(util.SEARCHWORDS_PREFIX):
      raise bigquery_client.BigqueryInvalidQueryError(
          'Cannot decrypt searchwords encryption. Decryption of SEARCHWORDS '
          'is limited to PROBABILISTIC_SEARCHWORDS encryption.', None, None,
          None)
    elif encrypted_name.startswith(util.HOMOMORPHIC_INT_PREFIX):
      queried_values[fields[i]['name']] = (
          _DecryptValues(fields[i]['name'], rows, i, ciphers, schema,
                         util.HOMOMORPHIC_INT_PREFIX))
    elif encrypted_name.startswith(util.HOMOMORPHIC_FLOAT_PREFIX):
      queried_values[fields[i]['name']] = (
          _DecryptValues(fields[i]['name'], rows, i, ciphers, schema,
                         util.HOMOMORPHIC_FLOAT_PREFIX))
    elif (encrypted_name.startswith(util.UNENCRYPTED_ALIAS_PREFIX) and
          encrypted_name.endswith('_')):
      queried_values[fields[i]['name']] = (
          _GetUnencryptedValuesWithType(rows, i, fields[i]['type']))
    elif encrypted_name.startswith('f') and encrypted_name.endswith('_'):
      index = int(fields[i]['name'][1:-1])
      original_fieldname = aggregation_query_list[index]
      original_fieldname = original_fieldname.strip()
      if (len(original_fieldname.split(' ')) >= 3 and
          original_fieldname.split(' ')[-2].lower() == 'within'):
        actual_field = original_fieldname.split(' ')[:-2]
        actual_field = ' '.join(actual_field)
      else:
        actual_field = original_fieldname
      if original_fieldname.startswith(util.GROUP_CONCAT_PREFIX):
        concat_field = actual_field.split(
            util.GROUP_CONCAT_PREFIX)[1][:-1].strip()
        encrypted_name = concat_field.split('.')[-1]
        if encrypted_name.startswith(util.PROBABILISTIC_PREFIX):
          queried_values[original_fieldname] = (
              _DecryptGroupConcatValues(original_fieldname, rows, i, ciphers,
                                        schema, util.PROBABILISTIC_PREFIX))
        elif encrypted_name.startswith(util.PSEUDONYM_PREFIX):
          queried_values[original_fieldname] = (
              _DecryptGroupConcatValues(original_fieldname, rows, i, ciphers,
                                        schema, util.PSEUDONYM_PREFIX))
        elif (encrypted_name.startswith(util.HOMOMORPHIC_INT_PREFIX) or
              encrypted_name.startswith(util.HOMOMORPHIC_FLOAT_PREFIX)):
          raise bigquery_client.BigqueryInvalidQueryError(
              'GROUP_CONCAT only accepts string type.', None, None, None)
        elif encrypted_name.startswith(util.SEARCHWORDS_PREFIX):
          raise bigquery_client.BigqueryInvalidQueryError(
              'Invalid query, cannot recover searchwords encryption.', None,
              None, None)
        else:
          for j in xrange(len(rows)):
            queried_values[original_fieldname].append(rows[j][i])
      elif (original_fieldname.startswith('COUNT(') or
            original_fieldname.startswith('AVG(') or
            original_fieldname.startswith('SUM(')):
        queried_values[original_fieldname] = (
            _GetUnencryptedValuesWithType(rows, i, fields[i]['type']))
      elif original_fieldname.startswith('TOP('):
        fieldname = actual_field.split('TOP(')[1][:-1].strip()
        fieldname = fieldname.split(',')[0].strip()
        if fieldname.split('.')[-1].startswith(util.PSEUDONYM_PREFIX):
          queried_values[original_fieldname] = (
              _DecryptValues(fieldname, rows, i, ciphers, schema,
                             util.PSEUDONYM_PREFIX))
        else:
          queried_values[original_fieldname] = (
              _GetUnencryptedValues(original_fieldname, rows, i, schema))
      elif original_fieldname.startswith(
          util.PAILLIER_SUM_PREFIX):
        sum_argument = original_fieldname.split(
            util.PAILLIER_SUM_PREFIX)[1]
        sum_argument = sum_argument.split(',')[0][:-1]
        sum_argument = sum_argument.split('.')[-1]
        real_fieldname = original_fieldname.split(
            util.PAILLIER_SUM_PREFIX)[1]
        real_fieldname = real_fieldname.split(',')[0][:-1]
        if sum_argument.startswith(util.HOMOMORPHIC_INT_PREFIX):
          queried_values[original_fieldname] = (
              _DecryptValues(real_fieldname, rows, i, ciphers, schema,
                             util.HOMOMORPHIC_INT_PREFIX))
        elif sum_argument.startswith(util.HOMOMORPHIC_FLOAT_PREFIX):
          queried_values[original_fieldname] = (
              _DecryptValues(real_fieldname, rows, i, ciphers, schema,
                             util.HOMOMORPHIC_FLOAT_PREFIX))
      else:
        queried_values[fields[i]['name']] = (
            _GetUnencryptedValuesWithType(rows, i, fields[i]['type']))
    else:
      queried_values[fields[i]['name']] = (
          _GetUnencryptedValuesWithType(rows, i, fields[i]['type']))

  return queried_values


def _DecryptValues(field, table, column_index, ciphers, schema, prefix):
  field = field.split('.')
  field[-1] = field[-1].split(prefix)[1]
  field = '.'.join(field)
  value_type = util.GetFieldType(field, schema)
  if value_type not in ['string', 'integer', 'float']:
    raise ValueError('Not an known type.')
  cipher = ciphers[prefix]
  decrypted_column = []
  for i in range(len(table)):
    if table[i][column_index] is None:
      decrypted_value = util.LiteralToken('null', None)
    else:
      decrypted_value = unicode(
          cipher.Decrypt(table[i][column_index].encode('utf-8'))).strip()
      if value_type == 'string':
        decrypted_value = util.StringLiteralToken('"%s"' % decrypted_value)
      elif value_type == 'integer':
        decrypted_value = long(decrypted_value)
      else:
        decrypted_value = float(decrypted_value)
    decrypted_column.append(decrypted_value)
  return decrypted_column


def _GetUnencryptedValuesWithType(table, column_index, value_type):
  if (value_type is None or
      value_type.lower() not in ['string', 'integer', 'float']):
    raise ValueError('Not an known type.')
  value_type = value_type.lower()
  value_column = []
  for i in range(len(table)):
    if table[i][column_index] is None:
      value = util.LiteralToken('null', None)
    else:
      value = table[i][column_index]
      if value_type == 'string':
        value = util.StringLiteralToken('"%s"' % str(value).strip())
      elif value_type == 'integer':
        value = long(value)
      else:
        value = float(value)
    value_column.append(value)
  return value_column


def _GetUnencryptedValues(field, table, column_index, schema):
  value_type = util.GetFieldType(field, schema)
  if value_type is None:
    raise ValueError('Cannot find type.')
  return _GetUnencryptedValuesWithType(table, column_index, value_type)


def _DecryptGroupConcatValues(field, table, column_index, ciphers, schema,
                              prefix):
  if not field.startswith(util.GROUP_CONCAT_PREFIX):
    raise ValueError('Not a GROUP_CONCAT aggregation.')
  if len(field.split(' ')) >= 3:
    field = ' '.join(field.split(' ')[:-2])
  field = field.split(util.GROUP_CONCAT_PREFIX)[1][:-1]
  field = field.split('.')
  field[-1] = field[-1].split(prefix)[1]
  field = '.'.join(field)
  value_type = util.GetFieldType(field, schema)
  if value_type not in ['string', 'integer', 'float']:
    raise ValueError('Not an known type.')
  if value_type != 'string':
    raise bigquery_client.BigqueryInvalidQueryError(
        'Cannot GROUP_CONCAT non-string type.', None, None, None)
  cipher = ciphers[prefix]
  decrypted_column = []
  for i in range(len(table)):
    if table[i][column_index] is None:
      decrypted_column.append(util.LiteralToken('null', None))
      continue
    list_words = table[i][column_index].split(',')
    for k in range(len(list_words)):
      list_words[k] = unicode(cipher.Decrypt(
          list_words[k].encode('utf-8'))).strip()
    decrypted_column.append(
        util.StringLiteralToken('"%s"' % ','.join(list_words)))
  return decrypted_column


def _GetTimestampValues(table, column_index):
  """Returns new rows with timestamp values converted from float to string."""
  values = []
  for i in range(len(table)):
    if table[i][column_index] is None:
      value = util.LiteralToken('null', None)
    else:
      f = float(table[i][column_index])  # this handles sci-notation too
      s = util.SecToTimestamp(f)
      value = util.LiteralToken('"%s"' % s, s)
    values.append(value)
  return values


def _ComputeRows(new_postfix_stack, queried_values, manifest=None):
  """Substitutes queries back to expressions and evaluates them.

  Args:
    new_postfix_stack: All expressions for each column.
    queried_values: A dictionary that represents the queried values to a list
    of values that were received from server (all have been decrypted).
    manifest: optional but recommended, query_lib.QueryManifest object.
  Returns:
    A new table with results of each expression after query substitution.
  Raises:
    BigqueryInvalidQueryError: When a query request has parsed the query
      response and has determined the response is invalid.
  """
  table_values = []
  num_rows = 0

  if queried_values:
    # Try to obtain the number of rows (records) returned from the manifest.
    if manifest is not None:
      num_rows = manifest.statistics.get(manifest.RECORDS_WRITTEN, 0)

    # No manifest or no value obtained. Try to find the length of the FIRST
    # key:value pair where len(value) > 0.
    if num_rows <= 0:
      for k in queried_values:
        if len(queried_values[k]) > num_rows:
          num_rows = len(queried_values[k])
          break
  else:
    for stack in new_postfix_stack:
      ans = interpreter.Evaluate(stack)
      if ans is None:
        ans = 'NULL'
      table_values.append(str(ans))
    return [table_values]

  # No num_rows able to be found or calculated, or num_rows too few
  if num_rows <= 0:
    return []

  # Substitute queried values back into postfix stacks and evaluate them.
  for i in xrange(num_rows):
    row_values = []
    for j in xrange(len(new_postfix_stack)):
      temp_stack = list(new_postfix_stack[j])
      for k in xrange(len(temp_stack)):
        if (isinstance(temp_stack[k], util.AggregationQueryToken) or
            isinstance(temp_stack[k], util.UnencryptedQueryToken) or
            isinstance(temp_stack[k], util.FieldToken)):
          k_use = None
          for k_try in [temp_stack[k].alias, temp_stack[k]]:
            if k_try and k_try in queried_values:
              k_use = k_try
          if not k_use:
            raise bigquery_client.BigqueryInvalidQueryError(
                'Required %s column does not exist.' % temp_stack[k],
                None, None, None)
          temp_stack[k] = queried_values[k_use][i]
      ans = interpreter.Evaluate(temp_stack)
      if ans is None:
        ans = 'NULL'
      row_values.append(str(ans))
    table_values.append(row_values)

  return table_values
