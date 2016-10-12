#!/usr/bin/env python
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Encrypts data file contents into a new file to upload to Bigquery.

Plaintext table data (either CSV or NEWLINE_DELIMITED_JSON) is encrypted based
on an extended table schema provided by the user where the extended schema is a
modification of the BigQuery schema to indicate which fields should be encrypted
and what kind of encryption to use. The possible encryption types are:
  1)"probabilistic" - randomized encryption or cbc with randomized IV.
  2)"pseudonym" - deterministic encryption or cbc with a fixed zero IV.
  3)"searchwords" - not really an encryption mode, but the hash of all possible
  word sequences are kept in a field with hashes separated by space.
  4)"probabilistic_searchwords" - instead of replacing the field data with word
  sequence hashes, it both a) probabilistically encrypts the field and b)
  appends another field to enable wordsearch over private data.
  5)"homomorphic" - encryption (i.e. paillier) of integers and floats that allow
  the server to sum over plaintexts by operating on ciphertexts.
  6)"none".
"""



import base64
from copy import deepcopy
import csv
import json
import os
import re

import gflags as flags
import logging

import bigquery_client
import common_crypto as ccrypto
import common_util as util
import ebq_crypto as ecrypto


FLAGS = flags.FLAGS

flags.DEFINE_boolean('debug', False, 'Enable printing of debug statements')
flags.DEFINE_string('table_schema_file', None,
                    'The path of the required file that contains the extended '
                    'table schema. The schema includes indications of which '
                    'fields should be encrypted.')
flags.DEFINE_string('encrypted_data_table_schema_file',
                    None,
                    'The path of the file to store the table schema '
                    'for the encrypted data. If none supplied then creates '
                    'the name using table_schema_file by replacing .schema '
                    'suffix with .enc_schema or else adding it if .schema '
                    'is not present.')
flags.DEFINE_string('table_data_file', None,
                    'The path of the required file that contains the table '
                    'data.')
flags.DEFINE_string('encrypted_table_data_file', None,
                    'The path of the file to store the table data with '
                    'specified fields encrypted. If none supplied then '
                    'creates the name using table_data_file by replacing '
                    '.data suffix with .enc_data or else adding it if .data '
                    'is not present.')


NONE = 'none'  # frequently used in JSON schema files


class EncryptConvertError(Exception):
  pass


def _UpdateFlags():
  """Update flags for encrypted data and its schema file."""
  if FLAGS.encrypted_data_table_schema_file is None:
    FLAGS.encrypted_data_table_schema_file = ToEnc(FLAGS.table_schema_file,
                                                   'schema')

  if FLAGS.encrypted_table_data_file is None:
    FLAGS.encrypted_data_table_file = ToEnc(FLAGS.table_data_file, 'data')

  def ToEnc(path, baseext):
    # returns basename in path, replacing baseext with ".enc_ + baseext" if
    # baseext is in pathname, else appending it.
    basename = os.path.basename(path)
    # replace '.ext' with '.enc_ext' or add it.
    (shortname, extension) = os.path.splittext(basename)
    newext = '.enc_' + baseext
    if extension == '.' + baseext:
      basename = shortname + newext
    else:
      basename += newext
    return basename


def ReadMasterKeyFile(filepath, create=False):
  """Read and return master key from file else create and store key in file."""
  if not filepath:
    raise bigquery_client.BigqueryNotFoundError(
        'Master key file not specified.', None, None, None)
  if not os.path.exists(filepath):
    if not create:
      raise bigquery_client.BigqueryNotFoundError(
          'Master key file does not exist.', None, None, None)
    print 'Key file does not exist. Generating a new key now.'
    _CreateAndStoreMasterKeyFile(filepath)
  with open(filepath, 'rt') as f:
    master_key = base64.b64decode(f.read())
    if len(master_key) < 16:
      raise EncryptConvertError('key in %s file is too short and may be '
                                'corrupted. Please supply a proper key file. '
                                % filepath)
  return master_key


def _CreateAndStoreMasterKeyFile(filepath):
  """Create and return a 16 byte random key and store it in a file."""
  key = ccrypto.GetRandBytes(16)
  _SecurelyCreateFile(filepath, 0600)
  with open(filepath, 'wt') as f:
    f.write(base64.b64encode(key))


def _SecurelyCreateFile(filepath, mode):
  """Create file exclusively with given mode or throw error (if file exists)."""
  try:
    fd = os.open(filepath, os.O_EXCL|os.O_CREAT, mode)
    os.close(fd)
  except (IOError, OSError):
    raise EncryptConvertError('Unable to create file %s', filepath)


def _ConvertSchemaFile():
  """Convert extended table schema file to basic schema file."""
  schema = ReadSchemaFile(FLAGS.table_schema_file)
  _ValidateExtendedSchema(schema)
  new_schema = RewriteSchema(schema)
  # write the new schema as a json file
  with open(FLAGS.encrypted_data_table_schema_file, 'wt') as f:
    json.dump(new_schema, f, indent=2)
  return (schema, new_schema)


def _ModifyFields(schema):
  for column in schema:
    for key in column.iterkeys():
      if key != 'name' and key != 'fields':
        if isinstance(column[key], basestring):
          column[key] = column[key].lower()
    if 'mode' not in column:
      column[u'mode'] = u'required'
    if column['type'] == 'record':
      _ModifyFields(column['fields'])
    elif 'encrypt' not in column:
      column[u'encrypt'] = unicode(NONE)


def _StrToUnicode(data):
  # Converts str to unicode (if needed). This is useful with json.load which
  # doesn't always seem to return unicode strings on various versions/platforms.
  if isinstance(data, dict):
    return {_StrToUnicode(k): _StrToUnicode(v) for k, v in data.iteritems()}
  elif isinstance(data, list):
    return [_StrToUnicode(d) for d in data]
  elif isinstance(data, str):
    return unicode(data)
  else:
    return data


def ReadSchemaFile(filepath):
  if not os.path.exists(filepath):
    raise EncryptConvertError('%s file does not exist', filepath)
  with open(filepath, 'rt') as f:
    schema = json.load(f)
    schema = _StrToUnicode(schema)
    _ValidateExtendedSchema(schema)
    _ModifyFields(schema)
    return schema


def _ValidateExtendedSchema(schema):
  """Validates extended bigquery table schema.

  Does some basic checks to catch errors in extended table schema. It
  checks that an 'encrypt' subfield is entered for each field.
  Args:
    schema: extended bigquery table schema.
  Raises:
    EncryptConvertError: when schema contains unexpected types.
  """
  for column in schema:
    if not isinstance(column, dict):
      raise EncryptConvertError('found a non dictionary element in schema: %s'
                                % column)
    if 'name' not in column or 'type' not in column:
      raise EncryptConvertError('Missing required keyfields in: %s' % column)
    elif column['type'] == 'record' and 'fields' not in column:
      raise EncryptConvertError('Missing either encrypt or fields keyfield.')
    elif 'fields' in column and column['type'] != 'record':
      raise EncryptConvertError('Cannot have fields keyfield if not record.')
    elif column.get('encrypt', NONE) != NONE and column['type'] == 'record':
      raise EncryptConvertError('Cannot encrypt a record type.')
    elif column.get('encrypt', NONE) != NONE and column['type'] == 'timestamp':
      raise EncryptConvertError('Cannot encrypt a timestamp type.')
    for key in column:
      if (key == 'type' and column[key].lower() in
          ['integer', 'string', 'float', 'timestamp']):
        continue
      elif (key == 'type' and column[key].lower() == 'record' and
            'fields' in column and isinstance(column['fields'], list)):
        continue
      elif (key == 'name' and (isinstance(column[key], unicode) or
          isinstance(column[key], str)) and column[key]):
        continue
      elif (key == 'mode' and column[key].lower() in
            ['required', 'nullable', 'repeated']):
        continue
      elif (key == 'encrypt' and column[key].lower() in
            [NONE, 'probabilistic', 'pseudonym', 'searchwords',
             'probabilistic_searchwords', 'homomorphic']):
        if (column['encrypt'].lower() in
            ['searchwords', 'probabilistic_searchwords'] and
            column['type'] != 'string'):
          raise EncryptConvertError('%s needs to be string type in column %s.'
                                    % (column['encrypt'], column))
        elif (column['encrypt'].lower() == 'homomorphic' and
              not column['type'] in ['integer', 'float']):
          raise EncryptConvertError('%s needs to be integer or float type in '
                                    'column %s.' % (column['encrypt'], column))
        continue
      elif (key == 'related' and
            isinstance(column[key], unicode)):
        if column.get('encrypt', '').lower() not in ['pseudonym']:
          raise EncryptConvertError('%s needs encrypt type pseudonym' % key)
        continue
      elif (key == 'searchwords_separator' and
            isinstance(column[key], unicode) and
            column[key] and column['encrypt'].lower()
            in ['searchwords', 'probabilistic_searchwords']):
        continue
      elif (key == 'max_word_sequence' and
            isinstance(column[key], int) and
            column['encrypt'].lower()
            in ['searchwords', 'probabilistic_searchwords']):
        continue
      elif (key == 'fields' and column['type'].lower() == 'record' and
            isinstance(column[key], list)):
        _ValidateExtendedSchema(column[key])
        continue
      else:
        error_string = ('Unexpected field key: %s, or unexpected field '
                        'value: %s' % (key, column[key]))
        raise EncryptConvertError(error_string)


def RewriteSchema(schema):
  """Rewrites the extended json table schema to handle encryption.

  The extended bigquery table schema indicates which field should be encrypted
  and what encryption to use. This function converts it to the basic bigquery
  format (without encrypt subfields) by changing field names to indicate
  encrypted ones by adding appropriate prefixes and to add field names for
  searchwords purposes. Returns a new_schema.
  Args:
    schema: extension of the BigQuery schema format to also describe which
      fields are encrypted and what type of encryption.

  Returns:
    the rewritten schema.

  Raises:
    EncryptConvertError: If a field is a nested type, but 'fields' is not
    a keyfield in the schema.
  """
  rewritten_schema = []
  for field in schema:
    if 'type' in field and field['type'] == 'record':
      if 'fields' not in field:
        raise EncryptConvertError(
            'If field is record type, it requires a fields keyfield.', None,
            None, None)
      inner_schema = {}
      for keys in field.iterkeys():
        inner_schema[keys] = field[keys]
      inner_schema['fields'] = RewriteSchema(field['fields'])
      rewritten_schema.append(inner_schema)
    else:
      _RewriteField(field, rewritten_schema)
  return rewritten_schema


def _RewriteField(field, schema):
  """Rewrites a single field inside the schema.

  It will prepend the proper prefixes to names depending on encryption type.

  Arguments:
    field: the original field that needs to be rewritten
    schema: the new schema that is going to contain the rewritten fields
  """

  new_field = deepcopy(field)
  # a separate count for new_schema since may insert field due to
  # probabilistic_searchwords mode for encrypt.
  if field['encrypt'] == NONE:
    del new_field['encrypt']
    schema.append(new_field)
    return
  if field['encrypt'] == 'probabilistic':
    new_field['name'] = util.PROBABILISTIC_PREFIX + field['name']
    new_field['type'] = 'string'
    del new_field['encrypt']
  elif field['encrypt'] == 'pseudonym':
    new_field['name'] = util.PSEUDONYM_PREFIX + field['name']
    new_field['type'] = 'string'
    del new_field['encrypt']
    if 'related' in new_field:
      del new_field['related']
  elif field['encrypt'] == 'searchwords':
    new_field['name'] = util.SEARCHWORDS_PREFIX + field['name']
    new_field['type'] = 'string'
    del new_field['encrypt']
    if 'searchwords_separator' in new_field:
      del new_field['searchwords_separator']
    if 'max_word_sequence' in new_field:
      del new_field['max_word_sequence']
  elif (field['encrypt'] == 'homomorphic' and
        field['type'] == 'integer'):
    new_field['name'] = util.HOMOMORPHIC_INT_PREFIX + field['name']
    new_field['type'] = 'string'
    del new_field['encrypt']
  elif field['encrypt'] == 'homomorphic' and field['type'] == 'float':
    new_field['name'] = util.HOMOMORPHIC_FLOAT_PREFIX + field['name']
    new_field['type'] = 'string'
    del new_field['encrypt']
  elif field['encrypt'] == 'probabilistic_searchwords':
    new_field['name'] = util.PROBABILISTIC_PREFIX + field['name']
    new_field['type'] = 'string'
    del new_field['encrypt']
    if 'searchwords_separator' in new_field:
      del new_field['searchwords_separator']
    if 'max_word_sequence' in new_field:
      del new_field['max_word_sequence']
    schema.append({'name': util.SEARCHWORDS_PREFIX + field['name'],
                   'type': 'string',
                   'mode': field['mode']})
  schema.append(new_field)


def _GenerateRelatedCiphers(schema, master_key, default_cipher):
  """Reads schema for pseudonym encrypt types and adds generating ciphers.

  Args:
    schema: list of dict, the db schema. modified by
    master_key: str, the master key
    default_cipher: obj, cipher that encrypt() can be called on.
  Returns:
    dict, mapping field names to index in schema.
  """
  map_name_to_index = {}
  for i in xrange(len(schema)):
    logging.warning(schema[i])
    map_name_to_index[schema[i]['name']] = i
    if schema[i].get('encrypt', None) == 'pseudonym':
      related = schema[i].get('related', None)
      if related is not None:
        pseudonym_cipher_related = ecrypto.PseudonymCipher(
            ecrypto.GeneratePseudonymCipherKey(
                master_key, str(related).encode('utf-8')))
        schema[i]['cipher'] = pseudonym_cipher_related
      else:
        schema[i]['cipher'] = default_cipher
  return map_name_to_index


def ConvertCsvDataFile(schema, master_key, table_id, infile, outfile):
  """Reads utf8 csv data, encrypts and stores into a new csv utf8 data file."""
  prob_cipher = ecrypto.ProbabilisticCipher(
      ecrypto.GenerateProbabilisticCipherKey(master_key, table_id))
  pseudonym_cipher = ecrypto.PseudonymCipher(
      ecrypto.GeneratePseudonymCipherKey(master_key, table_id))
  # TODO(user): ciphers and hash should not use the same key.
  string_hasher = ecrypto.StringHash(
      ecrypto.GenerateStringHashKey(master_key, table_id))
  homomorphic_int_cipher = ecrypto.HomomorphicIntCipher(
      ecrypto.GenerateHomomorphicCipherKey(master_key, table_id))
  homomorphic_float_cipher = ecrypto.HomomorphicFloatCipher(
      ecrypto.GenerateHomomorphicCipherKey(master_key, table_id))

  with open(infile, 'rb') as in_file:
    with open(outfile, 'wb') as out_file:
      num_columns = len(schema)
      csv_writer = csv.writer(out_file)
      _ValidateCsvDataFile(schema, infile)
      _GenerateRelatedCiphers(schema, master_key, pseudonym_cipher)
      csv_reader = _Utf8CsvReader(in_file, csv_writer)
      for row in csv_reader:
        new_row = []
        if len(row) != num_columns:
          raise EncryptConvertError('Number of fields in schema do not match '
                                    'in row: %s' % row)
        for i in xrange(num_columns):
          encrypt_mode = schema[i]['encrypt']
          if encrypt_mode == NONE:
            new_row.append(row[i].encode('utf-8'))
          elif encrypt_mode == 'probabilistic':
            new_row.append(
                prob_cipher.Encrypt(row[i]).encode('utf-8'))
          elif encrypt_mode == 'pseudonym':
            cipher = schema[i]['cipher']
            new_row.append(cipher.Encrypt(row[i]).encode('utf-8'))
          elif encrypt_mode == 'homomorphic' and schema[i]['type'] == 'integer':
            new_row.append(
                homomorphic_int_cipher.Encrypt(long(row[i])).encode('utf-8'))
          elif encrypt_mode == 'homomorphic' and schema[i]['type'] == 'float':
            new_row.append(
                homomorphic_float_cipher.Encrypt(float(row[i])).encode('utf-8'))
          elif encrypt_mode == 'searchwords':
            if 'searchwords_separator' in schema[i]:
              searchwords_separator = schema[i]['searchwords_separator']
            else:
              searchwords_separator = None
            if 'max_word_sequence' in schema[i]:
              max_word_sequence = schema[i]['max_word_sequence']
            else:
              max_word_sequence = 5
            new_row.append(string_hasher.GetHashesForWordSubsequencesWithIv(
                util.SEARCHWORDS_PREFIX + schema[i]['name'], row[i],
                separator=searchwords_separator,
                max_sequence_len=max_word_sequence).encode('utf-8'))
          elif encrypt_mode == 'probabilistic_searchwords':
            if 'searchwords_separator' in schema[i]:
              searchwords_separator = schema[i]['searchwords_separator']
            else:
              searchwords_separator = None
            if 'max_word_sequence' in schema[i]:
              max_word_sequence = schema[i]['max_word_sequence']
            else:
              max_word_sequence = 5
            new_row.append(string_hasher.GetHashesForWordSubsequencesWithIv(
                util.SEARCHWORDS_PREFIX + schema[i]['name'], row[i],
                separator=searchwords_separator,
                max_sequence_len=max_word_sequence).encode('utf-8'))
            new_row.append(
                prob_cipher.Encrypt(row[i]).encode('utf-8'))
        csv_writer.writerow(new_row)


def ConvertJsonDataFile(schema, master_key, table_id, infile, outfile):
  """Encrypts data in a json file based on schema provided.

  Arguments:
    schema: User defined values and types.
    master_key: Key to provide ciphers.
    table_id: Used to unique key for each table.
    infile: File to be encrypted.
    outfile: Location of encrypted file to outputted.
  """
  prob_cipher = ecrypto.ProbabilisticCipher(
      ecrypto.GenerateProbabilisticCipherKey(master_key, table_id))
  pseudonym_cipher = ecrypto.PseudonymCipher(
      ecrypto.GeneratePseudonymCipherKey(master_key, table_id))
  # TODO(user): ciphers and hash should not use the same key.
  string_hasher = ecrypto.StringHash(
      ecrypto.GenerateStringHashKey(master_key, table_id))
  homomorphic_int_cipher = ecrypto.HomomorphicIntCipher(
      ecrypto.GenerateHomomorphicCipherKey(master_key, table_id))
  homomorphic_float_cipher = ecrypto.HomomorphicFloatCipher(
      ecrypto.GenerateHomomorphicCipherKey(master_key, table_id))

  _ValidateJsonDataFile(schema, infile)
  with open(infile, 'rb') as in_file:
    with open(outfile, 'wb') as out_file:
      for line in in_file:
        data = json.loads(line)
        data = _StrToUnicode(data)
        rewritten_data = _ConvertJsonField(
            data, schema, prob_cipher, pseudonym_cipher, string_hasher,
            homomorphic_int_cipher, homomorphic_float_cipher)
        rewritten_data = json.dumps(rewritten_data)
        out_file.write(rewritten_data + '\n')


def _ConvertJsonField(data, schema, prob_cipher, pseudonym_cipher,
                      string_hasher, homomorphic_int_cipher,
                      homomorphic_float_cipher):
  rewritten_data = {}
  for schema_field in schema:
    field_name = schema_field['name']
    if field_name not in data:
      continue
    data_value = data[field_name]
    type_value = schema_field['type']
    mode_value = schema_field['mode']
    if mode_value == 'repeated' and type_value == 'record':
      list_value = []
      for single_data in data_value:
        list_value.append(_ConvertJsonField(
            single_data, schema_field['fields'], prob_cipher, pseudonym_cipher,
            string_hasher, homomorphic_int_cipher, homomorphic_float_cipher))
      rewritten_data[field_name] = list_value
    elif mode_value == 'repeated':
      encrypt_type = schema_field['encrypt']
      if encrypt_type == 'probabilistic_searchwords':
        searchwords_list = []
        for single_data in data_value:
          searchwords_list.append(_ConvertDataType(
              single_data, 'searchwords', schema_field, prob_cipher,
              pseudonym_cipher, string_hasher, homomorphic_int_cipher,
              homomorphic_float_cipher))
        rewritten_data[util.SEARCHWORDS_PREFIX +
                       field_name] = searchwords_list
        prob_list = []
        for single_data in data_value:
          prob_list.append(_ConvertDataType(
              single_data, 'probabilistic', schema_field, prob_cipher,
              pseudonym_cipher, string_hasher, homomorphic_int_cipher,
              homomorphic_float_cipher))
        rewritten_data[util.PROBABILISTIC_PREFIX + field_name] = prob_list
      else:
        list_value = []
        for single_data in data_value:
          list_value.append(_ConvertDataType(
              single_data, encrypt_type, schema_field, prob_cipher,
              pseudonym_cipher, string_hasher, homomorphic_int_cipher,
              homomorphic_float_cipher))
        rewritten_data[_RewriteFieldName(
            field_name, encrypt_type, type_value)] = list_value
    elif type_value == 'record':
      rewritten_data[field_name] = _ConvertJsonField(
          data_value, schema_field['fields'], prob_cipher, pseudonym_cipher,
          string_hasher, homomorphic_int_cipher, homomorphic_float_cipher)
    else:
      encrypt_type = schema_field['encrypt']
      if encrypt_type == 'probabilistic_searchwords':
        rewritten_data[util.SEARCHWORDS_PREFIX + field_name] = (
            _ConvertDataType(data_value, 'searchwords', schema_field,
                             prob_cipher, pseudonym_cipher, string_hasher,
                             homomorphic_int_cipher, homomorphic_float_cipher))
        rewritten_data[util.PROBABILISTIC_PREFIX + field_name] = (
            _ConvertDataType(data_value, 'probabilistic', schema_field,
                             prob_cipher, pseudonym_cipher, string_hasher,
                             homomorphic_int_cipher, homomorphic_float_cipher))
      else:
        rewritten_data[
            _RewriteFieldName(field_name, encrypt_type, type_value)] = (
                _ConvertDataType(data_value, encrypt_type, schema_field,
                                 prob_cipher, pseudonym_cipher, string_hasher,
                                 homomorphic_int_cipher,
                                 homomorphic_float_cipher))
  return rewritten_data


def _ConvertDataType(data_value, encrypt_type, schema, prob_cipher,
                     pseudonym_cipher, string_hasher, homomorphic_int_cipher,
                     homomorphic_float_cipher):
  type_value = schema['type']
  if encrypt_type == NONE:
    if type_value == 'string':
      return data_value.encode('utf-8')
    elif type_value == 'integer':
      return int(data_value)
    elif type_value == 'float':
      return float(data_value)
    elif type_value == 'timestamp':
      if (isinstance(data_value, (int, float, str, unicode)) and
          data_value != ''):  # pylint: disable=g-explicit-bool-comparison
        # valid input is an int, float, or non-empty string.
        # BQ is happy to accept epoch seconds timestamp values inside of
        # a string, so the safest transformation here is str().
        return str(data_value)
      else:
        return None
  elif encrypt_type == 'probabilistic':
    return prob_cipher.Encrypt(unicode(data_value)).encode('utf-8')
  elif encrypt_type == 'pseudonym':
    return pseudonym_cipher.Encrypt(unicode(data_value)).encode('utf-8')
  elif encrypt_type == 'homomorphic' and type_value == 'integer':
    return homomorphic_int_cipher.Encrypt(long(data_value)).encode('utf-8')
  elif encrypt_type == 'homomorphic' and type_value == 'float':
    return homomorphic_float_cipher.Encrypt(float(data_value)).encode('utf-8')
  elif encrypt_type == 'searchwords':
    if 'searchwords_separator' in schema:
      searchwords_separator = schema['searchwords_separator']
    else:
      searchwords_separator = None
    if 'max_word_sequence' in schema:
      max_word_sequence = schema['max_word_sequence']
    else:
      max_word_sequence = 5
    return string_hasher.GetHashesForWordSubsequencesWithIv(
        util.SEARCHWORDS_PREFIX + schema['name'], data_value,
        separator=searchwords_separator,
        max_sequence_len=max_word_sequence).encode('utf-8')


def _RewriteFieldName(name, encrypt_type, type_value):
  if encrypt_type == NONE:
    return name
  elif encrypt_type == 'homomorphic':
    if type_value == 'integer':
      return util.HOMOMORPHIC_INT_PREFIX + name
    elif type_value == 'float':
      return util.HOMOMORPHIC_FLOAT_PREFIX + name
  elif encrypt_type == 'probabilistic':
    return util.PROBABILISTIC_PREFIX + name
  elif encrypt_type == 'pseudonym':
    return util.PSEUDONYM_PREFIX + name
  elif encrypt_type == 'searchwords':
    return util.SEARCHWORDS_PREFIX + name
  else:
    raise ValueError('Unknown encryption type.')


def _ValidateCsvDataFile(schema, filepath):
  """Validates the data file against the table schema file provided.

  Does some basic checks to make sure number of fields are correct and that the
  type of data is consistent with the type specified in the table schema file.
  Args:
    schema: describes the type of data each field contains.
    filepath: the data file; currently only utf8 encoded data is supported.
  Raises:
    EncryptConvertError: if there is more or less data than the number of fields
      in a row as specified in the schema.
  """
  if not os.path.exists(filepath):
    raise EncryptConvertError('%s file does not exist' % filepath)
  num_columns = len(schema)
  with open(filepath, 'rb') as f:
    csv_reader = _Utf8CsvReader(f)
    row_num = 0
    for row in csv_reader:
      row_num += 1
      if len(row) != num_columns:
        raise EncryptConvertError('Incorrect number of fields in row %d: %s'
                                  % (row_num, row))
      for i in xrange(num_columns):
        _ValidateDataType(schema[i]['type'], row[i])


def _ValidateJsonDataFile(schema, filepath):
  if not os.path.exists(filepath):
    raise EncryptConvertError('%s file does not exist.' % filepath)
  with open(filepath, 'rb') as f:
    row_i = -1
    for line in f:
      row_i += 1
      data = json.loads(line)
      _ValidateJsonField(data, schema, row_i)


def _ValidateJsonField(data, schema, row_number):
  if not isinstance(data, dict):
    raise EncryptConvertError('Expected a dictionary: Got %s.' % str(data))
  for schema_field in schema:
    if schema_field['name'] not in data:
      if schema_field['mode'] != 'nullable':
        raise EncryptConvertError('%s not in data file.' % schema_field['name'])
      else:
        continue
    data_value = data[schema_field['name']]
    type_value = schema_field['type']
    mode_value = schema_field['mode']
    if mode_value == 'repeated' and type_value == 'record':
      if not isinstance(data_value, list):
        raise EncryptConvertError('Expected a repeated type for %s.'
                                  % schema_field['name'])
      for single_data in data_value:
        _ValidateJsonField(single_data, schema_field['fields'], row_number)
    elif mode_value == 'repeated':
      if not isinstance(data_value, list):
        raise EncryptConvertError('Expected a repeated type for %s.'
                                  % schema_field['name'])
      for single_data in data_value:
        _ValidateDataType(type_value, single_data)
    elif type_value == 'record':
      _ValidateJsonField(data_value, schema_field['fields'], row_number)
    else:
      _ValidateDataType(type_value, data_value)


def _ValidateDataTimestamp(data_value):
  """Validate a timestamp value.

  Args:
    data_value: str, one of the many BigQuery timestamp formats
  Returns:
    None
  Raises:
    ValueError: if this is not a valid timestamp
  """
  basic_transforms = [float, int]
  for transform in basic_transforms:
    try:
      v = transform(data_value)
      assert v <= 253402300800  # Y10K bug timestamp value used as max
      return
    except (ValueError, AssertionError):
      pass

  # Per the BigQuery documentation these formats are valid:
  # 2014-08-19 07:41:35.220 -05:00
  # 2014-08-19 12:41:35.220 UTC
  # 2014-08-19 12:41:35.220
  # 2014-08-19 12:41:35.220000
  # 2014-08-19T12:41:35.220Z
  # 1969-07-20 20:18:04
  # 1969-07-20 20:18:04 UTC
  # 1969-07-20T20:18:04

  timestamp_re = re.compile(
      r'^(?P<YMD>\d{4}-\d{2}-\d{2})[T ](?P<HMS>\d{2}:\d{2}:\d{2})'
      r'(?P<FRACTS>\.\d{1,6})?'
      r'((?P<ZULU>Z)|'
      r'\s(?P<UTC>)|'
      r'\s(?P<TZOFFSET>[\+\-]\d{2}:\d{2})|'
      r'\s(?P<TZ>[A-Z]{3})|'
      r')$'
  )

  m = timestamp_re.search(data_value)
  if m is None:
    raise ValueError('timestamp format')
  # TODO(user): One could perform further validation on portions of
  # the timestamp value to make sure individual components are in range.
  # However strftime() and strptime() don't maintain perfect feature parity
  # in python's wrapper of it even though linux glibc tries to. Also
  # there are things like leap seconds to consider, where 00:00:60 is a valid
  # HH:MM:SS time.


def _ValidateDataType(type_value, data_value):
  if type_value == 'integer':
    try:
      int(data_value)
    except ValueError:
      raise EncryptConvertError('Expected to convert string to int, '
                                'cannot convert %s.' % data_value)
  elif type_value == 'float':
    try:
      float(data_value)
    except ValueError:
      raise EncryptConvertError('Expected to convert string to float, '
                                'cannot convert %s.' % data_value)

  elif type_value == 'timestamp':
    try:
      _ValidateDataTimestamp(data_value)
    except ValueError, e:
      raise EncryptConvertError('Expected to parse timestamp, '
                                'cannot convert %s: %s' % (data_value, e))


def _Utf8CsvReader(utf8_csv_data, skip_rows_writer=None, **kwargs):
  """Read from utf8_csv_data filename and yield rows.

  Args:
    utf8_csv_data: str, filename like 'foo.csv'
    skip_rows_writer: callable like csv.writer, optional, where to write
        rows to if FLAGS.skip_leading_rows > 0
    **kwargs: other options supplied directly to the csv.reader instance.
  Yields:
    rows of rows from utf8_csv_data file, read as a CSV file.
  Raises:
    EncryptConvertError: if flag allow_quoted_newlines is supplied and false.
  """
  # normally the CSV format-related flags e.g. skip_leading_rows
  # are just uploaded to the cloud as part of the load RPC. however
  # since ebq preprocesses the CSV into encrypted format the flags
  # must be respected here also.
  if kwargs.get('dialect', None) is None:
    # set this to a likely sane default
    kwargs['dialect'] = 'excel'
  csv_reader = csv.reader(utf8_csv_data, **kwargs)
  if FLAGS.skip_leading_rows is not None:
    logging.debug('skip_leading_rows: %d', FLAGS.skip_leading_rows)
    for _ in xrange(FLAGS.skip_leading_rows):
      row = csv_reader.next()
      if skip_rows_writer is not None:
        skip_rows_writer.writerow(row)
  if FLAGS.allow_quoted_newlines is not None:
    # not sure how to configure csv reader to do this, and it is a bad idea.
    if not FLAGS.allow_quoted_newlines:
      raise EncryptConvertError('ebq cannot be configured to not allow '
                                'quoted newlines')
  for row in csv_reader:
    # decode UTF-8 back to Unicode, cell by cell:
    yield [unicode(cell, 'utf-8') for cell in row]


def _PrintDebug(schema):
  logging.debug('table_schema_file path is:\n\t ' + FLAGS.table_schema_file)
  logging.debug('encrypted_data_table_schema_file path is:\n\t ' +
                FLAGS.encrypted_data_table_schema_file)
  logging.debug('table_data_file is:\n\t ' + FLAGS.table_data_file)
  logging.debug('encrypted_table_data_file is:\n\t ' +
                FLAGS.encrypted_table_data_file)
  logging.debug(json.dumps(schema, indent=4))
