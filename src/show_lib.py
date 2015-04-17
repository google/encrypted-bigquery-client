#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Library used by Encrypted Bigquery show command."""



from copy import deepcopy


import common_util as util


def RewriteShowSchema(original_schema):
  """Rewrites schema removing internal prefixes prepended during load/query.

  Arguments:
    original_schema: Schema of table with ugly prefixes.

  Returns:
    New schema without prefixes.
  """
  # Showing a table, rewriting is necessary.
  if 'schema' in original_schema and 'fields' in original_schema['schema']:
    original_schema['schema']['fields'] = (
        _RewriteShowSchema(original_schema['schema']['fields']))
  # Showing a job or project, rewriting is not necessary.
  return original_schema


def _RewriteShowSchema(original_schema):
  rewritten_schema = deepcopy(original_schema)
  for i in range(len(rewritten_schema)):
    # Record type, recursively edit all fields inside record.
    if rewritten_schema[i]['type'] == 'RECORD':
      rewritten_schema[i]['fields'] = (
          _RewriteShowSchema(rewritten_schema[i]['fields']))
    else:
      field_name = rewritten_schema[i]['name']
      # If encrypted field, change back to original name.
      # Also, change type from string to ciphertext.
      for prefix in util.ENCRYPTED_FIELD_PREFIXES:
        if field_name.startswith(prefix):
          rewritten_schema[i]['name'] = field_name.split(prefix)[1]
          rewritten_schema[i]['type'] = 'CIPHERTEXT'
          break
  return rewritten_schema
