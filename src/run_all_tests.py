#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""A script that runs all EBQ tests."""



import os

_EBQ_TESTS = [
    'common_crypto_test',
    'common_util_test',
    'ebq_crypto_test',
    'encrypted_bigquery_client_test',
    'load_lib_test',
    'number_test',
    'paillier_test',
    'query_interpreter_test',
    'query_parser_test',
    'show_lib_test',
]


def _RunTests():
  for test in _EBQ_TESTS:
    print 'Running %s.' % test
    os.system('python %s.py' % test)
    print


if __name__ == '__main__':
  _RunTests()
