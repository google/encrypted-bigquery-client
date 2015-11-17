#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""A script that runs all EBQ tests."""



import importlib
import os
import sys

_MODULE_PKG_HINTS = {
    'mox': 'mox==0.5.3',
    'stubout': 'mox==0.5.3',
}

_MODULE_DEPS = [
    'mox',
    'stubout',
]

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


def _CheckDeps():
  for module_name in _MODULE_DEPS:
    try:
      importlib.import_module(module_name)
    except ImportError, e:
      s = e.args[0]
      pkg_hint = _MODULE_PKG_HINTS.get(module_name, None)
      if pkg_hint:
        s = '%s, try:\n  easy_install --user %s' % (s, pkg_hint)
      raise SystemExit(s)


def _RunTests():
  for test in _EBQ_TESTS:
    print 'Running %s:' % test
    rc = os.system('python %s.py' % test)
    if rc != 0:
      raise SystemExit('Test %s failed' % test)
    print


def main():
  try:
    _CheckDeps()
    _RunTests()
    return 0
  except SystemExit, e:
    print >>sys.stderr, e.args[0]
    return 1


if __name__ == '__main__':
  sys.exit(main())
