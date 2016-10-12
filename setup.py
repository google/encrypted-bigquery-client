#!/usr/bin/env python
#
# Copyright 2013 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Setup configuration."""

import platform

from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup  # pylint: disable=g-import-not-at-top

# Configure the required packages and scripts to install, depending on
# Python versions and OS.
REQUIRED_PACKAGES = [
    'google-apputils==0.4.2',
    'python-gflags==2.0',
    'ipaddr==2.1.10',
    'bigquery==2.0.14',
    'pyparsing==1.5.5',
    'pycrypto==2.6.1',
    'httplib2==0.8',
    'mox==0.5.3',
    ]
# TODO: Export tests.
CONSOLE_SCRIPTS = [
    'ebq = ebq:run_main',
    ]

if platform.system() == 'Windows':
  REQUIRED_PACKAGES.append('pyreadline')

py_version = platform.python_version()
if py_version < '2.7' or py_version >= '3':
  raise ValueError('Encrypted BigQuery requires Python >= 2.7 and < 3.')

_EBQ_VERSION = '1.46'  # keep in sync with BUILD:VERSION

setup(name='encrypted_bigquery',
      version=_EBQ_VERSION,
      description='Encrypted BigQuery command-line tool',
      url='http://code.google.com/p/encrypted-bigquery-client/',
      author='Google Inc.',
      author_email='ebq-team@google.com',
      # Contained modules and scripts.
      package_dir={'': 'src'},
      py_modules=[
          'common_crypto',
          'common_crypto_test',
          'common_util',
          'common_util_test',
          'ebq',
          'ebq_crypto',
          'ebq_crypto_test',
          'encrypted_bigquery_client',
          'encrypted_bigquery_client_test',
          'load_lib',
          'load_lib_test',
          'number',
          'number_test',
          'paillier',
          'paillier_test',
          'query_interpreter',
          'query_interpreter_test',
          'query_lib',
          'query_lib_test',
          'query_parser',
          'query_parser_test',
          'show_lib',
          'show_lib_test',
          'test_util',
          'run_all_tests',
          ],
      entry_points={
          'console_scripts': CONSOLE_SCRIPTS,
          },
      install_requires=REQUIRED_PACKAGES,
      provides=[
          'encrypted_bigquery (%s)' % (_EBQ_VERSION,),
      ],
      # PyPI package information.
      classifiers=[
          'Development Status :: 4 - Beta',
          'Environment :: Console',
          'Intended Audience :: Developers',
          'Intended Audience :: End Users/Desktop',
          'License :: OSI Approved :: Apache Software License',
          'Operating System :: MacOS :: MacOS X',
          'Operating System :: Microsoft :: Windows',
          'Operating System :: POSIX :: Linux',
          'Programming Language :: Python :: 2.7',
          'Topic :: Database :: Front-Ends',
          'Topic :: Software Development :: Libraries',
          'Topic :: Software Development :: Libraries :: Python Modules',
          ],
      license='Apache 2.0',
      keywords='google encrypted bigquery library',
     )
