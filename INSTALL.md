# encrypted-bigquery-client

This file includes instructions for installing and using the ebq command line
tool or client.

## Installing and running ebq

1. If you already have Python and setuptools installed, skip to step 2.
  * Install Python 2.7 or newer.
    * Your Linux distribution should likely already have packages for Python.
    * Otherwise see [python downloads](http://www.python.org/download/).
  * Install setuptools
    * [setuptools](http://pypi.python.org/pypi/setuptools)
    * The linked page describes how to download and install setuptools for your Python distribution.
2. Install ebq. There are two methods, `easy_install` and by manual installation:
  * easy_install
    * To install via easy_install, just type: `easy_install encrypted_bigquery`
  * Manual installation
    1. Get encrypted_bigquery-x.y.z archive from pypi and extract contents: `tar -zxvf encrypted_bigquery-x.y.tar.gz`
    2. Change to the ebq directory: `cd encrypted_bigquery-x.y.tar.gz`
    3. Change to the src dir: `cd src`
    4. Run the install script: `python setup.py install [--install-scripts=target_installation_directory]`

## Run the tests

From the `src/` directory under the location where the tarball
was extracted, run all tests before continuing:

```
$ python run_all_tests.py
```

Make sure that you only see messages like `OK, Ran xx tests in xx seconds` and
no failures before continuing.

## Running ebq from the command line

1. Try out ebq by displaying a list of available commands:
```
$ target_installation_directory/ebq
```
2. To display help information about a particular command:
```
$ target_installation_directory/ebq help command_name
```

## Authorizing bq to access your BigQuery data

This is done in the same way as with the bq tool.

## Basic ebq tutorial

See [the tutorial](tutorial.md) for more information.
