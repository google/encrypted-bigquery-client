#!/usr/bin/env python
#
# Copyright 2013 Google Inc. All Rights Reserved.

"""Python script for interacting with BigQuery using encrypted data."""



import pkg_resources
pkg_resources.require('google_apputils==0.4.2')
import sys


from google.apputils import app
from google.apputils import appcommands
import gflags as flags

import bigquery_client
import bq
import encrypted_bigquery_client
import show_lib


flags.DEFINE_string(
    'master_key_filename', None,
    'The path of the file containing the master key to use in encrypting '
    'table data.')

FLAGS = flags.FLAGS


class _Load(bq._Load):  # pylint: disable=protected-access
  usage = """load --master_key_filename=<key filepath> <destination_table>
             <source> <ebq_schema>"""

  def RunWithArgs(self, destination_table, source, schema=None):
    """Perform a load operation of source into destination_table.

    Usage:
      load --master_key_filename=<key filepath> <destination_table>
      <source> <ebq_schema>

    The <destination_table> is the fully-qualified table name of table to
    create, or append to if the table already exists.

    The <source> argument can be a path to a single local file (CSV or
    NEWLINE_DELIMITED_JSON).

    The <schema> must be the name of a JSON file not a text schema.

    <schema> should be a file and it should contain a
    single array object, each entry of which should be an object with
    properties 'name', 'type', (optionally) 'mode' and (optionally) 'encrypt'.

    Examples:
      ebq load ds.new_tbl ./info.csv ./info_schema.json
      ebq load ds.new_tbl ./info.json ./info_schema.json

    Arguments:
      destination_table: Destination table name.
      source: Name of local file to import.
      schema: Filepath to JSON file, as above.
    """
    super(_Load, self).RunWithArgs(destination_table, source, schema)


class _Query(bq._Query):  # pylint: disable=protected-access
  usage = """query --master_key_filename=<key filepath> <sql_query>"""

  def RunWithArgs(self, *args):
    """Execute a query.

    Examples:
      ebq query --master_key_filename=key_file
      'select count(*) from publicdata:samples.shakespeare'

    Usage:
      query --master_key_filename=<key filepath> <sql_query>
    """
    super(_Query, self).RunWithArgs(*args)


# TODO(user): Eventualy rename columns just by index.
class _Show(bq._Show):  # pylint: disable=protected-access

  def RunWithArgs(self, identifier=''):
    """Show all information about an object.

    All fields that are encrypted are of type ciphertext.

    Examples:
      ebq show -j <job_id>
      ebq show dataset
      ebq show dataset.table
    """
    # pylint: disable=g-doc-exception
    client = bq.Client.Get()
    if self.j:
      reference = client.GetJobReference(identifier)
    elif self.d:
      reference = client.GetDatasetReference(identifier)
    else:
      reference = client.GetReference(identifier)
    if reference is None:
      raise app.UsageError('Must provide an identifier for show.')

    object_info = client.GetObjectInfo(reference)
    # Remove prefixes that were prepended during load/query.
    object_info = show_lib.RewriteShowSchema(object_info)

    # The JSON formats are handled separately so that they don't print
    # the record as a list of one record.
    if FLAGS.format in ['prettyjson', 'json']:
      bq._PrintFormattedJsonObject(object_info)  # pylint: disable=protected-access
    elif FLAGS.format in [None, 'sparse', 'pretty']:
      formatter = bq._GetFormatterFromFlags()  # pylint: disable=protected-access
      bigquery_client.BigqueryClient.ConfigureFormatter(
          formatter, type(reference), print_format='show')
      object_info = bigquery_client.BigqueryClient.FormatInfoByKind(object_info)
      formatter.AddDict(object_info)
      print '%s %s\n' % (reference.typename.capitalize(), reference)
      formatter.Print()
      print
      if (isinstance(reference, bigquery_client.ApiClientHelper.JobReference)
          and object_info['State'] == 'FAILURE'):
        error_result = object_info['status']['errorResult']
        error_ls = object_info['status'].get('errors', [])
        error = bigquery_client.BigqueryError.Create(
            error_result, error_result, error_ls)
        print 'Errors encountered during job execution. %s\n' % (error,)
    else:
      formatter = bq._GetFormatterFromFlags()  # pylint: disable=protected-access
      formatter.AddColumns(object_info.keys())
      formatter.AddDict(object_info)
      formatter.Print()


class _Update(bq._Update):  # pylint: disable=protected-access

  def RunWithArgs(self, identifier='', schema=''):
    """Updates a dataset of table with this name.

    See 'ebq help load' for more information on specifying the schema.

    Examples:
      ebq update --description "Dataset description" existing_dataset
      ebq update --description "My table" dataset.table
      ebq update -t new_dataset.newtable --master_key_filename= key_file
        newtable.schema
    """
    super(_Update, self).RunWithArgs(identifier, schema)


class _Make(bq._Make):  # pylint: disable=protected-access
  usage = """mk [-d] <identifier> OR mk [-t] --master_key_filename=<key_file>
             <identifier> <schema>"""

  def RunWithArgs(self, identifier='', schema=''):
    """Create a dataset or table with this name.

    See 'ebq help load' for more information on specifying the schema.

    Examples:
      ebq mk new_dataset
      ebq mk --master_key_filename=key_file new_dataset.new_table
      ebq --dataset=new_dataset mk --master_key_filename=key_file table
      ebq mk -t --master_key_filename=key_file new_dataset.newtable schema_file
    """
    super(_Make, self).RunWithArgs(identifier, schema)


class _Version(bq._Version):  # pylint: disable=protected-access

  @staticmethod
  def VersionNumber():
    """Return the version of ebq."""
    try:
      import pkg_resources  # pylint: disable=g-import-not-at-top
      version = pkg_resources.get_distribution('encrypted_bigquery').version
      return 'v%s' % (version,)
    except ImportError:
      return '<unknown>'

  def RunWithArgs(self):
    """Return the version of ebq."""
    version = type(self).VersionNumber()
    print 'This is Encrypted BigQuery CLI %s' % (version,)
    super(_Version, self).RunWithArgs()


# pylint: disable=g-bad-name
def run_main():
  """Function to be used as setuptools script entry point.

  Appcommands assumes that it always runs as __main__, but launching
  via a setuptools-generated entry_point breaks this rule. We do some
  trickery here to make sure that appcommands and flags find their
  state where they expect to by faking ourselves as __main__.
  """

  # Put the flags for this module somewhere the flags module will look
  # for them.
  # pylint: disable=protected-access
  new_name = sys.argv[0]
  sys.modules[new_name] = sys.modules['__main__']
  for flag in FLAGS.FlagsByModuleDict().get(__name__, []):
    FLAGS._RegisterFlagByModule(new_name, flag)
    for key_flag in FLAGS.KeyFlagsByModuleDict().get(__name__, []):
      FLAGS._RegisterKeyFlagForModule(new_name, key_flag)
  # pylint: enable=protected-access

  # Now set __main__ appropriately so that appcommands will be
  # happy.
  sys.modules['__main__'] = sys.modules[__name__]
  appcommands.Run()
  sys.modules['__main__'] = sys.modules.pop(new_name)


def main(unused_argv):
  bq.Factory.SetBigqueryClientFactory(
      encrypted_bigquery_client.EncryptedBigqueryClient)
  ebq_commands = {
      'load': _Load,
      'mk': _Make,
      'query': _Query,
      'show': _Show,
      'update': _Update,
      'version': _Version,
  }
  for command, function in ebq_commands.items():
    if command not in appcommands.GetCommandList():
      appcommands.AddCmd(command, function)
  # the __main__ module rewrite in run_main() or bq.run_main() etc only
  # works once and should only be run in the top-most setuptools entry_point
  # target.
  bq.main(unused_argv)


if __name__ == '__main__':
  app.run()
