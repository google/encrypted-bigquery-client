#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for show library module."""



from google.apputils import app
import logging
from google.apputils import basetest as googletest

import common_util as util
import show_lib

_PROJECT_SCHEMA = {
    u'datasetReference': {
        u'datasetId': u'test_dataset',
        u'projectId': u'hello:bye'
    },
    u'kind': u'bigquery#dataset',
    u'id': u'google.com:hello:bye',
    u'lastModifiedTime': u'1359656806229',
    u'etag': u'"1234"',
    u'creationTime': u'1359656806229',
    u'access': [
        {u'specialGroup': u'projectReaders', u'role': u'READER'},
        {u'specialGroup': u'projectWriters', u'role': u'WRITER'},
        {u'specialGroup': u'projectOwners', u'role': u'OWNER'},
        {u'userByEmail': u'john@abc.com', u'role': u'OWNER'}
    ],
    u'selfLink': (u'https://www.google.com/')
}

_JOB_SCHEMA = {
    u'jobReference': {
        u'jobId': u'job_2accd1c5a29147debac6bffa20298b8a',
        u'projectId': u'google.com:bigquerytestproject2'
    },
    u'configuration': {
        u'query': {
            u'destinationTable': {
                u'tableId': u'anon6bbd04d7_c1e0_43e8_9054_4718d1e051b0',
                u'projectId': u'google.com:bigquerytestproject2',
                u'datasetId': u'_65db679a0b3b9ac1ad9bebcd127a3eb6e5e540c0'
            },
            u'allowLargeResults': False,
            u'createDisposition': u'CREATE_IF_NEEDED',
            u'query': (u'SELECT Year as p698000442118338_ue0_ FROM '
                       'test_dataset.cars'),
            u'preserveNulls': True
        }
    },
    u'status': {u'state': u'DONE'},
    u'kind': u'bigquery#job',
    u'selfLink': (u'https://www.googleapis.com/bigquery/v2/projects/'
                  'google.com:bigquerytestproject2/jobs/'
                  'job_2accd1c5a29147debac6bffa20298b8a'),
    u'etag': u'"QfDcdHRltgUHaT5dWgEJvAgC8jo/d-3RDBihZ3i8-otdADIBcIKkh0I"',
    u'statistics': {
        u'startTime': u'1365615147209',
        u'query': {u'totalBytesProcessed': u'32'},
        u'totalBytesProcessed': u'32',
        u'endTime': u'1365615147497'
    },
    u'id': (u'google.com:bigquerytestproject2:'
            'job_2accd1c5a29147debac6bffa20298b8a')
}

_TABLE_SCHEMA = {
    u'schema': {
        u'fields': [
            {u'mode': u'REQUIRED', u'type': u'INTEGER', u'name': u'Year'},
            {u'mode': u'REQUIRED', u'type': u'STRING',
             u'name': util.PSEUDONYM_PREFIX + u'Make'},
            {u'mode': u'REQUIRED', u'type': u'STRING',
             u'name': util.SEARCHWORDS_PREFIX + u'Model'},
            {u'mode': u'REQUIRED', u'type': u'STRING',
             u'name': util.PROBABILISTIC_PREFIX + u'Model'},
            {u'mode': u'NULLABLE', u'type': u'STRING',
             u'name': util.SEARCHWORDS_PREFIX + u'Description'},
            {u'mode': u'NULLABLE', u'type': u'STRING',
             u'name': util.SEARCHWORDS_PREFIX + u'Website'},
            {u'mode': u'REQUIRED', u'type': u'STRING',
             u'name': util.PROBABILISTIC_PREFIX + u'Price'},
            {u'mode': u'REQUIRED', u'type': u'STRING',
             u'name': util.HOMOMORPHIC_INT_PREFIX + u'Invoice_Price'},
            {u'mode': u'REQUIRED', u'type': u'STRING',
             u'name': (util.HOMOMORPHIC_FLOAT_PREFIX +
                       u'Holdback_Percentage')}
        ]
    },
    u'kind': u'bigquery#table',
    u'selfLink': (u'https://www.googleapis.com/bigquery/v2/projects/'
                  'google.com:bigquerytestproject2/datasets/test_dataset/'
                  'tables/cars'),
    u'tableReference': {
        u'projectId': u'google.com:bigquerytestproject2',
        u'tableId': u'cars',
        u'datasetId': u'test_dataset'
    },
    u'creationTime': u'1365529054611',
    u'numRows': u'4',
    u'numBytes': u'4428',
    u'lastModifiedTime': u'1365529054611',
    u'id': u'google.com:bigquerytestproject2:test_dataset.cars',
    u'etag': u'"QfDcdHRltgUHaT5dWgEJvAgC8jo/VDWLlR4IJX4Ty6pfJQn0GeX-hig"'
}

_REWRITTEN_TABLE_SCHEMA = {
    u'schema': {
        u'fields': [
            {u'mode': u'REQUIRED', u'type': u'INTEGER', u'name': u'Year'},
            {u'mode': u'REQUIRED', u'type': u'CIPHERTEXT',
             u'name': u'Make'},
            {u'mode': u'REQUIRED', u'type': u'CIPHERTEXT',
             u'name': u'Model'},
            {u'mode': u'REQUIRED', u'type': u'CIPHERTEXT',
             u'name': u'Model'},
            {u'mode': u'NULLABLE', u'type': u'CIPHERTEXT',
             u'name': u'Description'},
            {u'mode': u'NULLABLE', u'type': u'CIPHERTEXT',
             u'name': u'Website'},
            {u'mode': u'REQUIRED', u'type': u'CIPHERTEXT',
             u'name': u'Price'},
            {u'mode': u'REQUIRED', u'type': u'CIPHERTEXT',
             u'name': u'Invoice_Price'},
            {u'mode': u'REQUIRED', u'type': u'CIPHERTEXT',
             u'name': u'Holdback_Percentage'}
        ]
    },
    u'kind': u'bigquery#table',
    u'selfLink': (u'https://www.googleapis.com/bigquery/v2/projects/'
                  'google.com:bigquerytestproject2/datasets/test_dataset/'
                  'tables/cars'),
    u'tableReference': {
        u'projectId': u'google.com:bigquerytestproject2',
        u'tableId': u'cars',
        u'datasetId': u'test_dataset'
    },
    u'creationTime': u'1365529054611',
    u'numRows': u'4',
    u'numBytes': u'4428',
    u'lastModifiedTime': u'1365529054611',
    u'id': u'google.com:bigquerytestproject2:test_dataset.cars',
    u'etag': u'"QfDcdHRltgUHaT5dWgEJvAgC8jo/VDWLlR4IJX4Ty6pfJQn0GeX-hig"'
}




class ShowLibraryTest(googletest.TestCase):

  def testShowJob(self):
    logging.debug('Running testShowJob method.')
    new_schema = show_lib.RewriteShowSchema(_JOB_SCHEMA)
    self.assertEqual(_JOB_SCHEMA, new_schema)

  def testShowProject(self):
    logging.debug('Running testShowProject method.')
    new_schema = show_lib.RewriteShowSchema(_PROJECT_SCHEMA)
    self.assertEqual(_PROJECT_SCHEMA, new_schema)

  def testShowTable(self):
    logging.debug('Running testShowTable method.')
    new_schema = show_lib.RewriteShowSchema(_TABLE_SCHEMA)
    self.assertEqual(_REWRITTEN_TABLE_SCHEMA, new_schema)


def main(_):
  googletest.main()

if __name__ == '__main__':
  app.run()
