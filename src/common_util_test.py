#!/usr/bin/env python
# Copyright 2013 Google Inc. All Rights Reserved.

"""Unit tests for common util functions."""



from google.apputils import app
import gflags as flags
from google.apputils import basetest as googletest

import bigquery_client
import common_util as util
import test_util

FLAGS = flags.FLAGS


class CommonUtilityTest(googletest.TestCase):

  def testGetEntryFromSchema(self):
    simple_schema = test_util.GetCarsSchema()
    nested_schema = test_util.GetJobsSchema()
    row = util.GetEntryFromSchema('Year', simple_schema)
    self.assertEqual(row['name'], 'Year')
    self.assertEqual(row['encrypt'], 'none')
    row = util.GetEntryFromSchema('citiesLived.place', nested_schema)
    self.assertEqual(row['name'], 'place')
    self.assertEqual(row['encrypt'], 'searchwords')
    row = util.GetEntryFromSchema(
        'citiesLived.job.position', nested_schema)
    self.assertEqual(row['name'], 'position')
    self.assertEqual(row['encrypt'], 'pseudonym')
    row = util.GetEntryFromSchema(
        'citiesLived.job', nested_schema)
    self.assertEqual(row, None)
    row = util.GetEntryFromSchema(
        'citiesLived.non_existent_field', nested_schema)
    self.assertEqual(row, None)



def main(_):
  googletest.main()

if __name__ == '__main__':
  app.run()
