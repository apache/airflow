# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
"""
This test script tests GCPSheetsHook on a live Google sheet, to do so:
Update below with the details to your GCP service_account connection.
Be sure add a SPREADSHEET_ID and SHARE the Google Sheet
with the email of your service account (full edit access)
Also, in the google cloud connection be sure to add a scope,
such as https://www.googleapis.com/auth/spreadsheets
"""
import unittest
import time
# from pprint import pprint

google_import = True
try:
    from airflow.contrib.hooks.gcp_sheets_hook import GCPSheetsHook
except ImportError:
    google_import = False

GCP_CONN_ID = 'google_cloud_default'
SPREADHSEET_ID = None
SHEET_NAME = 'hook'
SLEEP_TIME = 1


class TestGCPSheetsHook(unittest.TestCase):
    ''' Test of get, update, clear in GCPSheetsHook'''
    def setUp(self):
        ''' Load the spreadsheet with the values used for testing. '''
        self.sheet_name = SHEET_NAME
        self.sheet_range = self.sheet_name + '!A:E'
        self.sheet_range_a = self.sheet_name + '!A:A'
        self.sheet_range_b = self.sheet_name + '!B:B'
        self.sheet_range_c = self.sheet_name + '!C:C'

        self.sheet_clear_range_a = self.sheet_name + '!A1:A3'
        self.sheet_clear_range_b = self.sheet_name + '!B1:B3'

        self.sheet_row_dim = [['$A$1', '$B$1', '$C$1', '$D$1', '$E$1'],
                              ['$A$2', '$B$2', '$C$2', '$D$2', '$E$2'],
                              ['$A$3', '$B$3', '$C$3', '$D$3', '$E$3']]

        self.sheet_col_dim = [['$A$1', '$A$2', '$A$3'],
                              ['$B$1', '$B$2', '$B$3'],
                              ['$C$1', '$C$2', '$C$3'],
                              ['$D$1', '$D$2', '$D$3'],
                              ['$E$1', '$E$2', '$E$3']]

        self.update_values_a = [['A1', 'A2', 'A3']]
        self.update_values_b = [['B1', 'B2', 'B3']]

        self.gsheet_hook = GCPSheetsHook(
            gcp_conn_id=GCP_CONN_ID, spreadsheet_id=SPREADHSEET_ID)
        self.gsheet_hook.clear(range_=self.sheet_range)
        self.gsheet_hook.update_values(range_=self.sheet_range,
                                       values=self.sheet_row_dim,
                                       include_values_in_response=True,
                                       major_dimension='ROWS')
        # pprint(response)
        time.sleep(SLEEP_TIME)

    @unittest.skipUnless(google_import, 'import error. Try `pip install google-api-python-client`')
    @unittest.skipUnless(SPREADHSEET_ID, 'SPREADHSEET_ID not provided.')
    def test_get_values_by_rows(self):
        response = self.gsheet_hook.get_values(
            range_=self.sheet_range, major_dimension='ROWS')
        self.assertEqual(response['valueRanges'][0]
                         ['values'], self.sheet_row_dim)
        time.sleep(SLEEP_TIME)

    @unittest.skipUnless(google_import, 'import error. Try `pip install google-api-python-client`')
    @unittest.skipUnless(SPREADHSEET_ID, 'SPREADHSEET_ID not provided.')
    def test_get_values_by_columns(self):
        response = self.gsheet_hook.get_values(
            range_=self.sheet_range, major_dimension='COLUMNS')
        # pprint(response)
        self.assertEqual(response['valueRanges'][0]
                         ['values'], self.sheet_col_dim)
        time.sleep(SLEEP_TIME)

    @unittest.skipUnless(google_import, 'import error. Try `pip install google-api-python-client`')
    @unittest.skipUnless(SPREADHSEET_ID, 'SPREADHSEET_ID not provided.')
    def test_batch_get_values_by_rows(self):
        response = self.gsheet_hook.batch_get_values(ranges=[self.sheet_range_a, self.sheet_range_b],
                                                     major_dimension='ROWS')
        # pprint(response)
        self.assertEqual(response['valueRanges'][0]['values'], [
                         [row[0]] for row in self.sheet_row_dim])
        self.assertEqual(response['valueRanges'][1]['values'], [
                         [row[1]] for row in self.sheet_row_dim])
        time.sleep(SLEEP_TIME)

    @unittest.skipUnless(google_import, 'import error. Try `pip install google-api-python-client`')
    @unittest.skipUnless(SPREADHSEET_ID, 'SPREADHSEET_ID not provided.')
    def test_batch_get_values_by_columns(self):
        response = self.gsheet_hook.batch_get_values(ranges=[self.sheet_range_a, self.sheet_range_b],
                                                     major_dimension='COLUMNS')
        # pprint(response)
        self.assertEqual(response['valueRanges'][0]
                         ['values'], [self.sheet_col_dim[0]])
        self.assertEqual(response['valueRanges'][1]
                         ['values'], [self.sheet_col_dim[1]])
        time.sleep(SLEEP_TIME)

    @unittest.skipUnless(google_import, 'import error. Try `pip install google-api-python-client`')
    @unittest.skipUnless(SPREADHSEET_ID, 'SPREADHSEET_ID not provided.')
    def test_update_values(self):
        response = self.gsheet_hook.update_values(range_=self.sheet_range_a,
                                                  values=self.update_values_a,
                                                  include_values_in_response=True,
                                                  major_dimension='COLUMNS')
        # pprint(response)
        self.assertEqual(response['updatedData']
                         ['values'], self.update_values_a)
        time.sleep(SLEEP_TIME)

    @unittest.skipUnless(google_import, 'import error. Try `pip install google-api-python-client`')
    @unittest.skipUnless(SPREADHSEET_ID, 'SPREADHSEET_ID not provided.')
    def test_batch_update_values(self):
        response = self.gsheet_hook.batch_update_values(ranges=[self.sheet_range_a, self.sheet_range_b],
                                                        values=[self.update_values_a, self.update_values_b],
                                                        include_values_in_response=True,
                                                        major_dimension='COLUMNS')
        # pprint(response)
        self.assertEqual(response['responses'][0]
                         ['updatedData']['values'], self.update_values_a)
        self.assertEqual(response['responses'][1]
                         ['updatedData']['values'], self.update_values_b)
        time.sleep(SLEEP_TIME)

    @unittest.skipUnless(google_import, 'import error. Try `pip install google-api-python-client`')
    @unittest.skipUnless(SPREADHSEET_ID, 'SPREADHSEET_ID not provided.')
    def test_clear_range(self):
        response = self.gsheet_hook.clear(range_=self.sheet_clear_range_a)
        # pprint(response)
        self.assertEqual(response['clearedRange'], self.sheet_clear_range_a)
        time.sleep(SLEEP_TIME)

    @unittest.skipUnless(google_import, 'import error. Try `pip install google-api-python-client`')
    @unittest.skipUnless(SPREADHSEET_ID, 'SPREADHSEET_ID not provided.')
    def test_batch_clear_range(self):
        response = self.gsheet_hook.batch_clear(ranges=[self.sheet_clear_range_a, self.sheet_clear_range_b])
        # pprint(response)
        self.assertEqual(response['clearedRanges'], [self.sheet_clear_range_a, self.sheet_clear_range_b])
        time.sleep(SLEEP_TIME)

    def tearDown(self):
        self.gsheet_hook.clear(range_=self.sheet_range)
