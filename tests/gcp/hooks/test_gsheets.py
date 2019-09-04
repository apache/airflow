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
Unit Tests for the GSheets Hook
"""

import unittest

from tests.compat import mock
from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

from airflow.gcp.hooks.gsheets import GSheetsHook
from airflow import AirflowException

GCP_CONN_ID = 'test'
SPREADHSEET_ID = '1234567890'
RANGE_ = 'test!A:E'
RANGES = ['test!A:Q', 'test!R:Z']
VALUES = [[1, 2, 3]]
VALUES_BATCH = [[[1, 2, 3]], [[4, 5, 6]]]
MAJOR_DIMENSION = 'ROWS'
VALUE_RENDER_OPTION = 'FORMATTED_VALUE'
DATE_TIME_RENDER_OPTION = 'SERIAL_NUMBER'
INCLUDE_VALUES_IN_RESPONSE = True
VALUE_INPUT_OPTION = 'RAW'
INSERT_DATA_OPTION = 'OVERWRITE'
NUM_RETRIES = 5
API_RESPONSE = {'test': 'repsonse'}


class TestGSheetsHook(unittest.TestCase):
    def setUp(self):
        with mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_default_project_id):
            self.hook = GSheetsHook(gcp_conn_id=GCP_CONN_ID, spreadsheet_id=SPREADHSEET_ID)

    @mock.patch("airflow.gcp.hooks.gsheets.GSheetsHook.get_conn")
    def test_get_values(self, get_conn):
        get_method = get_conn.return_value.spreadsheets.return_value.values.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.get_values(
            range_=RANGE_,
            major_dimension=MAJOR_DIMENSION,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION)
        self.assertIs(result, API_RESPONSE)
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        get_method.assert_called_once_with(
            spreadsheetId=SPREADHSEET_ID,
            range=RANGE_,
            majorDimension=MAJOR_DIMENSION,
            valueRenderOption=VALUE_RENDER_OPTION,
            dateTimeRenderOption=DATE_TIME_RENDER_OPTION
        )

    @mock.patch("airflow.gcp.hooks.gsheets.GSheetsHook.get_conn")
    def test_batch_get_values(self, get_conn):
        batch_get_method = get_conn.return_value.spreadsheets.return_value.values.return_value.batchGet
        execute_method = batch_get_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.batch_get_values(
            ranges=RANGES,
            major_dimension=MAJOR_DIMENSION,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION)
        self.assertIs(result, API_RESPONSE)
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        batch_get_method.assert_called_once_with(
            spreadsheetId=SPREADHSEET_ID,
            ranges=RANGES,
            majorDimension=MAJOR_DIMENSION,
            valueRenderOption=VALUE_RENDER_OPTION,
            dateTimeRenderOption=DATE_TIME_RENDER_OPTION
        )

    @mock.patch("airflow.gcp.hooks.gsheets.GSheetsHook.get_conn")
    def test_update_values(self, get_conn):
        update_method = get_conn.return_value.spreadsheets.return_value.values.return_value.update
        execute_method = update_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.update_values(
            range_=RANGE_,
            values=VALUES,
            major_dimension=MAJOR_DIMENSION,
            value_input_option=VALUE_INPUT_OPTION,
            include_values_in_response=INCLUDE_VALUES_IN_RESPONSE,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION)
        body = {
            "range": RANGE_,
            "majorDimension": MAJOR_DIMENSION,
            "values": VALUES
        }
        self.assertIs(result, API_RESPONSE)
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        update_method.assert_called_once_with(
            spreadsheetId=SPREADHSEET_ID,
            range=RANGE_,
            valueInputOption=VALUE_INPUT_OPTION,
            includeValuesInResponse=INCLUDE_VALUES_IN_RESPONSE,
            responseValueRenderOption=VALUE_RENDER_OPTION,
            responseDateTimeRenderOption=DATE_TIME_RENDER_OPTION,
            body=body
        )

    @mock.patch("airflow.gcp.hooks.gsheets.GSheetsHook.get_conn")
    def test_batch_update_values(self, get_conn):
        batch_update_method = get_conn.return_value.spreadsheets.return_value.values.return_value.batchUpdate
        execute_method = batch_update_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.batch_update_values(
            ranges=RANGES,
            values=VALUES_BATCH,
            major_dimension=MAJOR_DIMENSION,
            value_input_option=VALUE_INPUT_OPTION,
            include_values_in_response=INCLUDE_VALUES_IN_RESPONSE,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION)
        data = []
        for idx, range_ in enumerate(RANGES):
            value_range = {
                "range": range_,
                "majorDimension": MAJOR_DIMENSION,
                "values": VALUES_BATCH[idx]
            }
            data.append(value_range)
        body = {
            "valueInputOption": VALUE_INPUT_OPTION,
            "data": data,
            "includeValuesInResponse": INCLUDE_VALUES_IN_RESPONSE,
            "responseValueRenderOption": VALUE_RENDER_OPTION,
            "responseDateTimeRenderOption": DATE_TIME_RENDER_OPTION
        }
        self.assertIs(result, API_RESPONSE)
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        batch_update_method.assert_called_once_with(
            spreadsheetId=SPREADHSEET_ID,
            body=body
        )

    @mock.patch("airflow.gcp.hooks.gsheets.GSheetsHook.get_conn")
    def test_batch_update_values_with_bad_data(self, get_conn):
        batch_update_method = get_conn.return_value.spreadsheets.return_value.values.return_value.batchUpdate
        execute_method = batch_update_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        with self.assertRaises(AirflowException) as cm:
            self.hook.batch_update_values(
                ranges=['test!A1:B2', 'test!C1:C2'],
                values=[[1, 2, 3]],  # bad data
                major_dimension=MAJOR_DIMENSION,
                value_input_option=VALUE_INPUT_OPTION,
                include_values_in_response=INCLUDE_VALUES_IN_RESPONSE,
                value_render_option=VALUE_RENDER_OPTION,
                date_time_render_option=DATE_TIME_RENDER_OPTION)
        batch_update_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("must be of equal length.", str(err))

    @mock.patch("airflow.gcp.hooks.gsheets.GSheetsHook.get_conn")
    def test_append_values(self, get_conn):
        append_method = get_conn.return_value.spreadsheets.return_value.values.return_value.append
        execute_method = append_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.append_values(
            range_=RANGE_,
            values=VALUES,
            major_dimension=MAJOR_DIMENSION,
            value_input_option=VALUE_INPUT_OPTION,
            insert_data_option=INSERT_DATA_OPTION,
            include_values_in_response=INCLUDE_VALUES_IN_RESPONSE,
            value_render_option=VALUE_RENDER_OPTION,
            date_time_render_option=DATE_TIME_RENDER_OPTION)
        body = {
            "range": RANGE_,
            "majorDimension": MAJOR_DIMENSION,
            "values": VALUES
        }
        self.assertIs(result, API_RESPONSE)
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        append_method.assert_called_once_with(
            spreadsheetId=SPREADHSEET_ID,
            range=RANGE_,
            valueInputOption=VALUE_INPUT_OPTION,
            insertDataOption=INSERT_DATA_OPTION,
            includeValuesInResponse=INCLUDE_VALUES_IN_RESPONSE,
            responseValueRenderOption=VALUE_RENDER_OPTION,
            responseDateTimeRenderOption=DATE_TIME_RENDER_OPTION,
            body=body
        )

    @mock.patch("airflow.gcp.hooks.gsheets.GSheetsHook.get_conn")
    def test_clear_values(self, get_conn):
        clear_method = get_conn.return_value.spreadsheets.return_value.values.return_value.clear
        execute_method = clear_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.clear(range_=RANGE_)

        self.assertIs(result, API_RESPONSE)
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        clear_method.assert_called_once_with(
            spreadsheetId=SPREADHSEET_ID,
            range=RANGE_
        )

    @mock.patch("airflow.gcp.hooks.gsheets.GSheetsHook.get_conn")
    def test_batch_clear_values(self, get_conn):
        batch_clear_method = get_conn.return_value.spreadsheets.return_value.values.return_value.batchClear
        execute_method = batch_clear_method.return_value.execute
        execute_method.return_value = API_RESPONSE
        result = self.hook.batch_clear(ranges=RANGES)
        body = {"ranges": RANGES}
        self.assertIs(result, API_RESPONSE)
        execute_method.assert_called_once_with(num_retries=NUM_RETRIES)
        batch_clear_method.assert_called_once_with(
            spreadsheetId=SPREADHSEET_ID,
            body=body
        )
