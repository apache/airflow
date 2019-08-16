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

from __future__ import print_function
from googleapiclient.discovery import build
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook
from airflow.exceptions import AirflowException


class GCPSheetsHook(GoogleCloudBaseHook):
    """
    Interact with Google Sheets via GCP connection

    """

    def __init__(self,
                 gcp_conn_id: str = 'google_cloud_default',
                 spreadsheet_id: str = None,
                 api_version: str = 'v4',
                 delegate_to: str = None) -> None:
        """
        :param grpc_conn_id: The connection ID to use when fetching connection info.
        :type grpc_conn_id: str
        :param gsheet_id: The Google Sheet ID to interact with
        :type gsheet_id: str
        :param range_: The Range or Named Range to interact with
        :type range_: str
        """
        super().__init__(gcp_conn_id, delegate_to)
        self.gcp_conn_id = gcp_conn_id

        if not spreadsheet_id:
            raise AirflowException("The spreadsheet_id must be passed!")

        self.spreadsheet_id = spreadsheet_id
        self.api_version = api_version
        self.delegate_to = delegate_to
        self.num_retries = self._get_field('num_retries', 1),  # type: int
        self._conn = None

    def get_conn(self):
        """
        Retrieves connection to Google Sheets.
        :return: Google Sheets services object.
        :rtype: dict
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('sheets', self.api_version, http=http_authorized, cache_discovery=False)

        return self._conn

    """
    Reading and writing cells in Google Sheet:
    https://developers.google.com/sheets/api/guides/values
    """

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def get_values(self,
                   range_: str,
                   major_dimension: str = 'DIMENSION_UNSPECIFIED',
                   value_render_option: str = 'FORMATTED_VALUE',
                   date_time_render_option: str = 'SERIAL_NUMBER',
                   project_id: str = None) -> dict:
        """
        Gets values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get
        """
        assert project_id is not None
        service = self.get_conn()
        response = service.spreadsheets().values().batchGet(
            spreadsheetId=self.spreadsheet_id,
            ranges=range_,
            majorDimension=major_dimension,
            valueRenderOption=value_render_option,
            dateTimeRenderOption=date_time_render_option
        ).execute()

        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def batch_get_values(self,
                         ranges: list,
                         major_dimension: str = 'DIMENSION_UNSPECIFIED',
                         value_render_option: str = 'FORMATTED_VALUE',
                         date_time_render_option: str = 'SERIAL_NUMBER',
                         project_id: str = None) -> dict:
        """
        Gets values from Google Sheet from a list of ranges
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchGet
        """
        assert project_id is not None
        service = self.get_conn()
        response = service.spreadsheets().values().batchGet(
            spreadsheetId=self.spreadsheet_id,
            ranges=ranges,
            majorDimension=major_dimension,
            valueRenderOption=value_render_option,
            dateTimeRenderOption=date_time_render_option
        ).execute()

        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def update_values(self,
                      range_: str,
                      major_dimension: str = 'ROWS',
                      value_input_option: str = 'RAW',
                      include_values_in_response: bool = False,
                      response_value_render_option: str = 'FORMATTED_VALUE',
                      response_date_time_render_option: str = 'SERIAL_NUMBER',
                      values: list = [],
                      project_id: str = None) -> dict:
        """
        Updates values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/update        """
        assert project_id is not None
        service = self.get_conn()
        body = {
            "range": range_,
            "majorDimension": major_dimension,
            "values": values
        }
        response = service.spreadsheets().values().update(
            spreadsheetId=self.spreadsheet_id,
            range=range_,
            valueInputOption=value_input_option,
            includeValuesInResponse=include_values_in_response,
            responseValueRenderOption=response_value_render_option,
            responseDateTimeRenderOption=response_date_time_render_option,
            body=body
        ).execute()

        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def batch_update_values(self,
                            ranges: list = [],
                            values: list = [],
                            major_dimension: str = 'ROWS',
                            value_input_option: str = 'RAW',
                            include_values_in_response: bool = False,
                            response_value_render_option: str = 'FORMATTED_VALUE',
                            response_date_time_render_option: str = 'SERIAL_NUMBER',
                            project_id: str = None) -> dict:
        """
        Updates values from Google Sheet for multiple ranges
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate
        """
        assert project_id is not None
        service = self.get_conn()
        data = []
        for idx, range_ in enumerate(ranges):
            value_range = {
                "range": range_,
                "majorDimension": major_dimension,
                "values": values[idx]
            }
            data.append(value_range)
        body = {
            "valueInputOption": value_input_option,
            "data": data,
            "includeValuesInResponse": include_values_in_response,
            "responseValueRenderOption": response_value_render_option,
            "responseDateTimeRenderOption": response_date_time_render_option
        }
        response = service.spreadsheets().values().batchUpdate(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def append_values(self,
                      range_: str,
                      major_dimension: str = 'ROWS',
                      value_input_option: str = 'RAW',
                      insert_data_option: str = 'OVERWRITE',  # or INSERT_ROWS
                      include_values_in_response: bool = False,
                      response_value_render_option: str = 'FORMATTED_VALUE',
                      response_date_time_render_option: str = 'SERIAL_NUMBER',
                      values: list = [],
                      project_id: str = None) -> dict:
        """
        Append values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append
        """
        assert project_id is not None
        service = self.get_conn()
        body = {
            "range": range_,
            "majorDimension": major_dimension,
            "values": values
        }
        response = service.spreadsheets().values().append(
            spreadsheetId=self.spreadsheet_id,
            range=range_,
            valueInputOption=value_input_option,
            insertDataOption=insert_data_option,
            includeValuesInResponse=include_values_in_response,
            responseValueRenderOption=response_value_render_option,
            responseDateTimeRenderOption=response_date_time_render_option,
            body=body
        ).execute()

        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def clear(self, range_: str, project_id: str = None) -> dict:
        """
        Clear values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/clear        """
        assert project_id is not None
        service = self.get_conn()
        response = service.spreadsheets().values().clear(
            spreadsheetId=self.spreadsheet_id,
            range=range_
        ).execute()

        return response

    @GoogleCloudBaseHook.fallback_to_default_project_id
    def batch_clear(self, ranges: list = [], project_id: str = None) -> dict:
        """
        Clear values from Google Sheet from a list of ranges
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchClear        """
        assert project_id is not None
        service = self.get_conn()
        body = {
            "ranges": ranges
        }
        response = service.spreadsheets().values().batchClear(
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute()

        return response
