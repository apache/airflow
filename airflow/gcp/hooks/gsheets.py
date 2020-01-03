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
This module contains a Google Sheets API hook
"""

from typing import Any, Dict, List, Optional

from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.gcp.hooks.base import CloudBaseHook


class GSheetsHook(CloudBaseHook):
    """
    Interact with Google Sheets via GCP connection
    Reading and writing cells in Google Sheet:
    https://developers.google.com/sheets/api/guides/values

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param spreadsheet_id: The Google Sheet ID to interact with
    :type spreadsheet_id: str
    :param api_version: API Version
    :type api_version: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    def __init__(
        self,
        spreadsheet_id: str,
        gcp_conn_id: str = 'google_cloud_default',
        api_version: str = 'v4',
        delegate_to: Optional[str] = None
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.spreadsheet_id = spreadsheet_id
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.delegate_to = delegate_to
        self._conn = None

    def get_conn(self) -> Any:
        """
        Retrieves connection to Google Sheets.

        :return: Google Sheets services object.
        :rtype: Any
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build('sheets', self.api_version, http=http_authorized, cache_discovery=False)

        return self._conn

    @CloudBaseHook.catch_http_exception
    def get_values(
        self,
        range_: str,
        major_dimension: str = 'DIMENSION_UNSPECIFIED',
        value_render_option: str = 'FORMATTED_VALUE',
        date_time_render_option: str = 'SERIAL_NUMBER'
    ) -> Dict:
        """
        Gets values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get

        :param range_: The A1 notation of the values to retrieve.
        :type range_: str
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :type major_dimension: str
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :type value_render_option: str
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :type date_time_render_option: str
        :return: Google Sheets API response.
        :rtype: Dict
        """
        service = self.get_conn()
        response = service.spreadsheets().values().get(  # pylint: disable=no-member
            spreadsheetId=self.spreadsheet_id,
            range=range_,
            majorDimension=major_dimension,
            valueRenderOption=value_render_option,
            dateTimeRenderOption=date_time_render_option
        ).execute(num_retries=self.num_retries)

        return response

    @CloudBaseHook.catch_http_exception
    def batch_get_values(
        self,
        ranges: List,
        major_dimension: str = 'DIMENSION_UNSPECIFIED',
        value_render_option: str = 'FORMATTED_VALUE',
        date_time_render_option: str = 'SERIAL_NUMBER'
    ) -> Dict:
        """
        Gets values from Google Sheet from a list of ranges
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchGet

        :param ranges: The A1 notation of the values to retrieve.
        :type ranges: List
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :type major_dimension: str
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :type value_render_option: str
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :type date_time_render_option: str
        :return: Google Sheets API response.
        :rtype: Dict
        """
        service = self.get_conn()
        response = service.spreadsheets().values().batchGet(  # pylint: disable=no-member
            spreadsheetId=self.spreadsheet_id,
            ranges=ranges,
            majorDimension=major_dimension,
            valueRenderOption=value_render_option,
            dateTimeRenderOption=date_time_render_option
        ).execute(num_retries=self.num_retries)

        return response

    @CloudBaseHook.catch_http_exception
    def update_values(
        self,
        range_: str,
        values: List,
        major_dimension: str = 'ROWS',
        value_input_option: str = 'RAW',
        include_values_in_response: bool = False,
        value_render_option: str = 'FORMATTED_VALUE',
        date_time_render_option: str = 'SERIAL_NUMBER'
    ) -> Dict:
        """
        Updates values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/update

        :param range_: The A1 notation of the values to retrieve.
        :type range_: str
        :param values: Data within a range of the spreadsheet.
        :type values: List
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :type major_dimension: str
        :param value_input_option: Determines how input data should be interpreted.
            RAW or USER_ENTERED
        :type value_input_option: str
        :param include_values_in_response: Determines if the update response should
            include the values of the cells that were updated.
        :type include_values_in_response: bool
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :type value_render_option: str
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :type date_time_render_option: str
        :return: Google Sheets API response.
        :rtype: Dict
        """
        service = self.get_conn()
        body = {
            "range": range_,
            "majorDimension": major_dimension,
            "values": values
        }
        response = service.spreadsheets().values().update(  # pylint: disable=no-member
            spreadsheetId=self.spreadsheet_id,
            range=range_,
            valueInputOption=value_input_option,
            includeValuesInResponse=include_values_in_response,
            responseValueRenderOption=value_render_option,
            responseDateTimeRenderOption=date_time_render_option,
            body=body
        ).execute(num_retries=self.num_retries)

        return response

    @CloudBaseHook.catch_http_exception
    def batch_update_values(
        self,
        ranges: List,
        values: List,
        major_dimension: str = 'ROWS',
        value_input_option: str = 'RAW',
        include_values_in_response: bool = False,
        value_render_option: str = 'FORMATTED_VALUE',
        date_time_render_option: str = 'SERIAL_NUMBER'
    ) -> Dict:
        """
        Updates values from Google Sheet for multiple ranges
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate

        :param ranges: The A1 notation of the values to retrieve.
        :type ranges: List
        :param values: Data within a range of the spreadsheet.
        :type values: List
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :type major_dimension: str
        :param value_input_option: Determines how input data should be interpreted.
            RAW or USER_ENTERED
        :type value_input_option: str
        :param include_values_in_response: Determines if the update response should
            include the values of the cells that were updated.
        :type include_values_in_response: bool
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :type value_render_option: str
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :type date_time_render_option: str
        :return: Google Sheets API response.
        :rtype: Dict
        """
        if len(ranges) != len(values):
            raise AirflowException(
                "'Ranges' and and 'Lists' must be of equal length. \n \
                'Ranges' is of length: {} and \n \
                'Values' is of length: {}.".format(str(len(ranges)), str(len(values))))
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
            "responseValueRenderOption": value_render_option,
            "responseDateTimeRenderOption": date_time_render_option
        }
        response = service.spreadsheets().values().batchUpdate(  # pylint: disable=no-member
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute(num_retries=self.num_retries)

        return response

    @CloudBaseHook.catch_http_exception
    def append_values(
        self,
        range_: str,
        values: List,
        major_dimension: str = 'ROWS',
        value_input_option: str = 'RAW',
        insert_data_option: str = 'OVERWRITE',
        include_values_in_response: bool = False,
        value_render_option: str = 'FORMATTED_VALUE',
        date_time_render_option: str = 'SERIAL_NUMBER'
    ) -> Dict:
        """
        Append values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append

        :param range_: The A1 notation of the values to retrieve.
        :type range_: str
        :param values: Data within a range of the spreadsheet.
        :type values: List
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :type major_dimension: str
        :param value_input_option: Determines how input data should be interpreted.
            RAW or USER_ENTERED
        :type value_input_option: str
        :param insert_data_option: Determines how existing data is changed when new data is input.
            OVERWRITE or INSERT_ROWS
        :type insert_data_option: str
        :param include_values_in_response: Determines if the update response should
            include the values of the cells that were updated.
        :type include_values_in_response: bool
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :type value_render_option: str
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :type date_time_render_option: str
        :return: Google Sheets API response.
        :rtype: Dict
        """
        service = self.get_conn()
        body = {
            "range": range_,
            "majorDimension": major_dimension,
            "values": values
        }
        response = service.spreadsheets().values().append(  # pylint: disable=no-member
            spreadsheetId=self.spreadsheet_id,
            range=range_,
            valueInputOption=value_input_option,
            insertDataOption=insert_data_option,
            includeValuesInResponse=include_values_in_response,
            responseValueRenderOption=value_render_option,
            responseDateTimeRenderOption=date_time_render_option,
            body=body
        ).execute(num_retries=self.num_retries)

        return response

    @CloudBaseHook.catch_http_exception
    def clear(self, range_: str) -> Dict:
        """
        Clear values from Google Sheet from a single range
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/clear

        :param range_: The A1 notation of the values to retrieve.
        :type range_: str
        :return: Google Sheets API response.
        :rtype: Dict
        """
        service = self.get_conn()
        response = service.spreadsheets().values().clear(  # pylint: disable=no-member
            spreadsheetId=self.spreadsheet_id,
            range=range_
        ).execute(num_retries=self.num_retries)

        return response

    @CloudBaseHook.catch_http_exception
    def batch_clear(self, ranges: List) -> Dict:
        """
        Clear values from Google Sheet from a list of ranges
        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchClear

        :param ranges: The A1 notation of the values to retrieve.
        :type ranges: List
        :return: Google Sheets API response.
        :rtype: Dict
        """
        service = self.get_conn()
        body = {
            "ranges": ranges
        }
        response = service.spreadsheets().values().batchClear(  # pylint: disable=no-member
            spreadsheetId=self.spreadsheet_id,
            body=body
        ).execute(num_retries=self.num_retries)

        return response
