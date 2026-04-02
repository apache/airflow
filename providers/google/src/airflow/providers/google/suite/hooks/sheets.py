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
"""This module contains a Google Sheets API hook."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from google.api_core.client_options import ClientOptions
from googleapiclient.discovery import build

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GSheetsHook(GoogleBaseHook):
    """
    Interact with Google Sheets via Google Cloud connection.

    Reading and writing cells in Google Sheet: https://developers.google.com/sheets/api/guides/values

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param api_version: API Version
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :param api_endpoint: Optional. Custom API endpoint, i.e: regional or private endpoint.
        This can be used to target private VPC or restricted access endpoints.
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        api_version: str = "v4",
        impersonation_chain: str | Sequence[str] | None = None,
        api_endpoint: str | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.gcp_conn_id = gcp_conn_id
        self.api_version = api_version
        self.api_endpoint = api_endpoint
        self._conn = None

    def get_conn(self) -> Any:
        """
        Retrieve connection to Google Sheets.

        :return: Google Sheets services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            client_options = None
            if self.api_endpoint:
                client_options = ClientOptions(api_endpoint=self.api_endpoint)
            self._conn = build(
                "sheets",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
                client_options=client_options,
            )

        return self._conn

    def get_values(
        self,
        spreadsheet_id: str,
        range_: str,
        major_dimension: str = "DIMENSION_UNSPECIFIED",
        value_render_option: str = "FORMATTED_VALUE",
        date_time_render_option: str = "SERIAL_NUMBER",
    ) -> list:
        """
        Get values from Google Sheet from a single range.

        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get

        :param spreadsheet_id: The Google Sheet ID to interact with
        :param range_: The A1 notation of the values to retrieve.
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :return: An array of sheet values from the specified sheet.
        """
        service = self.get_conn()

        response = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=spreadsheet_id,
                range=range_,
                majorDimension=major_dimension,
                valueRenderOption=value_render_option,
                dateTimeRenderOption=date_time_render_option,
            )
            .execute(num_retries=self.num_retries)
        )

        return response.get("values", [])

    def batch_get_values(
        self,
        spreadsheet_id: str,
        ranges: list,
        major_dimension: str = "DIMENSION_UNSPECIFIED",
        value_render_option: str = "FORMATTED_VALUE",
        date_time_render_option: str = "SERIAL_NUMBER",
    ) -> dict:
        """
        Get values from Google Sheet from a list of ranges.

        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchGet

        :param spreadsheet_id: The Google Sheet ID to interact with
        :param ranges: The A1 notation of the values to retrieve.
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :return: Google Sheets API response.
        """
        service = self.get_conn()

        response = (
            service.spreadsheets()
            .values()
            .batchGet(
                spreadsheetId=spreadsheet_id,
                ranges=ranges,
                majorDimension=major_dimension,
                valueRenderOption=value_render_option,
                dateTimeRenderOption=date_time_render_option,
            )
            .execute(num_retries=self.num_retries)
        )

        return response

    def update_values(
        self,
        spreadsheet_id: str,
        range_: str,
        values: list,
        major_dimension: str = "ROWS",
        value_input_option: str = "RAW",
        include_values_in_response: bool = False,
        value_render_option: str = "FORMATTED_VALUE",
        date_time_render_option: str = "SERIAL_NUMBER",
    ) -> dict:
        """
        Update values from Google Sheet from a single range.

        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/update

        :param spreadsheet_id: The Google Sheet ID to interact with.
        :param range_: The A1 notation of the values to retrieve.
        :param values: Data within a range of the spreadsheet.
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :param value_input_option: Determines how input data should be interpreted.
            RAW or USER_ENTERED
        :param include_values_in_response: Determines if the update response should
            include the values of the cells that were updated.
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :return: Google Sheets API response.
        """
        service = self.get_conn()
        body = {"range": range_, "majorDimension": major_dimension, "values": values}

        response = (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueInputOption=value_input_option,
                includeValuesInResponse=include_values_in_response,
                responseValueRenderOption=value_render_option,
                responseDateTimeRenderOption=date_time_render_option,
                body=body,
            )
            .execute(num_retries=self.num_retries)
        )

        return response

    def batch_update_values(
        self,
        spreadsheet_id: str,
        ranges: list,
        values: list,
        major_dimension: str = "ROWS",
        value_input_option: str = "RAW",
        include_values_in_response: bool = False,
        value_render_option: str = "FORMATTED_VALUE",
        date_time_render_option: str = "SERIAL_NUMBER",
    ) -> dict:
        """
        Update values from Google Sheet for multiple ranges.

        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate

        :param spreadsheet_id: The Google Sheet ID to interact with
        :param ranges: The A1 notation of the values to retrieve.
        :param values: Data within a range of the spreadsheet.
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :param value_input_option: Determines how input data should be interpreted.
            RAW or USER_ENTERED
        :param include_values_in_response: Determines if the update response should
            include the values of the cells that were updated.
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :return: Google Sheets API response.
        """
        if len(ranges) != len(values):
            raise AirflowException(
                f"'Ranges' and 'Lists' must be of equal length. "
                f"'Ranges' is of length: {len(ranges)} and 'Values' is of length: {len(values)}."
            )
        service = self.get_conn()
        data = []
        for idx, range_ in enumerate(ranges):
            value_range = {"range": range_, "majorDimension": major_dimension, "values": values[idx]}
            data.append(value_range)
        body = {
            "valueInputOption": value_input_option,
            "data": data,
            "includeValuesInResponse": include_values_in_response,
            "responseValueRenderOption": value_render_option,
            "responseDateTimeRenderOption": date_time_render_option,
        }

        response = (
            service.spreadsheets()
            .values()
            .batchUpdate(spreadsheetId=spreadsheet_id, body=body)
            .execute(num_retries=self.num_retries)
        )

        return response

    def append_values(
        self,
        spreadsheet_id: str,
        range_: str,
        values: list,
        major_dimension: str = "ROWS",
        value_input_option: str = "RAW",
        insert_data_option: str = "OVERWRITE",
        include_values_in_response: bool = False,
        value_render_option: str = "FORMATTED_VALUE",
        date_time_render_option: str = "SERIAL_NUMBER",
    ) -> dict:
        """
        Append values from Google Sheet from a single range.

        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append

        :param spreadsheet_id: The Google Sheet ID to interact with
        :param range_: The A1 notation of the values to retrieve.
        :param values: Data within a range of the spreadsheet.
        :param major_dimension: Indicates which dimension an operation should apply to.
            DIMENSION_UNSPECIFIED, ROWS, or COLUMNS
        :param value_input_option: Determines how input data should be interpreted.
            RAW or USER_ENTERED
        :param insert_data_option: Determines how existing data is changed when new data is input.
            OVERWRITE or INSERT_ROWS
        :param include_values_in_response: Determines if the update response should
            include the values of the cells that were updated.
        :param value_render_option: Determines how values should be rendered in the output.
            FORMATTED_VALUE, UNFORMATTED_VALUE, or FORMULA
        :param date_time_render_option: Determines how dates should be rendered in the output.
            SERIAL_NUMBER or FORMATTED_STRING
        :return: Google Sheets API response.
        """
        service = self.get_conn()
        body = {"range": range_, "majorDimension": major_dimension, "values": values}

        response = (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=spreadsheet_id,
                range=range_,
                valueInputOption=value_input_option,
                insertDataOption=insert_data_option,
                includeValuesInResponse=include_values_in_response,
                responseValueRenderOption=value_render_option,
                responseDateTimeRenderOption=date_time_render_option,
                body=body,
            )
            .execute(num_retries=self.num_retries)
        )

        return response

    def clear(self, spreadsheet_id: str, range_: str) -> dict:
        """
        Clear values from Google Sheet from a single range.

        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/clear

        :param spreadsheet_id: The Google Sheet ID to interact with
        :param range_: The A1 notation of the values to retrieve.
        :return: Google Sheets API response.
        """
        service = self.get_conn()

        response = (
            service.spreadsheets()
            .values()
            .clear(spreadsheetId=spreadsheet_id, range=range_)
            .execute(num_retries=self.num_retries)
        )

        return response

    def batch_clear(self, spreadsheet_id: str, ranges: list) -> dict:
        """
        Clear values from Google Sheet from a list of ranges.

        https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchClear

        :param spreadsheet_id: The Google Sheet ID to interact with
        :param ranges: The A1 notation of the values to retrieve.
        :return: Google Sheets API response.
        """
        service = self.get_conn()
        body = {"ranges": ranges}

        response = (
            service.spreadsheets()
            .values()
            .batchClear(spreadsheetId=spreadsheet_id, body=body)
            .execute(num_retries=self.num_retries)
        )

        return response

    def get_spreadsheet(self, spreadsheet_id: str):
        """
        Retrieve spreadsheet matching the given id.

        :param spreadsheet_id: The spreadsheet id.
        :return: An spreadsheet that matches the sheet filter.
        """
        response = (
            self.get_conn()
            .spreadsheets()
            .get(spreadsheetId=spreadsheet_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def get_sheet_titles(self, spreadsheet_id: str, sheet_filter: list[str] | None = None):
        """
        Retrieve the sheet titles from a spreadsheet matching the given id and sheet filter.

        :param spreadsheet_id: The spreadsheet id.
        :param sheet_filter: List of sheet title to retrieve from sheet.
        :return: An list of sheet titles from the specified sheet that match
            the sheet filter.
        """
        response = self.get_spreadsheet(spreadsheet_id=spreadsheet_id)

        if sheet_filter:
            titles = [
                sh["properties"]["title"]
                for sh in response["sheets"]
                if sh["properties"]["title"] in sheet_filter
            ]
        else:
            titles = [sh["properties"]["title"] for sh in response["sheets"]]
        return titles

    def create_spreadsheet(self, spreadsheet: dict[str, Any]) -> dict[str, Any]:
        """
        Create a spreadsheet, returning the newly created spreadsheet.

        :param spreadsheet: an instance of Spreadsheet
            https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#Spreadsheet
        :return: An spreadsheet object.
        """
        self.log.info("Creating spreadsheet: %s", spreadsheet["properties"]["title"])

        response = (
            self.get_conn().spreadsheets().create(body=spreadsheet).execute(num_retries=self.num_retries)
        )
        self.log.info("Spreadsheet: %s created", spreadsheet["properties"]["title"])
        return response
