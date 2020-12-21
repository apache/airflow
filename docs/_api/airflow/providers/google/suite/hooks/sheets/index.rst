:mod:`airflow.providers.google.suite.hooks.sheets`
==================================================

.. py:module:: airflow.providers.google.suite.hooks.sheets

.. autoapi-nested-parse::

   This module contains a Google Sheets API hook



Module Contents
---------------

.. py:class:: GSheetsHook(gcp_conn_id: str = 'google_cloud_default', api_version: str = 'v4', delegate_to: Optional[str] = None, impersonation_chain: Optional[Union[str, Sequence[str]]] = None)

   Bases: :class:`airflow.providers.google.common.hooks.base_google.GoogleBaseHook`

   Interact with Google Sheets via Google Cloud connection
   Reading and writing cells in Google Sheet:
   https://developers.google.com/sheets/api/guides/values

   :param gcp_conn_id: The connection ID to use when fetching connection info.
   :type gcp_conn_id: str
   :param api_version: API Version
   :type api_version: str
   :param delegate_to: The account to impersonate using domain-wide delegation of authority,
       if any. For this to work, the service account making the request must have
       domain-wide delegation enabled.
   :type delegate_to: str
   :param impersonation_chain: Optional service account to impersonate using short-term
       credentials, or chained list of accounts required to get the access_token
       of the last account in the list, which will be impersonated in the request.
       If set as a string, the account must grant the originating account
       the Service Account Token Creator IAM role.
       If set as a sequence, the identities from the list must grant
       Service Account Token Creator IAM role to the directly preceding identity, with first
       account from the list granting this role to the originating account.
   :type impersonation_chain: Union[str, Sequence[str]]

   
   .. method:: get_conn(self)

      Retrieves connection to Google Sheets.

      :return: Google Sheets services object.
      :rtype: Any



   
   .. method:: get_values(self, spreadsheet_id: str, range_: str, major_dimension: str = 'DIMENSION_UNSPECIFIED', value_render_option: str = 'FORMATTED_VALUE', date_time_render_option: str = 'SERIAL_NUMBER')

      Gets values from Google Sheet from a single range
      https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/get

      :param spreadsheet_id: The Google Sheet ID to interact with
      :type spreadsheet_id: str
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
      :return: An array of sheet values from the specified sheet.
      :rtype: List



   
   .. method:: batch_get_values(self, spreadsheet_id: str, ranges: List, major_dimension: str = 'DIMENSION_UNSPECIFIED', value_render_option: str = 'FORMATTED_VALUE', date_time_render_option: str = 'SERIAL_NUMBER')

      Gets values from Google Sheet from a list of ranges
      https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchGet

      :param spreadsheet_id: The Google Sheet ID to interact with
      :type spreadsheet_id: str
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



   
   .. method:: update_values(self, spreadsheet_id: str, range_: str, values: List, major_dimension: str = 'ROWS', value_input_option: str = 'RAW', include_values_in_response: bool = False, value_render_option: str = 'FORMATTED_VALUE', date_time_render_option: str = 'SERIAL_NUMBER')

      Updates values from Google Sheet from a single range
      https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/update

      :param spreadsheet_id: The Google Sheet ID to interact with.
      :type spreadsheet_id: str
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



   
   .. method:: batch_update_values(self, spreadsheet_id: str, ranges: List, values: List, major_dimension: str = 'ROWS', value_input_option: str = 'RAW', include_values_in_response: bool = False, value_render_option: str = 'FORMATTED_VALUE', date_time_render_option: str = 'SERIAL_NUMBER')

      Updates values from Google Sheet for multiple ranges
      https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchUpdate

      :param spreadsheet_id: The Google Sheet ID to interact with
      :type spreadsheet_id: str
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



   
   .. method:: append_values(self, spreadsheet_id: str, range_: str, values: List, major_dimension: str = 'ROWS', value_input_option: str = 'RAW', insert_data_option: str = 'OVERWRITE', include_values_in_response: bool = False, value_render_option: str = 'FORMATTED_VALUE', date_time_render_option: str = 'SERIAL_NUMBER')

      Append values from Google Sheet from a single range
      https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/append

      :param spreadsheet_id: The Google Sheet ID to interact with
      :type spreadsheet_id: str
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



   
   .. method:: clear(self, spreadsheet_id: str, range_: str)

      Clear values from Google Sheet from a single range
      https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/clear

      :param spreadsheet_id: The Google Sheet ID to interact with
      :type spreadsheet_id: str
      :param range_: The A1 notation of the values to retrieve.
      :type range_: str
      :return: Google Sheets API response.
      :rtype: Dict



   
   .. method:: batch_clear(self, spreadsheet_id: str, ranges: list)

      Clear values from Google Sheet from a list of ranges
      https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets.values/batchClear

      :param spreadsheet_id: The Google Sheet ID to interact with
      :type spreadsheet_id: str
      :param ranges: The A1 notation of the values to retrieve.
      :type ranges: List
      :return: Google Sheets API response.
      :rtype: Dict



   
   .. method:: get_spreadsheet(self, spreadsheet_id: str)

      Retrieves spreadsheet matching the given id.

      :param spreadsheet_id: The spreadsheet id.
      :type spreadsheet_id: str
      :return: An spreadsheet that matches the sheet filter.



   
   .. method:: get_sheet_titles(self, spreadsheet_id: str, sheet_filter: Optional[List[str]] = None)

      Retrieves the sheet titles from a spreadsheet matching the given id and sheet filter.

      :param spreadsheet_id: The spreadsheet id.
      :type spreadsheet_id: str
      :param sheet_filter: List of sheet title to retrieve from sheet.
      :type sheet_filter: List[str]
      :return: An list of sheet titles from the specified sheet that match
          the sheet filter.



   
   .. method:: create_spreadsheet(self, spreadsheet: Dict[str, Any])

      Creates a spreadsheet, returning the newly created spreadsheet.

      :param spreadsheet: an instance of Spreadsheet
          https://developers.google.com/sheets/api/reference/rest/v4/spreadsheets#Spreadsheet
      :type spreadsheet: Dict[str, Any]
      :return: An spreadsheet object.




