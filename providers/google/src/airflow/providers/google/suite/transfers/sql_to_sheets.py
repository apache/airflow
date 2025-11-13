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
from __future__ import annotations

import datetime
import logging
import numbers
from collections.abc import Iterable, Mapping, Sequence
from contextlib import closing
from typing import Any

from airflow.providers.common.sql.operators.sql import BaseSQLOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook


class SQLToGoogleSheetsOperator(BaseSQLOperator):
    """
    Copy data from SQL results to provided Google Spreadsheet.

    :param sql: The SQL to execute.
    :param spreadsheet_id: The Google Sheet ID to interact with.
    :param conn_id: the connection ID used to connect to the database.
    :param parameters: The parameters to render the SQL query with.
    :param database: name of database which overwrite the defined one in connection
    :param spreadsheet_range: The A1 notation of the values to retrieve.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "sql",
        "spreadsheet_id",
        "spreadsheet_range",
        "impersonation_chain",
    )

    template_fields_renderers = {"sql": "sql"}
    template_ext: Sequence[str] = (".sql",)

    ui_color = "#a0e08c"

    def __init__(
        self,
        *,
        sql: str,
        spreadsheet_id: str,
        sql_conn_id: str,
        parameters: Iterable | Mapping[str, Any] | None = None,
        database: str | None = None,
        spreadsheet_range: str = "Sheet1",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.sql = sql
        self.conn_id = sql_conn_id
        self.database = database
        self.parameters = parameters
        self.gcp_conn_id = gcp_conn_id
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range
        self.impersonation_chain = impersonation_chain

    def _data_prep(self, data):
        for row in data:
            item_list = []
            for item in row:
                if isinstance(item, (datetime.date, datetime.datetime)):
                    item_list.append(item.isoformat())
                elif isinstance(item, int):  # To exclude int from the number check.
                    item_list.append(item)
                elif isinstance(item, numbers.Number):
                    item_list.append(float(item))
                else:
                    item_list.append(item)
            yield item_list

    def _get_data(self):
        hook = self.get_db_hook()
        with closing(hook.get_conn()) as conn, closing(conn.cursor()) as cur:
            self.log.info("Executing query")
            cur.execute(self.sql, self.parameters or ())

            yield [field[0] for field in cur.description]
            yield from self._data_prep(cur.fetchall())

    def execute(self, context: Any) -> None:
        self.log.info("Getting data")
        values = list(self._get_data())

        self.log.info("Connecting to Google")
        sheet_hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        if self.log.isEnabledFor(logging.INFO):
            url = f"https://docs.google.com/spreadsheets/d/{self.spreadsheet_id}"
            self.log.info("Uploading data to %s", url)

        sheet_hook.update_values(
            spreadsheet_id=self.spreadsheet_id,
            range_=self.spreadsheet_range,
            values=values,
        )
