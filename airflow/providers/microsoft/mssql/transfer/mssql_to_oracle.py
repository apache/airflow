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

from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.utils.decorators import apply_defaults


class MsSqlToOracleOperator(BaseOperator):
    """
    Moves data from MSSQL to Oracle.


    :param oracle_destination_conn_id: destination Oracle connection.
    :type oracle_destination_conn_id: str
    :param destination_table: destination table to insert rows.
    :type destination_table: str
    :param mssql_source_conn_id: source MSSQL connection.
    :type mssql_source_conn_id: str
    :param source_sql: SQL query to execute against the source MSSQL
        database. (templated)
    :type source_sql: str
    :param source_sql_params: Parameters to use in sql query. (templated)
    :type source_sql_params: dict
    :param rows_chunk: number of rows per chunk to commit.
    :type rows_chunk: int
    """

    template_fields = ('source_sql', 'source_sql_params')
    ui_color = '#e08c8c'

    @apply_defaults
    def __init__(
            self,
            oracle_destination_conn_id: str,
            destination_table: str,
            mssql_source_conn_id: str,
            source_sql: str,
            source_sql_params: Optional[dict] = None,
            rows_chunk: int = 5000,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        if source_sql_params is None:
            source_sql_params = {}
        self.oracle_destination_conn_id = oracle_destination_conn_id
        self.destination_table = destination_table
        self.mssql_source_conn_id = mssql_source_conn_id
        self.source_sql = source_sql
        self.source_sql_params = source_sql_params
        self.rows_chunk = rows_chunk

    # pylint: disable=unused-argument
    def _execute(self, src_hook: OdbcHook, dest_hook: OracleHook):
        with src_hook.get_conn() as src_conn:
            cursor = src_conn.cursor()
            self.log.info("Querying data from source: %s", self.mssql_source_conn_id)
            cursor.execute(self.source_sql, self.source_sql_params)
            target_fields = list(map(lambda field: field[0], cursor.description))

            rows_total = 0
            rows = cursor.fetchmany(self.rows_chunk)
            while len(rows) > 0:
                rows_total = rows_total + len(rows)
                dest_hook.bulk_insert_rows(self.destination_table, rows,
                                           target_fields=target_fields,
                                           commit_every=self.rows_chunk)
                rows = cursor.fetchmany(self.rows_chunk)
                self.log.info("Total inserted: %s rows", rows_total)

            self.log.info("Finished data transfer.")
            cursor.close()

    def execute(self, context):
        src_hook = OdbcHook(odbc_conn_id=self.mssql_source_conn_id)
        dest_hook = OracleHook(oracle_conn_id=self.oracle_destination_conn_id)
        self._execute(src_hook, dest_hook)
