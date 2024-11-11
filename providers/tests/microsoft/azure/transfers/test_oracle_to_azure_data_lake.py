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
from __future__ import annotations

import csv
import os
from unittest import mock
from unittest.mock import MagicMock

from airflow.providers.microsoft.azure.transfers.oracle_to_azure_data_lake import (
    OracleToAzureDataLakeOperator,
)


class TestOracleToAzureDataLakeTransfer:
    mock_module_path = "airflow.providers.microsoft.azure.transfers.oracle_to_azure_data_lake"

    def test_write_temp_file(self, tmp_path):
        csv_path = tmp_path / "testfile.csv"
        task_id = "some_test_id"
        sql = "some_sql"
        sql_params = {":p_data": "2018-01-01"}
        oracle_conn_id = "oracle_conn_id"
        azure_data_lake_conn_id = "azure_data_lake_conn_id"
        azure_data_lake_path = "azure_data_lake_path"
        delimiter = "|"
        encoding = "utf-8"
        cursor_description = [
            ("id", "<class 'oracledb.NUMBER'>", 39, None, 38, 0, 0),
            ("description", "<class 'oracledb.STRING'>", 60, 240, None, None, 1),
        ]
        cursor_rows = [[1, "description 1"], [2, "description 2"]]
        mock_cursor = MagicMock()
        mock_cursor.description = cursor_description
        mock_cursor.__iter__.return_value = cursor_rows

        op = OracleToAzureDataLakeOperator(
            task_id=task_id,
            filename=csv_path.name,
            oracle_conn_id=oracle_conn_id,
            sql=sql,
            sql_params=sql_params,
            azure_data_lake_conn_id=azure_data_lake_conn_id,
            azure_data_lake_path=azure_data_lake_path,
            delimiter=delimiter,
            encoding=encoding,
        )

        op._write_temp_file(mock_cursor, os.fspath(csv_path))
        assert csv_path.exists()

        with csv_path.open() as f:
            csvr = csv.reader(f, delimiter=delimiter)
            header = next(csvr)
            assert header[0] == "id"
            assert header[1] == "description"
            for csv_rec, exp_rec in zip(csvr, cursor_rows):
                assert csv_rec[0] == str(exp_rec[0])
                assert csv_rec[1] == exp_rec[1]

    @mock.patch(mock_module_path + ".OracleHook", autospec=True)
    @mock.patch(mock_module_path + ".AzureDataLakeHook", autospec=True)
    def test_execute(self, mock_data_lake_hook, mock_oracle_hook):
        task_id = "some_test_id"
        sql = "some_sql"
        sql_params = {":p_data": "2018-01-01"}
        oracle_conn_id = "oracle_conn_id"
        filename = "some_filename"
        azure_data_lake_conn_id = "azure_data_lake_conn_id"
        azure_data_lake_path = "azure_data_lake_path"
        delimiter = "|"
        encoding = "latin-1"
        cursor_description = [
            ("id", "<class 'oracledb.NUMBER'>", 39, None, 38, 0, 0),
            ("description", "<class 'oracledb.STRING'>", 60, 240, None, None, 1),
        ]
        cursor_rows = [[1, "description 1"], [2, "description 2"]]
        cursor_mock = MagicMock()
        cursor_mock.description.return_value = cursor_description
        cursor_mock.__iter__.return_value = cursor_rows
        mock_oracle_conn = MagicMock()
        mock_oracle_conn.cursor().return_value = cursor_mock
        mock_oracle_hook.get_conn().return_value = mock_oracle_conn

        op = OracleToAzureDataLakeOperator(
            task_id=task_id,
            filename=filename,
            oracle_conn_id=oracle_conn_id,
            sql=sql,
            sql_params=sql_params,
            azure_data_lake_conn_id=azure_data_lake_conn_id,
            azure_data_lake_path=azure_data_lake_path,
            delimiter=delimiter,
            encoding=encoding,
        )

        op.execute(None)

        mock_oracle_hook.assert_called_once_with(oracle_conn_id=oracle_conn_id)
        mock_data_lake_hook.assert_called_once_with(azure_data_lake_conn_id=azure_data_lake_conn_id)
