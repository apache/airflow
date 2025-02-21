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

import os
from unittest.mock import Mock, PropertyMock, call, patch

import pytest

from airflow.providers.apache.hive.transfers.mssql_to_hive import MsSqlToHiveOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

AIRFLOW_CONN_MSSQL_DEFAULT = "mssql://sa:airflow123@mssql:1433/"
TEST_TABLE_ID = "Persons"


@pytest.mark.integration("mssql")
class TestMsSqlToHiveTransfer:
    def setup_method(self, mocker):
        os.environ["AIRFLOW_CONN_MSSQL_DEFAULT"] = AIRFLOW_CONN_MSSQL_DEFAULT
        hook = MsSqlHook()
        conn = hook.get_conn()
        hook.set_autocommit(conn, True)
        self.cursor = conn.cursor()
        self.cursor.execute(f"""CREATE TABLE {TEST_TABLE_ID} (
                            PersonID int,
                            LastName varchar(255),
                            FirstName varchar(255),
                            Address varchar(255),
                            City varchar(255)
                            )""")
        self.kwargs = dict(
            sql=f"SELECT * FROM {TEST_TABLE_ID}", hive_table="table", task_id="test_mssql_to_hive", dag=None
        )

    def teardown_method(self):
        self.cursor.execute(f"DROP TABLE {TEST_TABLE_ID}")

    @patch("airflow.providers.apache.hive.transfers.mssql_to_hive.csv")
    @patch("airflow.providers.apache.hive.transfers.mssql_to_hive.NamedTemporaryFile")
    @patch("airflow.providers.apache.hive.transfers.mssql_to_hive.HiveCliHook")
    def test_execute(self, mock_hive_hook, mock_tmp_file, mock_csv):
        type(mock_tmp_file).name = PropertyMock(return_value="tmp_file")
        mock_tmp_file.return_value.__enter__ = Mock(return_value=mock_tmp_file)
        mssql_to_hive_transfer = MsSqlToHiveOperator(**self.kwargs)
        mssql_to_hive_transfer.execute(context={})

        mock_tmp_file.assert_called_with(mode="w", encoding="utf-8")
        mock_csv.writer.assert_called_once_with(mock_tmp_file, delimiter=mssql_to_hive_transfer.delimiter)
        mock_hive_hook.return_value.load_file.assert_has_calls(
            [
                call(
                    "tmp_file",
                    "table",
                    field_dict={
                        "PersonID": "INT",
                        "LastName": "STRING",
                        "FirstName": "STRING",
                        "Address": "STRING",
                        "City": "STRING",
                    },
                    create=True,
                    partition={},
                    delimiter="\x01",
                    recreate=False,
                    tblproperties=None,
                )
            ]
        )
