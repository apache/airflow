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
from unittest import mock

import pytest

from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

AIRFLOW_CONN_MSSQL_DEFAULT = "mssql://sa:airflow123@mssql:1433/"

TASK_ID = "test-mssql-to-gcs"
TEST_TABLE_ID = "test_table_id"
MSSQL_CONN_ID = "mssql_default"
BUCKET = "gs://test"
JSON_FILENAME = "test_{}.ndjson"
GZIP = False

NDJSON_LINES = [
    b'{"Address": "1000 N West Street, Suite 1200", "City": "Wilmington, NC, USA", "FirstName": "Apache",'
    b' "LastName": "Airflow", "PersonID": 0}\n',
]


@pytest.mark.integration("mssql")
class TestMsSqlToGoogleCloudStorageOperator:
    def setup_method(self):
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
        q = f"""INSERT INTO {TEST_TABLE_ID} (
        PersonID, LastName, FirstName, Address, City
        ) VALUES
        (0, 'Airflow', 'Apache', '1000 N West Street, Suite 1200', 'Wilmington, NC, USA')
        """
        # raise Exception(q)
        self.cursor.execute(q)

    def teardown_method(self):
        self.cursor.execute(f"DROP TABLE {TEST_TABLE_ID}")

    @mock.patch("airflow.providers.google.cloud.transfers.sql_to_gcs.GCSHook")
    def test_execute(self, gcs_hook_mock_class):
        """Test successful run of execute function for JSON"""
        op = MSSQLToGCSOperator(
            task_id=TASK_ID,
            mssql_conn_id=MSSQL_CONN_ID,
            sql=f"SELECT * FROM {TEST_TABLE_ID}",
            bucket=BUCKET,
            filename=JSON_FILENAME,
        )
        gcs_hook_mock = gcs_hook_mock_class.return_value

        def _assert_upload(
            bucket, obj, tmp_filename, mime_type=None, gzip=False, metadata=None
        ):
            assert BUCKET == bucket
            assert JSON_FILENAME.format(0) == obj
            assert "application/json" == mime_type
            assert GZIP == gzip
            with open(tmp_filename, "rb") as file:
                assert b"".join(NDJSON_LINES) == file.read()

        gcs_hook_mock.upload.side_effect = _assert_upload
        op.execute(context=mock.MagicMock())
