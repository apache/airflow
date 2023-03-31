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

from unittest import mock

from airflow.providers.snowflake.transfers.copy_into_snowflake import CopyFromExternalStageToSnowflakeOperator


class TestCopyFromExternalStageToSnowflake:
    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_execute(self, mock_hook):
        CopyFromExternalStageToSnowflakeOperator(
            table="table",
            file_format="CSV",
            stage="stage",
            prefix="prefix",
            columns_array=["col1, col2"],
            files=["file1.csv", "file2.csv"],
            pattern="*.csv",
            warehouse="warehouse",
            database="database",
            role="role",
            schema="schema",
            authenticator="authenticator",
            copy_options="copy_options",
            validation_mode="validation_mode",
            task_id="test",
        ).execute(None)

        mock_hook.assert_called_once_with(
            snowflake_conn_id="snowflake_default",
            warehouse="warehouse",
            database="database",
            role="role",
            schema="schema",
            authenticator="authenticator",
            session_parameters=None,
        )

        sql = """
        COPY INTO schema.table(col1, col2)
             FROM  @stage/prefix
        FILES=('file1.csv','file2.csv')
        PATTERN='*.csv'
        FILE_FORMAT=CSV
        copy_options
        validation_mode
        """

        mock_hook.return_value.run.assert_called_once_with(sql=sql, autocommit=True)
