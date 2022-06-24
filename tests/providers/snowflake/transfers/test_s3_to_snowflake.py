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

from unittest import mock

import pytest

from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator


class TestS3ToSnowflakeTransfer:
    @pytest.mark.parametrize("pattern", [None, '.*[.]csv'])
    @pytest.mark.parametrize("columns_array", [None, ['col1', 'col2', 'col3']])
    @pytest.mark.parametrize("s3_keys", [None, ['1.csv', '2.csv']])
    @pytest.mark.parametrize("prefix", [None, 'prefix'])
    @pytest.mark.parametrize("schema", [None, 'schema'])
    @mock.patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook.run")
    def test_execute(self, mock_run, schema, prefix, s3_keys, columns_array, pattern):
        table = 'table'
        stage = 'stage'
        file_format = 'file_format'

        S3ToSnowflakeOperator(
            s3_keys=s3_keys,
            table=table,
            stage=stage,
            prefix=prefix,
            file_format=file_format,
            schema=schema,
            columns_array=columns_array,
            pattern=pattern,
            task_id="task_id",
            dag=None,
        ).execute(None)

        copy_query = "COPY INTO "
        if schema:
            copy_query += f"{schema}.{table}"
        else:
            copy_query += table
        if columns_array:
            copy_query += f"({','.join(columns_array)})"

        copy_query += f"\nFROM @{stage}/{prefix or ''}"

        if s3_keys:
            files = ", ".join(f"'{key}'" for key in s3_keys)
            copy_query += f"\nfiles=({files})"

        copy_query += f"\nfile_format={file_format}"

        if pattern:
            copy_query += f"\npattern='{pattern}'"

        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == copy_query
