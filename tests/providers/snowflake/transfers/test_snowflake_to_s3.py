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
from airflow.providers.snowflake.transfers.snowflake_to_s3 import SnowflakeToS3Operator

import pytest

from airflow.providers.snowflake.transfers.snowflake_to_s3 import SnowflakeToS3Operator


class TestSnowflakeToS3Transfer:
    @pytest.mark.parametrize("prefix", [None, 'prefix'])
    @pytest.mark.parametrize("schema", [None, 'schema'])
    @pytest.mark.parametrize("unload_sql", ['unload_sql'])
    @pytest.mark.parametrize("on_error", [None, 'CONTINUE', 'SKIP_FILE', 'ABORT_STATEMENT'])
    @pytest.mark.parametrize("header", [None, True, False])
    @pytest.mark.parametrize("overwrite", [None, True, False])
    @pytest.mark.parametrize("single", [None, True, False])
    @mock.patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook.run")
    def test_execute(self, mock_run, schema, prefix, unload_sql, on_error, header, overwrite, single):
        stage = 'stage'
        file_format = 'file_format'

        SnowflakeToS3Operator(
            stage=stage,
            prefix=prefix,
            file_format=file_format,
            schema=schema,
            on_error=on_error,
            unload_sql=unload_sql,
            header=header,
            overwrite=overwrite,
            single=single,
            task_id="task_id",
            dag=None,
        ).execute(None)

        sql_parts = [
            f"COPY INTO @{stage}/{prefix or ''}",
            f"FROM ({unload_sql})",
        ]

        sql_parts.append(f"file_format={file_format}")

        if on_error:
            sql_parts.append(f"on_error={on_error}")

        if header:
            sql_parts.append(f"header={header}")

        if overwrite:
            sql_parts.append(f"overwrite={overwrite}")
        
        if single:
            sql_parts.append(f"single={single}")

        copy_query = "\n".join(sql_parts)

        mock_run.assert_called_once()
        assert mock_run.call_args[0][0] == copy_query
