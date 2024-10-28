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

from typing import Callable
from unittest import mock

import pytest

from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    Error,
    ExternalQueryRunFacet,
    ExtractionErrorRunFacet,
    SQLJobFacet,
)
from airflow.providers.openlineage.extractors import OperatorLineage
from airflow.providers.openlineage.sqlparser import DatabaseInfo
from airflow.providers.snowflake.transfers.copy_into_snowflake import (
    CopyFromExternalStageToSnowflakeOperator,
)


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

        mock_hook.return_value.run.assert_called_once_with(
            sql=sql, autocommit=True, return_dictionaries=True, handler=mock.ANY
        )

        handler = mock_hook.return_value.run.mock_calls[0].kwargs.get("handler")
        assert isinstance(handler, Callable)

    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_get_openlineage_facets_on_complete(self, mock_hook):
        mock_hook().run.return_value = [
            {"file": "s3://aws_bucket_name/dir1/file.csv"},
            {"file": "s3://aws_bucket_name_2"},
            {"file": "gcs://gcs_bucket_name/dir2/file.csv"},
            {"file": "gcs://gcs_bucket_name_2"},
            {
                "file": "azure://my_account.blob.core.windows.net/azure_container/dir3/file.csv"
            },
            {"file": "azure://my_account.blob.core.windows.net/azure_container_2"},
        ]
        mock_hook().get_openlineage_database_info.return_value = DatabaseInfo(
            scheme="snowflake_scheme", authority="authority", database="actual_database"
        )
        mock_hook().get_openlineage_default_schema.return_value = "actual_schema"
        mock_hook().query_ids = ["query_id_123"]

        expected_inputs = [
            Dataset(namespace="gcs://gcs_bucket_name", name="dir2"),
            Dataset(namespace="gcs://gcs_bucket_name_2", name="/"),
            Dataset(namespace="s3://aws_bucket_name", name="dir1"),
            Dataset(namespace="s3://aws_bucket_name_2", name="/"),
            Dataset(namespace="wasbs://azure_container@my_account", name="dir3"),
            Dataset(namespace="wasbs://azure_container_2@my_account", name="/"),
        ]
        expected_outputs = [
            Dataset(
                namespace="snowflake_scheme://authority",
                name="actual_database.actual_schema.table",
            )
        ]
        expected_sql = """COPY INTO schema.table\n FROM @stage/\n FILE_FORMAT=CSV"""

        op = CopyFromExternalStageToSnowflakeOperator(
            task_id="test",
            table="table",
            stage="stage",
            database="",
            schema="schema",
            file_format="CSV",
        )
        op.execute(None)
        result = op.get_openlineage_facets_on_complete(None)
        assert result == OperatorLineage(
            inputs=expected_inputs,
            outputs=expected_outputs,
            run_facets={
                "externalQuery": ExternalQueryRunFacet(
                    externalQueryId="query_id_123", source="snowflake_scheme://authority"
                )
            },
            job_facets={"sql": SQLJobFacet(query=expected_sql)},
        )

    @pytest.mark.parametrize("rows", (None, []))
    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_get_openlineage_facets_on_complete_with_empty_inputs(self, mock_hook, rows):
        mock_hook().run.return_value = rows
        mock_hook().get_openlineage_database_info.return_value = DatabaseInfo(
            scheme="snowflake_scheme", authority="authority", database="actual_database"
        )
        mock_hook().get_openlineage_default_schema.return_value = "actual_schema"
        mock_hook().query_ids = ["query_id_123"]

        expected_outputs = [
            Dataset(
                namespace="snowflake_scheme://authority",
                name="actual_database.actual_schema.table",
            )
        ]
        expected_sql = """COPY INTO schema.table\n FROM @stage/\n FILE_FORMAT=CSV"""

        op = CopyFromExternalStageToSnowflakeOperator(
            task_id="test",
            table="table",
            stage="stage",
            database="",
            schema="schema",
            file_format="CSV",
        )
        op.execute(None)
        result = op.get_openlineage_facets_on_complete(None)
        assert result == OperatorLineage(
            inputs=[],
            outputs=expected_outputs,
            run_facets={
                "externalQuery": ExternalQueryRunFacet(
                    externalQueryId="query_id_123", source="snowflake_scheme://authority"
                )
            },
            job_facets={"sql": SQLJobFacet(query=expected_sql)},
        )

    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_get_openlineage_facets_on_complete_unsupported_azure_uri(self, mock_hook):
        mock_hook().run.return_value = [
            {"file": "s3://aws_bucket_name/dir1/file.csv"},
            {"file": "gs://gcp_bucket_name/dir2/file.csv"},
            {"file": "azure://my_account.weird-url.net/azure_container/dir3/file.csv"},
            {"file": "azure://my_account.another_weird-url.net/con/file.csv"},
        ]
        mock_hook().get_openlineage_database_info.return_value = DatabaseInfo(
            scheme="snowflake_scheme", authority="authority", database="actual_database"
        )
        mock_hook().get_openlineage_default_schema.return_value = "actual_schema"
        mock_hook().query_ids = ["query_id_123"]

        expected_inputs = [
            Dataset(namespace="gs://gcp_bucket_name", name="dir2"),
            Dataset(namespace="s3://aws_bucket_name", name="dir1"),
        ]
        expected_outputs = [
            Dataset(
                namespace="snowflake_scheme://authority",
                name="actual_database.actual_schema.table",
            )
        ]
        expected_sql = """COPY INTO schema.table\n FROM @stage/\n FILE_FORMAT=CSV"""
        expected_run_facets = {
            "extractionError": ExtractionErrorRunFacet(
                totalTasks=4,
                failedTasks=2,
                errors=[
                    Error(
                        errorMessage="Unable to extract Dataset namespace and name.",
                        stackTrace=None,
                        task="azure://my_account.another_weird-url.net/con/file.csv",
                        taskNumber=None,
                    ),
                    Error(
                        errorMessage="Unable to extract Dataset namespace and name.",
                        stackTrace=None,
                        task="azure://my_account.weird-url.net/azure_container/dir3/file.csv",
                        taskNumber=None,
                    ),
                ],
            ),
            "externalQuery": ExternalQueryRunFacet(
                externalQueryId="query_id_123", source="snowflake_scheme://authority"
            ),
        }

        op = CopyFromExternalStageToSnowflakeOperator(
            task_id="test",
            table="table",
            stage="stage",
            database="",
            schema="schema",
            file_format="CSV",
        )
        op.execute(None)
        result = op.get_openlineage_facets_on_complete(None)
        assert result == OperatorLineage(
            inputs=expected_inputs,
            outputs=expected_outputs,
            run_facets=expected_run_facets,
            job_facets={"sql": SQLJobFacet(query=expected_sql)},
        )
