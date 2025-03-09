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
            {"file": "azure://my_account.blob.core.windows.net/azure_container/dir3/file.csv"},
            {"file": "azure://my_account.blob.core.windows.net/azure_container_2"},
        ]
        mock_hook().get_openlineage_database_info.return_value = DatabaseInfo(
            scheme="snowflake_scheme", authority="authority", database="actual_database"
        )
        mock_hook().get_openlineage_default_schema.return_value = "actual_schema"
        mock_hook().query_ids = ["query_id_123"]

        expected_inputs = [
            Dataset(namespace="gcs://gcs_bucket_name", name="dir2/file.csv"),
            Dataset(namespace="gcs://gcs_bucket_name_2", name="/"),
            Dataset(namespace="s3://aws_bucket_name", name="dir1/file.csv"),
            Dataset(namespace="s3://aws_bucket_name_2", name="/"),
            Dataset(namespace="wasbs://azure_container@my_account", name="dir3/file.csv"),
            Dataset(namespace="wasbs://azure_container_2@my_account", name="/"),
        ]
        expected_outputs = [
            Dataset(namespace="snowflake_scheme://authority", name="actual_database.actual_schema.table")
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
            Dataset(namespace="snowflake_scheme://authority", name="actual_database.actual_schema.table")
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
            Dataset(namespace="gs://gcp_bucket_name", name="dir2/file.csv"),
            Dataset(namespace="s3://aws_bucket_name", name="dir1/file.csv"),
        ]
        expected_outputs = [
            Dataset(namespace="snowflake_scheme://authority", name="actual_database.actual_schema.table")
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

    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_get_openlineage_facets_on_complete_with_multiple_files_in_same_directory(self, mock_hook):
        # Test that files in the same directory are tracked individually
        mock_hook().run.return_value = [
            {"file": "s3://bucket/same_dir/file1.csv"},
            {"file": "s3://bucket/same_dir/file2.csv"},
            {"file": "s3://bucket/same_dir/file3.csv"},
            {"file": "gcs://bucket/another_dir/file1.csv"},
            {"file": "gcs://bucket/another_dir/file2.csv"},
        ]
        mock_hook().get_openlineage_database_info.return_value = DatabaseInfo(
            scheme="snowflake_scheme", authority="authority", database="actual_database"
        )
        mock_hook().get_openlineage_default_schema.return_value = "actual_schema"
        mock_hook().query_ids = ["query_id_123"]

        expected_inputs = [
            Dataset(namespace="gcs://bucket", name="another_dir/file1.csv"),
            Dataset(namespace="gcs://bucket", name="another_dir/file2.csv"),
            Dataset(namespace="s3://bucket", name="same_dir/file1.csv"),
            Dataset(namespace="s3://bucket", name="same_dir/file2.csv"),
            Dataset(namespace="s3://bucket", name="same_dir/file3.csv"),
        ]
        expected_outputs = [
            Dataset(namespace="snowflake_scheme://authority", name="actual_database.actual_schema.table")
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

    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_extract_openlineage_unique_dataset_paths_directly(self, mock_hook):
        # Direct test of the _extract_openlineage_unique_dataset_paths method
        test_cases = [
            # Multiple files in same directory
            (
                [
                    {"file": "s3://bucket/dir/file1.csv"},
                    {"file": "s3://bucket/dir/file2.csv"},
                ],
                [
                    ("s3://bucket", "dir/file1.csv"),
                    ("s3://bucket", "dir/file2.csv"),
                ],
                [],
            ),
            # Azure files in same container
            (
                [
                    {"file": "azure://account.blob.core.windows.net/container/dir/file1.csv"},
                    {"file": "azure://account.blob.core.windows.net/container/dir/file2.csv"},
                ],
                [
                    ("wasbs://container@account", "dir/file1.csv"),
                    ("wasbs://container@account", "dir/file2.csv"),
                ],
                [],
            ),
            # Mixed cloud providers
            (
                [
                    {"file": "s3://bucket/file.csv"},
                    {"file": "gcs://bucket/file.csv"},
                    {"file": "azure://account.blob.core.windows.net/container/file.csv"},
                ],
                [
                    ("gcs://bucket", "file.csv"),
                    ("s3://bucket", "file.csv"),
                    ("wasbs://container@account", "file.csv"),
                ],
                [],
            ),
            # Test with invalid URIs mixed with valid ones
            (
                [
                    {"file": "s3://bucket/valid.csv"},
                    {"file": "s3:/invalid-uri"},  # Missing slash
                    {"file": "gcs://bucket/valid.csv"},
                    {"file": "gcs:invalid-uri"},  # Missing slashes
                    {"file": "azure://account.blob.core.windows.net/container/valid.csv"},
                    {"file": "azure://account.invalid-domain.net/container/invalid.csv"},  # Invalid domain
                ],
                [
                    ("gcs://bucket", "valid.csv"),
                    ("s3://bucket", "valid.csv"),
                    ("wasbs://container@account", "valid.csv"),
                ],
                [
                    "s3:/invalid-uri",
                    "gcs:invalid-uri",
                    "azure://account.invalid-domain.net/container/invalid.csv",
                ],
            ),
            # Test with all invalid URIs
            (
                [
                    {"file": "s3:/invalid-uri-1"},
                    {"file": "gcs:invalid-uri-2"},
                    {"file": "azure://invalid.uri/container/file.csv"},
                ],
                [],
                [
                    "s3:/invalid-uri-1",
                    "gcs:invalid-uri-2",
                    "azure://invalid.uri/container/file.csv",
                ],
            ),
        ]

        for input_rows, expected_paths, expected_errors in test_cases:
            paths, errors = (
                CopyFromExternalStageToSnowflakeOperator._extract_openlineage_unique_dataset_paths(input_rows)
            )
            assert sorted(paths) == sorted(expected_paths)
            assert sorted([e for e in errors if e is not None]) == sorted(
                [e for e in expected_errors if e is not None]
            )

    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_get_openlineage_facets_on_complete_with_actual_snowflake_output(self, mock_hook):
        # Test with actual output format from Snowflake
        mock_hook().run.return_value = [
            {
                "file": "s3://bucket-name/path/to/file.csv",
                "status": "LOADED",
                "rows_parsed": 2,
                "rows_loaded": 2,
                "error_limit": 2,
                "errors_seen": 0,
                "first_error": None,
                "first_error_line": None,
                "first_error_character": None,
                "first_error_column_name": None,
            }
        ]
        mock_hook().get_openlineage_database_info.return_value = DatabaseInfo(
            scheme="snowflake_scheme", authority="authority", database="actual_database"
        )
        mock_hook().get_openlineage_default_schema.return_value = "actual_schema"
        mock_hook().query_ids = ["query_id_123"]

        expected_inputs = [
            Dataset(namespace="s3://bucket-name", name="path/to/file.csv"),
        ]
        expected_outputs = [
            Dataset(namespace="snowflake_scheme://authority", name="actual_database.actual_schema.table")
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

    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_get_openlineage_facets_on_complete_with_mixed_valid_and_invalid_uris(self, mock_hook):
        # Test with a mix of valid and invalid URIs from different cloud providers
        mock_hook().run.return_value = [
            # Valid URIs
            {"file": "s3://valid-bucket/path/to/file.csv"},
            {"file": "gs://valid-bucket/path/to/file.csv"},
            {"file": "azure://account.blob.core.windows.net/container/path/to/file.csv"},
            # Invalid URIs
            {"file": "s3:/invalid-s3-uri"},  # Missing slash
            {"file": "gcs:invalid-gcs-uri"},  # Missing slashes
            {"file": "azure://account.invalid-domain.net/container/file.csv"},  # Invalid Azure domain
        ]
        mock_hook().get_openlineage_database_info.return_value = DatabaseInfo(
            scheme="snowflake_scheme", authority="authority", database="actual_database"
        )
        mock_hook().get_openlineage_default_schema.return_value = "actual_schema"
        mock_hook().query_ids = ["query_id_123"]

        expected_inputs = [
            Dataset(namespace="wasbs://container@account", name="path/to/file.csv"),
            Dataset(namespace="gs://valid-bucket", name="path/to/file.csv"),
            Dataset(namespace="s3://valid-bucket", name="path/to/file.csv"),
        ]
        expected_outputs = [
            Dataset(namespace="snowflake_scheme://authority", name="actual_database.actual_schema.table")
        ]
        expected_sql = """COPY INTO schema.table\n FROM @stage/\n FILE_FORMAT=CSV"""

        expected_errors = [
            Error(
                errorMessage="Unable to extract Dataset namespace and name.",
                stackTrace=None,
                task="azure://account.invalid-domain.net/container/file.csv",
                taskNumber=None,
            ),
            Error(
                errorMessage="Unable to extract Dataset namespace and name.",
                stackTrace=None,
                task="gcs:invalid-gcs-uri",
                taskNumber=None,
            ),
            Error(
                errorMessage="Unable to extract Dataset namespace and name.",
                stackTrace=None,
                task="s3:/invalid-s3-uri",
                taskNumber=None,
            ),
        ]

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

        # Check inputs and outputs
        assert set((d.namespace, d.name) for d in result.inputs) == set(
            (d.namespace, d.name) for d in expected_inputs
        )
        assert result.outputs == expected_outputs

        # Check error facets
        assert "extractionError" in result.run_facets
        error_facet = result.run_facets["extractionError"]
        assert error_facet.totalTasks == 6
        assert error_facet.failedTasks == 3

        # Check that all expected errors are present (order might vary)
        actual_error_messages = [error.task for error in error_facet.errors]
        expected_error_messages = [error.task for error in expected_errors]
        assert set(actual_error_messages) == set(expected_error_messages)

        # Check other facets
        assert "externalQuery" in result.run_facets
        assert result.job_facets["sql"].query == expected_sql

    @mock.patch("airflow.providers.snowflake.transfers.copy_into_snowflake.SnowflakeHook")
    def test_get_openlineage_facets_on_complete_with_empty_directory(self, mock_hook):
        # Test with a response indicating no files were processed (empty directory)
        mock_hook().run.return_value = [
            {
                "status": "Copy executed with 0 files processed",
                "file": None,
                "rows_parsed": 0,
                "rows_loaded": 0,
                "error_limit": 0,
                "errors_seen": 0,
                "first_error": None,
                "first_error_line": None,
                "first_error_character": None,
                "first_error_column_name": None,
            }
        ]
        mock_hook().get_openlineage_database_info.return_value = DatabaseInfo(
            scheme="snowflake_scheme", authority="authority", database="actual_database"
        )
        mock_hook().get_openlineage_default_schema.return_value = "actual_schema"
        mock_hook().query_ids = ["query_id_123"]

        expected_outputs = [
            Dataset(namespace="snowflake_scheme://authority", name="actual_database.actual_schema.table")
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
            inputs=[],  # No inputs since no files were processed
            outputs=expected_outputs,
            run_facets={
                "externalQuery": ExternalQueryRunFacet(
                    externalQueryId="query_id_123", source="snowflake_scheme://authority"
                )
            },
            job_facets={"sql": SQLJobFacet(query=expected_sql)},
        )
