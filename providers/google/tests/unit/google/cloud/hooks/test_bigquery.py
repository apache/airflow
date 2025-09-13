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

from datetime import datetime
from unittest import mock
from unittest.mock import AsyncMock

import google.auth
import pytest
from gcloud.aio.bigquery import Job, Table as Table_async
from google.api_core import page_iterator
from google.auth.exceptions import RefreshError
from google.cloud.bigquery import (
    DEFAULT_RETRY,
    CopyJob,
    DatasetReference,
    QueryJob,
    Table,
    TableReference,
)
from google.cloud.bigquery.dataset import AccessEntry, Dataset, DatasetListItem
from google.cloud.bigquery.table import _EmptyRowIterator
from google.cloud.exceptions import NotFound

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.assets import Asset
from airflow.providers.google.cloud.hooks.bigquery import (
    BigQueryAsyncHook,
    BigQueryHook,
    BigQueryTableAsyncHook,
    _api_resource_configs_duplication_check,
    _cleanse_time_partitioning,
    _format_schema_for_description,
    _validate_src_fmt_configs,
    _validate_value,
)

pytestmark = pytest.mark.filterwarnings("error::airflow.exceptions.AirflowProviderDeprecationWarning")

PROJECT_ID = "bq-project"
CREDENTIALS = "bq-credentials"
DATASET_ID = "bq_dataset"
TABLE_ID = "bq_table"
PARTITION_ID = "20200101"
VIEW_ID = "bq_view"
JOB_ID = "1234"
LOCATION = "europe-north1"
TABLE_REFERENCE_REPR = {
    "tableId": TABLE_ID,
    "datasetId": DATASET_ID,
    "projectId": PROJECT_ID,
}
TABLE_REFERENCE = TableReference.from_api_repr(TABLE_REFERENCE_REPR)


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class _BigQueryBaseTestClass:
    def setup_method(self) -> None:
        class MockedBigQueryHook(BigQueryHook):
            def get_credentials_and_project_id(self):
                return CREDENTIALS, PROJECT_ID

        self.hook = MockedBigQueryHook()


@pytest.mark.db_test
class TestBigQueryHookMethods(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryConnection")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    def test_bigquery_client_creation(self, mock_build, mock_authorize, mock_bigquery_connection):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            "bigquery", "v2", http=mock_authorize.return_value, cache_discovery=False
        )
        mock_bigquery_connection.assert_called_once_with(
            service=mock_build.return_value,
            project_id=PROJECT_ID,
            hook=self.hook,
            use_legacy_sql=self.hook.use_legacy_sql,
            location=self.hook.location,
            num_retries=self.hook.num_retries,
        )
        assert mock_bigquery_connection.return_value == result

    def test_bigquery_insert_rows_not_implemented(self):
        with pytest.raises(NotImplementedError):
            self.hook.insert_rows(table="table", rows=[1, 2])

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_bigquery_table_exists_true(self, mock_client):
        result = self.hook.table_exists(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)
        mock_client.return_value.get_table.assert_called_once_with(TABLE_REFERENCE)
        mock_client.assert_called_once_with(project_id=PROJECT_ID)
        assert result is True

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_bigquery_table_exists_false(self, mock_client):
        mock_client.return_value.get_table.side_effect = NotFound("Dataset not found")
        result = self.hook.table_exists(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)
        mock_client.return_value.get_table.assert_called_once_with(TABLE_REFERENCE)
        mock_client.assert_called_once_with(project_id=PROJECT_ID)
        assert result is False

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_bigquery_table_partition_exists_true(self, mock_client):
        mock_client.return_value.list_partitions.return_value = [PARTITION_ID]
        result = self.hook.table_partition_exists(
            project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID, partition_id=PARTITION_ID
        )
        mock_client.return_value.list_partitions.assert_called_once_with(TABLE_REFERENCE)
        mock_client.assert_called_once_with(project_id=PROJECT_ID)
        assert result is True

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_bigquery_table_partition_exists_false_no_table(self, mock_client):
        mock_client.return_value.get_table.side_effect = NotFound("Dataset not found")
        result = self.hook.table_partition_exists(
            project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID, partition_id=PARTITION_ID
        )
        mock_client.return_value.list_partitions.assert_called_once_with(TABLE_REFERENCE)
        mock_client.assert_called_once_with(project_id=PROJECT_ID)
        assert result is False

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_bigquery_table_partition_exists_false_no_partition(self, mock_client):
        mock_client.return_value.list_partitions.return_value = []
        result = self.hook.table_partition_exists(
            project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID, partition_id=PARTITION_ID
        )
        mock_client.return_value.list_partitions.assert_called_once_with(TABLE_REFERENCE)
        mock_client.assert_called_once_with(project_id=PROJECT_ID)
        assert result is False

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.read_gbq")
    @pytest.mark.parametrize("df_type", ["pandas", "polars"])
    def test_get_df(self, mock_read_gbq, df_type):
        import pandas as pd
        import polars as pl

        mock_read_gbq.return_value = pd.DataFrame({"a": [1, 2, 3]})
        result = self.hook.get_df("select 1", df_type=df_type)

        expected_type = pd.DataFrame if df_type == "pandas" else pl.DataFrame
        assert isinstance(result, expected_type)
        assert result.shape == (3, 1)
        assert result.columns == ["a"]
        if df_type == "pandas":
            assert result["a"].tolist() == [1, 2, 3]
        else:
            assert result.to_series().to_list() == [1, 2, 3]
        mock_read_gbq.assert_called_once_with(
            "select 1", credentials=CREDENTIALS, dialect="legacy", project_id=PROJECT_ID
        )

    def test_validate_value(self):
        with pytest.raises(
            TypeError, match="case_1 argument must have a type <class 'dict'> not <class 'str'>"
        ):
            _validate_value("case_1", "a", dict)
        assert _validate_value("case_2", 0, int) is None

    def test_duplication_check(self):
        key_one = True
        with pytest.raises(
            ValueError,
            match=r"Values of key_one param are duplicated. api_resource_configs contained key_one param in"
            r" `query` config and key_one was also provided with arg to run_query\(\) method. "
            r"Please remove duplicates.",
        ):
            _api_resource_configs_duplication_check("key_one", key_one, {"key_one": False})
        assert _api_resource_configs_duplication_check("key_one", key_one, {"key_one": True}) is None

    def test_validate_src_fmt_configs(self):
        source_format = "test_format"
        valid_configs = ["test_config_known", "compatibility_val"]
        backward_compatibility_configs = {"compatibility_val": "val"}

        src_fmt_configs = {"test_config_unknown": "val"}
        with pytest.raises(
            ValueError, match="test_config_unknown is not a valid src_fmt_configs for type test_format."
        ):
            # This config should raise a value error.
            _validate_src_fmt_configs(
                source_format, src_fmt_configs, valid_configs, backward_compatibility_configs
            )

        src_fmt_configs = {"test_config_known": "val"}
        src_fmt_configs = _validate_src_fmt_configs(
            source_format, src_fmt_configs, valid_configs, backward_compatibility_configs
        )
        assert "test_config_known" in src_fmt_configs, (
            "src_fmt_configs should contain al known src_fmt_configs"
        )

        assert "compatibility_val" in src_fmt_configs, (
            "_validate_src_fmt_configs should add backward_compatibility config"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.SchemaField")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_list_rows(self, mock_client, mock_schema, mock_table):
        mock_row_iterator = _EmptyRowIterator()
        mock_client.return_value.list_rows.return_value = mock_row_iterator

        for return_iterator, expected in [(False, []), (True, mock_row_iterator)]:
            actual = self.hook.list_rows(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                max_results=10,
                selected_fields=["field_1", "field_2"],
                page_token="page123",
                start_index=5,
                location=LOCATION,
                return_iterator=return_iterator,
            )
            mock_table.from_api_repr.assert_called_once_with({"tableReference": TABLE_REFERENCE_REPR})
            mock_schema.assert_has_calls([mock.call(x, "") for x in ["field_1", "field_2"]])
            mock_client.return_value.list_rows.assert_called_once_with(
                table=mock_table.from_api_repr.return_value,
                max_results=10,
                selected_fields=mock.ANY,
                page_token="page123",
                start_index=5,
                retry=DEFAULT_RETRY,
            )
            assert actual == expected
            mock_table.from_api_repr.reset_mock()
            mock_client.return_value.list_rows.reset_mock()

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_list_rows_with_empty_selected_fields(self, mock_client, mock_table):
        mock_row_iterator = _EmptyRowIterator()
        mock_client.return_value.list_rows.return_value = mock_row_iterator

        for return_iterator, expected in [(False, []), (True, mock_row_iterator)]:
            actual = self.hook.list_rows(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                max_results=10,
                page_token="page123",
                selected_fields=[],
                start_index=5,
                location=LOCATION,
                return_iterator=return_iterator,
            )
            mock_table.from_api_repr.assert_called_once_with({"tableReference": TABLE_REFERENCE_REPR})
            mock_client.return_value.list_rows.assert_called_once_with(
                table=mock_table.from_api_repr.return_value,
                max_results=10,
                page_token="page123",
                selected_fields=None,
                start_index=5,
                retry=DEFAULT_RETRY,
            )
            assert actual == expected
            mock_table.from_api_repr.reset_mock()
            mock_client.return_value.list_rows.reset_mock()

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.create_table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables")
    def test_table_upsert_create_new_table(self, mock_get, mock_create):
        table_resource = {"tableReference": {"tableId": TABLE_ID}}
        mock_get.return_value = []

        self.hook.run_table_upsert(dataset_id=DATASET_ID, table_resource=table_resource)

        mock_get.assert_called_once_with(project_id=PROJECT_ID, dataset_id=DATASET_ID)
        mock_create.assert_called_once_with(
            dataset_id=DATASET_ID, table_id=TABLE_ID, table_resource=table_resource, project_id=PROJECT_ID
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset_tables")
    def test_table_upsert_already_exists(self, mock_get, mock_update):
        table_resource = {"tableReference": {"tableId": TABLE_ID}}
        mock_get.return_value = [{"tableId": TABLE_ID}]

        self.hook.run_table_upsert(dataset_id=DATASET_ID, table_resource=table_resource)

        mock_get.assert_called_once_with(project_id=PROJECT_ID, dataset_id=DATASET_ID)
        mock_update.assert_called_once_with(table_resource=table_resource)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset")
    def test_run_grant_dataset_view_access_granting(self, mock_update, mock_get):
        view_table = f"{TABLE_ID}_view"
        view_dataset = f"{DATASET_ID}_view"
        view_access = AccessEntry(
            role=None,
            entity_type="view",
            entity_id={"projectId": PROJECT_ID, "datasetId": view_dataset, "tableId": view_table},
        )

        dataset = Dataset(DatasetReference.from_string(DATASET_ID, PROJECT_ID))
        dataset.access_entries = []
        mock_get.return_value = dataset

        self.hook.run_grant_dataset_view_access(
            source_dataset=DATASET_ID, view_dataset=view_dataset, view_table=view_table
        )

        mock_get.assert_called_once_with(project_id=PROJECT_ID, dataset_id=DATASET_ID)
        assert any(
            entry.role == view_access.role
            and entry.entity_type == view_access.entity_type
            and entry.entity_id == view_access.entity_id
            for entry in dataset.access_entries
        ), f"View access entry not found in {dataset.access_entries}"
        mock_update.assert_called_once_with(
            fields=["access"],
            dataset_resource=dataset.to_api_repr(),
            project_id=PROJECT_ID,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_dataset")
    def test_run_grant_dataset_view_access_already_granted(self, mock_update, mock_get):
        view_table = f"{TABLE_ID}_view"
        view_dataset = f"{DATASET_ID}_view"
        view_access = AccessEntry(
            role=None,
            entity_type="view",
            entity_id={"projectId": PROJECT_ID, "datasetId": view_dataset, "tableId": view_table},
        )

        dataset = Dataset(DatasetReference.from_string(DATASET_ID, PROJECT_ID))
        dataset.access_entries = [view_access]
        mock_get.return_value = dataset

        self.hook.run_grant_dataset_view_access(
            source_dataset=DATASET_ID, view_dataset=view_dataset, view_table=view_table
        )

        mock_get.assert_called_once_with(project_id=PROJECT_ID, dataset_id=DATASET_ID)
        assert len(mock_update.calls) == 0

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_poll_job_complete(self, mock_client):
        self.hook.poll_job_complete(job_id=JOB_ID, location=LOCATION, project_id=PROJECT_ID)
        mock_client.assert_called_once_with(location=LOCATION, project_id=PROJECT_ID)
        mock_client.return_value.get_job.assert_called_once_with(job_id=JOB_ID)
        mock_client.return_value.get_job.return_value.done.assert_called_once_with(retry=DEFAULT_RETRY)

    @mock.patch("tenacity.nap.time.sleep", mock.MagicMock())
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_get_job_credentials_refresh_error(self, mock_client):
        error = "Unable to acquire impersonated credentials"
        response_body = "<!DOCTYPE html>\n<html lang=en>\n  <meta charset=utf-8>\n"
        mock_job = mock.MagicMock(
            job_id="123456_hash",
            error_result=False,
            state="PENDING",
            done=lambda: False,
        )
        mock_client.return_value.get_job.side_effect = [RefreshError(error, response_body), mock_job]

        job = self.hook.get_job(job_id=JOB_ID, location=LOCATION, project_id=PROJECT_ID)
        mock_client.assert_any_call(location=LOCATION, project_id=PROJECT_ID)
        assert mock_client.call_count == 2
        assert job == mock_job

    @pytest.mark.parametrize(
        "error",
        [
            RefreshError("Other error", "test body"),
            ValueError(),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_get_job_credentials_error(self, mock_client, error):
        mock_client.return_value.get_job.side_effect = error
        with pytest.raises(type(error)):
            self.hook.get_job(job_id=JOB_ID, location=LOCATION, project_id=PROJECT_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_schema(self, mock_client):
        table = {
            "tableReference": TABLE_REFERENCE_REPR,
            "schema": {
                "fields": [
                    {"name": "id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                ]
            },
        }

        mock_client.return_value.get_table.return_value = Table.from_api_repr(table)
        result = self.hook.get_schema(dataset_id=DATASET_ID, table_id=TABLE_ID)

        mock_client.return_value.get_table.assert_called_once_with(TABLE_REFERENCE)
        assert "fields" in result
        assert len(result["fields"]) == 2

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_schema")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_table")
    def test_update_table_schema_with_policy_tags(self, mock_update, mock_get_schema):
        mock_get_schema.return_value = {
            "fields": [
                {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                {
                    "name": "salary",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                    "policyTags": {"names": ["sensitive"]},
                },
                {"name": "not_changed", "type": "INTEGER", "mode": "REQUIRED"},
                {
                    "name": "subrecord",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {
                            "name": "field_1",
                            "type": "STRING",
                            "mode": "REQUIRED",
                            "policyTags": {"names": ["sensitive"]},
                        },
                    ],
                },
            ]
        }

        schema_fields_updates = [
            {"name": "emp_name", "description": "Name of employee", "policyTags": {"names": ["sensitive"]}},
            {
                "name": "salary",
                "description": "Monthly salary in USD",
                "policyTags": {},
            },
            {
                "name": "subrecord",
                "description": "Some Desc",
                "fields": [
                    {"name": "field_1", "description": "Some nested desc"},
                ],
            },
        ]

        expected_result_schema = {
            "fields": [
                {
                    "name": "emp_name",
                    "type": "STRING",
                    "mode": "REQUIRED",
                    "description": "Name of employee",
                    "policyTags": {"names": ["sensitive"]},
                },
                {
                    "name": "salary",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                    "description": "Monthly salary in USD",
                    "policyTags": {},
                },
                {"name": "not_changed", "type": "INTEGER", "mode": "REQUIRED"},
                {
                    "name": "subrecord",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "description": "Some Desc",
                    "fields": [
                        {
                            "name": "field_1",
                            "type": "STRING",
                            "mode": "REQUIRED",
                            "description": "Some nested desc",
                            "policyTags": {"names": ["sensitive"]},
                        }
                    ],
                },
            ]
        }

        self.hook.update_table_schema(
            schema_fields_updates=schema_fields_updates,
            include_policy_tags=True,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
        )

        mock_update.assert_called_once_with(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            project_id=PROJECT_ID,
            table_resource={"schema": expected_result_schema},
            fields=["schema"],
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_schema")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.update_table")
    def test_update_table_schema_without_policy_tags(self, mock_update, mock_get_schema):
        mock_get_schema.return_value = {
            "fields": [
                {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                {"name": "salary", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "not_changed", "type": "INTEGER", "mode": "REQUIRED"},
                {
                    "name": "subrecord",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "fields": [
                        {"name": "field_1", "type": "STRING", "mode": "REQUIRED"},
                    ],
                },
            ]
        }

        schema_fields_updates = [
            {"name": "emp_name", "description": "Name of employee"},
            {
                "name": "salary",
                "description": "Monthly salary in USD",
                "policyTags": {"names": ["sensitive"]},
            },
            {
                "name": "subrecord",
                "description": "Some Desc",
                "fields": [
                    {"name": "field_1", "description": "Some nested desc"},
                ],
            },
        ]

        expected_result_schema = {
            "fields": [
                {"name": "emp_name", "type": "STRING", "mode": "REQUIRED", "description": "Name of employee"},
                {
                    "name": "salary",
                    "type": "INTEGER",
                    "mode": "REQUIRED",
                    "description": "Monthly salary in USD",
                },
                {"name": "not_changed", "type": "INTEGER", "mode": "REQUIRED"},
                {
                    "name": "subrecord",
                    "type": "RECORD",
                    "mode": "REQUIRED",
                    "description": "Some Desc",
                    "fields": [
                        {
                            "name": "field_1",
                            "type": "STRING",
                            "mode": "REQUIRED",
                            "description": "Some nested desc",
                        }
                    ],
                },
            ]
        }

        self.hook.update_table_schema(
            schema_fields_updates=schema_fields_updates,
            include_policy_tags=False,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
        )

        mock_update.assert_called_once_with(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            project_id=PROJECT_ID,
            table_resource={"schema": expected_result_schema},
            fields=["schema"],
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_insert_all_succeed(self, mock_client):
        rows = [{"json": {"a_key": "a_value_0"}}]

        self.hook.insert_all(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            rows=rows,
            ignore_unknown_values=True,
            skip_invalid_rows=True,
        )
        mock_client.return_value.get_table.assert_called_once_with(TABLE_REFERENCE)
        mock_client.return_value.insert_rows.assert_called_once_with(
            table=mock_client.return_value.get_table.return_value,
            rows=rows,
            ignore_unknown_values=True,
            skip_invalid_rows=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_insert_all_fail(self, mock_client):
        rows = [{"json": {"a_key": "a_value_0"}}]

        mock_client.return_value.insert_rows.return_value = ["some", "errors"]
        with pytest.raises(AirflowException, match="insert error"):
            self.hook.insert_all(
                project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID, rows=rows, fail_on_error=True
            )

    @pytest.mark.parametrize("nowait", [True, False])
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.QueryJob")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_insert_job(self, mock_client, mock_query_job, nowait):
        job_conf = {
            "query": {
                "query": "SELECT * FROM test",
                "useLegacySql": "False",
            }
        }
        mock_query_job._JOB_TYPE = "query"

        self.hook.insert_job(
            configuration=job_conf, job_id=JOB_ID, project_id=PROJECT_ID, location=LOCATION, nowait=nowait
        )

        mock_client.assert_called_once_with(
            project_id=PROJECT_ID,
            location=LOCATION,
        )

        mock_query_job.from_api_repr.assert_called_once_with(
            {
                "configuration": job_conf,
                "jobReference": {"jobId": JOB_ID, "projectId": PROJECT_ID, "location": LOCATION},
            },
            mock_client.return_value,
        )
        if nowait:
            mock_query_job.from_api_repr.return_value._begin.assert_called_once()
            mock_query_job.from_api_repr.return_value.result.assert_not_called()
        else:
            mock_query_job.from_api_repr.return_value._begin.assert_not_called()
            mock_query_job.from_api_repr.return_value.result.assert_called_once()

    def test_dbapi_get_uri(self):
        assert self.hook.get_uri().startswith("bigquery://")

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.md5")
    @pytest.mark.parametrize(
        "test_dag_id, expected_job_id",
        [("test-dag-id-1.1", "airflow_test_dag_id_1_1_test_job_id_2020_01_23T00_00_00_hash")],
        ids=["test-dag-id-1.1"],
    )
    def test_job_id_validity(self, mock_md5, test_dag_id, expected_job_id):
        hash_ = "hash"
        mock_md5.return_value.hexdigest.return_value = hash_
        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }

        job_id = self.hook.generate_job_id(
            job_id=None,
            dag_id=test_dag_id,
            task_id="test_job_id",
            logical_date=datetime(2020, 1, 23),
            configuration=configuration,
        )
        assert job_id == expected_job_id

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_job",
        return_value=mock.MagicMock(spec=CopyJob),
    )
    def test_query_results__not_query_job_exception(self, _):
        with pytest.raises(AirflowException, match="query job"):
            self.hook.get_query_results(job_id=JOB_ID, location=LOCATION)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_job",
        return_value=mock.MagicMock(spec=QueryJob, state="RUNNING"),
    )
    def test_query_results__job_not_done_exception(self, _):
        with pytest.raises(AirflowException, match="DONE state"):
            self.hook.get_query_results(job_id=JOB_ID, location=LOCATION)

    @pytest.mark.parametrize(
        "selected_fields, result",
        [
            (None, [{"a": 1, "b": 2}, {"a": 3, "b": 4}]),
            ("a", [{"a": 1}, {"a": 3}]),
            ("a,b", [{"a": 1, "b": 2}, {"a": 3, "b": 4}]),
            ("b,a", [{"a": 1, "b": 2}, {"a": 3, "b": 4}]),
        ],
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_job",
        return_value=mock.MagicMock(
            spec=QueryJob,
            state="DONE",
            result=mock.MagicMock(return_value=[{"a": 1, "b": 2}, {"a": 3, "b": 4}]),
        ),
    )
    def test_query_results(self, _, selected_fields, result):
        assert (
            self.hook.get_query_results(job_id=JOB_ID, location=LOCATION, selected_fields=selected_fields)
            == result
        )

    def test_split_tablename_internal_need_default_project(self):
        with pytest.raises(ValueError, match="INTERNAL: No default project is specified"):
            self.hook.split_tablename("dataset.table", None)

    @pytest.mark.parametrize(
        "project_expected, dataset_expected, table_expected, table_input",
        [
            ("project", "dataset", "table", "dataset.table"),
            ("alternative", "dataset", "table", "alternative:dataset.table"),
            ("alternative", "dataset", "table", "alternative.dataset.table"),
            ("alt1:alt", "dataset", "table", "alt1:alt.dataset.table"),
            ("alt1:alt", "dataset", "table", "alt1:alt:dataset.table"),
        ],
    )
    def test_split_tablename(self, project_expected, dataset_expected, table_expected, table_input):
        default_project_id = "project"
        project, dataset, table = self.hook.split_tablename(table_input, default_project_id)
        assert project_expected == project
        assert dataset_expected == dataset
        assert table_expected == table

    @pytest.mark.parametrize(
        "table_input, var_name, exception_message",
        [
            ("alt1:alt2:alt3:dataset.table", None, "Use either : or . to specify project got {}"),
            (
                "alt1.alt.dataset.table",
                None,
                r"Expect format of \(<project\.\|<project\:\)<dataset>\.<table>, got {}",
            ),
            (
                "alt1:alt2:alt.dataset.table",
                "var_x",
                "Format exception for var_x: Use either : or . to specify project got {}",
            ),
            (
                "alt1:alt2:alt:dataset.table",
                "var_x",
                "Format exception for var_x: Use either : or . to specify project got {}",
            ),
            (
                "alt1.alt.dataset.table",
                "var_x",
                r"Format exception for var_x: Expect format of "
                r"\(<project\.\|<project:\)<dataset>.<table>, got {}",
            ),
        ],
    )
    def test_split_tablename_invalid_syntax(self, table_input, var_name, exception_message):
        default_project_id = "project"
        with pytest.raises(ValueError, match=exception_message.format(table_input)):
            self.hook.split_tablename(table_input, default_project_id, var_name)


@pytest.mark.db_test
class TestTableOperations(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table.from_api_repr")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_table_view(self, mock_bq_client, mock_table):
        table_resource = {
            "view": {
                "query": "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*`",
                "useLegacySql": False,
            },
        }
        self.hook.create_table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            table_resource=table_resource,
            retry=DEFAULT_RETRY,
        )

        mock_bq_client.return_value.create_table.assert_called_once_with(
            table=mock_table.return_value,
            exists_ok=True,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table.from_api_repr")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_table_succeed(self, mock_bq_client, mock_table):
        body = {
            "tableReference": {
                "tableId": TABLE_ID,
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
            }
        }
        self.hook.create_table(
            project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID, table_resource=body
        )
        mock_bq_client.return_value.create_table.assert_called_once_with(
            table=mock_table.return_value,
            exists_ok=True,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table.from_api_repr")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    @pytest.mark.parametrize(
        "partitioning",
        [
            {"timePartitioning": {"field": "created", "type": "DAY"}},
            {"rangePartitioning": {"field": "grade", "range": {"start": 0, "end": 100, "interval": 20}}},
        ],
    )
    def test_create_table_with_extras_succeed(self, mock_bq_client, mock_table, partitioning):
        schema_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "created", "type": "DATE", "mode": "REQUIRED"},
            {"name": "grade", "type": "INTEGER", "mode": "REQUIRED"},
        ]
        cluster_fields = ["name"]
        body = {
            "tableReference": {
                "tableId": TABLE_ID,
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
            },
            "schema": {"fields": schema_fields},
            "clustering": {"fields": cluster_fields},
        }
        body.update(partitioning)
        self.hook.create_table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            table_resource=body,
        )

        mock_bq_client.return_value.create_table.assert_called_once_with(
            table=mock_table.return_value,
            exists_ok=True,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_tables_list(self, mock_client):
        table_list = [
            {
                "kind": "bigquery#table",
                "id": "your-project:your_dataset.table1",
                "tableReference": {
                    "projectId": "your-project",
                    "datasetId": "your_dataset",
                    "tableId": "table1",
                },
                "type": "TABLE",
                "creationTime": "1565781859261",
            },
            {
                "kind": "bigquery#table",
                "id": "your-project:your_dataset.table2",
                "tableReference": {
                    "projectId": "your-project",
                    "datasetId": "your_dataset",
                    "tableId": "table2",
                },
                "type": "TABLE",
                "creationTime": "1565782713480",
            },
        ]
        table_list_response = [Table.from_api_repr(t) for t in table_list]
        mock_client.return_value.list_tables.return_value = table_list_response

        dataset_reference = DatasetReference(PROJECT_ID, DATASET_ID)
        result = self.hook.get_dataset_tables(dataset_id=DATASET_ID, project_id=PROJECT_ID)

        mock_client.return_value.list_tables.assert_called_once_with(
            dataset=dataset_reference,
            max_results=None,
            retry=DEFAULT_RETRY,
        )
        for res, exp in zip(result, table_list):
            assert res["tableId"] == exp["tableReference"]["tableId"]

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table.from_api_repr")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_materialized_view(self, mock_bq_client, mock_table):
        query = """
            SELECT product, SUM(amount)
            FROM `test-project-id.test_dataset_id.test_table_prefix*`
            GROUP BY product
            """
        materialized_view = {
            "query": query,
            "enableRefresh": True,
            "refreshIntervalMs": 2000000,
        }
        body = {"tableReference": TABLE_REFERENCE_REPR, "materializedView": materialized_view}
        self.hook.create_table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            table_resource=body,
            retry=DEFAULT_RETRY,
        )

        mock_bq_client.return_value.create_table.assert_called_once_with(
            table=mock_table.return_value,
            exists_ok=True,
            retry=DEFAULT_RETRY,
            timeout=None,
        )


@pytest.mark.db_test
class TestBigQueryCursor(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job")
    def test_execute_with_parameters(self, mock_insert, _):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.execute("SELECT %(foo)s", {"foo": "bar"})
        conf = {
            "query": {
                "query": "SELECT 'bar'",
                "priority": "INTERACTIVE",
                "useLegacySql": True,
                "schemaUpdateOptions": [],
            }
        }
        mock_insert.assert_called_once_with(configuration=conf, project_id=PROJECT_ID, location=None)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job")
    def test_execute_many(self, mock_insert, _):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.executemany("SELECT %(foo)s", [{"foo": "bar"}, {"foo": "baz"}])
        assert mock_insert.call_count == 2
        mock_insert.assert_has_calls(
            [
                mock.call(
                    location=None,
                    configuration={
                        "query": {
                            "query": "SELECT 'bar'",
                            "priority": "INTERACTIVE",
                            "useLegacySql": True,
                            "schemaUpdateOptions": [],
                        }
                    },
                    project_id=PROJECT_ID,
                ),
                mock.call(
                    location=None,
                    configuration={
                        "query": {
                            "query": "SELECT 'baz'",
                            "priority": "INTERACTIVE",
                            "useLegacySql": True,
                            "schemaUpdateOptions": [],
                        }
                    },
                    project_id=PROJECT_ID,
                ),
            ]
        )

    def test_format_schema_for_description(self):
        test_query_result = {
            "schema": {
                "fields": [
                    {"name": "field_1", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "field_2", "type": "STRING"},
                    {"name": "field_3", "type": "STRING", "mode": "REPEATED"},
                ]
            },
        }
        description = _format_schema_for_description(test_query_result["schema"])
        assert description == [
            ("field_1", "STRING", None, None, None, None, True),
            ("field_2", "STRING", None, None, None, None, True),
            ("field_3", "STRING", None, None, None, None, False),
        ]

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job")
    def test_description(self, mock_insert, mock_build):
        mock_get_query_results = mock_build.return_value.jobs.return_value.getQueryResults
        mock_execute = mock_get_query_results.return_value.execute
        mock_execute.return_value = {
            "schema": {
                "fields": [
                    {"name": "ts", "type": "TIMESTAMP", "mode": "NULLABLE"},
                ]
            },
        }

        bq_cursor = self.hook.get_cursor()
        bq_cursor.execute("SELECT CURRENT_TIMESTAMP() as ts")
        assert bq_cursor.description == [("ts", "TIMESTAMP", None, None, None, None, True)]

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job")
    def test_description_no_schema(self, mock_insert, mock_build):
        mock_get_query_results = mock_build.return_value.jobs.return_value.getQueryResults
        mock_execute = mock_get_query_results.return_value.execute
        mock_execute.return_value = {}

        bq_cursor = self.hook.get_cursor()
        bq_cursor.execute("UPDATE airflow.test_table SET foo = 'bar'")
        assert bq_cursor.description == []

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_close(self, mock_get_client):
        bq_cursor = self.hook.get_cursor()
        result = bq_cursor.close()
        assert result is None

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_rowcount(self, mock_get_client):
        bq_cursor = self.hook.get_cursor()
        result = bq_cursor.rowcount
        assert result == -1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.next")
    def test_fetchone(self, mock_next, mock_get_client):
        bq_cursor = self.hook.get_cursor()
        result = bq_cursor.fetchone()
        mock_next.call_count == 1
        assert mock_next.return_value == result

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.fetchone", side_effect=[1, 2, 3, None]
    )
    def test_fetchall(self, mock_fetchone, mock_get_client):
        bq_cursor = self.hook.get_cursor()
        result = bq_cursor.fetchall()
        assert result == [1, 2, 3]

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.fetchone")
    def test_fetchmany(self, mock_fetchone, mock_get_client):
        side_effect_values = [1, 2, 3, None]
        bq_cursor = self.hook.get_cursor()
        mock_fetchone.side_effect = side_effect_values
        result = bq_cursor.fetchmany()
        assert result == [1]

        mock_fetchone.side_effect = side_effect_values
        result = bq_cursor.fetchmany(2)
        assert result == [1, 2]

        mock_fetchone.side_effect = side_effect_values
        result = bq_cursor.fetchmany(5)
        assert result == [1, 2, 3]

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_next_no_jobid(self, mock_get_client):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.job_id = None
        result = bq_cursor.next()
        assert result is None

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_next_buffer(self, mock_get_client):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.job_id = JOB_ID
        bq_cursor.buffer = [1, 2]
        result = bq_cursor.next()
        assert result == 1
        result = bq_cursor.next()
        assert result == 2
        bq_cursor.all_pages_loaded = True
        result = bq_cursor.next()
        assert result is None

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    def test_next(self, mock_build):
        mock_get_query_results = mock_build.return_value.jobs.return_value.getQueryResults
        mock_execute = mock_get_query_results.return_value.execute
        mock_execute.return_value = {
            "rows": [
                {"f": [{"v": "one"}, {"v": 1}]},
                {"f": [{"v": "two"}, {"v": 2}]},
            ],
            "pageToken": None,
            "schema": {
                "fields": [
                    {"name": "field_1", "type": "STRING"},
                    {"name": "field_2", "type": "INTEGER"},
                ]
            },
        }

        bq_cursor = self.hook.get_cursor()
        bq_cursor.job_id = JOB_ID
        bq_cursor.location = LOCATION

        result = bq_cursor.next()
        assert result == ["one", 1]

        result = bq_cursor.next()
        assert result == ["two", 2]

        mock_get_query_results.assert_called_once_with(
            jobId=JOB_ID, location=LOCATION, pageToken=None, projectId="bq-project"
        )
        mock_execute.assert_called_once_with(num_retries=bq_cursor.num_retries)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.flush_results")
    def test_next_no_rows(self, mock_flush_results, mock_build):
        mock_get_query_results = mock_build.return_value.jobs.return_value.getQueryResults
        mock_execute = mock_get_query_results.return_value.execute
        mock_execute.return_value = {}

        bq_cursor = self.hook.get_cursor()
        bq_cursor.job_id = JOB_ID

        result = bq_cursor.next()

        assert result is None
        mock_get_query_results.assert_called_once_with(
            jobId=JOB_ID, location=None, pageToken=None, projectId="bq-project"
        )
        mock_execute.assert_called_once_with(num_retries=bq_cursor.num_retries)
        assert mock_flush_results.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.flush_results")
    def test_flush_cursor_in_execute(self, _, mock_insert, mock_build):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.execute("SELECT %(foo)s", {"foo": "bar"})
        assert mock_insert.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_flush_cursor(self, mock_get_client):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.page_token = "456dcea9-fcbf-4f02-b570-83f5297c685e"
        bq_cursor.job_id = "c0a79ae4-0e72-4593-a0d0-7dbbf726f193"
        bq_cursor.all_pages_loaded = True
        bq_cursor.buffer = [("a", 100, 200), ("b", 200, 300)]
        bq_cursor.flush_results()
        assert bq_cursor.page_token is None
        assert bq_cursor.job_id is None
        assert not bq_cursor.all_pages_loaded
        assert bq_cursor.buffer == []

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_client")
    def test_arraysize(self, mock_get_client):
        bq_cursor = self.hook.get_cursor()
        assert bq_cursor.buffersize is None
        assert bq_cursor.arraysize == 1
        bq_cursor.set_arraysize(10)
        assert bq_cursor.buffersize == 10
        assert bq_cursor.arraysize == 10


@pytest.mark.db_test
class TestDatasetsOperations(_BigQueryBaseTestClass):
    def test_create_empty_dataset_no_dataset_id_err(self):
        with pytest.raises(ValueError, match=r"Please specify `datasetId`"):
            self.hook.create_empty_dataset(dataset_id=None, project_id=None)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_dataset_with_params(self, mock_client, mock_dataset):
        self.hook.create_empty_dataset(project_id=PROJECT_ID, dataset_id=DATASET_ID, location=LOCATION)
        expected_body = {
            "location": LOCATION,
            "datasetReference": {"datasetId": DATASET_ID, "projectId": PROJECT_ID},
        }

        api_repr = mock_dataset.from_api_repr
        api_repr.assert_called_once_with(expected_body)
        mock_client.return_value.create_dataset.assert_called_once_with(
            dataset=api_repr.return_value, exists_ok=True
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_dataset_with_object(self, mock_client, mock_dataset):
        dataset = {
            "location": "LOCATION",
            "datasetReference": {"datasetId": "DATASET_ID", "projectId": "PROJECT_ID"},
        }
        self.hook.create_empty_dataset(dataset_reference=dataset)

        api_repr = mock_dataset.from_api_repr
        api_repr.assert_called_once_with(dataset)
        mock_client.return_value.create_dataset.assert_called_once_with(
            dataset=api_repr.return_value, exists_ok=True
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_dataset_use_values_from_object(self, mock_client, mock_dataset):
        dataset = {
            "location": "LOCATION",
            "datasetReference": {"datasetId": "DATASET_ID", "projectId": "PROJECT_ID"},
        }
        self.hook.create_empty_dataset(
            dataset_reference=dataset,
            location="Unknown location",
            dataset_id="Fashionable Dataset",
            project_id="Amazing Project",
        )

        api_repr = mock_dataset.from_api_repr
        api_repr.assert_called_once_with(dataset)
        mock_client.return_value.create_dataset.assert_called_once_with(
            dataset=api_repr.return_value, exists_ok=True
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_dataset_without_datasetreference_key(self, mock_client, mock_dataset):
        dataset = {"defaultTableExpirationMs": str(1000 * 60 * 60 * 24 * 30)}
        dataset_copy = dataset.copy()
        self.hook.create_empty_dataset(
            dataset_reference=dataset, dataset_id="DATASET_ID", project_id="PROJECT_ID"
        )
        assert dataset["defaultTableExpirationMs"] == dataset_copy["defaultTableExpirationMs"]
        assert dataset["datasetReference"] == {"datasetId": "DATASET_ID", "projectId": "PROJECT_ID"}
        api_repr = mock_dataset.from_api_repr
        api_repr.assert_called_once_with(dataset)
        mock_client.return_value.create_dataset.assert_called_once_with(
            dataset=api_repr.return_value, exists_ok=True
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_dataset(self, mock_client):
        _expected_result = {
            "kind": "bigquery#dataset",
            "location": "US",
            "id": "your-project:dataset_2_test",
            "datasetReference": {"projectId": "your-project", "datasetId": "dataset_2_test"},
        }
        expected_result = Dataset.from_api_repr(_expected_result)
        mock_client.return_value.get_dataset.return_value = expected_result

        result = self.hook.get_dataset(dataset_id=DATASET_ID, project_id=PROJECT_ID)
        mock_client.return_value.get_dataset.assert_called_once_with(
            dataset_ref=DatasetReference(PROJECT_ID, DATASET_ID)
        )

        assert result == expected_result

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_datasets_list(self, mock_client):
        datasets = [
            {
                "kind": "bigquery#dataset",
                "location": "US",
                "id": "your-project:dataset_2_test",
                "datasetReference": {"projectId": "your-project", "datasetId": "dataset_2_test"},
            },
            {
                "kind": "bigquery#dataset",
                "location": "US",
                "id": "your-project:dataset_1_test",
                "datasetReference": {"projectId": "your-project", "datasetId": "dataset_1_test"},
            },
        ]
        return_value = [DatasetListItem(d) for d in datasets]
        mock_client.return_value.list_datasets.return_value = return_value

        result = self.hook.get_datasets_list(project_id=PROJECT_ID)

        mock_client.return_value.list_datasets.assert_called_once_with(
            project=PROJECT_ID,
            include_all=False,
            filter=None,
            max_results=None,
            page_token=None,
            retry=DEFAULT_RETRY,
        )
        for exp, res in zip(datasets, result):
            assert res.full_dataset_id == exp["id"]

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_datasets_list_returns_iterator(self, mock_client):
        client = mock.sentinel.client
        mock_iterator = page_iterator.HTTPIterator(
            client, mock.sentinel.api_request, "/foo", mock.sentinel.item_to_value
        )
        mock_client.return_value.list_datasets.return_value = mock_iterator
        actual = self.hook.get_datasets_list(project_id=PROJECT_ID, return_iterator=True)

        mock_client.return_value.list_datasets.assert_called_once_with(
            project=PROJECT_ID,
            include_all=False,
            filter=None,
            max_results=None,
            page_token=None,
            retry=DEFAULT_RETRY,
        )
        assert actual == mock_iterator

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_delete_dataset(self, mock_client):
        delete_contents = True
        self.hook.delete_dataset(
            project_id=PROJECT_ID, dataset_id=DATASET_ID, delete_contents=delete_contents
        )

        mock_client.return_value.delete_dataset.assert_called_once_with(
            dataset=DatasetReference(PROJECT_ID, DATASET_ID),
            delete_contents=delete_contents,
            retry=DEFAULT_RETRY,
            not_found_ok=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_update_dataset(self, mock_client, mock_dataset):
        dataset_resource = {
            "kind": "bigquery#dataset",
            "location": "US",
            "id": "your-project:dataset_2_test",
            "datasetReference": {"projectId": "your-project", "datasetId": "dataset_2_test"},
        }

        method = mock_client.return_value.update_dataset
        dataset = Dataset.from_api_repr(dataset_resource)
        mock_dataset.from_api_repr.return_value = dataset
        method.return_value = dataset

        result = self.hook.update_dataset(
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            dataset_resource=dataset_resource,
            fields=["location"],
        )

        mock_dataset.from_api_repr.assert_called_once_with(dataset_resource)
        method.assert_called_once_with(
            dataset=dataset,
            fields=["location"],
            retry=DEFAULT_RETRY,
        )
        assert result == dataset


@pytest.mark.db_test
class TestTimePartitioningInRunJob(_BigQueryBaseTestClass):
    def test_dollar_makes_partition(self):
        tp_out = _cleanse_time_partitioning("test.teast$20170101", {})
        expect = {"type": "DAY"}
        assert tp_out == expect

    def test_extra_time_partitioning_options(self):
        tp_out = _cleanse_time_partitioning(
            "test.teast", {"type": "DAY", "field": "test_field", "expirationMs": 1000}
        )

        expect = {"type": "DAY", "field": "test_field", "expirationMs": 1000}
        assert tp_out == expect


@pytest.mark.db_test
class TestBigQueryHookLegacySql(_BigQueryBaseTestClass):
    """Ensure `use_legacy_sql` param in `BigQueryHook` propagates properly."""

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor._get_query_result")
    def test_hook_uses_legacy_sql_by_default(self, mock_get_query_result, mock_insert, _):
        mock_get_query_result.return_value = {}
        self.hook.get_first("query")
        _, kwargs = mock_insert.call_args
        assert kwargs["configuration"]["query"]["useLegacySql"] is True

    @mock.patch(
        "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id",
        return_value=(CREDENTIALS, PROJECT_ID),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.insert_job")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor._get_query_result")
    def test_legacy_sql_override_propagates_properly(
        self, mock_get_query_result, mock_insert, mock_build, mock_get_creds_and_proj_id
    ):
        mock_get_query_result.return_value = {}
        bq_hook = BigQueryHook(use_legacy_sql=False)
        bq_hook.get_first("query")
        _, kwargs = mock_insert.call_args
        assert kwargs["configuration"]["query"]["useLegacySql"] is False


@pytest.mark.db_test
class TestBigQueryWithKMS(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_update_table(self, mock_client, mock_table):
        description_patched = "Test description."
        expiration_time_patched = 2524608000000
        friendly_name_patched = "Test friendly name."
        labels_patched = {"label1": "test1", "label2": "test2"}
        schema_patched = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "balance", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "new_field", "type": "STRING", "mode": "NULLABLE"},
        ]
        time_partitioning_patched = {"expirationMs": 10000000}
        require_partition_filter_patched = True
        view_patched = {
            "query": "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 500",
            "useLegacySql": False,
        }

        body = {
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_ID,
                "tableId": TABLE_ID,
            },
            "description": description_patched,
            "expirationTime": expiration_time_patched,
            "friendlyName": friendly_name_patched,
            "labels": labels_patched,
            "schema": {"fields": schema_patched},
            "timePartitioning": time_partitioning_patched,
            "view": view_patched,
            "requirePartitionFilter": require_partition_filter_patched,
        }

        fields = list(body.keys())

        self.hook.update_table(
            table_resource=body,
            fields=fields,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            project_id=PROJECT_ID,
        )

        mock_table.from_api_repr.assert_called_once_with(body)

        mock_client.return_value.update_table.assert_called_once_with(
            table=mock_table.from_api_repr.return_value, fields=fields
        )


class TestBigQueryAsyncHookMethods:
    def setup_method(self) -> None:
        class MockedBigQueryAsyncHook(BigQueryAsyncHook):
            def get_credentials_and_project_id(self):
                return CREDENTIALS, PROJECT_ID

        self.hook = MockedBigQueryAsyncHook()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("google.auth.default")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.ClientSession")
    async def test_get_job_instance(self, mock_session, mock_auth_default):
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_credentials.token = "ACCESS_TOKEN"
        mock_auth_default.return_value = (mock_credentials, PROJECT_ID)
        hook = BigQueryAsyncHook()
        result = await hook.get_job_instance(project_id=PROJECT_ID, job_id=JOB_ID, session=mock_session)
        assert isinstance(result, Job)

    @pytest.mark.parametrize(
        "job_state, error_result, expected",
        [
            ("DONE", None, {"status": "success", "message": "Job completed"}),
            ("DONE", {"message": "Timeout"}, {"status": "error", "message": "Timeout"}),
            ("RUNNING", None, {"status": "running", "message": "Job running"}),
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook._get_job")
    async def test_get_job_status(self, mock_get_job, job_state, error_result, expected):
        hook = BigQueryAsyncHook()
        mock_get_job.return_value = mock.MagicMock(state=job_state, error_result=error_result)
        resp = await hook.get_job_status(job_id=JOB_ID, project_id=PROJECT_ID)
        assert resp == expected

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_instance")
    async def test_get_job_output_assert_once_with(self, mock_job_instance):
        hook = BigQueryAsyncHook()
        mock_job_client = AsyncMock(Job)
        mock_job_instance.return_value = mock_job_client
        response = "success"
        mock_job_instance.return_value.get_query_results.return_value = response
        resp = await hook.get_job_output(job_id=JOB_ID, project_id=PROJECT_ID)
        assert resp == response

    @pytest.mark.asyncio
    @pytest.mark.db_test
    @mock.patch("google.auth.default")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Job")
    async def test_cancel_job_success(self, mock_job, mock_auth_default):
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_credentials.token = "ACCESS_TOKEN"
        mock_auth_default.return_value = (mock_credentials, PROJECT_ID)
        job_id = "test_job_id"
        project_id = "test_project"
        location = "US"

        mock_job_instance = AsyncMock()
        mock_job_instance.cancel.return_value = None
        mock_job.return_value = mock_job_instance

        await self.hook.cancel_job(job_id=job_id, project_id=project_id, location=location)

        mock_job_instance.cancel.assert_called_once()

    @pytest.mark.asyncio
    @pytest.mark.db_test
    @mock.patch("google.auth.default")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Job")
    async def test_cancel_job_failure(self, mock_job, mock_auth_default):
        """
        Test that BigQueryAsyncHook handles exceptions during job cancellation correctly.
        """
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_credentials.token = "ACCESS_TOKEN"
        mock_auth_default.return_value = (mock_credentials, PROJECT_ID)

        mock_job_instance = AsyncMock()
        mock_job_instance.cancel.side_effect = Exception("Cancellation failed")
        mock_job.return_value = mock_job_instance

        hook = BigQueryAsyncHook()

        job_id = "test_job_id"
        project_id = "test_project"
        location = "US"

        with pytest.raises(Exception) as excinfo:
            await hook.cancel_job(job_id=job_id, project_id=project_id, location=location)

        assert "Cancellation failed" in str(excinfo.value), "Exception message not passed correctly"

        mock_job_instance.cancel.assert_called_once()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.ClientSession")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_instance")
    async def test_create_job_for_partition_get_with_table(self, mock_job_instance, mock_client_session):
        hook = BigQueryAsyncHook()
        mock_job_client = AsyncMock(Job)
        mock_job_instance.return_value = mock_job_client
        mock_session = AsyncMock()
        mock_client_session.return_value.__aenter__.return_value = mock_session
        expected_query_request = {
            "query": "SELECT partition_id "
            f"FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.PARTITIONS`"
            f" WHERE table_name='{TABLE_ID}'",
            "useLegacySql": False,
        }
        await hook.create_job_for_partition_get(
            dataset_id=DATASET_ID, table_id=TABLE_ID, project_id=PROJECT_ID
        )
        mock_job_client.query.assert_called_once_with(expected_query_request, mock_session)

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.ClientSession")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_instance")
    async def test_create_job_for_partition_get(self, mock_job_instance, mock_client_session):
        hook = BigQueryAsyncHook()
        mock_job_client = AsyncMock(Job)
        mock_job_instance.return_value = mock_job_client
        mock_session = AsyncMock()
        mock_client_session.return_value.__aenter__.return_value = mock_session
        expected_query_request = {
            "query": f"SELECT partition_id FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.PARTITIONS`",
            "useLegacySql": False,
        }
        await hook.create_job_for_partition_get(dataset_id=DATASET_ID, project_id=PROJECT_ID)
        mock_job_client.query.assert_called_once_with(expected_query_request, mock_session)

    def test_interval_check_for_airflow_exception(self):
        """
        Assert that check return AirflowException
        """
        hook = BigQueryAsyncHook()

        row1, row2, metrics_thresholds, ignore_zero, ratio_formula = (
            None,
            "0",
            {"COUNT(*)": 1.5},
            True,
            "max_over_min",
        )
        with pytest.raises(AirflowException):
            hook.interval_check(row1, row2, metrics_thresholds, ignore_zero, ratio_formula)

        row1, row2, metrics_thresholds, ignore_zero, ratio_formula = (
            "0",
            None,
            {"COUNT(*)": 1.5},
            True,
            "max_over_min",
        )
        with pytest.raises(AirflowException):
            hook.interval_check(row1, row2, metrics_thresholds, ignore_zero, ratio_formula)

        row1, row2, metrics_thresholds, ignore_zero, ratio_formula = (
            "1",
            "1",
            {"COUNT(*)": 0},
            True,
            "max_over_min",
        )
        with pytest.raises(AirflowException):
            hook.interval_check(row1, row2, metrics_thresholds, ignore_zero, ratio_formula)

    def test_interval_check_for_success(self):
        """
        Assert that check return None
        """
        hook = BigQueryAsyncHook()

        row1, row2, metrics_thresholds, ignore_zero, ratio_formula = (
            "0",
            "0",
            {"COUNT(*)": 1.5},
            True,
            "max_over_min",
        )
        response = hook.interval_check(row1, row2, metrics_thresholds, ignore_zero, ratio_formula)
        assert response is None

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryAsyncHook.get_job_instance")
    async def test_get_job_output(self, mock_job_instance):
        """
        Tests to check if a particular object in Google Cloud Storage
        is found or not
        """
        response = {
            "kind": "bigquery#tableDataList",
            "etag": "test_etag",
            "schema": {"fields": [{"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"}]},
            "jobReference": {
                "projectId": "test_astronomer-airflow-providers",
                "jobId": "test_jobid",
                "location": "US",
            },
            "totalRows": "10",
            "rows": [{"f": [{"v": "42"}, {"v": "monthy python"}]}, {"f": [{"v": "42"}, {"v": "fishy fish"}]}],
            "totalBytesProcessed": "0",
            "jobComplete": True,
            "cacheHit": False,
        }
        hook = BigQueryAsyncHook()
        mock_job_client = AsyncMock(Job)
        mock_job_instance.return_value = mock_job_client
        mock_job_client.get_query_results.return_value = response
        resp = await hook.get_job_output(job_id=JOB_ID, project_id=PROJECT_ID)
        assert resp == response

    @pytest.mark.parametrize(
        "records,pass_value,tolerance", [(["str"], "str", None), ([2], 2, None), ([0], 2, 1), ([4], 2, 1)]
    )
    def test_value_check_success(self, records, pass_value, tolerance):
        """
        Assert that value_check method execution succeed
        """
        hook = BigQueryAsyncHook()
        query = "SELECT COUNT(*) from Any"
        response = hook.value_check(query, pass_value, records, tolerance)
        assert response is None

    @pytest.mark.parametrize(
        "records,pass_value,tolerance",
        [([], "", None), (["str"], "str1", None), ([2], 21, None), ([5], 2, 1), (["str"], 2, None)],
    )
    def test_value_check_fail(self, records, pass_value, tolerance):
        """Assert that check raise AirflowException"""
        hook = BigQueryAsyncHook()
        query = "SELECT COUNT(*) from Any"

        with pytest.raises(AirflowException) as ex:
            hook.value_check(query, pass_value, records, tolerance)
        assert isinstance(ex.value, AirflowException)

    @pytest.mark.parametrize(
        "records,pass_value,tolerance, expected",
        [
            ([2.0], 2.0, None, [True]),
            ([2.0], 2.1, None, [False]),
            ([2.0], 2.0, 0.5, [True]),
            ([1.0], 2.0, 0.5, [True]),
            ([3.0], 2.0, 0.5, [True]),
            ([0.9], 2.0, 0.5, [False]),
            ([3.1], 2.0, 0.5, [False]),
        ],
    )
    def test_get_numeric_matches(self, records, pass_value, tolerance, expected):
        """Assert the if response list have all element match with pass_value with tolerance"""

        assert BigQueryAsyncHook._get_numeric_matches(records, pass_value, tolerance) == expected

    @pytest.mark.parametrize("test_input,expected", [(5.0, 5.0), (5, 5.0), ("5", 5), ("str", "str")])
    def test_convert_to_float_if_possible(self, test_input, expected):
        """
        Assert that type casting succeed for the possible value
        Otherwise return the same value
        """

        assert BigQueryAsyncHook._convert_to_float_if_possible(test_input) == expected

    @pytest.mark.db_test
    @pytest.mark.asyncio
    @mock.patch("google.auth.default")
    @mock.patch("aiohttp.client.ClientSession")
    async def test_get_table_client(self, mock_session, mock_auth_default):
        """Test get_table_client async function and check whether the return value is a
        Table instance object"""
        mock_credentials = mock.MagicMock(spec=google.auth.compute_engine.Credentials)
        mock_auth_default.return_value = (mock_credentials, PROJECT_ID)
        hook = BigQueryTableAsyncHook()
        result = await hook.get_table_client(
            dataset=DATASET_ID, project_id=PROJECT_ID, table_id=TABLE_ID, session=mock_session
        )
        assert isinstance(result, Table_async)

    def test_get_records_return_type(self):
        query_result = {
            "kind": "bigquery#getQueryResultsResponse",
            "etag": "test_etag",
            "schema": {
                "fields": [
                    {"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "f1_", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "f2_", "type": "STRING", "mode": "NULLABLE"},
                ]
            },
            "jobReference": {
                "projectId": "test_airflow-providers",
                "jobId": "test_jobid",
                "location": "US",
            },
            "totalRows": "1",
            "rows": [{"f": [{"v": "22"}, {"v": "3.14"}, {"v": "PI"}]}],
            "totalBytesProcessed": "0",
            "jobComplete": True,
            "cacheHit": False,
        }
        hook = BigQueryAsyncHook()
        result = hook.get_records(query_result)
        assert isinstance(result[0][0], int)
        assert isinstance(result[0][1], float)
        assert isinstance(result[0][2], str)

    def test_get_records_as_dict(self):
        query_result = {
            "kind": "bigquery#getQueryResultsResponse",
            "etag": "test_etag",
            "schema": {
                "fields": [
                    {"name": "f0_", "type": "INTEGER", "mode": "NULLABLE"},
                    {"name": "f1_", "type": "FLOAT", "mode": "NULLABLE"},
                    {"name": "f2_", "type": "STRING", "mode": "NULLABLE"},
                ]
            },
            "jobReference": {
                "projectId": "test_airflow-providers",
                "jobId": "test_jobid",
                "location": "US",
            },
            "totalRows": "1",
            "rows": [{"f": [{"v": "22"}, {"v": "3.14"}, {"v": "PI"}]}],
            "totalBytesProcessed": "0",
            "jobComplete": True,
            "cacheHit": False,
        }
        hook = BigQueryAsyncHook()
        result = hook.get_records(query_result, as_dict=True)
        assert result == [{"f0_": 22, "f1_": 3.14, "f2_": "PI"}]


@pytest.mark.db_test
class TestHookLevelLineage(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_update_table_collects_assets(self, mock_bq_client, hook_lineage_collector):
        mock_bq_client.return_value.update_table.return_value = Table(TABLE_REFERENCE)

        self.hook.update_table(table_resource={"tableReference": TABLE_REFERENCE_REPR})

        assert len(hook_lineage_collector.collected_assets.inputs) == 0
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/{TABLE_ID}"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_run_table_upsert_collects_assets_when_creating(self, mock_bq_client, hook_lineage_collector):
        mock_bq_client.return_value.create_table.return_value = Table(TABLE_REFERENCE)

        self.hook.run_table_upsert(
            table_resource={"tableReference": TABLE_REFERENCE_REPR}, dataset_id=DATASET_ID
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 0
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/{TABLE_ID}"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_run_table_upsert_collects_assets_when_updating(self, mock_bq_client, hook_lineage_collector):
        mock_bq_client.return_value.update_table.return_value = Table(TABLE_REFERENCE)
        mock_bq_client.return_value.list_tables.return_value = [Table(TABLE_REFERENCE)]

        self.hook.run_table_upsert(
            table_resource={"tableReference": TABLE_REFERENCE_REPR}, dataset_id=DATASET_ID
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 0
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/{TABLE_ID}"
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_update_table_schema_collects_assets(self, mock_bq_client, hook_lineage_collector):
        mock_bq_client.return_value.update_table.return_value = Table(TABLE_REFERENCE)
        table = Table.from_api_repr(
            {
                "tableReference": TABLE_REFERENCE_REPR,
                "schema": {
                    "fields": [
                        {"name": "field1", "type": "STRING", "description": "field1 description"},
                        {"name": "field2", "type": "INTEGER"},
                    ]
                },
            }
        )
        mock_bq_client.return_value.get_table.return_value = table

        self.hook.update_table_schema(
            dataset_id="d",
            table_id="t",
            schema_fields_updates=[{"name": "field1", "type": "STRING", "description": "other description"}],
            include_policy_tags=False,
        )

        assert len(hook_lineage_collector.collected_assets.inputs) == 0
        assert len(hook_lineage_collector.collected_assets.outputs) == 1
        assert hook_lineage_collector.collected_assets.outputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/{TABLE_ID}"
        )

    @pytest.mark.parametrize(
        ("table_id", "project_id"),
        (
            (f"{DATASET_ID}.{TABLE_ID}", None),
            (f"{DATASET_ID}.{TABLE_ID}", PROJECT_ID),
            (f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", None),
            (f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", "some_other_project"),
        ),
    )
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_delete_table_collects_assets(self, mock_bq_client, table_id, project_id, hook_lineage_collector):
        self.hook.delete_table(table_id=table_id, project_id=project_id)

        assert len(hook_lineage_collector.collected_assets.inputs) == 1
        assert len(hook_lineage_collector.collected_assets.outputs) == 0
        assert hook_lineage_collector.collected_assets.inputs[0].asset == Asset(
            uri=f"bigquery://{PROJECT_ID}/{DATASET_ID}/{TABLE_ID}"
        )
