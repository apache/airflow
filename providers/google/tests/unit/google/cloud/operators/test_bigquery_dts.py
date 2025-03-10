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

from unittest import mock

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.bigquery_datatransfer_v1 import StartManualTransferRunsResponse, TransferConfig, TransferRun

from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator,
    BigQueryDataTransferServiceStartTransferRunsOperator,
    BigQueryDeleteDataTransferConfigOperator,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

PROJECT_ID = "id"

TRANSFER_CONFIG = {
    "data_source_id": "google_cloud_storage",
    "destination_dataset_id": "example_dataset",
    "params": {},
    "display_name": "example-transfer",
    "disabled": False,
    "data_refresh_window_days": 0,
    "schedule": "first sunday of quarter 00:00",
}

TRANSFER_CONFIG_ID = "id1234"

TRANSFER_CONFIG_NAME = "projects/123abc/locations/321cba/transferConfig/1a2b3c"
RUN_NAME = "projects/123abc/locations/321cba/transferConfig/1a2b3c/runs/123"
transfer_config = TransferConfig(
    name=TRANSFER_CONFIG_NAME, params={"secret_access_key": "AIRFLOW_KEY", "access_key_id": "AIRFLOW_KEY_ID"}
)


class TestBigQueryCreateDataTransferOperator:
    @mock.patch(
        "airflow.providers.google.cloud.operators.bigquery_dts.BiqQueryDataTransferServiceHook",
        **{"return_value.create_transfer_config.return_value": transfer_config},
    )
    def test_execute(self, mock_hook):
        op = BigQueryCreateDataTransferOperator(
            transfer_config=TRANSFER_CONFIG, project_id=PROJECT_ID, task_id="id"
        )
        ti = mock.MagicMock()

        return_value = op.execute({"ti": ti})

        mock_hook.return_value.create_transfer_config.assert_called_once_with(
            authorization_code=None,
            metadata=(),
            transfer_config=TRANSFER_CONFIG,
            project_id=PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )
        if AIRFLOW_V_3_0_PLUS:
            ti.xcom_push.assert_called_with(key="transfer_config_id", value="1a2b3c")
        else:
            ti.xcom_push.assert_called_with(key="transfer_config_id", value="1a2b3c", execution_date=None)

        assert "secret_access_key" not in return_value.get("params", {})
        assert "access_key_id" not in return_value.get("params", {})


class TestBigQueryDeleteDataTransferConfigOperator:
    @mock.patch("airflow.providers.google.cloud.operators.bigquery_dts.BiqQueryDataTransferServiceHook")
    def test_execute(self, mock_hook):
        op = BigQueryDeleteDataTransferConfigOperator(
            transfer_config_id=TRANSFER_CONFIG_ID, task_id="id", project_id=PROJECT_ID
        )
        op.execute(None)
        mock_hook.return_value.delete_transfer_config.assert_called_once_with(
            metadata=(),
            transfer_config_id=TRANSFER_CONFIG_ID,
            project_id=PROJECT_ID,
            retry=DEFAULT,
            timeout=None,
        )


class TestBigQueryDataTransferServiceStartTransferRunsOperator:
    OPERATOR_MODULE_PATH = "airflow.providers.google.cloud.operators.bigquery_dts"

    @mock.patch(
        f"{OPERATOR_MODULE_PATH}.BigQueryDataTransferServiceStartTransferRunsOperator"
        f"._wait_for_transfer_to_be_done",
        mock.MagicMock(),
    )
    @mock.patch(
        f"{OPERATOR_MODULE_PATH}.BiqQueryDataTransferServiceHook",
        **{
            "return_value.start_manual_transfer_runs.return_value": StartManualTransferRunsResponse(
                runs=[TransferRun(name=RUN_NAME)]
            ),
        },
    )
    def test_execute(self, mock_hook):
        op = BigQueryDataTransferServiceStartTransferRunsOperator(
            transfer_config_id=TRANSFER_CONFIG_ID, task_id="id", project_id=PROJECT_ID
        )
        ti = mock.MagicMock()

        op.execute({"ti": ti})

        mock_hook.return_value.start_manual_transfer_runs.assert_called_once_with(
            transfer_config_id=TRANSFER_CONFIG_ID,
            project_id=PROJECT_ID,
            requested_time_range=None,
            requested_run_time=None,
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        if AIRFLOW_V_3_0_PLUS:
            ti.xcom_push.assert_called_with(key="run_id", value="123")
        else:
            ti.xcom_push.assert_called_with(key="run_id", value="123", execution_date=None)

    @mock.patch(
        f"{OPERATOR_MODULE_PATH}.BiqQueryDataTransferServiceHook",
        **{
            "return_value.start_manual_transfer_runs.return_value": StartManualTransferRunsResponse(
                runs=[TransferRun(name=RUN_NAME)]
            ),
        },
    )
    @mock.patch(f"{OPERATOR_MODULE_PATH}.BigQueryDataTransferServiceStartTransferRunsOperator.defer")
    def test_defer_mode(self, _, defer_method):
        op = BigQueryDataTransferServiceStartTransferRunsOperator(
            transfer_config_id=TRANSFER_CONFIG_ID,
            task_id="id",
            project_id=PROJECT_ID,
            deferrable=True,
        )
        ti = mock.MagicMock()

        op.execute({"ti": ti})

        defer_method.assert_called_once()

    @pytest.mark.parametrize(
        ("data_source_id", "params", "expected_input_namespace", "expected_input_name"),
        (
            (
                "google_cloud_storage",
                {
                    "data_path_template": "gs://bucket/path/to/file.txt",
                    "destination_table_name_template": "bq_table",
                },
                "gs://bucket",
                "path/to/file.txt",
            ),
            (
                "amazon_s3",
                {
                    "data_path": "s3://bucket/path/to/file.txt",
                    "destination_table_name_template": "bq_table",
                },
                "s3://bucket",
                "path/to/file.txt",
            ),
            (
                "azure_blob_storage",
                {
                    "storage_account": "account_id",
                    "container": "container_id",
                    "data_path": "/path/to/file.txt",
                    "destination_table_name_template": "bq_table",
                },
                "abfss://container_id@account_id.dfs.core.windows.net",
                "path/to/file.txt",
            ),
        ),
    )
    @mock.patch(
        f"{OPERATOR_MODULE_PATH}.BigQueryDataTransferServiceStartTransferRunsOperator"
        f"._wait_for_transfer_to_be_done"
    )
    @mock.patch(f"{OPERATOR_MODULE_PATH}.BiqQueryDataTransferServiceHook")
    def test_get_openlineage_facets_on_complete_with_blob_storage_sources(
        self, mock_hook, mock_wait, data_source_id, params, expected_input_namespace, expected_input_name
    ):
        mock_hook.return_value.start_manual_transfer_runs.return_value = StartManualTransferRunsResponse(
            runs=[TransferRun(name=RUN_NAME)]
        )
        mock_wait.return_value = {
            "error_status": {"code": 0, "message": "", "details": []},
            "data_source_id": data_source_id,
            "destination_dataset_id": "bq_dataset",
            "params": params,
        }

        op = BigQueryDataTransferServiceStartTransferRunsOperator(
            transfer_config_id=TRANSFER_CONFIG_ID, task_id="id", project_id=PROJECT_ID
        )
        op.execute({"ti": mock.MagicMock()})
        result = op.get_openlineage_facets_on_complete(None)
        assert not result.run_facets
        assert not result.job_facets
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1
        assert result.inputs[0].namespace == expected_input_namespace
        assert result.inputs[0].name == expected_input_name
        assert result.outputs[0].namespace == "bigquery"
        assert result.outputs[0].name == f"{PROJECT_ID}.bq_dataset.bq_table"

    @pytest.mark.parametrize(
        ("data_source_id", "params", "expected_input_namespace", "expected_input_names"),
        (
            (
                "postgresql",
                {
                    "connector.endpoint.host": "127.0.0.1",
                    "connector.endpoint.port": 5432,
                    "assets": [
                        "db1/sch1/tb1",
                        "db2/sch2/tb2",
                    ],
                },
                "postgres://127.0.0.1:5432",
                ["db1.sch1.tb1", "db2.sch2.tb2"],
            ),
            (
                "oracle",
                {
                    "connector.endpoint.host": "127.0.0.1",
                    "connector.endpoint.port": 1234,
                    "assets": [
                        "db1/sch1/tb1",
                        "db2/sch2/tb2",
                    ],
                },
                "oracle://127.0.0.1:1234",
                ["db1.sch1.tb1", "db2.sch2.tb2"],
            ),
            (
                "mysql",
                {
                    "connector.endpoint.host": "127.0.0.1",
                    "connector.endpoint.port": 3306,
                    "assets": [
                        "db1/tb1",
                        "db2/tb2",
                    ],
                },
                "mysql://127.0.0.1:3306",
                ["db1.tb1", "db2.tb2"],
            ),
        ),
    )
    @mock.patch(
        f"{OPERATOR_MODULE_PATH}.BigQueryDataTransferServiceStartTransferRunsOperator"
        f"._wait_for_transfer_to_be_done"
    )
    @mock.patch(f"{OPERATOR_MODULE_PATH}.BiqQueryDataTransferServiceHook")
    def test_get_openlineage_facets_on_complete_with_sql_sources(
        self, mock_hook, mock_wait, data_source_id, params, expected_input_namespace, expected_input_names
    ):
        mock_hook.return_value.start_manual_transfer_runs.return_value = StartManualTransferRunsResponse(
            runs=[TransferRun(name=RUN_NAME)]
        )
        mock_wait.return_value = {
            "error_status": {"code": 0, "message": "", "details": []},
            "data_source_id": data_source_id,
            "destination_dataset_id": "bq_dataset",
            "params": params,
        }

        op = BigQueryDataTransferServiceStartTransferRunsOperator(
            transfer_config_id=TRANSFER_CONFIG_ID, task_id="id", project_id=PROJECT_ID
        )
        op.execute({"ti": mock.MagicMock()})
        result = op.get_openlineage_facets_on_complete(None)
        assert not result.run_facets
        assert not result.job_facets
        assert len(result.inputs) == 2
        assert len(result.outputs) == 2
        assert result.inputs[0].namespace == expected_input_namespace
        assert result.inputs[0].name == expected_input_names[0]
        assert result.inputs[1].namespace == expected_input_namespace
        assert result.inputs[1].name == expected_input_names[1]
        assert result.outputs[0].namespace == "bigquery"
        assert result.outputs[0].name == f"{PROJECT_ID}.bq_dataset.{expected_input_names[0].split('.')[-1]}"
        assert result.outputs[1].namespace == "bigquery"
        assert result.outputs[1].name == f"{PROJECT_ID}.bq_dataset.{expected_input_names[1].split('.')[-1]}"

    @mock.patch(
        f"{OPERATOR_MODULE_PATH}.BigQueryDataTransferServiceStartTransferRunsOperator"
        f"._wait_for_transfer_to_be_done"
    )
    @mock.patch(f"{OPERATOR_MODULE_PATH}.BiqQueryDataTransferServiceHook")
    def test_get_openlineage_facets_on_complete_with_scheduled_query(self, mock_hook, mock_wait):
        mock_hook.return_value.start_manual_transfer_runs.return_value = StartManualTransferRunsResponse(
            runs=[TransferRun(name=RUN_NAME)]
        )
        mock_wait.return_value = {
            "error_status": {"code": 0, "message": "", "details": []},
            "data_source_id": "scheduled_query",
            "destination_dataset_id": "bq_dataset",
            "params": {"query": "SELECT a,b,c from x.y.z;", "destination_table_name_template": "bq_table"},
        }

        op = BigQueryDataTransferServiceStartTransferRunsOperator(
            transfer_config_id=TRANSFER_CONFIG_ID, task_id="id", project_id=PROJECT_ID
        )
        op.execute({"ti": mock.MagicMock()})
        result = op.get_openlineage_facets_on_complete(None)
        assert len(result.job_facets) == 1
        assert result.job_facets["sql"].query == "SELECT a,b,c from x.y.z"
        assert not result.run_facets
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1
        assert result.inputs[0].namespace == "bigquery"
        assert result.inputs[0].name == "x.y.z"
        assert result.outputs[0].namespace == "bigquery"
        assert result.outputs[0].name == f"{PROJECT_ID}.bq_dataset.bq_table"

    @mock.patch(
        f"{OPERATOR_MODULE_PATH}.BigQueryDataTransferServiceStartTransferRunsOperator"
        f"._wait_for_transfer_to_be_done"
    )
    @mock.patch(f"{OPERATOR_MODULE_PATH}.BiqQueryDataTransferServiceHook")
    def test_get_openlineage_facets_on_complete_with_error(self, mock_hook, mock_wait):
        mock_hook.return_value.start_manual_transfer_runs.return_value = StartManualTransferRunsResponse(
            runs=[TransferRun(name=RUN_NAME)]
        )
        mock_wait.return_value = {
            "error_status": {
                "code": 1,
                "message": "Sample message error.",
                "details": [{"@type": "123", "field1": "test1"}, {"@type": "456", "field2": "test2"}],
            },
            "data_source_id": "google_cloud_storage",
            "destination_dataset_id": "bq_dataset",
            "params": {
                "data_path_template": "gs://bucket/path/to/file.txt",
                "destination_table_name_template": "bq_table",
            },
        }

        op = BigQueryDataTransferServiceStartTransferRunsOperator(
            transfer_config_id=TRANSFER_CONFIG_ID, task_id="id", project_id=PROJECT_ID
        )
        op.execute({"ti": mock.MagicMock()})
        result = op.get_openlineage_facets_on_complete(None)
        assert not result.job_facets
        assert len(result.run_facets) == 1
        assert result.run_facets["errorMessage"].message == "Sample message error."
        assert (
            result.run_facets["errorMessage"].stackTrace
            == "[{'@type': '123', 'field1': 'test1'}, {'@type': '456', 'field2': 'test2'}]"
        )
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1
        assert result.inputs[0].namespace == "gs://bucket"
        assert result.inputs[0].name == "path/to/file.txt"
        assert result.outputs[0].namespace == "bigquery"
        assert result.outputs[0].name == f"{PROJECT_ID}.bq_dataset.bq_table"

    @mock.patch(f"{OPERATOR_MODULE_PATH}.BiqQueryDataTransferServiceHook")
    @mock.patch(f"{OPERATOR_MODULE_PATH}.BigQueryDataTransferServiceStartTransferRunsOperator.defer")
    def test_get_openlineage_facets_on_complete_deferred(self, mock_defer, mock_hook):
        mock_hook.return_value.start_manual_transfer_runs.return_value = StartManualTransferRunsResponse(
            runs=[TransferRun(name=RUN_NAME)]
        )
        mock_hook.return_value.get_transfer_run.return_value = TransferRun(
            {
                "error_status": {"code": 0, "message": "", "details": []},
                "data_source_id": "google_cloud_storage",
                "destination_dataset_id": "bq_dataset",
                "params": {
                    "data_path_template": "gs://bucket/path/to/file.txt",
                    "destination_table_name_template": "bq_table",
                },
            }
        )

        op = BigQueryDataTransferServiceStartTransferRunsOperator(
            transfer_config_id=TRANSFER_CONFIG_ID, task_id="id", project_id=PROJECT_ID, deferrable=True
        )
        op.execute({"ti": mock.MagicMock()})
        # `defer` is mocked so it will not call the `execute_completed`, so we do it manually.
        op.execute_completed(
            mock.MagicMock(), {"status": "done", "run_id": 123, "config_id": 321, "message": "msg"}
        )
        result = op.get_openlineage_facets_on_complete(None)
        assert not result.job_facets
        assert not result.run_facets
        assert len(result.inputs) == 1
        assert len(result.outputs) == 1
        assert result.inputs[0].namespace == "gs://bucket"
        assert result.inputs[0].name == "path/to/file.txt"
        assert result.outputs[0].namespace == "bigquery"
        assert result.outputs[0].name == f"{PROJECT_ID}.bq_dataset.bq_table"
