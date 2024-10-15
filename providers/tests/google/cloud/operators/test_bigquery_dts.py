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

from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.bigquery_datatransfer_v1 import StartManualTransferRunsResponse, TransferConfig, TransferRun
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS

from airflow.providers.google.cloud.operators.bigquery_dts import (
    BigQueryCreateDataTransferOperator,
    BigQueryDataTransferServiceStartTransferRunsOperator,
    BigQueryDeleteDataTransferConfigOperator,
)

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
