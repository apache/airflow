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
from unittest.mock import MagicMock as MM

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.bigquery_datatransfer_v1 import TransferState

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.sensors.bigquery_dts import BigQueryDataTransferServiceTransferRunSensor

TRANSFER_CONFIG_ID = "config_id"
RUN_ID = "run_id"
PROJECT_ID = "project_id"
LOCATION = "europe"
GCP_CONN_ID = "google_cloud_default"


class TestBigQueryDataTransferServiceTransferRunSensor:
    @mock.patch(
        "airflow.providers.google.cloud.sensors.bigquery_dts.BiqQueryDataTransferServiceHook",
        return_value=MM(get_transfer_run=MM(return_value=MM(state=TransferState.FAILED))),
    )
    def test_poke_returns_false(self, mock_hook):
        op = BigQueryDataTransferServiceTransferRunSensor(
            transfer_config_id=TRANSFER_CONFIG_ID,
            run_id=RUN_ID,
            task_id="id",
            project_id=PROJECT_ID,
            expected_statuses={"SUCCEEDED"},
        )

        with pytest.raises(AirflowException, match="Transfer"):
            op.poke({})

        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=None, location=None)
        mock_hook.return_value.get_transfer_run.assert_called_once_with(
            transfer_config_id=TRANSFER_CONFIG_ID,
            run_id=RUN_ID,
            project_id=PROJECT_ID,
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(
        "airflow.providers.google.cloud.sensors.bigquery_dts.BiqQueryDataTransferServiceHook",
        return_value=MM(get_transfer_run=MM(return_value=MM(state=TransferState.SUCCEEDED))),
    )
    def test_poke_returns_true(self, mock_hook):
        op = BigQueryDataTransferServiceTransferRunSensor(
            transfer_config_id=TRANSFER_CONFIG_ID,
            run_id=RUN_ID,
            task_id="id",
            project_id=PROJECT_ID,
            expected_statuses={"SUCCEEDED"},
            location=LOCATION,
        )
        result = op.poke({})

        assert result is True

        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID, impersonation_chain=None, location=LOCATION
        )
        mock_hook.return_value.get_transfer_run.assert_called_once_with(
            transfer_config_id=TRANSFER_CONFIG_ID,
            run_id=RUN_ID,
            project_id=PROJECT_ID,
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
