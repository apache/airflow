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

from asyncio import Future
from copy import deepcopy
from unittest import mock
from unittest.mock import AsyncMock

import pytest
from google.api_core.gapic_v1.method import DEFAULT
from google.cloud.bigquery_datatransfer_v1.types import TransferConfig

from airflow.providers.google.cloud.hooks.bigquery_dts import (
    AsyncBiqQueryDataTransferServiceHook,
    BiqQueryDataTransferServiceHook,
)

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_no_default_project_id,
)

CREDENTIALS = "test-creds"
PROJECT_ID = "id"

PARAMS = {
    "field_delimiter": ",",
    "max_bad_records": 0,
    "skip_leading_rows": 1,
    "data_path_template": "bucket",
    "destination_table_name_template": "name",
    "file_format": "CSV",
}

TRANSFER_CONFIG = TransferConfig(
    destination_dataset_id="dataset",
    display_name="GCS Test Config",
    data_source_id="google_cloud_storage",
    params=PARAMS,
)

TRANSFER_CONFIG_ID = "id1234"
RUN_ID = "id1234"


class TestBigQueryDataTransferHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            BiqQueryDataTransferServiceHook(
                gcp_conn_id="GCP_CONN_ID", delegate_to="delegate_to"
            )

    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.cloud.hooks.bigquery_dts.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = BiqQueryDataTransferServiceHook()
            self.hook.get_credentials = mock.MagicMock(return_value=CREDENTIALS)  # type: ignore

    def test_disable_auto_scheduling(self):
        expected = deepcopy(TRANSFER_CONFIG)
        expected.schedule_options.disable_auto_scheduling = True
        assert expected == self.hook._disable_auto_scheduling(TRANSFER_CONFIG)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_dts."
        "DataTransferServiceClient.create_transfer_config"
    )
    def test_create_transfer_config(self, service_mock):
        self.hook.create_transfer_config(
            transfer_config=TRANSFER_CONFIG, project_id=PROJECT_ID
        )
        parent = f"projects/{PROJECT_ID}"
        expected_config = deepcopy(TRANSFER_CONFIG)
        expected_config.schedule_options.disable_auto_scheduling = True
        service_mock.assert_called_once_with(
            request=dict(
                parent=parent, transfer_config=expected_config, authorization_code=None
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_dts."
        "DataTransferServiceClient.delete_transfer_config"
    )
    def test_delete_transfer_config(self, service_mock):
        self.hook.delete_transfer_config(
            transfer_config_id=TRANSFER_CONFIG_ID, project_id=PROJECT_ID
        )

        name = f"projects/{PROJECT_ID}/transferConfigs/{TRANSFER_CONFIG_ID}"
        service_mock.assert_called_once_with(
            request=dict(name=name), metadata=(), retry=DEFAULT, timeout=None
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_dts."
        "DataTransferServiceClient.start_manual_transfer_runs"
    )
    def test_start_manual_transfer_runs(self, service_mock):
        self.hook.start_manual_transfer_runs(
            transfer_config_id=TRANSFER_CONFIG_ID, project_id=PROJECT_ID
        )

        parent = f"projects/{PROJECT_ID}/transferConfigs/{TRANSFER_CONFIG_ID}"
        service_mock.assert_called_once_with(
            request=dict(
                parent=parent, requested_time_range=None, requested_run_time=None
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_dts.DataTransferServiceClient.get_transfer_run"
    )
    def test_get_transfer_run(self, service_mock):
        self.hook.get_transfer_run(
            run_id=RUN_ID, transfer_config_id=TRANSFER_CONFIG_ID, project_id=PROJECT_ID
        )

        name = f"projects/{PROJECT_ID}/transferConfigs/{TRANSFER_CONFIG_ID}/runs/{RUN_ID}"
        service_mock.assert_called_once_with(
            request=dict(name=name), metadata=(), retry=DEFAULT, timeout=None
        )


class TestAsyncBiqQueryDataTransferServiceHook:
    HOOK_MODULE_PATH = "airflow.providers.google.cloud.hooks.bigquery_dts"

    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            AsyncBiqQueryDataTransferServiceHook(
                gcp_conn_id="GCP_CONN_ID", delegate_to="delegate_to"
            )

    @pytest.fixture
    def mock_client(self):
        with mock.patch(
            f"{self.HOOK_MODULE_PATH}.AsyncBiqQueryDataTransferServiceHook._get_conn",
            new_callable=AsyncMock,
        ) as mock_client:
            transfer_result = Future()
            transfer_result.set_result(mock.MagicMock())

            mock_client.return_value.get_transfer_run = mock.MagicMock(
                return_value=transfer_result
            )
            yield mock_client

    @pytest.fixture
    def hook(self):
        return AsyncBiqQueryDataTransferServiceHook()

    @pytest.mark.db_test
    @pytest.mark.asyncio
    async def test_get_transfer_run(self, mock_client, hook):
        await hook.get_transfer_run(
            run_id=RUN_ID,
            config_id=TRANSFER_CONFIG_ID,
            project_id=PROJECT_ID,
        )

        name = f"projects/{PROJECT_ID}/transferConfigs/{TRANSFER_CONFIG_ID}/runs/{RUN_ID}"
        mock_client.return_value.get_transfer_run.assert_called_once_with(
            name=name,
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
