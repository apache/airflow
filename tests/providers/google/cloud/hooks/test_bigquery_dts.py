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

import unittest
from copy import deepcopy
from unittest import mock

from google.cloud.bigquery_datatransfer_v1 import DataTransferServiceClient
from google.cloud.bigquery_datatransfer_v1.types import TransferConfig
from google.protobuf.json_format import ParseDict

from airflow.providers.google.cloud.hooks.bigquery_dts import BiqQueryDataTransferServiceHook
from airflow.version import version
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id

CREDENTIALS = "test-creds"
PROJECT_ID = "id"

PARAMS = {
    "field_delimiter": ",",
    "max_bad_records": "0",
    "skip_leading_rows": "1",
    "data_path_template": "bucket",
    "destination_table_name_template": "name",
    "file_format": "CSV",
}

TRANSFER_CONFIG = ParseDict(
    {
        "destination_dataset_id": "dataset",
        "display_name": "GCS Test Config",
        "data_source_id": "google_cloud_storage",
        "params": PARAMS,
    },
    TransferConfig(),
)

TRANSFER_CONFIG_ID = "id1234"


class BigQueryDataTransferHookTestCase(unittest.TestCase):
    def setUp(self) -> None:
        with mock.patch(
            "airflow.providers.google.cloud.hooks.bigquery_dts.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.hook = BiqQueryDataTransferServiceHook()
            self.hook._get_credentials = mock.MagicMock(return_value=CREDENTIALS)  # type: ignore

    def test_version_information(self):
        expected_version = "airflow_v" + version
        self.assertEqual(expected_version, self.hook.client_info.client_library_version)

    def test_disable_auto_scheduling(self):
        expected = deepcopy(TRANSFER_CONFIG)
        expected.schedule_options.disable_auto_scheduling = True
        self.assertEqual(expected, self.hook._disable_auto_scheduling(TRANSFER_CONFIG))

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_dts."
        "DataTransferServiceClient.create_transfer_config"
    )
    def test_create_transfer_config(self, service_mock):
        self.hook.create_transfer_config(transfer_config=TRANSFER_CONFIG, project_id=PROJECT_ID)
        parent = DataTransferServiceClient.project_path(PROJECT_ID)
        expected_config = deepcopy(TRANSFER_CONFIG)
        expected_config.schedule_options.disable_auto_scheduling = True
        service_mock.assert_called_once_with(
            parent=parent,
            transfer_config=expected_config,
            authorization_code=None,
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_dts."
        "DataTransferServiceClient.delete_transfer_config"
    )
    def test_delete_transfer_config(self, service_mock):
        self.hook.delete_transfer_config(transfer_config_id=TRANSFER_CONFIG_ID, project_id=PROJECT_ID)

        name = DataTransferServiceClient.project_transfer_config_path(PROJECT_ID, TRANSFER_CONFIG_ID)
        service_mock.assert_called_once_with(name=name, metadata=None, retry=None, timeout=None)

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery_dts."
        "DataTransferServiceClient.start_manual_transfer_runs"
    )
    def test_start_manual_transfer_runs(self, service_mock):
        self.hook.start_manual_transfer_runs(transfer_config_id=TRANSFER_CONFIG_ID, project_id=PROJECT_ID)

        parent = DataTransferServiceClient.project_transfer_config_path(PROJECT_ID, TRANSFER_CONFIG_ID)
        service_mock.assert_called_once_with(
            parent=parent,
            requested_time_range=None,
            requested_run_time=None,
            metadata=None,
            retry=None,
            timeout=None,
        )
