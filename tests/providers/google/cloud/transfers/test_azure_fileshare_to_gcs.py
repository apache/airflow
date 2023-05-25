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

from airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs import AzureFileShareToGCSOperator

TASK_ID = "test-azure-fileshare-to-gcs"
AZURE_FILESHARE_SHARE = "test-share"
AZURE_FILESHARE_DIRECTORY_NAME = "/path/to/dir"
GCS_PATH_PREFIX = "gs://gcs-bucket/data/"
MOCK_FILES = ["TEST1.csv", "TEST2.csv", "TEST3.csv"]
AZURE_FILESHARE_CONN_ID = "azure_fileshare_default"
GCS_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestAzureFileShareToGCSOperator:
    def test_init(self):
        """Test AzureFileShareToGCSOperator instance is properly initialized."""

        operator = AzureFileShareToGCSOperator(
            task_id=TASK_ID,
            share_name=AZURE_FILESHARE_SHARE,
            directory_name=AZURE_FILESHARE_DIRECTORY_NAME,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )

        assert operator.task_id == TASK_ID
        assert operator.share_name == AZURE_FILESHARE_SHARE
        assert operator.directory_name == AZURE_FILESHARE_DIRECTORY_NAME
        assert operator.azure_fileshare_conn_id == AZURE_FILESHARE_CONN_ID
        assert operator.gcp_conn_id == GCS_CONN_ID
        assert operator.dest_gcs == GCS_PATH_PREFIX
        assert operator.google_impersonation_chain == IMPERSONATION_CHAIN

    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareHook")
    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.GCSHook")
    def test_execute(self, gcs_mock_hook, azure_fileshare_mock_hook):
        """Test the execute function when the run is successful."""

        operator = AzureFileShareToGCSOperator(
            task_id=TASK_ID,
            share_name=AZURE_FILESHARE_SHARE,
            directory_name=AZURE_FILESHARE_DIRECTORY_NAME,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
        )

        azure_fileshare_mock_hook.return_value.list_files.return_value = MOCK_FILES

        uploaded_files = operator.execute(None)

        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call("gcs-bucket", "data/TEST1.csv", mock.ANY, gzip=False),
                mock.call("gcs-bucket", "data/TEST3.csv", mock.ANY, gzip=False),
                mock.call("gcs-bucket", "data/TEST2.csv", mock.ANY, gzip=False),
            ],
            any_order=True,
        )

        azure_fileshare_mock_hook.assert_called_once_with(AZURE_FILESHARE_CONN_ID)

        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        assert sorted(MOCK_FILES) == sorted(uploaded_files)

    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.AzureFileShareHook")
    @mock.patch("airflow.providers.google.cloud.transfers.azure_fileshare_to_gcs.GCSHook")
    def test_execute_with_gzip(self, gcs_mock_hook, azure_fileshare_mock_hook):
        """Test the execute function when the run is successful."""

        operator = AzureFileShareToGCSOperator(
            task_id=TASK_ID,
            share_name=AZURE_FILESHARE_SHARE,
            directory_name=AZURE_FILESHARE_DIRECTORY_NAME,
            azure_fileshare_conn_id=AZURE_FILESHARE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            dest_gcs=GCS_PATH_PREFIX,
            google_impersonation_chain=IMPERSONATION_CHAIN,
            gzip=True,
        )

        azure_fileshare_mock_hook.return_value.list_files.return_value = MOCK_FILES

        operator.execute(None)

        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call("gcs-bucket", "data/TEST1.csv", mock.ANY, gzip=True),
                mock.call("gcs-bucket", "data/TEST3.csv", mock.ANY, gzip=True),
                mock.call("gcs-bucket", "data/TEST2.csv", mock.ANY, gzip=True),
            ],
            any_order=True,
        )
