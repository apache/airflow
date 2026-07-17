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

from airflow.providers.google.cloud.transfers.adls_to_gcs import ADLSToGCSOperator

TASK_ID = "test-adls-gcs-operator"
ADLS_PATH_1 = "*"
GCS_PATH = "gs://test/"
TEST_FILE_SYSTEM_NAME = "test-container"
MOCK_FILES = [
    "test/TEST1.csv",
    "test/TEST2.csv",
    "test/path/TEST3.csv",
    "test/path/PARQUET.parquet",
    "test/path/PIC.png",
]
AZURE_CONN_ID = "azure_data_lake_default"
GCS_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestAdlsToGoogleCloudStorageOperator:
    def test_init(self):
        """Test AdlsToGoogleCloudStorageOperator instance is properly initialized."""

        operator = ADLSToGCSOperator(
            task_id=TASK_ID,
            src_adls=ADLS_PATH_1,
            dest_gcs=GCS_PATH,
            file_system_name=TEST_FILE_SYSTEM_NAME,
            replace=False,
            azure_data_lake_conn_id=AZURE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            return_gcs_uris=True,
        )

        assert operator.task_id == TASK_ID
        assert operator.src_adls == ADLS_PATH_1
        assert operator.dest_gcs == GCS_PATH
        assert operator.replace is False
        assert operator.gcp_conn_id == GCS_CONN_ID
        assert operator.azure_data_lake_conn_id == AZURE_CONN_ID

    @mock.patch("airflow.providers.google.cloud.transfers.adls_to_gcs.AzureDataLakeHook")
    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeStorageV2Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.adls_to_gcs.GCSHook")
    def test_execute(self, gcs_mock_hook, adls_one_mock_hook, adls_two_mock_hook):
        """Test the execute function when the run is successful."""

        operator = ADLSToGCSOperator(
            task_id=TASK_ID,
            src_adls=ADLS_PATH_1,
            dest_gcs=GCS_PATH,
            file_system_name=TEST_FILE_SYSTEM_NAME,
            replace=False,
            azure_data_lake_conn_id=AZURE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            google_impersonation_chain=IMPERSONATION_CHAIN,
            return_gcs_uris=True,
        )

        adls_one_mock_hook.return_value.list_files_directory.return_value = MOCK_FILES
        adls_two_mock_hook.return_value.list.return_value = MOCK_FILES

        # gcs_mock_hook.return_value.upload.side_effect = _assert_upload
        uploaded_files = operator.execute(None)
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(
                    bucket_name="test", filename=mock.ANY, object_name="test/path/PARQUET.parquet", gzip=False
                ),
                mock.call(
                    bucket_name="test", filename=mock.ANY, object_name="test/path/TEST3.csv", gzip=False
                ),
                mock.call(bucket_name="test", filename=mock.ANY, object_name="test/path/PIC.png", gzip=False),
                mock.call(bucket_name="test", filename=mock.ANY, object_name="test/TEST1.csv", gzip=False),
                mock.call(bucket_name="test", filename=mock.ANY, object_name="test/TEST2.csv", gzip=False),
            ],
            any_order=True,
        )

        adls_one_mock_hook.assert_called_once_with(adls_conn_id=AZURE_CONN_ID)
        adls_two_mock_hook.assert_called_once_with(azure_data_lake_conn_id=AZURE_CONN_ID)
        gcs_mock_hook.assert_called_once_with(
            gcp_conn_id=GCS_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )

        # Verify that the return value is a list of destination GCS URIs
        assert isinstance(uploaded_files, list)
        assert len(uploaded_files) == len(MOCK_FILES)
        expected_uris = sorted([f"gs://test/{f}" for f in MOCK_FILES])
        assert sorted(uploaded_files) == expected_uris

    @mock.patch("airflow.providers.google.cloud.transfers.adls_to_gcs.AzureDataLakeHook")
    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeStorageV2Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.adls_to_gcs.GCSHook")
    def test_execute_with_gzip(self, gcs_mock_hook, adls_one_mock_hook, adls_two_mock_hook):
        """Test the execute function when the run is successful."""

        operator = ADLSToGCSOperator(
            task_id=TASK_ID,
            src_adls=ADLS_PATH_1,
            dest_gcs=GCS_PATH,
            file_system_name=TEST_FILE_SYSTEM_NAME,
            replace=False,
            azure_data_lake_conn_id=AZURE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            gzip=True,
            return_gcs_uris=True,
        )

        adls_one_mock_hook.return_value.list_files_directory.return_value = MOCK_FILES
        adls_two_mock_hook.return_value.list.return_value = MOCK_FILES

        # gcs_mock_hook.return_value.upload.side_effect = _assert_upload
        uploaded_files = operator.execute(None)
        gcs_mock_hook.return_value.upload.assert_has_calls(
            [
                mock.call(
                    bucket_name="test", filename=mock.ANY, object_name="test/path/PARQUET.parquet", gzip=True
                ),
                mock.call(
                    bucket_name="test", filename=mock.ANY, object_name="test/path/TEST3.csv", gzip=True
                ),
                mock.call(bucket_name="test", filename=mock.ANY, object_name="test/path/PIC.png", gzip=True),
                mock.call(bucket_name="test", filename=mock.ANY, object_name="test/TEST1.csv", gzip=True),
                mock.call(bucket_name="test", filename=mock.ANY, object_name="test/TEST2.csv", gzip=True),
            ],
            any_order=True,
        )

        # Verify that the return value is a list of destination GCS URIs
        assert isinstance(uploaded_files, list)
        assert len(uploaded_files) == len(MOCK_FILES)
        expected_uris = sorted([f"gs://test/{f}" for f in MOCK_FILES])
        assert sorted(uploaded_files) == expected_uris

    @mock.patch("airflow.providers.google.cloud.transfers.adls_to_gcs.AzureDataLakeHook")
    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeStorageV2Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.adls_to_gcs.GCSHook")
    def test_execute_return_gcs_uris_false(self, gcs_mock_hook, adls_one_mock_hook, adls_two_mock_hook):
        """Test that return_gcs_uris=False returns legacy ADLS file paths."""

        operator = ADLSToGCSOperator(
            task_id=TASK_ID,
            src_adls=ADLS_PATH_1,
            dest_gcs=GCS_PATH,
            file_system_name=TEST_FILE_SYSTEM_NAME,
            replace=False,
            azure_data_lake_conn_id=AZURE_CONN_ID,
            gcp_conn_id=GCS_CONN_ID,
            return_gcs_uris=False,
        )

        adls_one_mock_hook.return_value.list_files_directory.return_value = MOCK_FILES
        adls_two_mock_hook.return_value.list.return_value = MOCK_FILES

        result = operator.execute(None)

        # Legacy behavior: returns the list of ADLS file paths
        assert isinstance(result, list)
        assert sorted(result) == sorted(MOCK_FILES)

    @mock.patch("airflow.providers.google.cloud.transfers.adls_to_gcs.AzureDataLakeHook")
    @mock.patch("airflow.providers.microsoft.azure.operators.adls.AzureDataLakeStorageV2Hook")
    @mock.patch("airflow.providers.google.cloud.transfers.adls_to_gcs.GCSHook")
    def test_execute_return_gcs_uris_default_warns(
        self, gcs_mock_hook, adls_one_mock_hook, adls_two_mock_hook
    ):
        """Test that omitting return_gcs_uris emits FutureWarning and returns legacy file paths."""

        adls_one_mock_hook.return_value.list_files_directory.return_value = MOCK_FILES
        adls_two_mock_hook.return_value.list.return_value = MOCK_FILES

        with pytest.warns(
            FutureWarning,
            match=r"will change to list\[str\] of GCS URIs in a future release",
        ):
            operator = ADLSToGCSOperator(
                task_id=TASK_ID,
                src_adls=ADLS_PATH_1,
                dest_gcs=GCS_PATH,
                file_system_name=TEST_FILE_SYSTEM_NAME,
                replace=False,
                azure_data_lake_conn_id=AZURE_CONN_ID,
                gcp_conn_id=GCS_CONN_ID,
            )

        result = operator.execute(None)

        # Default (None → False): returns legacy file paths
        assert isinstance(result, list)
        assert sorted(result) == sorted(MOCK_FILES)
