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

from unittest.mock import patch

import pytest

from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException
from airflow.providers.common.sql.datafusion.object_storage_provider import (
    AzureObjectStorageProvider,
    GCSObjectStorageProvider,
    LocalObjectStorageProvider,
    S3ObjectStorageProvider,
    get_object_storage_provider,
)


class TestObjectStorageProvider:
    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.AmazonS3")
    def test_s3_provider_success(self, mock_s3):
        provider = S3ObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="aws_default",
            credentials={"access_key_id": "fake_key", "secret_access_key": "fake_secret"},
        )

        store = provider.create_object_store("s3://demo-data/path", connection_config)

        mock_s3.assert_called_once_with(
            access_key_id="fake_key", secret_access_key="fake_secret", bucket_name="demo-data"
        )
        assert store == mock_s3.return_value
        assert provider.get_storage_type == StorageType.S3
        assert provider.get_scheme() == "s3://"

    def test_s3_provider_failure(self):
        provider = S3ObjectStorageProvider()
        connection_config = ConnectionConfig(conn_id="aws_default")

        with patch(
            "airflow.providers.common.sql.datafusion.object_storage_provider.AmazonS3",
            side_effect=Exception("Error"),
        ):
            with pytest.raises(ObjectStoreCreationException, match="Failed to create S3 object store"):
                provider.create_object_store("s3://demo-data/path", connection_config)

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.LocalFileSystem")
    def test_local_provider(self, mock_local):
        provider = LocalObjectStorageProvider()
        assert provider.get_storage_type == StorageType.LOCAL
        assert provider.get_scheme() == "file://"
        local_store = provider.create_object_store("file://path")
        assert local_store == mock_local.return_value

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.GoogleCloud", autospec=True)
    def test_gcs_provider_success(self, mock_gcs):
        provider = GCSObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="gcp_default",
            credentials={"service_account_path": "/path/to/keyfile.json"},
        )

        store = provider.create_object_store("gs://my-bucket/path", connection_config)

        mock_gcs.assert_called_once_with(
            service_account_path="/path/to/keyfile.json", bucket_name="my-bucket"
        )
        assert store == mock_gcs.return_value
        assert provider.get_storage_type == StorageType.GCS
        assert provider.get_scheme() == "gs://"

    def test_gcs_provider_no_connection_config_raises(self):
        provider = GCSObjectStorageProvider()

        with pytest.raises(ValueError, match="connection_config must be provided"):
            provider.create_object_store("gs://my-bucket/path", connection_config=None)

    def test_gcs_provider_failure(self):
        provider = GCSObjectStorageProvider()
        connection_config = ConnectionConfig(conn_id="gcp_default")

        with patch(
            "airflow.providers.common.sql.datafusion.object_storage_provider.GoogleCloud",
            side_effect=Exception("GCS Error"),
        ):
            with pytest.raises(ObjectStoreCreationException, match="Failed to create GCS object store"):
                provider.create_object_store("gs://my-bucket/path", connection_config)

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure", autospec=True)
    def test_azure_provider_success(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "myaccount", "access_key": "mykey"},
        )

        store = provider.create_object_store("az://my-container/path", connection_config)

        mock_azure.assert_called_once_with(
            account="myaccount", access_key="mykey", container_name="my-container"
        )
        assert store == mock_azure.return_value
        assert provider.get_storage_type == StorageType.AZURE
        assert provider.get_scheme() == "az://"

    def test_azure_provider_no_connection_config_raises(self):
        provider = AzureObjectStorageProvider()

        with pytest.raises(ValueError, match="connection_config must be provided"):
            provider.create_object_store("az://my-container/path", connection_config=None)

    def test_azure_provider_failure(self):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(conn_id="wasb_default")

        with patch(
            "airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure",
            side_effect=Exception("Azure Error"),
        ):
            with pytest.raises(ObjectStoreCreationException, match="Failed to create Azure object store"):
                provider.create_object_store("az://my-container/path", connection_config)

    def test_get_object_storage_provider(self):
        assert isinstance(get_object_storage_provider(StorageType.S3), S3ObjectStorageProvider)
        assert isinstance(get_object_storage_provider(StorageType.LOCAL), LocalObjectStorageProvider)
        assert isinstance(get_object_storage_provider(StorageType.GCS), GCSObjectStorageProvider)
        assert isinstance(get_object_storage_provider(StorageType.AZURE), AzureObjectStorageProvider)

        with pytest.raises(ValueError, match="Unsupported storage type"):
            get_object_storage_provider("invalid")
