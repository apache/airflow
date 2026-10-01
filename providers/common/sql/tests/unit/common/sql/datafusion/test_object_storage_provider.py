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

import json
import os
from unittest.mock import patch

import pytest

from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException
from airflow.providers.common.sql.datafusion.object_storage_provider import (
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

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.GoogleCloud")
    def test_gcs_provider_success_with_key_path(self, mock_gcs, tmp_path):
        key_path = tmp_path / "key.json"
        key_path.write_text("{}")
        provider = GCSObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="google_cloud_default",
            credentials={"key_path": str(key_path)},
        )

        store = provider.create_object_store("gs://demo-data/path", connection_config)

        mock_gcs.assert_called_once_with(bucket_name="demo-data", service_account_path=str(key_path))
        assert store == mock_gcs.return_value
        assert provider.get_storage_type == StorageType.GCS
        assert provider.get_scheme() == "gs://"

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.GoogleCloud")
    def test_gcs_provider_success_with_keyfile_dict(self, mock_gcs):
        provider = GCSObjectStorageProvider()
        keyfile_dict = {"type": "service_account", "private_key": "fake"}
        connection_config = ConnectionConfig(
            conn_id="google_cloud_default",
            credentials={"keyfile_dict": keyfile_dict},
        )

        provider.create_object_store("gs://demo-data/path", connection_config)

        mock_gcs.assert_called_once()
        temp_path = mock_gcs.call_args.kwargs["service_account_path"]
        assert not os.path.exists(temp_path), "temp key file should be deleted after use"

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.GoogleCloud")
    def test_gcs_provider_writes_keyfile_dict_content_before_cleanup(self, mock_gcs):
        written_content = {}

        def _capture_path(*, bucket_name, service_account_path):
            with open(service_account_path) as f:
                written_content["data"] = json.load(f)

        mock_gcs.side_effect = _capture_path
        provider = GCSObjectStorageProvider()
        keyfile_dict = {"type": "service_account", "private_key": "fake"}
        connection_config = ConnectionConfig(
            conn_id="google_cloud_default",
            credentials={"keyfile_dict": keyfile_dict},
        )

        provider.create_object_store("gs://demo-data/path", connection_config)

        assert written_content["data"] == keyfile_dict

    def test_gcs_provider_failure(self):
        provider = GCSObjectStorageProvider()
        connection_config = ConnectionConfig(conn_id="google_cloud_default")

        with patch(
            "airflow.providers.common.sql.datafusion.object_storage_provider.GoogleCloud",
            side_effect=Exception("Error"),
        ):
            with pytest.raises(ObjectStoreCreationException, match="Failed to create GCS object store"):
                provider.create_object_store("gs://demo-data/path", connection_config)

    def test_gcs_provider_missing_key_file_raises_clear_error(self):
        """Uses the real GoogleCloud binding, not a mock, since it's the one that panics."""
        provider = GCSObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="google_cloud_default",
            credentials={"key_path": "/nonexistent/key.json"},
        )

        with pytest.raises(ObjectStoreCreationException, match="Failed to create GCS object store"):
            provider.create_object_store("gs://demo-data/path", connection_config)

    def test_gcs_provider_requires_connection_config(self):
        provider = GCSObjectStorageProvider()

        with pytest.raises(ValueError, match="connection_config must be provided for gcs"):
            provider.create_object_store("gs://demo-data/path")

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.LocalFileSystem")
    def test_local_provider(self, mock_local):
        provider = LocalObjectStorageProvider()
        assert provider.get_storage_type == StorageType.LOCAL
        assert provider.get_scheme() == "file://"
        local_store = provider.create_object_store("file://path")
        assert local_store == mock_local.return_value

    def test_get_object_storage_provider(self):
        assert isinstance(get_object_storage_provider(StorageType.S3), S3ObjectStorageProvider)
        assert isinstance(get_object_storage_provider(StorageType.GCS), GCSObjectStorageProvider)
        assert isinstance(get_object_storage_provider(StorageType.LOCAL), LocalObjectStorageProvider)

        with pytest.raises(ValueError, match="Unsupported storage type"):
            get_object_storage_provider("invalid")

    def test_s3_provider_requires_connection_config(self):
        """The message names the storage type rather than rendering as a tuple of format args."""
        provider = S3ObjectStorageProvider()

        with pytest.raises(ValueError, match="connection_config must be provided for s3"):
            provider.create_object_store("s3://demo-data/path", connection_config=None)
