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

from airflow.providers.common.ai.datafusion.object_storage_provider import (
    LocalObjectStorageProvider,
    ObjectStorageProviderFactory,
    S3ObjectStorageProvider,
)
from airflow.providers.common.ai.exceptions import ObjectStoreCreationException
from airflow.providers.common.ai.utils.config import ConnectionConfig, StorageType


class TestObjectStorageProvider:
    @patch("airflow.providers.common.ai.datafusion.object_storage_provider.AmazonS3")
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
        assert provider.get_storage_type() == StorageType.S3.value
        assert provider.get_scheme() == "s3://"

    def test_s3_provider_failure(self):
        provider = S3ObjectStorageProvider()
        connection_config = ConnectionConfig(conn_id="aws_default")

        with patch(
            "airflow.providers.common.ai.datafusion.object_storage_provider.AmazonS3",
            side_effect=Exception("Error"),
        ):
            with pytest.raises(ObjectStoreCreationException, match="Failed to create S3 object store"):
                provider.create_object_store("s3://demo-data/path", connection_config)

    @patch("airflow.providers.common.ai.datafusion.object_storage_provider.LocalFileSystem")
    def test_local_provider(self, mock_local):
        provider = LocalObjectStorageProvider()
        assert provider.get_storage_type() == StorageType.LOCAL.value
        assert provider.get_scheme() == "file://"
        local_store = provider.create_object_store("file://path")
        assert local_store == mock_local.return_value

    def test_factory_create_provider(self):
        assert isinstance(
            ObjectStorageProviderFactory.create_provider(StorageType.S3.value), S3ObjectStorageProvider
        )
        assert isinstance(
            ObjectStorageProviderFactory.create_provider(StorageType.LOCAL.value), LocalObjectStorageProvider
        )

        with pytest.raises(ValueError, match="Unsupported storage type"):
            ObjectStorageProviderFactory.create_provider("invalid")
