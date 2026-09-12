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

from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException


class TestS3ObjectStorageProvider:
    """Tests for S3ObjectStorageProvider in the amazon provider package."""

    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="aws_default",
                conn_type="aws",
                login="fake_id",
                password="fake_secret",
                extra='{"region": "us-east-1"}',
            )
        )

    @patch(
        "airflow.providers.amazon.aws.datafusion.object_storage.AmazonS3",
        autospec=True,
    )
    @patch(
        "airflow.providers.amazon.aws.datafusion.object_storage.AwsGenericHook",
        autospec=True,
    )
    def test_s3_provider_with_login_password(self, mock_hook_cls, mock_s3):
        """Login/password on the connection override hook credentials."""
        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        mock_creds = MagicMock()
        mock_creds.access_key = "hook_key"
        mock_creds.secret_key = "hook_secret"
        mock_creds.token = None
        mock_hook_cls.return_value.get_credentials.return_value = mock_creds

        provider = S3ObjectStorageProvider()
        config = ConnectionConfig(conn_id="aws_default")

        store = provider.create_object_store("s3://demo-data/path", connection_config=config)

        mock_s3.assert_called_once_with(
            access_key_id="fake_id",
            secret_access_key="fake_secret",
            region="us-east-1",
            bucket_name="demo-data",
        )
        assert store == mock_s3.return_value
        assert provider.get_storage_type == StorageType.S3
        assert provider.get_scheme() == "s3://"

    @patch(
        "airflow.providers.amazon.aws.datafusion.object_storage.AmazonS3",
        autospec=True,
    )
    @patch(
        "airflow.providers.amazon.aws.datafusion.object_storage.AwsGenericHook",
        autospec=True,
    )
    def test_s3_provider_falls_back_to_hook_credentials(self, mock_hook_cls, mock_s3):
        """When login/password are empty, hook credentials are used."""
        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        mock_creds = MagicMock()
        mock_creds.access_key = "hook_key"
        mock_creds.secret_key = "hook_secret"
        mock_creds.token = "session_tok"
        mock_hook_cls.return_value.get_credentials.return_value = mock_creds

        provider = S3ObjectStorageProvider()
        config = ConnectionConfig(conn_id="aws_no_login")

        with patch(
            "airflow.providers.amazon.aws.datafusion.object_storage.BaseHook.get_connection",
            return_value=Connection(
                conn_id="aws_no_login",
                conn_type="aws",
                extra='{"endpoint": "http://localhost:4566"}',
            ),
        ):
            store = provider.create_object_store("s3://bucket/path", connection_config=config)

        mock_s3.assert_called_once_with(
            access_key_id="hook_key",
            secret_access_key="hook_secret",
            session_token="session_tok",
            endpoint="http://localhost:4566",
            bucket_name="bucket",
        )
        assert store == mock_s3.return_value

    @patch(
        "airflow.providers.amazon.aws.datafusion.object_storage.AmazonS3",
        autospec=True,
    )
    @patch(
        "airflow.providers.amazon.aws.datafusion.object_storage.AwsGenericHook",
        autospec=True,
    )
    def test_s3_provider_session_token(self, mock_hook_cls, mock_s3):
        """Session token from hook is forwarded when present."""
        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        mock_creds = MagicMock()
        mock_creds.access_key = "hook_key"
        mock_creds.secret_key = "hook_secret"
        mock_creds.token = "my_session_token"
        mock_hook_cls.return_value.get_credentials.return_value = mock_creds

        provider = S3ObjectStorageProvider()
        config = ConnectionConfig(conn_id="aws_default")

        store = provider.create_object_store("s3://bucket/path", connection_config=config)

        call_kwargs = mock_s3.call_args.kwargs
        assert call_kwargs["session_token"] == "my_session_token"
        assert store == mock_s3.return_value

    def test_s3_provider_missing_connection_config(self):
        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        provider = S3ObjectStorageProvider()
        with pytest.raises(ValueError, match="connection_config must be provided"):
            provider.create_object_store("s3://bucket/path", connection_config=None)

    @patch(
        "airflow.providers.amazon.aws.datafusion.object_storage.AmazonS3",
        autospec=True,
    )
    @patch(
        "airflow.providers.amazon.aws.datafusion.object_storage.AwsGenericHook",
        autospec=True,
    )
    def test_s3_provider_creation_failure(self, mock_hook_cls, mock_s3):
        """Internal exceptions are wrapped in ObjectStoreCreationException."""
        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        mock_creds = MagicMock()
        mock_creds.access_key = "k"
        mock_creds.secret_key = "s"
        mock_creds.token = None
        mock_hook_cls.return_value.get_credentials.return_value = mock_creds
        mock_s3.side_effect = Exception("boom")

        provider = S3ObjectStorageProvider()
        config = ConnectionConfig(conn_id="aws_default")

        with pytest.raises(ObjectStoreCreationException, match="Failed to create S3 object store"):
            provider.create_object_store("s3://bucket/path", connection_config=config)

    def test_s3_provider_bucket_extraction(self):
        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        provider = S3ObjectStorageProvider()
        assert provider.get_bucket("s3://my-bucket/prefix/file.parquet") == "my-bucket"
        assert provider.get_bucket("s3://another-bucket/") == "another-bucket"
        assert provider.get_bucket("file:///local/path") is None
