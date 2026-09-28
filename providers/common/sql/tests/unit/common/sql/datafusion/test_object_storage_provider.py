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
import sys
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException
from airflow.providers.common.sql.datafusion.object_storage_provider import (
    AzureObjectStorageProvider,
    GCSObjectStorageProvider,
    LocalObjectStorageProvider,
    get_object_storage_provider,
)


class TestGCSObjectStorageProvider:
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

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure")
    def test_azure_provider_success(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "myaccount", "access_key": "fake_key"},
        )

        store = provider.create_object_store("az://demo-container/path", connection_config)

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="myaccount", access_key="fake_key"
        )
        assert store == mock_azure.return_value
        assert provider.get_storage_type == StorageType.AZURE
        assert provider.get_scheme() == "az://"

    @pytest.mark.parametrize(
        "uri",
        [
            "abfs://demo-container@myaccount.dfs.core.windows.net/path",
            "abfss://demo-container@myaccount.dfs.core.windows.net/path",
            "abfss://demo-container@myaccount.blob.core.windows.net/path",
            "abfss://demo-container@myaccount.DFS.Core.Windows.Net/path",
        ],
    )
    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure")
    def test_azure_provider_success_with_abfs_uri(self, mock_azure, uri, monkeypatch):
        # Without these, a public-cloud host check that is too strict would still pass.
        monkeypatch.delenv("AZURE_STORAGE_ENDPOINT", raising=False)
        monkeypatch.delenv("AZURE_ENDPOINT", raising=False)
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "myaccount", "access_key": "fake_key"},
        )

        store = provider.create_object_store(uri, connection_config)

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="myaccount", access_key="fake_key"
        )
        assert store == mock_azure.return_value
        assert provider.get_bucket(uri) == "demo-container"

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure")
    def test_azure_provider_abfs_uri_account_mismatch_raises(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "connection-account", "access_key": "fake_key"},
        )

        with pytest.raises(ObjectStoreCreationException, match="names storage account 'uri-account'"):
            provider.create_object_store(
                "abfss://demo-container@uri-account.dfs.core.windows.net/path", connection_config
            )

        mock_azure.assert_not_called()

    @pytest.mark.parametrize(
        "uri",
        [
            "abfs://demo-container/path",  # missing @account entirely
            "abfss://demo-container@/path",  # empty account
        ],
    )
    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure")
    def test_azure_provider_malformed_abfs_uri_raises_clear_error(self, mock_azure, uri):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "myaccount", "access_key": "fake_key"},
        )

        with pytest.raises(ObjectStoreCreationException, match="does not match the required"):
            provider.create_object_store(uri, connection_config)

        mock_azure.assert_not_called()

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure")
    def test_azure_provider_abfs_uri_account_used_when_connection_resolves_none(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"access_key": "fake_key"},
        )

        provider.create_object_store(
            "abfss://demo-container@uri-account.dfs.core.windows.net/path", connection_config
        )

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="uri-account", access_key="fake_key"
        )

    @pytest.mark.parametrize("account", [None, "usgovacct"])
    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure")
    def test_azure_provider_abfs_uri_sovereign_host_without_endpoint_raises(
        self, mock_azure, account, monkeypatch
    ):
        monkeypatch.delenv("AZURE_STORAGE_ENDPOINT", raising=False)
        monkeypatch.delenv("AZURE_ENDPOINT", raising=False)
        credentials = {"access_key": "fake_key"}
        if account:
            credentials["account"] = account
        connection_config = ConnectionConfig(conn_id="wasb_default", credentials=credentials)

        with pytest.raises(ObjectStoreCreationException, match="no endpoint override"):
            AzureObjectStorageProvider().create_object_store(
                "abfss://demo-container@usgovacct.dfs.core.usgovcloudapi.net/path", connection_config
            )

        mock_azure.assert_not_called()

    @pytest.mark.parametrize("env_var", ["AZURE_STORAGE_ENDPOINT", "AZURE_ENDPOINT"])
    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure")
    def test_azure_provider_abfs_uri_sovereign_host_with_endpoint_env_passes(
        self, mock_azure, env_var, monkeypatch
    ):
        monkeypatch.setenv(env_var, "https://usgovacct.blob.core.usgovcloudapi.net")
        connection_config = ConnectionConfig(conn_id="wasb_default", credentials={"access_key": "fake_key"})

        AzureObjectStorageProvider().create_object_store(
            "abfss://demo-container@usgovacct.dfs.core.usgovcloudapi.net/path", connection_config
        )

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="usgovacct", access_key="fake_key"
        )

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure")
    def test_azure_provider_abfs_uri_account_matches_case_insensitively(self, mock_azure):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"account": "MyAccount", "access_key": "fake_key"},
        )

        provider.create_object_store(
            "abfss://demo-container@myaccount.dfs.core.windows.net/path", connection_config
        )

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="MyAccount", access_key="fake_key"
        )

    def test_azure_provider_failure(self):
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(conn_id="wasb_default")

        with patch(
            "airflow.providers.common.sql.datafusion.object_storage_provider.MicrosoftAzure",
            side_effect=Exception("Error"),
        ):
            with pytest.raises(ObjectStoreCreationException, match="Failed to create Azure object store"):
                provider.create_object_store("az://demo-container/path", connection_config)

    def test_azure_provider_requires_connection_config(self):
        provider = AzureObjectStorageProvider()

        with pytest.raises(ValueError, match="connection_config must be provided for azure"):
            provider.create_object_store("az://demo-container/path")

    @pytest.mark.parametrize(
        ("uri", "expected"),
        [
            (
                "abfs://demo-container@myaccount.dfs.core.windows.net/path/to/data.parquet",
                "az://myaccount.demo-container/path/to/data.parquet",
            ),
            (
                "abfss://demo-container@myaccount.dfs.core.windows.net/path/to/data.parquet",
                "az://myaccount.demo-container/path/to/data.parquet",
            ),
            ("abfss://demo-container@myaccount.dfs.core.windows.net", "az://myaccount.demo-container"),
            ("az://demo-container/path", "az://demo-container/path"),
            # Same container name on different accounts maps to distinct hosts.
            ("abfss://data@devacct.dfs.core.windows.net/data.csv", "az://devacct.data/data.csv"),
            ("abfss://data@prodacct.dfs.core.windows.net/data.csv", "az://prodacct.data/data.csv"),
        ],
    )
    def test_azure_provider_normalize_uri(self, uri, expected):
        provider = AzureObjectStorageProvider()

        assert provider.normalize_uri(uri) == expected

    def test_azure_provider_partial_service_principal_raises_clear_error(self):
        """Uses the real MicrosoftAzure binding, not a mock, since it's the one that panics
        on a partial client_id/client_secret/tenant_id combination."""
        provider = AzureObjectStorageProvider()
        connection_config = ConnectionConfig(
            conn_id="wasb_default",
            credentials={"client_id": "only-this-one-set"},
        )

        with pytest.raises(ObjectStoreCreationException, match="Failed to create Azure object store"):
            provider.create_object_store("az://demo-container/path", connection_config)


class TestLocalObjectStorageProvider:
    @patch(
        "airflow.providers.common.sql.datafusion.object_storage_provider.LocalFileSystem",
        autospec=True,
    )
    def test_local_provider(self, mock_local):
        provider = LocalObjectStorageProvider()
        assert provider.get_storage_type == StorageType.LOCAL
        assert provider.get_scheme() == "file://"
        local_store = provider.create_object_store("file://path")
        assert local_store == mock_local.return_value


class TestGetObjectStorageProvider:
    def test_returns_local_provider_directly(self):
        provider = get_object_storage_provider(StorageType.LOCAL)
        assert isinstance(provider, LocalObjectStorageProvider)

    def test_returns_gcs_provider_directly(self):
        """GCS is still implemented in common-sql, so it resolves without the registry."""
        provider = get_object_storage_provider(StorageType.GCS)
        assert isinstance(provider, GCSObjectStorageProvider)

    def test_returns_azure_provider_directly(self):
        """Azure is still implemented in common-sql, so it resolves without the registry."""
        provider = get_object_storage_provider(StorageType.AZURE)
        assert isinstance(provider, AzureObjectStorageProvider)

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.import_string", autospec=True)
    @patch("airflow.providers_manager.ProvidersManager", autospec=True)
    def test_resolves_s3_via_registry(self, mock_pm_cls, mock_import_string):
        mock_provider_cls = MagicMock()
        mock_import_string.return_value = mock_provider_cls

        mock_pm_cls.return_value.object_storage_providers = {
            "s3": MagicMock(
                provider_class_name="airflow.providers.amazon.aws.datafusion.object_storage.S3ObjectStorageProvider",
            ),
        }

        provider = get_object_storage_provider(StorageType.S3)

        mock_import_string.assert_called_once_with(
            "airflow.providers.amazon.aws.datafusion.object_storage.S3ObjectStorageProvider"
        )
        assert provider == mock_provider_cls.return_value

    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.import_string", autospec=True)
    @patch("airflow.providers_manager.ProvidersManager", autospec=True)
    def test_registry_entry_with_unimportable_class_raises_install_hint(
        self, mock_pm_cls, mock_import_string
    ):
        """A registered provider whose module can't be imported yields the hinted ValueError."""
        mock_import_string.side_effect = ImportError("No module named 'datafusion'")
        mock_pm_cls.return_value.object_storage_providers = {
            "s3": MagicMock(
                provider_class_name="airflow.providers.amazon.aws.datafusion.object_storage.S3ObjectStorageProvider",
            ),
        }

        with pytest.raises(ValueError, match="apache-airflow-providers-amazon"):
            get_object_storage_provider(StorageType.S3)

    @patch("airflow.providers_manager.ProvidersManager", autospec=True)
    def test_unregistered_storage_type_raises(self, mock_pm_cls):
        mock_pm_cls.return_value.object_storage_providers = {}

        with pytest.raises(ValueError, match="No ObjectStorageProvider registered.*Install or upgrade"):
            get_object_storage_provider(StorageType.S3)

    def test_error_message_includes_install_hint_for_s3(self):
        with patch("airflow.providers_manager.ProvidersManager", autospec=True) as mock_pm_cls:
            mock_pm_cls.return_value.object_storage_providers = {}

            with pytest.raises(ValueError, match="apache-airflow-providers-amazon"):
                get_object_storage_provider(StorageType.S3)

    def test_legacy_core_resolves_s3_via_amazon_direct_import(self):
        """On an older core without the registry property, S3 resolves via a direct amazon import."""
        pytest.importorskip("airflow.providers.amazon")
        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        with patch("airflow.providers_manager.ProvidersManager") as mock_pm_cls:
            mock_pm_cls.return_value = MagicMock(spec=[])

            provider = get_object_storage_provider(StorageType.S3)

        assert isinstance(provider, S3ObjectStorageProvider)

    def test_legacy_core_s3_without_amazon_raises_install_hint(self):
        """On an older core without the registry and amazon missing, raise the hinted ValueError."""
        with patch("airflow.providers_manager.ProvidersManager") as mock_pm_cls:
            mock_pm_cls.return_value = MagicMock(spec=[])

            with patch.dict(
                sys.modules,
                {"airflow.providers.amazon.aws.datafusion.object_storage": None},
            ):
                with pytest.raises(ValueError, match="apache-airflow-providers-amazon"):
                    get_object_storage_provider(StorageType.S3)

    def test_no_amazon_imports_at_module_level(self):
        """Verify common-sql no longer statically imports amazon provider code at the top level."""
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        top_level_names = [
            name
            for name, obj in vars(mod).items()
            if not name.startswith("_")
            and hasattr(obj, "__module__")
            and "amazon" in getattr(obj, "__module__", "")
        ]
        assert top_level_names == [], f"Amazon symbols found at module level: {top_level_names}"


class TestS3DeprecationShim:
    def test_old_import_path_emits_deprecation_warning(self):
        """Importing S3ObjectStorageProvider from the old path still works but warns."""
        pytest.importorskip("airflow.providers.amazon")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            match="Import it from airflow.providers.amazon",
        ):
            cls = mod.S3ObjectStorageProvider

        assert cls.__name__ == "S3ObjectStorageProvider"

    def test_old_import_path_returns_same_class(self):
        """The shim re-exports the exact same class from the new location."""
        pytest.importorskip("airflow.providers.amazon")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            match="Import it from airflow.providers.amazon",
        ):
            old_cls = mod.S3ObjectStorageProvider

        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        assert old_cls is S3ObjectStorageProvider

    def test_old_import_path_honours_explicit_credentials(self):
        """The shim preserves the pre-relocation contract: explicit credentials reach AmazonS3."""
        pytest.importorskip("airflow.providers.amazon")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            match="Import it from airflow.providers.amazon",
        ):
            cls = mod.S3ObjectStorageProvider

        config = ConnectionConfig(
            conn_id="aws_default",
            credentials={"access_key_id": "fake_key", "secret_access_key": "fake_secret"},
        )
        # Patched inline rather than via decorator: the target module only exists when amazon is
        # installed, and a decorator would resolve it before importorskip can skip the test.
        with patch(
            "airflow.providers.amazon.aws.datafusion.object_storage.AmazonS3", autospec=True
        ) as mock_s3:
            store = cls().create_object_store("s3://demo-data/path", connection_config=config)

        mock_s3.assert_called_once_with(
            access_key_id="fake_key",
            secret_access_key="fake_secret",
            bucket_name="demo-data",
        )
        assert store == mock_s3.return_value

    def test_unknown_attr_raises_attribute_error(self):
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.raises(AttributeError, match="has no attribute"):
            _ = mod.NonExistentClass
