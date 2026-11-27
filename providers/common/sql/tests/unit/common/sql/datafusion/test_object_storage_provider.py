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

import sys
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.object_storage_provider import (
    LocalObjectStorageProvider,
    get_object_storage_provider,
)


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

    def test_rejects_unknown_storage_type_string(self):
        with pytest.raises(ValueError, match="'bogus' is not a valid StorageType"):
            get_object_storage_provider("bogus")

    @pytest.mark.parametrize(
        ("storage_type", "provider_class_name"),
        [
            (
                StorageType.S3,
                "airflow.providers.amazon.aws.datafusion.object_storage.S3ObjectStorageProvider",
            ),
            (
                StorageType.GCS,
                "airflow.providers.google.cloud.datafusion.object_storage.GCSObjectStorageProvider",
            ),
            (
                StorageType.AZURE,
                "airflow.providers.microsoft.azure.datafusion.object_storage.AzureObjectStorageProvider",
            ),
        ],
    )
    @patch("airflow.providers.common.sql.datafusion.object_storage_provider.import_string", autospec=True)
    @patch("airflow.providers_manager.ProvidersManager", autospec=True)
    def test_resolves_provider_owned_storage_via_registry(
        self, mock_pm_cls, mock_import_string, storage_type, provider_class_name
    ):
        mock_provider_cls = MagicMock()
        mock_import_string.return_value = mock_provider_cls

        mock_pm_cls.return_value.object_storage_providers = {
            storage_type.value: MagicMock(provider_class_name=provider_class_name),
        }

        provider = get_object_storage_provider(storage_type)

        mock_import_string.assert_called_once_with(provider_class_name)
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

    @pytest.mark.parametrize(
        ("storage_type", "hint"),
        [
            (StorageType.S3, r"apache-airflow-providers-amazon\[datafusion\]"),
            (StorageType.GCS, r"apache-airflow-providers-google\[datafusion\]"),
            (StorageType.AZURE, r"apache-airflow-providers-microsoft-azure\[datafusion\]"),
        ],
    )
    def test_error_message_includes_install_hint(self, storage_type, hint):
        with patch("airflow.providers_manager.ProvidersManager", autospec=True) as mock_pm_cls:
            mock_pm_cls.return_value.object_storage_providers = {}

            with pytest.raises(ValueError, match=hint):
                get_object_storage_provider(storage_type)

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

    def test_legacy_core_resolves_gcs_via_google_direct_import(self):
        """On an older core without the registry property, GCS resolves via a direct google import."""
        pytest.importorskip("airflow.providers.google")
        from airflow.providers.google.cloud.datafusion.object_storage import GCSObjectStorageProvider

        with patch("airflow.providers_manager.ProvidersManager") as mock_pm_cls:
            mock_pm_cls.return_value = MagicMock(spec=[])

            provider = get_object_storage_provider(StorageType.GCS)

        assert isinstance(provider, GCSObjectStorageProvider)

    def test_legacy_core_gcs_without_google_raises_install_hint(self):
        """On an older core without the registry and google missing, raise the hinted ValueError."""
        with patch("airflow.providers_manager.ProvidersManager") as mock_pm_cls:
            mock_pm_cls.return_value = MagicMock(spec=[])

            with patch.dict(
                sys.modules,
                {"airflow.providers.google.cloud.datafusion.object_storage": None},
            ):
                with pytest.raises(ValueError, match="apache-airflow-providers-google"):
                    get_object_storage_provider(StorageType.GCS)

    def test_legacy_core_resolves_azure_via_microsoft_azure_direct_import(self):
        """On an older core without the registry property, Azure resolves via a direct microsoft-azure import."""
        pytest.importorskip("airflow.providers.microsoft.azure")
        from airflow.providers.microsoft.azure.datafusion.object_storage import AzureObjectStorageProvider

        with patch("airflow.providers_manager.ProvidersManager") as mock_pm_cls:
            mock_pm_cls.return_value = MagicMock(spec=[])

            provider = get_object_storage_provider(StorageType.AZURE)

        assert isinstance(provider, AzureObjectStorageProvider)

    def test_legacy_core_azure_without_microsoft_azure_raises_install_hint(self):
        """On an older core without the registry and microsoft-azure missing, raise the hinted ValueError."""
        with patch("airflow.providers_manager.ProvidersManager") as mock_pm_cls:
            mock_pm_cls.return_value = MagicMock(spec=[])

            with patch.dict(
                sys.modules,
                {"airflow.providers.microsoft.azure.datafusion.object_storage": None},
            ):
                with pytest.raises(ValueError, match="apache-airflow-providers-microsoft-azure"):
                    get_object_storage_provider(StorageType.AZURE)

    def test_no_provider_imports_at_module_level(self):
        """Verify common-sql no longer statically imports amazon provider code at the top level."""
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        top_level_names = [
            name
            for name, obj in vars(mod).items()
            if not name.startswith("_")
            and hasattr(obj, "__module__")
            and any(p in getattr(obj, "__module__", "") for p in ("amazon", "google", "microsoft"))
        ]
        assert top_level_names == [], f"Provider symbols found at module level: {top_level_names}"


class TestS3DeprecationShim:
    def test_old_import_path_emits_deprecation_warning(self):
        """Importing S3ObjectStorageProvider from the old path still works but warns."""
        pytest.importorskip("airflow.providers.amazon")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Import it from airflow.providers.amazon",
        ):
            cls = mod.S3ObjectStorageProvider

        assert cls.__name__ == "S3ObjectStorageProvider"

    def test_old_import_path_returns_same_class(self):
        """The shim re-exports the exact same class from the new location."""
        pytest.importorskip("airflow.providers.amazon")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            AirflowProviderDeprecationWarning,
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
            AirflowProviderDeprecationWarning,
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


class TestGCSDeprecationShim:
    def test_old_import_path_warns_and_returns_same_class(self):
        """Importing GCSObjectStorageProvider from the old path still works, warns, and re-exports the class."""
        pytest.importorskip("airflow.providers.google")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Import it from airflow.providers.google.cloud.datafusion.object_storage",
        ):
            old_cls = mod.GCSObjectStorageProvider

        from airflow.providers.google.cloud.datafusion.object_storage import GCSObjectStorageProvider

        assert old_cls is GCSObjectStorageProvider

    def test_old_import_path_honours_explicit_credentials(self, tmp_path):
        """The shim preserves the pre-relocation contract: explicit credentials reach GoogleCloud."""
        pytest.importorskip("airflow.providers.google")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Import it from airflow.providers.google.cloud.datafusion.object_storage",
        ):
            cls = mod.GCSObjectStorageProvider

        key_path = tmp_path / "key.json"
        key_path.write_text("{}")
        config = ConnectionConfig(conn_id="google_cloud_default", credentials={"key_path": str(key_path)})
        # Patched inline rather than via decorator: the target module only exists when google is
        # installed, and a decorator would resolve it before importorskip can skip the test.
        with patch(
            "airflow.providers.google.cloud.datafusion.object_storage.GoogleCloud", autospec=True
        ) as mock_gcs:
            store = cls().create_object_store("gs://demo-data/path", connection_config=config)

        mock_gcs.assert_called_once_with(bucket_name="demo-data", service_account_path=str(key_path))
        assert store == mock_gcs.return_value


class TestAzureDeprecationShim:
    def test_old_import_path_warns_and_returns_same_class(self):
        """Importing AzureObjectStorageProvider from the old path still works, warns, and re-exports the class."""
        pytest.importorskip("airflow.providers.microsoft.azure")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Import it from airflow.providers.microsoft.azure.datafusion.object_storage",
        ):
            old_cls = mod.AzureObjectStorageProvider

        from airflow.providers.microsoft.azure.datafusion.object_storage import AzureObjectStorageProvider

        assert old_cls is AzureObjectStorageProvider

    def test_old_import_path_honours_explicit_credentials(self):
        """The shim preserves the pre-relocation contract: explicit credentials reach MicrosoftAzure."""
        pytest.importorskip("airflow.providers.microsoft.azure")
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="Import it from airflow.providers.microsoft.azure.datafusion.object_storage",
        ):
            cls = mod.AzureObjectStorageProvider

        config = ConnectionConfig(
            conn_id="wasb_default", credentials={"account": "myaccount", "access_key": "fake_key"}
        )
        # Patched inline rather than via decorator: the target module only exists when microsoft-azure
        # is installed, and a decorator would resolve it before importorskip can skip the test.
        with patch(
            "airflow.providers.microsoft.azure.datafusion.object_storage.MicrosoftAzure", autospec=True
        ) as mock_azure:
            store = cls().create_object_store("az://demo-container/path", connection_config=config)

        mock_azure.assert_called_once_with(
            container_name="demo-container", account="myaccount", access_key="fake_key"
        )
        assert store == mock_azure.return_value
