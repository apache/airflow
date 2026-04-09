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

from airflow.providers.common.sql.config import StorageType
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

    @patch("airflow._shared.module_loading.import_string", autospec=True)
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

    def test_unknown_attr_raises_attribute_error(self):
        import airflow.providers.common.sql.datafusion.object_storage_provider as mod

        with pytest.raises(AttributeError, match="has no attribute"):
            _ = mod.NonExistentClass
