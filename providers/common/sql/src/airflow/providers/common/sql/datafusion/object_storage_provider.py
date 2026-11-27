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

import warnings
from typing import Any

from datafusion.object_store import LocalFileSystem

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.base import ObjectStorageProvider


class LocalObjectStorageProvider(ObjectStorageProvider):
    """Local Object Storage Provider using DataFusion's LocalFileSystem."""

    @property
    def get_storage_type(self) -> StorageType:
        """Return the storage type."""
        return StorageType.LOCAL

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """Create a Local object store."""
        return LocalFileSystem()

    def get_scheme(self) -> str:
        """Return the scheme to a Local file system."""
        return "file://"


# Storage types still implemented here rather than contributed by a provider package.
# TODO: Add support for HTTP: https://datafusion.apache.org/python/autoapi/datafusion/object_store/index.html
EMBEDDED_PROVIDERS: dict[StorageType, type[ObjectStorageProvider]] = {
    StorageType.LOCAL: LocalObjectStorageProvider,
}

_STORAGE_TYPE_PROVIDER_HINTS: dict[str, str] = {
    "s3": "apache-airflow-providers-amazon[datafusion]",
    "gcs": "apache-airflow-providers-google[datafusion]",
    "azure": "apache-airflow-providers-microsoft-azure[datafusion]",
}


def _missing_provider_message(type_key: str) -> str:
    hint = _STORAGE_TYPE_PROVIDER_HINTS.get(type_key, "the appropriate provider package")
    return f"No ObjectStorageProvider registered for storage type '{type_key}'. Install or upgrade {hint}."


def _get_legacy_object_storage_provider(type_key: str) -> ObjectStorageProvider:
    if type_key == StorageType.S3.value:
        try:
            from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider
        except ImportError as err:
            raise ValueError(_missing_provider_message(type_key)) from err
        return S3ObjectStorageProvider()

    if type_key == StorageType.GCS.value:
        try:
            from airflow.providers.google.cloud.datafusion.object_storage import GCSObjectStorageProvider
        except ImportError as err:
            raise ValueError(_missing_provider_message(type_key)) from err
        return GCSObjectStorageProvider()

    if type_key == StorageType.AZURE.value:
        try:
            from airflow.providers.microsoft.azure.datafusion.object_storage import (
                AzureObjectStorageProvider,
            )
        except ImportError as err:
            raise ValueError(_missing_provider_message(type_key)) from err
        return AzureObjectStorageProvider()

    raise ValueError(_missing_provider_message(type_key))


def get_object_storage_provider(storage_type: StorageType) -> ObjectStorageProvider:
    """Get an object storage provider based on the storage type."""
    storage_type = StorageType(storage_type)
    if provider_cls := EMBEDDED_PROVIDERS.get(storage_type):
        return provider_cls()

    type_key = storage_type.value

    from airflow.providers_manager import ProvidersManager

    manager = ProvidersManager()
    if not hasattr(manager, "object_storage_providers"):
        return _get_legacy_object_storage_provider(type_key)

    registry = manager.object_storage_providers
    if type_key in registry:
        try:
            provider_cls = import_string(registry[type_key].provider_class_name)
        except ImportError as err:
            raise ValueError(_missing_provider_message(type_key)) from err
        return provider_cls()

    raise ValueError(_missing_provider_message(type_key))


_MOVED_PROVIDER_MODULES: dict[str, str] = {
    "S3ObjectStorageProvider": "airflow.providers.amazon.aws.datafusion.object_storage",
    "GCSObjectStorageProvider": "airflow.providers.google.cloud.datafusion.object_storage",
    "AzureObjectStorageProvider": "airflow.providers.microsoft.azure.datafusion.object_storage",
}


def __getattr__(name: str) -> Any:
    if module_path := _MOVED_PROVIDER_MODULES.get(name):
        warnings.warn(
            f"Importing {name} from "
            "airflow.providers.common.sql.datafusion.object_storage_provider is deprecated. "
            f"Import it from {module_path} instead.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return import_string(f"{module_path}.{name}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
