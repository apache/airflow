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


_STORAGE_TYPE_PROVIDER_HINTS: dict[str, str] = {
    "s3": "apache-airflow-providers-amazon[datafusion]",
}


def get_object_storage_provider(storage_type: StorageType) -> ObjectStorageProvider:
    """Get an object storage provider based on the storage type."""
    if storage_type == StorageType.LOCAL:
        return LocalObjectStorageProvider()

    from airflow._shared.module_loading import import_string
    from airflow.providers_manager import ProvidersManager

    registry = ProvidersManager().object_storage_providers
    type_key = storage_type.value

    if type_key in registry:
        provider_cls = import_string(registry[type_key].provider_class_name)
        return provider_cls()

    hint = _STORAGE_TYPE_PROVIDER_HINTS.get(type_key, "the appropriate provider package")
    raise ValueError(
        f"No ObjectStorageProvider registered for storage type '{type_key}'. Install or upgrade {hint}."
    )


def __getattr__(name: str) -> Any:
    if name == "S3ObjectStorageProvider":
        warnings.warn(
            "Importing S3ObjectStorageProvider from "
            "airflow.providers.common.sql.datafusion.object_storage_provider is deprecated. "
            "Import it from airflow.providers.amazon.aws.datafusion.object_storage instead.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        from airflow.providers.amazon.aws.datafusion.object_storage import S3ObjectStorageProvider

        return S3ObjectStorageProvider
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
