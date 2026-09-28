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
import tempfile
import warnings
from pathlib import Path
from typing import Any

from datafusion.object_store import GoogleCloud, LocalFileSystem

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.module_loading import import_string
from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.base import ObjectStorageProvider
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException


class GCSObjectStorageProvider(ObjectStorageProvider):
    """GCS Object Storage Provider using DataFusion's GoogleCloud."""

    @property
    def get_storage_type(self) -> StorageType:
        """Return the storage type."""
        return StorageType.GCS

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """Create a GCS object store using DataFusion's GoogleCloud."""
        if connection_config is None:
            raise ValueError(f"connection_config must be provided for {self.get_storage_type.value}")

        credentials = connection_config.credentials
        key_path = credentials.get("key_path")
        keyfile_dict = credentials.get("keyfile_dict")
        temp_key_path: str | None = None

        try:
            bucket = self.get_bucket(path)

            if not key_path and keyfile_dict:
                # GoogleCloud only accepts a file path, not inline JSON; safe to delete once
                # constructed since the credentials are read only at construction time.
                key_content = keyfile_dict if isinstance(keyfile_dict, str) else json.dumps(keyfile_dict)
                with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as key_file:
                    key_file.write(key_content)
                temp_key_path = key_path = key_file.name

            if key_path is not None and not Path(key_path).is_file():
                raise FileNotFoundError(f"Service account key file not found: {key_path}")

            gcs_store = GoogleCloud(bucket_name=bucket, service_account_path=key_path)
            self.log.info("Created GCS object store for bucket %s", bucket)

            return gcs_store

        except BaseException as e:
            # A bad key file panics as pyo3_runtime.PanicException, not a plain Exception.
            raise ObjectStoreCreationException(f"Failed to create GCS object store: {e}")

        finally:
            if temp_key_path is not None:
                Path(temp_key_path).unlink(missing_ok=True)

    def get_scheme(self) -> str:
        """Return the scheme for GCS."""
        return "gs://"


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
# TODO: Add support for Azure, HTTP: https://datafusion.apache.org/python/autoapi/datafusion/object_store/index.html
EMBEDDED_PROVIDERS: dict[StorageType, type[ObjectStorageProvider]] = {
    StorageType.LOCAL: LocalObjectStorageProvider,
    StorageType.GCS: GCSObjectStorageProvider,
}

_STORAGE_TYPE_PROVIDER_HINTS: dict[str, str] = {
    "s3": "apache-airflow-providers-amazon[datafusion]",
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

    raise ValueError(_missing_provider_message(type_key))


def get_object_storage_provider(storage_type: StorageType) -> ObjectStorageProvider:
    """Get an object storage provider based on the storage type."""
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
