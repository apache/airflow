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

from datafusion.object_store import AmazonS3, LocalFileSystem

from airflow.providers.common.ai.datafusion.base import ObjectStorageProvider
from airflow.providers.common.ai.exceptions import ObjectStoreCreationException
from airflow.providers.common.ai.utils.config import ConnectionConfig, StorageType


class S3ObjectStorageProvider(ObjectStorageProvider):
    """S3 Object Storage Provider using DataFusion's AmazonS3."""

    def get_storage_type(self) -> str:
        """Return the storage type."""
        return StorageType.S3.value

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """Create an S3 object store using DataFusion's AmazonS3."""
        if connection_config is None:
            raise ValueError("connection_config must be provided")

        try:
            credentials = connection_config.credentials
            bucket = self.get_bucket(path)

            s3_store = AmazonS3(**credentials, **connection_config.extra_config, bucket_name=bucket)
            self.log.info("Created S3 object store for bucket %s", bucket)

            return s3_store

        except Exception as e:
            raise ObjectStoreCreationException("Failed to create S3 object store", e)

    def get_scheme(self) -> str:
        """Return the scheme for S3."""
        return "s3://"


class LocalObjectStorageProvider(ObjectStorageProvider):
    """Local Object Storage Provider using DataFusion's LocalFileSystem."""

    def get_storage_type(self) -> str:
        """Return the storage type."""
        return StorageType.LOCAL.value

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """Create a Local object store."""
        return LocalFileSystem()

    def get_scheme(self) -> str:
        """Return the scheme to a Local file system."""
        return "file://"


class ObjectStorageProviderFactory:
    """Factory to create object storage providers based on storage type."""

    # TODO: Add support for GCS, Azure, HTTP: https://datafusion.apache.org/python/autoapi/datafusion/object_store/index.html
    _providers: dict[str, type] = {
        StorageType.S3: S3ObjectStorageProvider,
        StorageType.LOCAL: LocalObjectStorageProvider,
    }

    @classmethod
    def create_provider(cls, storage_type: StorageType) -> ObjectStorageProvider:
        """Create a storage provider instance."""
        if storage_type not in cls._providers:
            raise ValueError(
                f"Unsupported storage type: {storage_type}. Supported types: {list(cls._providers.keys())}"
            )

        provider_class = cls._providers[storage_type]
        return provider_class()
