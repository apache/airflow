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
import re
import tempfile
from pathlib import Path

from datafusion.object_store import AmazonS3, GoogleCloud, LocalFileSystem, MicrosoftAzure

from airflow.providers.common.sql.config import STORAGE_TYPE_SCHEMES, ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.base import ObjectStorageProvider
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException


class S3ObjectStorageProvider(ObjectStorageProvider):
    """S3 Object Storage Provider using DataFusion's AmazonS3."""

    SCHEMES = STORAGE_TYPE_SCHEMES[StorageType.S3]

    @property
    def get_storage_type(self) -> StorageType:
        """Return the storage type."""
        return StorageType.S3

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """Create an S3 object store using DataFusion's AmazonS3."""
        if connection_config is None:
            raise ValueError(f"connection_config must be provided for {self.get_storage_type.value}")

        try:
            credentials = connection_config.credentials
            bucket = self.get_bucket(path)

            s3_store = AmazonS3(**credentials, **connection_config.extra_config, bucket_name=bucket)
            self.log.info("Created S3 object store for bucket %s", bucket)

            return s3_store

        except Exception as e:
            raise ObjectStoreCreationException(f"Failed to create S3 object store: {e}")


class GCSObjectStorageProvider(ObjectStorageProvider):
    """GCS Object Storage Provider using DataFusion's GoogleCloud."""

    SCHEMES = STORAGE_TYPE_SCHEMES[StorageType.GCS]

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


# Host suffix matched loosely (not pinned to dfs.core.windows.net) to also cover sovereign
# clouds, consistent with DataFusionEngine._resolve_wasb_account's AZURE_STORAGE_ENDPOINT tolerance.
_ABFS_URI_RE = re.compile(r"^abfss?://(?P<container>[^@/]+)@(?P<account>[^./]+)\.[^/]+(?P<path>/.*)?$")
_ABFS_SCHEMES = ("abfs://", "abfss://")


class AzureObjectStorageProvider(ObjectStorageProvider):
    """Azure Object Storage Provider using DataFusion's MicrosoftAzure."""

    SCHEMES = STORAGE_TYPE_SCHEMES[StorageType.AZURE]

    @property
    def get_storage_type(self) -> StorageType:
        """Return the storage type."""
        return StorageType.AZURE

    def get_bucket(self, path: str) -> str | None:
        """Extract the container name, from either the ``az://`` or ``abfs(s)://`` URI shape."""
        if match := _ABFS_URI_RE.match(path):
            return match.group("container")
        if path.startswith(_ABFS_SCHEMES):
            raise ValueError(
                f"{path!r} does not match the required abfs(s)://<container>@<account>.<host>/<path> shape"
            )
        return super().get_bucket(path)

    def _get_uri_account(self, path: str) -> str | None:
        """Return the storage account embedded in an ``abfs(s)://`` URI, if any."""
        match = _ABFS_URI_RE.match(path)
        return match.group("account") if match else None

    def normalize_uri(self, path: str) -> str:
        """
        Rewrite ``abfs(s)://<container>@<account>.<host>/<path>`` to ``az://<account>.<container>/<path>``.

        DataFusion's registry keys on (schema, host) alone, so container-only would collide for
        two different accounts sharing a container name. Account and container names never
        contain a dot, so joining on one is unambiguous. ``az://`` passes through unchanged --
        it never carried an account, so that collision is a pre-existing limit of the scheme
        itself, not something this normalization can resolve.
        """
        if match := _ABFS_URI_RE.match(path):
            return f"az://{match.group('account')}.{match.group('container')}{match.group('path') or ''}"
        return path

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """Create an Azure object store using DataFusion's MicrosoftAzure."""
        if connection_config is None:
            raise ValueError(f"connection_config must be provided for {self.get_storage_type.value}")

        try:
            credentials = connection_config.credentials
            container = self.get_bucket(path)

            uri_account = self._get_uri_account(path)
            resolved_account = credentials.get("account")
            if uri_account and resolved_account and uri_account.lower() != resolved_account.lower():
                raise ValueError(
                    f"URI {path!r} names storage account {uri_account!r}, but connection "
                    f"{connection_config.conn_id!r} resolves to account {resolved_account!r}. Point "
                    "the URI and the connection at the same account, or omit the account from one of "
                    "them."
                )
            if uri_account and not resolved_account:
                # Without this, MicrosoftAzure falls back to AZURE_STORAGE_ACCOUNT_NAME, which can
                # silently point at a different account than the one named in the URI.
                credentials = {**credentials, "account": uri_account}

            azure_store = MicrosoftAzure(container_name=container, **credentials)
            self.log.info("Created Azure object store for container %s", container)

            return azure_store

        except BaseException as e:
            # A bad credential combination panics as pyo3_runtime.PanicException, not a plain Exception.
            raise ObjectStoreCreationException(f"Failed to create Azure object store: {e}")


class LocalObjectStorageProvider(ObjectStorageProvider):
    """Local Object Storage Provider using DataFusion's LocalFileSystem."""

    SCHEMES = STORAGE_TYPE_SCHEMES[StorageType.LOCAL]

    @property
    def get_storage_type(self) -> StorageType:
        """Return the storage type."""
        return StorageType.LOCAL

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """Create a Local object store."""
        return LocalFileSystem()

    def get_scheme(self, uri: str) -> str:
        """
        Return "file://" regardless of ``uri``.

        A bare path with no prefix is only reachable via an explicit
        ``storage_type=StorageType.LOCAL``, so matching against ``uri`` won't work.
        """
        return "file://"


def get_object_storage_provider(storage_type: StorageType) -> ObjectStorageProvider:
    """Get an object storage provider based on the storage type."""
    # TODO: Add support for HTTP: https://datafusion.apache.org/python/autoapi/datafusion/object_store/index.html
    providers: dict[StorageType, type] = {
        StorageType.S3: S3ObjectStorageProvider,
        StorageType.GCS: GCSObjectStorageProvider,
        StorageType.AZURE: AzureObjectStorageProvider,
        StorageType.LOCAL: LocalObjectStorageProvider,
    }

    if storage_type not in providers:
        raise ValueError(
            f"Unsupported storage type: {storage_type}. Supported types: {list(providers.keys())}"
        )

    provider_class = providers[storage_type]
    return provider_class()
