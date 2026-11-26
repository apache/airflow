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
import tempfile
from pathlib import Path
from typing import Any

from datafusion.object_store import GoogleCloud

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.base import ObjectStorageProvider
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException
from airflow.providers.google.common.hooks.base_google import get_field

_UNSUPPORTED_CONNECTION_FIELDS = ("key_secret_name", "credential_config_file", "impersonation_chain")


class GCSObjectStorageProvider(ObjectStorageProvider):
    """
    GCS Object Storage Provider using DataFusion's GoogleCloud.

    ``ObjectStoragePath`` reaches ``gs://`` through :mod:`airflow.providers.google.cloud.fs.gcs`
    instead, which hands ``GoogleBaseHook.get_credentials()`` to gcsfs. DataFusion's ``GoogleCloud``
    only accepts a service account key file, so this class supports ``key_path``, ``keyfile_dict``,
    ``GOOGLE_APPLICATION_CREDENTIALS`` and ambient credentials, and rejects the hook's other
    credential sources (``key_secret_name``, ``credential_config_file``, ``impersonation_chain``).
    Keep the two in mind together when changing how a ``google_cloud_platform`` connection is read.
    """

    @property
    def get_storage_type(self) -> StorageType:
        """Return the storage type."""
        return StorageType.GCS

    def _credentials_from_connection(self, conn_id: str) -> dict[str, Any]:
        """Derive GCS credentials from an Airflow connection; only ``google_cloud_platform`` connections are supported."""
        conn = BaseHook.get_connection(conn_id)
        if conn.conn_type != "google_cloud_platform":
            raise ValueError(f"Unknown connection type {conn.conn_type}")

        extra_dejson = conn.extra_dejson
        for unsupported_field in _UNSUPPORTED_CONNECTION_FIELDS:
            if get_field(extra_dejson, unsupported_field):
                raise ValueError(
                    f"Connection field {unsupported_field!r} is not supported for DataFusion "
                    "GCS access; only key_path, keyfile_dict, GOOGLE_APPLICATION_CREDENTIALS, or "
                    "ambient credentials (gcloud ADC file / metadata server) are used."
                )
        key_path = get_field(extra_dejson, "key_path") or None
        keyfile_dict = get_field(extra_dejson, "keyfile_dict") or None
        if key_path and keyfile_dict:
            raise ValueError(
                "The `keyfile_dict` and `key_path` fields are mutually exclusive. "
                "Please provide only one value."
            )
        if not key_path and not keyfile_dict:
            key_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
        credentials = {"key_path": key_path, "keyfile_dict": keyfile_dict}
        return {k: v for k, v in credentials.items() if v is not None}

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """
        Create a GCS object store using DataFusion's GoogleCloud.

        Credentials supplied on ``connection_config`` take precedence. Only when
        ``connection_config.credentials`` is empty are they resolved from the
        ``google_cloud_platform`` Airflow connection named by ``conn_id``.
        """
        if connection_config is None:
            raise ValueError(f"connection_config must be provided for {self.get_storage_type.value}")

        credentials = connection_config.credentials
        if not credentials:
            credentials = self._credentials_from_connection(connection_config.conn_id)

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
