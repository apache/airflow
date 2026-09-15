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

from typing import Any

from datafusion.object_store import AmazonS3

from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.sql.config import ConnectionConfig, StorageType
from airflow.providers.common.sql.datafusion.base import ObjectStorageProvider
from airflow.providers.common.sql.datafusion.exceptions import ObjectStoreCreationException


class S3ObjectStorageProvider(ObjectStorageProvider):
    """S3 Object Storage Provider using DataFusion's AmazonS3."""

    @property
    def get_storage_type(self) -> StorageType:
        """Return the storage type."""
        return StorageType.S3

    def _credentials_from_connection(self, conn_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
        """Derive S3 credentials and endpoint settings from an Airflow connection."""
        conn = BaseHook.get_connection(conn_id)
        aws_hook: AwsGenericHook = AwsGenericHook(aws_conn_id=conn.conn_id, client_type="s3")
        creds = aws_hook.get_credentials()

        credentials = {
            "access_key_id": conn.login or creds.access_key,
            "secret_access_key": conn.password or creds.secret_key,
            "session_token": creds.token if creds.token else None,
        }
        credentials = {k: v for k, v in credentials.items() if v is not None}
        extra_config = {k: conn.extra_dejson[k] for k in ("region", "endpoint") if k in conn.extra_dejson}
        return credentials, extra_config

    def create_object_store(self, path: str, connection_config: ConnectionConfig | None = None):
        """
        Create an S3 object store using DataFusion's AmazonS3.

        Credentials supplied on ``connection_config`` take precedence. Only when
        ``connection_config.credentials`` is empty are they resolved from the Airflow
        connection named by ``conn_id`` via :class:`AwsGenericHook`, which picks up the
        standard AWS credential chain. Caller-supplied ``extra_config`` keys always
        override the ``region`` / ``endpoint`` derived from the connection.
        """
        if connection_config is None:
            raise ValueError(f"connection_config must be provided for {self.get_storage_type}")

        try:
            credentials = connection_config.credentials
            extra_config = connection_config.extra_config

            if not credentials:
                credentials, conn_extra_config = self._credentials_from_connection(connection_config.conn_id)
                extra_config = {**conn_extra_config, **extra_config}

            bucket = self.get_bucket(path)
            s3_store = AmazonS3(**credentials, **extra_config, bucket_name=bucket)
            self.log.info("Created S3 object store for bucket %s", bucket)
            return s3_store

        except ObjectStoreCreationException:
            raise
        except Exception as e:
            raise ObjectStoreCreationException(f"Failed to create S3 object store: {e}")

    def get_scheme(self) -> str:
        """Return the scheme for S3."""
        return "s3://"
