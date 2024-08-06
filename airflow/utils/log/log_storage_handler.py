#
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

import os
from abc import ABC, abstractmethod

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.s3 import S3Hook, provide_bucket_name, unify_bucket_name_and_key
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.log.logging_mixin import LoggingMixin


class StorageHandler(ABC, LoggingMixin):
    """Abstract base class for storage handlers."""

    @abstractmethod
    def get_content_length(self, bucket_name: str, object_name: str) -> int:
        """Retrieve the content length of the log file in the specified bucket."""
        pass


class S3StorageHandler(StorageHandler):
    """Handler for S3 storage."""

    def __init__(self):
        """Initialize the S3Hook."""
        self.s3_hook = S3Hook()

    @unify_bucket_name_and_key
    @provide_bucket_name
    def get_content_length(self, bucket_name: str, object_name: str) -> int:
        """Get the content length of an object in S3."""
        return self.s3_hook.get_content_length(key=object_name, bucket_name=bucket_name)


class GCSStorageHandler(StorageHandler):
    """Handler for Google Cloud Storage."""

    def __init__(self, conn_id="google_cloud_default"):
        """Initialize the GCSHook with a specific connection ID."""
        self.conn_id = conn_id
        self.gcs_hook = GCSHook(gcp_conn_id=self.conn_id)

    def get_content_length(self, bucket_name: str, object_name: str) -> int:
        """Get the content length of an object in GCS."""
        return self.gcs_hook.get_size(bucket_name=bucket_name, object_name=object_name)


class AzureStorageHandler(StorageHandler):
    """Handler for Azure Blob Storage."""

    def get_content_length(self, bucket_name: str, object_name: str) -> int:
        """Retrieve the content length of the log file in Azure Blob Storage."""
        # Stub
        return -1


class LocalStorageHandler(StorageHandler):
    """Handler for local file system storage."""

    def get_content_length(self, bucket_name: str, object_name: str) -> int:
        """Retrieve the content length of the log file from the local file system."""
        return os.path.getsize(object_name)  # Here, object_name is the full path to the file


class LogContentReader:
    """Responsible for configuring log sizes and handling log storage backends."""

    def __init__(self):
        self.storage_handler = self.get_storage_handler()

    def get_storage_handler(self) -> StorageHandler:
        remote_logging_enabled = conf.getboolean("core", "remote_logging", fallback=False)
        if not remote_logging_enabled:
            return S3StorageHandler()

        remote_base = conf.get("core", "remote_base_log_folder", fallback="").lower()
        if remote_base.startswith("s3://"):
            return S3StorageHandler()
        elif remote_base.startswith("gs://"):
            return GCSStorageHandler()
        elif remote_base.startswith("wasb://"):
            return AzureStorageHandler()
        elif remote_base.startswith("local") or remote_base == "":
            return LocalStorageHandler()
        else:
            raise NotImplementedError(f"Storage type {remote_base} is not supported")

    def get_content_length(self, bucket_name: str, object_name: str) -> int:
        """Get the content length of the log file using the appropriate storage handler."""
        return self.storage_handler.get_content_length(bucket_name, object_name)
