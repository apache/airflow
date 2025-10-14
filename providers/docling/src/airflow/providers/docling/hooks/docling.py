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

import logging
import mimetypes
from abc import ABC, abstractmethod
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.utils.log.secrets_masker import mask_secret

try:
    from airflow.sdk import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook as BaseHook  # type: ignore

log = logging.getLogger(__name__)


class DoclingHook(HttpHook):
    """Interacts with the Docling Webserver API."""

    conn_name_attr = "docling_conn_id"
    default_conn_name = "docling_default"
    conn_type = "docling"
    hook_name = "Docling"

    def __init__(self, http_conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, method="POST", **kwargs)

    def send_document(
        self,
        filename: str,
        file_content: bytes,
        data: dict[str, Any] | None = None,
        timeout: int = 3600,
    ) -> dict[str, Any]:
        """
        Submit document content to the /api/v1/convert/file endpoint.

        :param filename: The name of the file being processed.
        :param file_content: The binary content of the file.
        :param data: Optional dictionary of processing parameters.
        :param timeout: The request timeout in seconds.
        :return: The JSON response from the server.
        """
        self.log.info("Submitting document '%s' to Docling server for processing...", filename)

        if data:
            self.log.info("Processing with parameters: %s", data)

        try:
            file_format, _ = mimetypes.guess_type(filename)
            files = {
                "files": (
                    filename,
                    file_content,
                    file_format or "application/octet-stream",
                )
            }

            response = self.run(
                endpoint="/v1/convert/file",
                files=files,
                data=data,
                extra_options={"timeout": timeout},
            )
            response.raise_for_status()

            json_response = response.json()

            # Mask the markdown content from appearing in Airflow task logs
            if json_response and json_response.get("document", {}).get("md_content"):
                # This prevents the raw text from being printed in logs, but does not
                # affect the data passed via XCom.
                mask_secret(json_response["document"]["md_content"])

            self.log.info("Document '%s' processed successfully.", filename)
            return json_response

        except Exception as e:
            self.log.error("Failed to process document '%s' with Docling server: %s", filename, e)
            raise AirflowException(f"Docling API request failed for {filename}: {e}")

    def upload_source(
        self,
        source: str,
        data: dict[str, Any] | None = None,
        timeout: int = 3600,
    ) -> dict[str, Any]:
        """
        Get a source file from a URL using the /api/v1/convert/source endpoint.

        :param source: The URL of the source file.
        :param data: Optional dictionary of processing parameters (options).
        :return: The JSON response from the server.
        """
        self.log.info("Converting source %s using Docling server...", source)

        try:
            payload: dict[str, Any] = {
                "sources": [
                    {
                        "kind": "http",
                        "url": source,
                    }
                ]
            }

            if data:
                payload["options"] = data
                self.log.info("Processing with parameters: %s", data)

            response = self.run(
                endpoint="/v1/convert/source",
                json=payload,
                extra_options={"timeout": timeout},
            )
            response.raise_for_status()

            json_response = response.json()

            # Mask the markdown content from appearing in Airflow task logs
            if json_response and json_response.get("document", {}).get("md_content"):
                # This prevents the raw text from being printed in logs, but does not
                # affect the data passed via XCom.
                mask_secret(json_response["document"]["md_content"])

            self.log.info("Source '%s' converted successfully.", source)
            return json_response

        except Exception as e:
            self.log.error("Failed to use source '%s' with Docling server: %s", source, e)
            raise AirflowException(f"Docling source upload failed for {source}: {e}")


class BaseFileAccessorHook(ABC):
    """Abstract base class for reading a file from a storage system and returning its content."""

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    @abstractmethod
    def read_file(self, location: dict[str, Any]) -> tuple[str, bytes]:
        """
        Read a file from a specified location.

        :param location: A dictionary containing location-specific info
                        (e.g., {"bucket": "b", "key": "k"} for S3).
        :return: A tuple containing the filename (str) and the file content (bytes).
        """
        raise NotImplementedError


class S3FileAccessorHook(BaseFileAccessorHook):
    """File accessor for S3-compatible storage like MinIO."""

    def read_file(self, location: dict[str, Any]) -> tuple[str, bytes]:
        s3_hook = S3Hook(aws_conn_id=self.conn_id)
        bucket = location["bucket"]
        key = location["key"]

        log.info("Reading from S3: s3://{%s}/{%s}", bucket, key)
        file_obj = s3_hook.get_key(key=key, bucket_name=bucket)
        content = file_obj.get()["Body"].read()
        filename = key.split("/")[-1]

        return filename, content


class GCSFileAccessorHook(BaseFileAccessorHook):
    """File accessor for Google Cloud Storage."""

    def read_file(self, location: dict[str, Any]) -> tuple[str, bytes]:
        gcs_hook = GCSHook(gcp_conn_id=self.conn_id)
        bucket = location["bucket"]
        obj = location["object"]  # GCS uses "object" instead of "key"

        log.info("Reading from GCS: gs://{%s}/{%s}", bucket, obj)
        content = gcs_hook.download(bucket_name=bucket, object_name=obj)
        filename = obj.split("/")[-1]

        return filename, content


class AzureBlobStorageFileAccessorHook(BaseFileAccessorHook):
    """File accessor for Azure Blob Storage."""

    def read_file(self, location: dict[str, Any]) -> tuple[str, bytes]:
        wasb_hook = WasbHook(wasb_conn_id=self.conn_id)
        container = location["container"]
        blob = location["blob"]

        log.info("Reading from Azure Blob Storage: container='{%s}', blob='{%s}'", container, blob)
        content = wasb_hook.read_file(container_name=container, blob_name=blob)
        filename = blob.split("/")[-1]

        return filename, content


def get_file_accessor_hook(conn_id: str) -> BaseFileAccessorHook:
    """Get the appropriate file accessor hook based on connection type."""
    conn = BaseHook.get_connection(conn_id)

    if conn.conn_type == "aws":
        return S3FileAccessorHook(conn_id)
    if conn.conn_type == "google_cloud_platform":
        return GCSFileAccessorHook(conn_id)
    if conn.conn_type == "wasb":
        return AzureBlobStorageFileAccessorHook(conn_id)
    # Add other conditions here for 'fs', etc. in the future.
    raise ValueError(f"Unsupported connection type '{conn.conn_type}' for file access.")
