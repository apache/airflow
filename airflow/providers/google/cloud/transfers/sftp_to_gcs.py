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
"""This module contains SFTP to Google Cloud Storage operator."""
from __future__ import annotations

import os
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.sftp.hooks.sftp import SFTPHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


WILDCARD = "*"
_DEFAULT_CHUNKSIZE = 104857600  # 1024 * 1024 B * 100 = 100 MB

class SFTPToGCSOperator(BaseOperator):
    """
    Transfer files to Google Cloud Storage from SFTP server.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToGCSOperator`

    :param source_path: The sftp remote path. This is the specified file path
        for downloading the single file or multiple files from the SFTP server.
        You can use only one wildcard within your path. The wildcard can appear
        inside the path or at the end of the path.
    :param destination_bucket: The bucket to upload to.
    :param destination_path: The destination name of the object in the
        destination Google Cloud Storage bucket.
        If destination_path is not provided file/files will be placed in the
        main bucket path.
        If a wildcard is supplied in the destination_path argument, this is the
        prefix that will be prepended to the final destination objects' paths.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :param mime_type: The mime-type string
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param sftp_prefetch: Whether to enable SFTP prefetch, the default is True.
    :param use_stream: Whether to transfer files using streaming mode, the default is False.
    :param stream_chunk_size: Size of each chunk read from the source file during streaming. 
        Defaults to 104857600 (100 MB). When set below this default, a custom streaming 
        method is used; otherwise, it defaults to the 'upload_from_file' method from 
        Google Cloud Storage client library, which includes robust retry mechanisms. 
        While the default value is recommended for its reliability, reducing the chunk 
        size can lead to more frequent progress logging and lower memory usage, 
        which might be beneficial in scenarios with slower network speeds or limited 
        system resources.
    :param source_stream_wrapper: (Optional) A custom wrapper for the source_stream. 
        This can be used to implement custom progress logging or other functionalities. 
        Defaults to None, which means no custom wrapper is applied.
    :param log_interval: (Optional) Specifies the interval in bytes for logging progress 
        when a manual streaming approach is used (i.e., when use_stream is true and 
        stream_chunk_size is less than 104857600). If a custom logging frequency is 
        required outside these conditions, consider using source_stream_wrapper. 
        Default behavior is to log at the end of each chunk transfer.
    """

    template_fields: Sequence[str] = (
        "source_path",
        "destination_path",
        "destination_bucket",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        source_path: str,
        destination_bucket: str,
        destination_path: str | None = None,
        sftp_conn_id: str = "ssh_default",
        gcp_conn_id: str = "google_cloud_default",
        mime_type: str = "application/octet-stream",
        gzip: bool = False,
        move_object: bool = False,
        impersonation_chain: str | Sequence[str] | None = None,
        sftp_prefetch: bool = True,
        use_stream: bool = False,
        log_stream_progress: bool = False,
        stream_chunk_size = _DEFAULT_CHUNKSIZE, # 1024 * 1024 B * 100 = 100 MB
        source_stream_wrapper = None, 
        log_interval = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.source_path = source_path
        self.destination_path = self._set_destination_path(destination_path)
        self.destination_bucket = self._set_bucket_name(destination_bucket)
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.gzip = gzip
        self.sftp_conn_id = sftp_conn_id
        self.move_object = move_object
        self.impersonation_chain = impersonation_chain
        self.sftp_prefetch = sftp_prefetch
        self.use_stream = use_stream
        self.stream_chunk_size = stream_chunk_size
        self.source_stream_wrapper = source_stream_wrapper

    def execute(self, context: Context):
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        sftp_hook = SFTPHook(self.sftp_conn_id)

        if WILDCARD in self.source_path:
            total_wildcards = self.source_path.count(WILDCARD)
            if total_wildcards > 1:
                raise AirflowException(
                    "Only one wildcard '*' is allowed in source_path parameter. "
                    f"Found {total_wildcards} in {self.source_path}."
                )

            prefix, delimiter = self.source_path.split(WILDCARD, 1)
            base_path = os.path.dirname(prefix)

            files, _, _ = sftp_hook.get_tree_map(base_path, prefix=prefix, delimiter=delimiter)

            for file in files:
                destination_path = file.replace(base_path, self.destination_path, 1)
                if self.use_stream:
                    self._stream_single_object(sftp_hook, gcs_hook, file, destination_path)
                else:
                    self._copy_single_object(sftp_hook, gcs_hook, file, destination_path)

        else:
            destination_object = (
                self.destination_path if self.destination_path else self.source_path.rsplit("/", 1)[1]
            )
            self._copy_single_object(sftp_hook, gcs_hook, self.source_path, destination_object)

    def _copy_single_object(
        self,
        sftp_hook: SFTPHook,
        gcs_hook: GCSHook,
        source_path: str,
        destination_object: str,
    ) -> None:
        """Helper function to copy single object."""
        self.log.info(
            "Executing copy of %s to gs://%s/%s",
            source_path,
            self.destination_bucket,
            destination_object,
        )

        with NamedTemporaryFile("w") as tmp:
            sftp_hook.retrieve_file(source_path, tmp.name, prefetch=self.sftp_prefetch)

            gcs_hook.upload(
                bucket_name=self.destination_bucket,
                object_name=destination_object,
                filename=tmp.name,
                mime_type=self.mime_type,
                gzip=self.gzip,
            )

        if self.move_object:
            self.log.info("Executing delete of %s", source_path)
            sftp_hook.delete_file(source_path)

    def _stream_single_object(
        self, 
        sftp_hook: SFTPHook, 
        gcs_hook: GCSHook, 
        source_path: str, 
        destination_object: str
    ) -> None:
        """Helper function to stream a single object with robust handling and logging."""
        self.log.info(
            "Starting stream of %s to gs://%s/%s",
            source_path,
            self.destination_bucket,
            destination_object,
        )

        client = gcs_hook.get_conn()
        dest_bucket = client.bucket(self.destination_bucket)
        dest_blob = dest_bucket.blob(destination_object)
        temp_destination_object = f"{destination_object}.tmp"
        temp_dest_blob = dest_bucket.blob(temp_destination_object)

        # Check and delete any existing temp file from previous failed attempts
        if temp_dest_blob.exists():
            self.log.warning(f"Temporary file {temp_destination_object} found, deleting for fresh upload.")
            temp_dest_blob.delete()

        with sftp_hook.get_conn().file(source_path, 'rb') as source_stream:
            if self.source_stream_wrapper:
                source_stream = self.source_stream_wrapper(source_stream)

            total_bytes_uploaded = 0
            interval_bytes_uploaded = 0
            if self.stream_chunk_size and self.stream_chunk_size < _DEFAULT_CHUNKSIZE:
                # Use manual stream transfer
                with temp_dest_blob.open("wb") as write_stream:
                    while True:
                        chunk = source_stream.read(self.stream_chunk_size)
                        if not chunk:
                            break
                        write_stream.write(chunk)
                        total_bytes_uploaded += len(chunk)
                        interval_bytes_uploaded += len(chunk)

                        # Log upload progress at intervals
                        if self.log_interval and interval_bytes_uploaded >= self.log_interval:
                            self.log.info(f"Uploaded {total_bytes_uploaded} bytes so far.")
                            interval_bytes_uploaded %= self.log_interval
            else:
                # Use the upload_from_file method
                temp_dest_blob.upload_from_file(source_stream)

        # Copy from temp blob to final destination
        if temp_dest_blob.exists():
            self.log.info("Copying from temporary location to final destination.")
            dest_bucket.copy_blob(temp_dest_blob, dest_bucket, destination_object)
            temp_dest_blob.delete()  # Clean up the temp file
        else:
            self.log.error("Upload failed: Temporary file not found after upload.")

        if self.move_object:
            self.log.info("Deleting source file %s", source_path)
            sftp_hook.delete_file(source_path)
            
    @staticmethod
    def _set_destination_path(path: str | None) -> str:
        if path is not None:
            return path.lstrip("/") if path.startswith("/") else path
        return ""

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")
