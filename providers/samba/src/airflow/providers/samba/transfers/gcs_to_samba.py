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
"""This module contains Google Cloud Storage to Samba operator."""

from __future__ import annotations

import os
from collections.abc import Sequence
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowException, BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.samba.hooks.samba import SambaHook

WILDCARD = "*"

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class GCSToSambaOperator(BaseOperator):
    """
    Transfer files from a Google Cloud Storage bucket to SMB server.

    .. code-block:: python

        with models.DAG(
            "example_gcs_to_smb",
            start_date=datetime(2020, 6, 19),
            schedule=None,
        ) as dag:
            # downloads file to media/folder/subfolder/file.txt
            copy_file_from_gcs_to_smb = GCSToSambaOperator(
                task_id="file-copy-gcs-to-smb",
                source_bucket="test-gcs-sftp-bucket-name",
                source_object="folder/subfolder/file.txt",
                destination_path="media",
            )

            # moves file to media/data.txt
            move_file_from_gcs_to_smb = GCSToSambaOperator(
                task_id="file-move-gcs-to-smb",
                source_bucket="test-gcs-sftp-bucket-name",
                source_object="folder/subfolder/data.txt",
                destination_path="media",
                move_object=True,
                keep_directory_structure=False,
            )

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GCSToSambaOperator`

    :param source_bucket: The source Google Cloud Storage bucket where the
         object is. (templated)
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket. (templated)
        You can use only one wildcard for objects (filenames) within your
        bucket. The wildcard can appear inside the object name or at the
        end of the object name. Appending a wildcard to the bucket name is
        unsupported.
    :param destination_path: The SMB remote path. This is the specified directory path in
        the SMB share name for uploading files to the SMB server.
    :param keep_directory_structure: (Optional) When set to False the path of the file
         on the bucket is recreated within path passed in destination_path.
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud.
    :param samba_conn_id: The SMB connection id. The name or identifier for
        establishing a connection to the SMB server.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param buffer_size: Optional specification of the size in bytes of the chunks sent to
        Samba. Larger buffer lengths may decrease the time to upload large files. The default
        length is determined by shutil, which is 64 KB.
    """

    template_fields: Sequence[str] = (
        "source_bucket",
        "source_object",
        "destination_path",
        "impersonation_chain",
    )
    ui_color = "#f0eee4"

    def __init__(
        self,
        *,
        source_bucket: str,
        source_object: str,
        destination_path: str,
        keep_directory_structure: bool = True,
        move_object: bool = False,
        gcp_conn_id: str = "google_cloud_default",
        samba_conn_id: str = "samba_default",
        impersonation_chain: str | Sequence[str] | None = None,
        buffer_size: int | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_path = destination_path
        self.keep_directory_structure = keep_directory_structure
        self.move_object = move_object
        self.gcp_conn_id = gcp_conn_id
        self.samba_conn_id = samba_conn_id
        self.impersonation_chain = impersonation_chain
        self.sftp_dirs = None
        self.buffer_size = buffer_size

    def execute(self, context: Context):
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        samba_hook = SambaHook(samba_conn_id=self.samba_conn_id)

        if WILDCARD in self.source_object:
            total_wildcards = self.source_object.count(WILDCARD)
            if total_wildcards > 1:
                raise AirflowException(
                    "Only one wildcard '*' is allowed in source_object parameter. "
                    f"Found {total_wildcards} in {self.source_object}."
                )

            prefix, delimiter = self.source_object.split(WILDCARD, 1)
            prefix_dirname = os.path.dirname(prefix)
            objects = gcs_hook.list(self.source_bucket, prefix=prefix, delimiter=delimiter)
            # TODO: After deprecating delimiter and wildcards in source objects,
            #       remove the previous line and uncomment the following:
            # match_glob = f"**/*{delimiter}" if delimiter else None
            # objects = gcs_hook.list(self.source_bucket, prefix=prefix, match_glob=match_glob)

            for source_object in objects:
                destination_path = self._resolve_destination_path(source_object, prefix=prefix_dirname)
                self._copy_single_object(
                    gcs_hook, samba_hook, source_object, destination_path, self.buffer_size
                )

            self.log.info("Done. Uploaded '%d' files to %s", len(objects), self.destination_path)
        else:
            destination_path = self._resolve_destination_path(self.source_object)
            self._copy_single_object(
                gcs_hook, samba_hook, self.source_object, destination_path, self.buffer_size
            )
            self.log.info("Done. Uploaded '%s' file to %s", self.source_object, destination_path)

    def _resolve_destination_path(self, source_object: str, prefix: str | None = None) -> str:
        if not self.keep_directory_structure:
            if prefix:
                source_object = os.path.relpath(source_object, start=prefix)
            else:
                source_object = os.path.basename(source_object)
        return os.path.join(self.destination_path, source_object)

    def _copy_single_object(
        self,
        gcs_hook: GCSHook,
        samba_hook: SambaHook,
        source_object: str,
        destination_path: str,
        buffer_size: int | None = None,
    ) -> None:
        """Copy single object."""
        self.log.info(
            "Executing copy of gs://%s/%s to %s",
            self.source_bucket,
            source_object,
            destination_path,
        )

        dir_path = os.path.dirname(destination_path)
        samba_hook.makedirs(dir_path, exist_ok=True)

        with NamedTemporaryFile("w") as tmp:
            gcs_hook.download(
                bucket_name=self.source_bucket,
                object_name=source_object,
                filename=tmp.name,
            )
            samba_hook.push_from_local(destination_path, tmp.name, buffer_size=buffer_size)

        if self.move_object:
            self.log.info("Executing delete of gs://%s/%s", self.source_bucket, source_object)
            gcs_hook.delete(self.source_bucket, source_object)
