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
"""This file contains Google Drive operators"""
from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Sequence

from airflow.exceptions import AirflowFailException
from airflow.models import BaseOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFilesystemToGoogleDriveOperator(BaseOperator):
    """
    Upload a list of files to a Google Drive folder.
    This operator uploads a list of local files to a Google Drive folder.
    The local files can be deleted after upload (optional)

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LocalFilesystemToGoogleDriveOperator`

    :param local_paths: Python list of local file paths
    :param drive_folder: path of the Drive folder
    :param gcp_conn_id: Airflow Connection ID for GCP
    :param delete: should the local files be deleted after upload?
    :param ignore_if_missing: if True, then don't fail even if all files
        can't be uploaded.
    :param chunk_size: File will be uploaded in chunks of this many bytes. Only
        used if resumable=True. Pass in a value of -1 if the file is to be
        uploaded as a single chunk. Note that Google App Engine has a 5MB limit
        on request size, so you should never set your chunk size larger than 5MB,
        or to -1.
    :param resumable: True if this is a resumable upload. False means upload
        in a single request.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account
    :return: Remote file ids after upload
    :rtype: Sequence[str]
    """

    template_fields = (
        "local_paths",
        "drive_folder",
    )

    def __init__(
        self,
        local_paths: Sequence[Path] | Sequence[str],
        drive_folder: Path | str,
        gcp_conn_id: str = "google_cloud_default",
        delete: bool = False,
        ignore_if_missing: bool = False,
        chunk_size: int = 100 * 1024 * 1024,
        resumable: bool = False,
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.local_paths = local_paths
        self.drive_folder = drive_folder
        self.gcp_conn_id = gcp_conn_id
        self.delete = delete
        self.ignore_if_missing = ignore_if_missing
        self.chunk_size = chunk_size
        self.resumable = resumable
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> list[str]:
        hook = GoogleDriveHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        remote_file_ids = []

        for local_path in self.local_paths:
            self.log.info("Uploading file to Google Drive: %s", local_path)

            try:
                remote_file_id = hook.upload_file(
                    local_location=str(local_path),
                    remote_location=str(Path(self.drive_folder) / Path(local_path).name),
                    chunk_size=self.chunk_size,
                    resumable=self.resumable,
                )

                remote_file_ids.append(remote_file_id)

                if self.delete:
                    os.remove(local_path)
                    self.log.info("Deleted local file: %s", local_path)
            except FileNotFoundError:
                self.log.warning("File can't be found: %s", local_path)
            except OSError:
                self.log.warning("An OSError occurred for file: %s", local_path)

        if not self.ignore_if_missing and len(remote_file_ids) < len(self.local_paths):
            raise AirflowFailException("Some files couldn't be uploaded")
        return remote_file_ids
