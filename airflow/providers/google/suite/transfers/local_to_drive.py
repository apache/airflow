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

import os
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFilesystemToGoogleDriveOperator(BaseOperator):
    """
    Upload a list of files to a Google Drive folder.
    This operator uploads a list of local files to a Google Drive folder.
    The local files can be deleted after upload (optional)

    :param local_paths: Python list of local file paths
    :param drive_folder: path of the Drive folder
    :param gcp_conn_id: Airflow Connection ID for GCP
    :param delete: should the local files be deleted after upload?
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
        'local_paths',
        'drive_folder',
    )

    def __init__(
        self,
        local_paths: Union[Sequence[Path], Sequence[str]],
        drive_folder: Union[Path, str],
        gcp_conn_id: str = "google_cloud_default",
        delete: bool = False,
        chunk_size: int = 100 * 1024 * 1024,
        resumable: bool = False,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.local_paths = local_paths
        self.drive_folder = drive_folder
        self.gcp_conn_id = gcp_conn_id
        self.delete = delete
        self.chunk_size = chunk_size
        self.resumable = resumable
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: "Context") -> List[str]:
        hook = GoogleDriveHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        remote_file_ids = []

        for local_path in self.local_paths:
            self.log.info(f'Uploading file to Google Drive: {local_path}')

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
                    self.log.info(f'Deleted local file: {local_path}')
            except FileNotFoundError:
                self.log.warning(f"File {local_path} can't be found")

        return remote_file_ids
