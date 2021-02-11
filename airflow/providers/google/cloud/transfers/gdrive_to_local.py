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

from io import BytesIO
from typing import Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.utils.decorators import apply_defaults


class GoogleDriveToLocalOperator(BaseOperator):
    """
    Writes a Google Drive file into local Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDriveToGCSOperator`

    :param output_file: Path to downloaded file
    :type output_file: str
    :param folder_id: The folder id of the folder in which the Google Drive file resides
    :type folder_id: str
    :param filename: The name of the file residing in Google Drive
    :type filename: str
    :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
    :type drive_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = [
        "output_file",
        "folder_id",
        "filename",
        "drive_id",
        "impersonation_chain",
    ]

    @apply_defaults
    def __init__(
        self,
        *,
        output_file: str,
        filename: str,
        folder_id: str,
        drive_id: Optional[str] = None,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.output_file = output_file
        self.folder_id = folder_id
        self.drive_id = drive_id
        self.filename = filename
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.file_metadata = None

    def _set_file_metadata(self, gdrive_hook):
        if not self.file_metadata:
            self.file_metadata = gdrive_hook.get_file_id(
                folder_id=self.folder_id, file_name=self.filename, drive_id=self.drive_id
            )
        return self.file_metadata

    def _download_data(self, gdrive_hook: GoogleDriveHook, output_file: str):
        file_handle = BytesIO()
        self._set_file_metadata(gdrive_hook=gdrive_hook)
        file_id = self.file_metadata["id"]
        request = gdrive_hook.get_media_request(file_id=file_id)
        gdrive_hook.download_content_from_request(
            file_handle=file_handle, request=request, chunk_size=104857600
        )

        with open(output_file, "wb") as f:
            f.write(file_handle.getbuffer())

    def execute(self, context):
        self.log.info('Executing download: %s into %s', self.filename, self.output_file)
        gdrive_hook = GoogleDriveHook(
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self._download_data(gdrive_hook=gdrive_hook, output_file=self.output_file)
