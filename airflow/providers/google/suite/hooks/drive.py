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
"""Hook for Google Drive service"""
from typing import Any, Optional, Sequence, Union

from googleapiclient.discovery import Resource, build
from googleapiclient.http import MediaFileUpload

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleDriveHook(GoogleBaseHook):
    """
    Hook for the Google Drive APIs.

    :param api_version: API version used (for example v3).
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
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
        account from the list granting this role to the originating account.
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    _conn = None  # type: Optional[Resource]

    def __init__(
        self,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self) -> Any:
        """
        Retrieves the connection to Google Drive.

        :return: Google Drive services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build("drive", self.api_version, http=http_authorized, cache_discovery=False)
        return self._conn

    def _ensure_folders_exists(self, path: str) -> str:
        service = self.get_conn()
        current_parent = "root"
        folders = path.split("/")
        depth = 0
        # First tries to enter directories
        for current_folder in folders:
            self.log.debug("Looking for %s directory with %s parent", current_folder, current_parent)
            conditions = [
                "mimeType = 'application/vnd.google-apps.folder'",
                f"name='{current_folder}'",
                f"'{current_parent}' in parents",
            ]
            result = (
                service.files()  # pylint: disable=no-member
                .list(q=" and ".join(conditions), spaces="drive", fields="files(id, name)")
                .execute(num_retries=self.num_retries)
            )
            files = result.get("files", [])
            if not files:
                self.log.info("Not found %s directory", current_folder)
                # If the directory does not exist, break loops
                break
            depth += 1
            current_parent = files[0].get("id")

        # Check if there are directories to process
        if depth != len(folders):
            # Create missing directories
            for current_folder in folders[depth:]:
                file_metadata = {
                    "name": current_folder,
                    "mimeType": "application/vnd.google-apps.folder",
                    "parents": [current_parent],
                }
                file = (
                    service.files()  # pylint: disable=no-member
                    .create(body=file_metadata, fields="id")
                    .execute(num_retries=self.num_retries)
                )
                self.log.info("Created %s directory", current_folder)

                current_parent = file.get("id")
        # Return the ID of the last directory
        return current_parent

    def upload_file(self, local_location: str, remote_location: str) -> str:
        """
        Uploads a file that is available locally to a Google Drive service.

        :param local_location: The path where the file is available.
        :type local_location: str
        :param remote_location: The path where the file will be send
        :type remote_location: str
        :return: File ID
        :rtype: str
        """
        service = self.get_conn()
        directory_path, _, filename = remote_location.rpartition("/")
        if directory_path:
            parent = self._ensure_folders_exists(directory_path)
        else:
            parent = "root"

        file_metadata = {"name": filename, "parents": [parent]}
        media = MediaFileUpload(local_location)
        file = (
            service.files()  # pylint: disable=no-member
            .create(body=file_metadata, media_body=media, fields="id")
            .execute(num_retries=self.num_retries)
        )
        self.log.info("File %s uploaded to gdrive://%s.", local_location, remote_location)
        return file.get("id")
