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
"""Hook for Google Drive service."""

from __future__ import annotations

from typing import IO, Any, Sequence

from googleapiclient.discovery import Resource, build
from googleapiclient.errors import Error as GoogleApiClientError
from googleapiclient.http import HttpRequest, MediaFileUpload

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleDriveHook(GoogleBaseHook):
    """
    Hook for the Google Drive APIs.

    :param api_version: API version used (for example v3).
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        account from the list granting this role to the originating account.
    """

    _conn: Resource | None = None

    def __init__(
        self,
        api_version: str = "v3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self) -> Any:
        """
        Retrieve the connection to Google Drive.

        :return: Google Drive services object.
        """
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "drive", self.api_version, http=http_authorized, cache_discovery=False
            )
        return self._conn

    def _ensure_folders_exists(self, path: str, folder_id: str) -> str:
        service = self.get_conn()
        current_parent = folder_id
        folders = path.split("/")
        depth = 0
        # First tries to enter directories
        for current_folder in folders:
            self.log.debug(
                "Looking for %s directory with %s parent", current_folder, current_parent
            )
            conditions = [
                "trashed=false",
                "mimeType='application/vnd.google-apps.folder'",
                f"name='{current_folder}'",
                f"'{current_parent}' in parents",
            ]
            result = (
                service.files()
                .list(
                    q=" and ".join(conditions),
                    spaces="drive",
                    fields="files(id, name)",
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                )
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
                    service.files()
                    .create(
                        body=file_metadata,
                        fields="id",
                        supportsAllDrives=True,
                    )
                    .execute(num_retries=self.num_retries)
                )
                self.log.info("Created %s directory", current_folder)

                current_parent = file.get("id")
        # Return the ID of the last directory
        return current_parent

    def get_media_request(self, file_id: str) -> HttpRequest:
        """
        Return a get_media http request to a Google Drive object.

        :param file_id: The Google Drive file id
        :return: request
        """
        service = self.get_conn()
        request = service.files().get_media(fileId=file_id)
        return request

    def exists(
        self,
        folder_id: str,
        file_name: str,
        drive_id: str | None = None,
        *,
        include_trashed: bool = True,
    ) -> bool:
        """
        Check to see if a file exists within a Google Drive folder.

        :param folder_id: The id of the Google Drive folder in which the file resides
        :param file_name: The name of a file in Google Drive
        :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
        :param include_trashed: Whether to include objects in trash or not, default True as in Google API.

        :return: True if the file exists, False otherwise
        """
        return bool(
            self.get_file_id(
                folder_id=folder_id,
                file_name=file_name,
                include_trashed=include_trashed,
                drive_id=drive_id,
            )
        )

    def _get_file_info(self, file_id: str):
        """
        Return Google API file_info object containing id, name, parents in the response.

        https://developers.google.com/drive/api/v3/reference/files/get

        :param file_id: id as string representation of interested file
        :return: file
        """
        file_info = (
            self.get_conn()
            .files()
            .get(
                fileId=file_id,
                fields="id,name,parents",
                supportsAllDrives=True,
            )
            .execute(num_retries=2)
        )
        return file_info

    def _resolve_file_path(self, file_id: str) -> str:
        """
        Return the full Google Drive path for given file_id.

        :param file_id: The id of a file in Google Drive
        :return: Google Drive full path for a file
        """
        has_reached_root = False
        current_file_id = file_id
        path: str = ""
        while not has_reached_root:
            # current_file_id can be file or directory id, Google API treats them the same way.
            file_info = self._get_file_info(current_file_id)
            if current_file_id == file_id:
                path = f'{file_info["name"]}'
            else:
                path = f'{file_info["name"]}/{path}'

            # Google API returns parents array if there is at least one object inside
            if "parents" in file_info and len(file_info["parents"]) == 1:
                # https://developers.google.com/drive/api/guides/ref-single-parent
                current_file_id = file_info["parents"][0]
            else:
                has_reached_root = True
        return path

    def get_file_id(
        self,
        folder_id: str,
        file_name: str,
        drive_id: str | None = None,
        *,
        include_trashed: bool = True,
    ) -> dict:
        """
        Return the file id of a Google Drive file.

        :param folder_id: The id of the Google Drive folder in which the file resides
        :param file_name: The name of a file in Google Drive
        :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
        :param include_trashed: Whether to include objects in trash or not, default True as in Google API.

        :return: Google Drive file id if the file exists, otherwise None
        """
        query = f"name = '{file_name}'"
        if folder_id:
            query += f" and parents in '{folder_id}'"

        if not include_trashed:
            query += " and trashed=false"

        service = self.get_conn()
        if drive_id:
            files = (
                service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="files(id, mimeType)",
                    orderBy="modifiedTime desc",
                    driveId=drive_id,
                    includeItemsFromAllDrives=True,
                    supportsAllDrives=True,
                    corpora="drive",
                )
                .execute(num_retries=self.num_retries)
            )
        else:
            files = (
                service.files()
                .list(
                    q=query,
                    spaces="drive",
                    fields="files(id, mimeType)",
                    orderBy="modifiedTime desc",
                )
                .execute(num_retries=self.num_retries)
            )
        file_metadata = {}
        if files["files"]:
            file_metadata = {
                "id": files["files"][0]["id"],
                "mime_type": files["files"][0]["mimeType"],
            }
        return file_metadata

    def upload_file(
        self,
        local_location: str,
        remote_location: str,
        chunk_size: int = 100 * 1024 * 1024,
        resumable: bool = False,
        folder_id: str = "root",
        show_full_target_path: bool = True,
    ) -> str:
        """
        Upload a file that is available locally to a Google Drive service.

        :param local_location: The path where the file is available.
        :param remote_location: The path where the file will be send
        :param chunk_size: File will be uploaded in chunks of this many bytes. Only
            used if resumable=True. Pass in a value of -1 if the file is to be
            uploaded as a single chunk. Note that Google App Engine has a 5MB limit
            on request size, so you should never set your chunk size larger than 5MB,
            or to -1.
        :param resumable: True if this is a resumable upload. False means upload
            in a single request.
        :param folder_id: The base/root folder id for remote_location (part of the drive URL of a folder).
        :param show_full_target_path: If true then it reveals full available file path in the logs.
        :return: File ID
        """
        service = self.get_conn()
        directory_path, _, file_name = remote_location.rpartition("/")
        if directory_path:
            parent = self._ensure_folders_exists(path=directory_path, folder_id=folder_id)
        else:
            parent = folder_id

        file_metadata = {"name": file_name, "parents": [parent]}
        media = MediaFileUpload(local_location, chunksize=chunk_size, resumable=resumable)
        file = (
            service.files()
            .create(
                body=file_metadata, media_body=media, fields="id", supportsAllDrives=True
            )
            .execute(num_retries=self.num_retries)
        )
        file_id = file.get("id")

        upload_location = remote_location

        if folder_id != "root":
            try:
                upload_location = self._resolve_file_path(folder_id)
            except GoogleApiClientError as e:
                self.log.warning(
                    "A problem has been encountered when trying to resolve file path: ", e
                )

        if show_full_target_path:
            self.log.info(
                "File %s uploaded to gdrive://%s.", local_location, upload_location
            )
        else:
            self.log.info(
                "File %s has been uploaded successfully to gdrive", local_location
            )
        return file_id

    def download_file(
        self, file_id: str, file_handle: IO, chunk_size: int = 100 * 1024 * 1024
    ):
        """
        Download a file from Google Drive.

        :param file_id: the id of the file
        :param file_handle: file handle used to write the content to
        :param chunk_size: File will be downloaded in chunks of this many bytes.
        """
        request = self.get_media_request(file_id=file_id)
        self.download_content_from_request(
            file_handle=file_handle, request=request, chunk_size=chunk_size
        )
