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
from typing import Collection, Optional

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property
from google.api_core.client_info import ClientInfo
from google.cloud import storage

from airflow import version
from airflow.providers.google.cloud.utils.credentials_provider import get_credentials_and_project_id
from airflow.utils.log.remote_file_task_handler import RemoteFileTaskHandler

_DEFAULT_SCOPESS = frozenset(
    [
        "https://www.googleapis.com/auth/devstorage.read_write",
    ]
)


class GCSTaskHandler(RemoteFileTaskHandler):
    """
    GCSTaskHandler is a python log handler that handles and reads
    task instance logs. It extends airflow RemoteFileTaskHandler and
    uploads to and reads from GCS remote storage. Upon log reading
    failure, it reads from host machine's local disk.

    :param base_log_folder: Base log folder to place logs.
    :type base_log_folder: str
    :param gcs_log_folder: Path to a remote location where logs will be saved. It must have the prefix
        ``gs://``. For example: ``gs://bucket/remote/log/location``
    :type gcs_log_folder: str
    :param filename_template: template filename string
    :type filename_template: str
    :param gcp_key_path: Path to Google Cloud Service Account file (JSON). Mutually exclusive with
        gcp_keyfile_dict.
        If omitted, authorization based on `the Application Default Credentials
        <https://cloud.google.com/docs/authentication/production#finding_credentials_automatically>`__ will
        be used.
    :type gcp_key_path: str
    :param gcp_keyfile_dict: Dictionary of keyfile parameters. Mutually exclusive with gcp_key_path.
    :type gcp_keyfile_dict: dict
    :param gcp_scopes: Comma-separated string containing OAuth2 scopes
    :type gcp_scopes: str
    :param project_id: Project ID to read the secrets from. If not passed, the project ID from credentials
        will be used.
    :type project_id: str
    """

    def __init__(
        self,
        *,
        base_log_folder: str,
        gcs_log_folder: str,
        filename_template: str,
        gcp_key_path: Optional[str] = None,
        gcp_keyfile_dict: Optional[dict] = None,
        gcp_scopes: Optional[Collection[str]] = _DEFAULT_SCOPESS,
        project_id: Optional[str] = None,
    ):
        super().__init__(base_log_folder, filename_template, gcs_log_folder)
        self._hook = None
        self.gcp_key_path = gcp_key_path
        self.gcp_keyfile_dict = gcp_keyfile_dict
        self.scopes = gcp_scopes
        self.project_id = project_id

    @cached_property
    def client(self) -> storage.Client:
        """Returns GCS Client."""
        credentials, project_id = get_credentials_and_project_id(
            key_path=self.gcp_key_path,
            keyfile_dict=self.gcp_keyfile_dict,
            scopes=self.scopes,
            disable_logging=True,
        )
        return storage.Client(
            credentials=credentials,
            client_info=ClientInfo(client_library_version='airflow_v' + version.version),
            project=self.project_id if self.project_id else project_id,
        )

    def remote_write(self, log: str, remote_log_location: str, append: bool = True) -> bool:
        try:
            blob = storage.Blob.from_string(remote_log_location, self.client)
            old_log = blob.download_as_bytes().decode()
            log = '\n'.join([old_log, log]) if old_log else log
        except Exception as e:
            if not hasattr(e, 'resp') or e.resp.get('status') != '404':
                log = f'*** Previous log discarded: {str(e)}\n\n' + log
                self.log.info("Previous log discarded: %s", e)

        try:
            blob = storage.Blob.from_string(remote_log_location, self.client)
            blob.upload_from_string(log, content_type="text/plain")
        except Exception as e:
            self.log.error('Could not write logs to %s: %s', remote_log_location, e)
            return False
        else:
            return True

    def remote_read(self, remote_log_location: str) -> str:
        blob = storage.Blob.from_string(remote_log_location, self.client)
        remote_log = blob.download_as_bytes().decode()
        return remote_log

    def remote_log_exists(self, remote_log_location: str) -> bool:
        return storage.Blob.from_string(remote_log_location, self.client).exists()
