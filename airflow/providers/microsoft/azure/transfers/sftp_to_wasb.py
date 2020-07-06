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
"""
This module contains SFTP to Azure Blob Storage operator.
"""
import os
from tempfile import NamedTemporaryFile

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.utils.decorators import apply_defaults

WILDCARD = "*"
SFTP_FILE_PATH = "SFTP_FILE_PATH"
BLOB_NAME = "BLOB_NAME"


class SFTPToWasbOperator(BaseOperator):
    """
    Transfer files to Azure Blob Storage from SFTP server.

    :param sftp_source_path: The sftp remote path. This is the specified file path
        for downloading the single file or multiple files from the SFTP server.
        You can use only one wildcard within your path. The wildcard can appear
        inside the path or at the end of the path.
    :type sftp_source_path: str
    :param container_name: Name of the container.
    :type container_name: str
    :param blob_name: Name of the blob.
    :type blob_name: str
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param wasb_conn_id: Reference to the wasb connection.
    :type wasb_conn_id: str
    :param load_options: Optional keyword arguments that
        `WasbHook.load_file()` takes.
    :type load_options: dict
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :type move_object: bool
    """
    template_fields = ("sftp_source_path", "container_name", "blob_name")

    @apply_defaults
    def __init__(
        self,
        sftp_source_path: str,
        container_name: str,
        blob_name: str,
        sftp_conn_id: str = "ssh_default",
        wasb_conn_id: str = 'wasb_default',
        load_options=None,
        move_object: bool = False,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.sftp_source_path = sftp_source_path
        self.sftp_conn_id = sftp_conn_id
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.blob_name = blob_name
        self.wasb_conn_id = wasb_conn_id
        self.load_options = load_options if load_options is not None else {}
        self.move_object = move_object

    def execute(self, context):
        """Upload a file from SFTP to Azure Blob Storage."""
        (sftp_hook, sftp_files) = self.get_sftp_files_map()
        uploaded_files = self.upload_wasb(sftp_hook, sftp_files)
        self.sftp_move(sftp_hook, uploaded_files)

    def get_sftp_files_map(self):
        """Get SFTP files from the source path, it may use a WILDCARD to this end."""
        sftp_files = []
        sftp_hook = SFTPHook(self.sftp_conn_id)

        if WILDCARD in self.sftp_source_path:
            self.check_wildcards_limit()

            prefix, delimiter = self.sftp_source_path.split(WILDCARD, 1)
            sftp_complete_path = os.path.dirname(prefix)

            found_files, _, _ = sftp_hook.get_tree_map(
                sftp_complete_path, prefix=prefix, delimiter=delimiter
            )

            self.log.info(
                "Found %s files at sftp source path: %s",
                str(len(found_files)),
                self.sftp_source_path
            )

            for file in found_files:
                future_blob_name = os.path.basename(file)
                sftp_files.append({SFTP_FILE_PATH: file, BLOB_NAME: future_blob_name})

        else:
            sftp_files.append({SFTP_FILE_PATH: self.sftp_source_path, BLOB_NAME: self.blob_name})

        return sftp_hook, sftp_files

    def check_wildcards_limit(self):
        """Check if there is multiple Wildcard."""
        total_wildcards = self.sftp_source_path.count(WILDCARD)
        if total_wildcards > 1:
            raise AirflowException(
                "Only one wildcard '*' is allowed in sftp_source_path parameter. "
                "Found {} in {}.".format(total_wildcards, self.sftp_source_path)
            )

    def upload_wasb(self, sftp_hook, sftp_files):
        """Upload a list of files from sftp_files to Azure Blob Storage with a new Blob Name."""
        uploaded_files = []
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        for file in sftp_files:

            with NamedTemporaryFile("w") as tmp:
                sftp_hook.retrieve_file(file[SFTP_FILE_PATH], tmp.name)
                self.log.info(
                    'Uploading %s to wasb://%s as %s',
                    file[SFTP_FILE_PATH],
                    self.container_name,
                    file[BLOB_NAME],
                )
                wasb_hook.load_file(tmp.name,
                                    self.container_name,
                                    file[BLOB_NAME],
                                    **self.load_options)

                uploaded_files.append(file[SFTP_FILE_PATH])

        return uploaded_files

    def sftp_move(self, sftp_hook, uploaded_files):
        """Performs a move of a list of files at SFTP to Azure Blob Storage."""
        if self.move_object:
            for sftp_file_path in uploaded_files:
                self.log.info("Executing delete of %s", sftp_file_path)
                sftp_hook.delete_file(sftp_file_path)
