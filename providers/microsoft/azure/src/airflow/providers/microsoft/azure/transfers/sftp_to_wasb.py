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
"""This module contains SFTP to Azure Blob Storage operator."""

from __future__ import annotations

import os
from collections import namedtuple
from collections.abc import Sequence
from functools import cached_property
from tempfile import NamedTemporaryFile
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowException, BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.azure.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.providers.sftp.hooks.sftp import SFTPHook

if TYPE_CHECKING:
    from airflow.sdk import Context

WILDCARD = "*"
SftpFile = namedtuple("SftpFile", "sftp_file_path, blob_name")


class SFTPToWasbOperator(BaseOperator):
    """
    Transfer files to Azure Blob Storage from SFTP server.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SFTPToWasbOperator`

    :param sftp_source_path: The sftp remote path. This is the specified file path
        for downloading the single file or multiple files from the SFTP server.
        You can use only one wildcard within your path. The wildcard can appear
        inside the path or at the end of the path.
    :param container_name: Name of the container.
    :param blob_prefix: Prefix to name a blob.
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :param wasb_conn_id: Reference to the wasb connection.
    :param load_options: Optional keyword arguments that
        ``WasbHook.load_file()`` takes.
    :param move_object: When move object is True, the object is moved instead
        of copied to the new location. This is the equivalent of a mv command
        as opposed to a cp command.
    :param wasb_overwrite_object: Whether the blob to be uploaded
        should overwrite the current data.
        When wasb_overwrite_object is True, it will overwrite the existing data.
        If set to False, the operation might fail with
        ResourceExistsError in case a blob object already exists.
    :param create_container: Attempt to create the target container prior to uploading the blob. This is
        useful if the target container may not exist yet. Defaults to False.
    """

    template_fields: Sequence[str] = ("sftp_source_path", "container_name", "blob_prefix")

    def __init__(
        self,
        *,
        sftp_source_path: str,
        container_name: str,
        blob_prefix: str = "",
        sftp_conn_id: str = "sftp_default",
        wasb_conn_id: str = "wasb_default",
        load_options: dict | None = None,
        move_object: bool = False,
        wasb_overwrite_object: bool = False,
        create_container: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.sftp_source_path = sftp_source_path
        self.blob_prefix = blob_prefix
        self.sftp_conn_id = sftp_conn_id
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.wasb_conn_id = wasb_conn_id
        self.load_options = load_options or {"overwrite": wasb_overwrite_object}
        self.move_object = move_object
        self.create_container = create_container

    def dry_run(self) -> None:
        if not AIRFLOW_V_3_0_PLUS:
            raise NotImplementedError("Not implemented for Airflow 3.")
        super().dry_run()
        sftp_files: list[SftpFile] = self.get_sftp_files_map()
        for file in sftp_files:
            self.log.info(
                "Process will upload file from (SFTP) %s to wasb://%s as %s",
                file.sftp_file_path,
                self.container_name,
                file.blob_name,
            )
            if self.move_object:
                self.log.info("Executing delete of %s", file)

    def execute(self, context: Context) -> None:
        """Upload a file from SFTP to Azure Blob Storage."""
        sftp_files: list[SftpFile] = self.get_sftp_files_map()
        uploaded_files = self.copy_files_to_wasb(sftp_files)
        if self.move_object:
            self.delete_files(uploaded_files)

    def get_sftp_files_map(self) -> list[SftpFile]:
        """Get SFTP files from the source path, it may use a WILDCARD to this end."""
        sftp_files = []

        sftp_complete_path, prefix, delimiter = self.get_tree_behavior()

        found_files, _, _ = self.sftp_hook.get_tree_map(
            sftp_complete_path, prefix=prefix, delimiter=delimiter
        )

        self.log.info("Found %d files at sftp source path: %s", len(found_files), self.sftp_source_path)

        for file in found_files:
            future_blob_name = self.get_full_path_blob(file)
            sftp_files.append(SftpFile(file, future_blob_name))

        return sftp_files

    def get_tree_behavior(self) -> tuple[str, str | None, str | None]:
        """Extract from source path the tree behavior to interact with the remote folder."""
        self.check_wildcards_limit()

        if self.source_path_contains_wildcard:
            prefix, delimiter = self.sftp_source_path.split(WILDCARD, 1)

            sftp_complete_path = os.path.dirname(prefix)

            return sftp_complete_path, prefix, delimiter

        return self.sftp_source_path, None, None

    def check_wildcards_limit(self) -> None:
        """Check if there are multiple wildcards used in the SFTP source path."""
        total_wildcards = self.sftp_source_path.count(WILDCARD)
        if total_wildcards > 1:
            raise AirflowException(
                "Only one wildcard '*' is allowed in sftp_source_path parameter. "
                f"Found {total_wildcards} in {self.sftp_source_path}."
            )

    @property
    def source_path_contains_wildcard(self) -> bool:
        """Checks if the SFTP source path contains a wildcard."""
        return WILDCARD in self.sftp_source_path

    @cached_property
    def sftp_hook(self) -> SFTPHook:
        """Property of sftp hook to be reused."""
        return SFTPHook(self.sftp_conn_id)

    def get_full_path_blob(self, file: str) -> str:
        """Get a blob name based on the previous name and a blob_prefix variable."""
        return self.blob_prefix + os.path.basename(file)

    def copy_files_to_wasb(self, sftp_files: list[SftpFile]) -> list[str]:
        """Upload a list of files from sftp_files to Azure Blob Storage with a new Blob Name."""
        uploaded_files = []
        wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        for file in sftp_files:
            with NamedTemporaryFile("w") as tmp:
                self.sftp_hook.retrieve_file(file.sftp_file_path, tmp.name)
                self.log.info(
                    "Uploading %s to wasb://%s as %s",
                    file.sftp_file_path,
                    self.container_name,
                    file.blob_name,
                )
                wasb_hook.load_file(
                    tmp.name,
                    self.container_name,
                    file.blob_name,
                    self.create_container,
                    **self.load_options,
                )

                uploaded_files.append(file.sftp_file_path)

        return uploaded_files

    def delete_files(self, uploaded_files: list[str]) -> None:
        """Delete files at SFTP which have been moved to Azure Blob Storage."""
        for sftp_file_path in uploaded_files:
            self.log.info("Executing delete of %s", sftp_file_path)
            self.sftp_hook.delete_file(sftp_file_path)
