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
from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LocalFilesystemToWasbOperator(BaseOperator):
    """
    Uploads a file to Azure Blob Storage.

    :param file_path: Path to the file to load. (templated)
    :param container_name: Name of the container. (templated)
    :param blob_name: Name of the blob. (templated)
    :param wasb_conn_id: Reference to the wasb connection.
    :param create_container: Attempt to create the target container prior to uploading the blob. This is
        useful if the target container may not exist yet. Defaults to False.
    :param load_options: Optional keyword arguments that
        `WasbHook.load_file()` takes.
    """

    template_fields: Sequence[str] = ("file_path", "container_name", "blob_name")

    def __init__(
        self,
        *,
        file_path: str,
        container_name: str,
        blob_name: str,
        wasb_conn_id: str = "wasb_default",
        create_container: bool = False,
        load_options: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if load_options is None:
            load_options = {}
        self.file_path = file_path
        self.container_name = container_name
        self.blob_name = blob_name
        self.wasb_conn_id = wasb_conn_id
        self.create_container = create_container
        self.load_options = load_options

    def execute(self, context: Context) -> None:
        """Upload a file to Azure Blob Storage."""
        hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        self.log.info(
            "Uploading %s to wasb://%s as %s",
            self.file_path,
            self.container_name,
            self.blob_name,
        )
        hook.load_file(
            file_path=self.file_path,
            container_name=self.container_name,
            blob_name=self.blob_name,
            create_container=self.create_container,
            **self.load_options,
        )
