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

import os
from pathlib import Path

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class WasbDagBundle(BaseDagBundle):
    """
    WASB Dag bundle - exposes a directory in Azure Blob Storage as a Dag bundle.

    This allows Airflow to load Dags directly from an Azure Blob Storage container.

    :param wasb_conn_id: Airflow connection ID for Azure Blob Storage. Defaults to WasbHook.default_conn_name.
    :param container_name: The name of the blob container containing the Dag files.
    :param prefix: Optional subdirectory within the container where the Dags are stored.
        If empty, Dags are assumed to be at the root of the container.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        wasb_conn_id: str = WasbHook.default_conn_name,
        container_name: str,
        prefix: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.prefix = prefix
        self.wasb_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            container_name=self.container_name,
            prefix=self.prefix,
            wasb_conn_id=self.wasb_conn_id,
        )
        self._wasb_hook: WasbHook | None = None

    def _initialize(self):
        with self.lock():
            if not self.wasb_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.wasb_dags_dir)
                os.makedirs(self.wasb_dags_dir)

            if not self.wasb_dags_dir.is_dir():
                raise AirflowException(f"Local Dags path: {self.wasb_dags_dir} is not a directory.")

            if not self.wasb_hook.check_for_container(container_name=self.container_name):
                raise AirflowException(f"WASB container '{self.container_name}' does not exist.")

            if self.prefix:
                if not self.wasb_hook.check_for_prefix(
                    container_name=self.container_name, prefix=self.prefix, delimiter="/"
                ):
                    raise AirflowException(
                        f"WASB prefix 'wasb://{self.container_name}/{self.prefix}' does not exist."
                    )
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def wasb_hook(self):
        if self._wasb_hook is None:
            self._wasb_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        return self._wasb_hook

    def __repr__(self):
        return (
            f"<WasbDagBundle("
            f"name={self.name!r}, "
            f"container_name={self.container_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> str | None:
        """Return the current version of the Dag bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the Dag files."""
        return self.wasb_dags_dir

    def refresh(self) -> None:
        """Refresh the Dag bundle by re-downloading the Dags from Azure Blob Storage."""
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            self._log.debug(
                "Downloading Dags from wasb://%s/%s to %s",
                self.container_name,
                self.prefix,
                self.wasb_dags_dir,
            )
            self.wasb_hook.sync_to_local_dir(
                container_name=self.container_name,
                prefix=self.prefix,
                local_dir=self.wasb_dags_dir,
                delete_stale=True,
            )

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the Dags in Azure Blob Storage. Currently, versioning is not supported.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the Dags in Azure Blob Storage. Currently, versioning is not supported."""
        if self.version:
            raise AirflowException("WASB url with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            return self._view_url_template
        account_name = self.wasb_hook.blob_service_client.account_name
        url = f"https://{account_name}.blob.core.windows.net/{self.container_name}"
        if self.prefix:
            url += f"/{self.prefix}"
        return url
