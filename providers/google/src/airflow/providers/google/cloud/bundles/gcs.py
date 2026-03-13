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
from google.api_core.exceptions import NotFound

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GCSDagBundle(BaseDagBundle):
    """
    GCS Dag bundle - exposes a directory in GCS as a Dag bundle.

    This allows Airflow to load Dags directly from a GCS bucket.

    :param gcp_conn_id: Airflow connection ID for GCS.  Defaults to GoogleBaseHook.default_conn_name.
    :param bucket_name: The name of the GCS bucket containing the Dag files.
    :param prefix:  Optional subdirectory within the GCS bucket where the Dags are stored.
                    If None, Dags are assumed to be at the root of the bucket (Optional).
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        # Local path where GCS Dags are downloaded
        self.gcs_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            gcp_conn_id=self.gcp_conn_id,
        )
        self._gcs_hook: GCSHook | None = None

    def _initialize(self):
        with self.lock():
            if not self.gcs_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.gcs_dags_dir)
                os.makedirs(self.gcs_dags_dir)

            if not self.gcs_dags_dir.is_dir():
                raise NotADirectoryError(f"Local Dags path: {self.gcs_dags_dir} is not a directory.")

            try:
                self.gcs_hook.get_bucket(bucket_name=self.bucket_name)
            except NotFound:
                raise ValueError(f"GCS bucket '{self.bucket_name}' does not exist.")

            if self.prefix:
                # don't check when prefix is ""
                if not self.gcs_hook.list(bucket_name=self.bucket_name, prefix=self.prefix):
                    raise ValueError(f"GCS prefix 'gs://{self.bucket_name}/{self.prefix}' does not exist.")
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def gcs_hook(self):
        if self._gcs_hook is None:
            try:
                self._gcs_hook: GCSHook = GCSHook(gcp_conn_id=self.gcp_conn_id)  # Initialize GCS hook.
            except AirflowException as e:
                self._log.warning("Could not create GCSHook for connection %s: %s", self.gcp_conn_id, e)
        return self._gcs_hook

    def __repr__(self):
        return (
            f"<GCSDagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
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
        return self.gcs_dags_dir  # Path where Dags are downloaded.

    def refresh(self) -> None:
        """Refresh the Dag bundle by re-downloading the Dags from GCS."""
        if self.version:
            raise ValueError("Refreshing a specific version is not supported")

        with self.lock():
            self._log.debug(
                "Downloading Dags from gs://%s/%s to %s", self.bucket_name, self.prefix, self.gcs_dags_dir
            )
            self.gcs_hook.sync_to_local_dir(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                local_dir=self.gcs_dags_dir,
                delete_stale=True,
            )

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the Dags in GCS. Currently, versioning is not supported.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the Dags in GCS. Currently, versioning is not supported."""
        if self.version:
            raise ValueError("GCS url with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            # Because we use this method in the view_url method, we need to handle
            # backward compatibility for Airflow versions that doesn't have the
            # _view_url_template attribute. Should be removed when we drop support for Airflow 3.0
            return self._view_url_template
        # https://console.cloud.google.com/storage/browser/<bucket-name>/<prefix>
        url = f"https://console.cloud.google.com/storage/browser/{self.bucket_name}"
        if self.prefix:
            url += f"/{self.prefix}"

        return url
