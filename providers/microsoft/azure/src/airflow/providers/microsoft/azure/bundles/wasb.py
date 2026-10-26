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
import shutil
import tarfile
import tempfile
from functools import cached_property
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import structlog
from azure.core import MatchConditions
from azure.core.exceptions import ResourceNotFoundError

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook


class WasbDagBundle(BaseDagBundle):
    """
    WASB Dag bundle - exposes a directory in Azure Blob Storage as a Dag bundle.

    This allows Airflow to load Dags directly from an Azure Blob Storage container.

    :param wasb_conn_id: Airflow connection ID for Azure Blob Storage. Defaults to WasbHook.default_conn_name.
    :param container_name: The name of the blob container containing the Dag files.
    :param prefix: Optional subdirectory within the container where the Dags are stored.
        If empty, Dags are assumed to be at the root of the container. Mutually exclusive
        with ``archive_blob``.
    :param archive_blob: Optional blob name of a ``.tar.gz`` archive holding the Dag files.
        When set, the bundle is staged by downloading this single blob and unpacking it
        locally, instead of downloading every blob under a prefix one at a time.
        The archive is then the only source of the Dags: ``archive_blob`` is mutually
        exclusive with ``prefix``, so there is no second copy to keep in sync and no
        fallback to a per-blob sync. Members are extracted relative to the bundle
        root, e.g. created with ``tar -C <dags_dir> -czf dags.tar.gz .``.
    """

    supports_versioning = False

    archive_etag_marker = ".airflow_bundle_archive_etag"

    def __init__(
        self,
        *,
        wasb_conn_id: str = WasbHook.default_conn_name,
        container_name: str,
        prefix: str = "",
        archive_blob: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if prefix and archive_blob:
            raise ValueError(
                "WasbDagBundle accepts either 'prefix' or 'archive_blob', not both. When "
                "'archive_blob' is set the archive is the only staging source."
            )
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.prefix = prefix
        self.archive_blob = archive_blob
        self.wasb_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            container_name=self.container_name,
            prefix=self.prefix,
            archive_blob=self.archive_blob,
            wasb_conn_id=self.wasb_conn_id,
        )

    def _initialize(self):
        with self.lock():
            if not self.wasb_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.wasb_dags_dir)
                os.makedirs(self.wasb_dags_dir)

            if not self.wasb_dags_dir.is_dir():
                raise NotADirectoryError(f"Local Dags path: {self.wasb_dags_dir} is not a directory.")

            if not self.wasb_hook.check_for_container(container_name=self.container_name):
                raise ValueError(f"WASB container '{self.container_name}' does not exist.")

            if self.prefix:
                if not self.wasb_hook.check_for_prefix(
                    container_name=self.container_name, prefix=f"{self.prefix.rstrip('/')}/", delimiter="/"
                ):
                    raise ValueError(
                        f"WASB prefix 'wasb://{self.container_name}/{self.prefix}' does not exist."
                    )
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @cached_property
    def wasb_hook(self) -> WasbHook:
        return WasbHook(wasb_conn_id=self.wasb_conn_id)

    def __repr__(self):
        return (
            f"<WasbDagBundle("
            f"name={self.name!r}, "
            f"container_name={self.container_name!r}, "
            f"prefix={self.prefix!r}, "
            f"archive_blob={self.archive_blob!r}, "
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
            raise ValueError("Refreshing a specific version is not supported")

        with self.lock():
            if self.archive_blob:
                self._refresh_from_archive(self.archive_blob)
                return
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

    def _refresh_from_archive(self, archive_blob: str) -> None:
        """Stage the Dag bundle by downloading and unpacking the single archive blob."""
        blob = self.wasb_hook.get_conn().get_blob_client(self.container_name, archive_blob)
        try:
            props = blob.get_blob_properties()
        except ResourceNotFoundError:
            raise ValueError(f"WASB archive 'wasb://{self.container_name}/{archive_blob}' does not exist.")
        etag = props.etag or ""

        marker = self.wasb_dags_dir / self.archive_etag_marker
        if etag and marker.is_file() and marker.read_text() == etag:
            self._log.debug(
                "Dag bundle archive 'wasb://%s/%s' is unchanged (ETag %s), skipping staging",
                self.container_name,
                archive_blob,
                etag,
            )
            return

        staging_dir = Path(tempfile.mkdtemp(dir=self.wasb_dags_dir.parent, prefix=".wasb-archive-staging-"))
        try:
            archive_path = staging_dir / "_bundle_archive"
            # Pin the bytes to the ETag just read. An overwrite between the
            # properties read and the download then fails, instead of storing
            # the new bytes under the old ETag.
            download_kwargs = {}
            if etag:
                download_kwargs["etag"] = etag
                download_kwargs["match_condition"] = MatchConditions.IfNotModified
            payload = blob.download_blob(**download_kwargs).readall()
            archive_path.write_bytes(payload)

            unpack_dir = staging_dir / "unpacked"
            unpack_dir.mkdir()
            with tarfile.open(archive_path, "r:*") as tar:
                tar.extractall(unpack_dir, filter="data")
            archive_path.unlink()

            if etag:
                (unpack_dir / self.archive_etag_marker).write_text(etag)

            # Swap the unpacked tree into place. The previous tree is kept inside the
            # staging directory and renamed back if the second rename fails, so a
            # half-unpacked tree is never left at self.wasb_dags_dir. There is a short
            # gap between the two renames where the directory does not exist. Readers
            # do not take self.lock(), so a scan during that gap sees nothing.
            backup_dir = staging_dir / "previous"
            if self.wasb_dags_dir.exists():
                self.wasb_dags_dir.rename(backup_dir)
            try:
                unpack_dir.rename(self.wasb_dags_dir)
            except Exception:
                if backup_dir.exists():
                    backup_dir.rename(self.wasb_dags_dir)
                raise

            self._log.debug(
                "Staged Dag bundle from archive 'wasb://%s/%s' (%s bytes) to %s",
                self.container_name,
                archive_blob,
                len(payload),
                self.wasb_dags_dir,
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

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
            raise ValueError("WASB url with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            return self._view_url_template
        account_url = self.wasb_hook.blob_service_client.url
        scheme, netloc, path, _, _ = urlsplit(account_url)
        path = f"{path.rstrip('/')}/{self.container_name}"
        if self.archive_blob:
            path = f"{path}/{self.archive_blob}"
        elif self.prefix:
            path = f"{path}/{self.prefix}"
        return urlunsplit((scheme, netloc, path, "", ""))
