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


def _require_tar_data_filter() -> None:
    if hasattr(tarfile, "data_filter"):
        return
    raise RuntimeError("Archive Dag bundles require Python 3.11.4 or later (tarfile filter='data').")


def _extract_archive(archive_path: Path, unpack_dir: Path) -> None:
    """Unpack a bundle archive without writing outside ``unpack_dir``."""
    _require_tar_data_filter()
    with tarfile.open(archive_path, "r:*") as tar:
        tar.extractall(unpack_dir, filter="data")


def _archive_state_dir(bundle_dir: Path, bundle_name: str) -> Path:
    return bundle_dir.parent / f".{bundle_name}"


def _archive_etag_path(bundle_dir: Path, bundle_name: str) -> Path:
    return _archive_state_dir(bundle_dir, bundle_name) / "etag"


class _WasbDagBundleBase(BaseDagBundle):
    """
    WASB connection, local directory, and blob URL shared by the WASB bundle classes.

    :meta private:
    """

    def __init__(
        self,
        *,
        wasb_conn_id: str = WasbHook.default_conn_name,
        container_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.container_name = container_name
        self.wasb_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            container_name=self.container_name,
            wasb_conn_id=self.wasb_conn_id,
            **self._log_fields(),
        )

    def _log_fields(self) -> dict[str, str]:
        return {}

    def _view_url_key(self) -> str:
        return ""

    def _validate_remote(self) -> None:
        """Check the remote source before the first refresh."""

    def _initialize(self):
        with self.lock():
            if not self.wasb_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.wasb_dags_dir)
                os.makedirs(self.wasb_dags_dir)

            if not self.wasb_dags_dir.is_dir():
                raise NotADirectoryError(f"Local Dags path: {self.wasb_dags_dir} is not a directory.")

            if not self.wasb_hook.check_for_container(container_name=self.container_name):
                raise ValueError(f"WASB container '{self.container_name}' does not exist.")

            self._validate_remote()
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @cached_property
    def wasb_hook(self) -> WasbHook:
        return WasbHook(wasb_conn_id=self.wasb_conn_id)

    def get_current_version(self) -> str | None:
        """Return the current version of the Dag bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the Dag files."""
        return self.wasb_dags_dir

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
        key = self._view_url_key()
        if key:
            path = f"{path}/{key}"
        return urlunsplit((scheme, netloc, path, "", ""))

    def _reject_version_refresh(self) -> None:
        if self.version:
            raise ValueError("Refreshing a specific version is not supported")


class WasbDagBundle(_WasbDagBundleBase):
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
        self.prefix = prefix
        super().__init__(wasb_conn_id=wasb_conn_id, container_name=container_name, **kwargs)

    def _log_fields(self) -> dict[str, str]:
        return {"prefix": self.prefix}

    def _view_url_key(self) -> str:
        return self.prefix

    def _validate_remote(self) -> None:
        if self.prefix:
            if not self.wasb_hook.check_for_prefix(
                container_name=self.container_name, prefix=f"{self.prefix.rstrip('/')}/", delimiter="/"
            ):
                raise ValueError(f"WASB prefix 'wasb://{self.container_name}/{self.prefix}' does not exist.")

    def __repr__(self):
        return (
            f"<WasbDagBundle("
            f"name={self.name!r}, "
            f"container_name={self.container_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def refresh(self) -> None:
        """Refresh the Dag bundle by re-downloading the Dags from Azure Blob Storage."""
        self._reject_version_refresh()

        with self.lock():
            # Same bundle name may previously have been a WasbArchiveDagBundle. Drop its
            # ETag so switching back to the archive restages instead of keeping these files.
            _archive_etag_path(self.wasb_dags_dir, self.name).unlink(missing_ok=True)
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


class WasbArchiveDagBundle(_WasbDagBundleBase):
    """
    WASB Dag bundle staged from a single ``.tar.gz`` blob.

    The archive is the only source of the Dags. Members are extracted relative to the
    bundle root, for example ``tar -C <dags_dir> -czf dags.tar.gz .``. Requires Python
    3.11.4 or later.

    :param wasb_conn_id: Airflow connection ID for Azure Blob Storage. Defaults to WasbHook.default_conn_name.
    :param container_name: The name of the blob container containing the archive.
    :param archive_blob: Blob name of the ``.tar.gz`` archive holding the Dag files.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        wasb_conn_id: str = WasbHook.default_conn_name,
        container_name: str,
        archive_blob: str,
        **kwargs,
    ) -> None:
        _require_tar_data_filter()
        self.archive_blob = archive_blob
        super().__init__(wasb_conn_id=wasb_conn_id, container_name=container_name, **kwargs)

    def _log_fields(self) -> dict[str, str]:
        return {"archive_blob": self.archive_blob}

    def _view_url_key(self) -> str:
        return self.archive_blob

    def __repr__(self):
        return (
            f"<WasbArchiveDagBundle("
            f"name={self.name!r}, "
            f"container_name={self.container_name!r}, "
            f"archive_blob={self.archive_blob!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def _archive_state_dir(self) -> Path:
        return _archive_state_dir(self.wasb_dags_dir, self.name)

    def _archive_etag_path(self) -> Path:
        return _archive_etag_path(self.wasb_dags_dir, self.name)

    def _bundle_dir_has_files(self) -> bool:
        return self.wasb_dags_dir.is_dir() and any(path.is_file() for path in self.wasb_dags_dir.rglob("*"))

    def _remove_stale_archive_staging_dirs(self) -> None:
        state_dir = self._archive_state_dir()
        if not state_dir.is_dir():
            return
        for leftover in state_dir.iterdir():
            if leftover.is_dir() and leftover.name.startswith("archive-staging-"):
                shutil.rmtree(leftover, ignore_errors=True)

    def refresh(self) -> None:
        """Refresh the Dag bundle by downloading and unpacking the archive blob."""
        self._reject_version_refresh()

        with self.lock():
            self._remove_stale_archive_staging_dirs()
            blob = self.wasb_hook.get_conn().get_blob_client(self.container_name, self.archive_blob)
            try:
                props = blob.get_blob_properties()
            except ResourceNotFoundError:
                raise ValueError(
                    f"WASB archive 'wasb://{self.container_name}/{self.archive_blob}' does not exist."
                )
            etag = props.etag or ""

            marker = self._archive_etag_path()
            if etag and marker.is_file() and marker.read_text() == etag and self._bundle_dir_has_files():
                self._log.debug(
                    "Dag bundle archive 'wasb://%s/%s' is unchanged (ETag %s), skipping staging",
                    self.container_name,
                    self.archive_blob,
                    etag,
                )
                return

            state_dir = self._archive_state_dir()
            state_dir.mkdir(parents=True, exist_ok=True)
            staging_dir = Path(tempfile.mkdtemp(dir=state_dir, prefix="archive-staging-"))
            try:
                archive_path = staging_dir / "_bundle_archive"
                download_kwargs = {}
                if etag:
                    download_kwargs["etag"] = etag
                    download_kwargs["match_condition"] = MatchConditions.IfNotModified
                payload = blob.download_blob(**download_kwargs).readall()
                archive_path.write_bytes(payload)

                unpack_dir = staging_dir / "unpacked"
                unpack_dir.mkdir()
                _extract_archive(archive_path, unpack_dir)
                archive_path.unlink()

                backup_dir = staging_dir / "previous"
                if self.wasb_dags_dir.exists():
                    self.wasb_dags_dir.rename(backup_dir)
                try:
                    unpack_dir.rename(self.wasb_dags_dir)
                except Exception:
                    if backup_dir.exists():
                        backup_dir.rename(self.wasb_dags_dir)
                    raise

                if etag:
                    self._archive_etag_path().write_text(etag)

                self._log.debug(
                    "Staged Dag bundle from archive 'wasb://%s/%s' (%s bytes) to %s",
                    self.container_name,
                    self.archive_blob,
                    len(payload),
                    self.wasb_dags_dir,
                )
            finally:
                shutil.rmtree(staging_dir, ignore_errors=True)
