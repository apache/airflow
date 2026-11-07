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
from pathlib import Path

import structlog
from google.api_core.exceptions import NotFound

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


def _require_tar_data_filter() -> None:
    if hasattr(tarfile, "data_filter"):
        return
    raise RuntimeError("Archive Dag bundles require Python 3.11.4 or later (tarfile filter='data').")


def _extract_archive(archive_path: Path, unpack_dir: Path) -> None:
    """Unpack a bundle archive without writing outside ``unpack_dir``."""
    with tarfile.open(archive_path, "r:*") as tar:
        tar.extractall(unpack_dir, filter="data")


class _GCSDagBundleBase(BaseDagBundle):
    """
    GCS connection, local directory, and console URL shared by the GCS bundle classes.

    :meta private:
    """

    def __init__(
        self,
        *,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        # Local path where GCS Dags are downloaded
        self.gcs_dags_dir: Path = self.base_dir
        self._log = self._get_logger()
        self._gcs_hook: GCSHook | None = None

    def _log_bindings(self) -> dict[str, str | None]:
        """Return log context shared by every GCS Dag bundle."""
        return {
            "bundle_name": self.name,
            "version": self.version,
            "bucket_name": self.bucket_name,
            "gcp_conn_id": self.gcp_conn_id,
        }

    def _get_logger(self):
        """Return a logger bound with the context common to every GCS Dag bundle."""
        return structlog.get_logger(__name__).bind(**self._log_bindings())

    def _console_url(self, domain: str) -> str:
        raise NotImplementedError

    def _validate_remote(self) -> None:
        """Check the remote source before the first refresh."""

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

            self._validate_remote()
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

    def get_current_version(self) -> str | None:
        """Return the current version of the Dag bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the Dag files."""
        return self.gcs_dags_dir  # Path where Dags are downloaded.

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
        return self._console_url(GoogleBaseHook.get_high_value_cookie_domain())

    def _reject_version_refresh(self) -> None:
        if self.version:
            raise ValueError("Refreshing a specific version is not supported")

    def refresh(self) -> None:
        """Refresh the Dag bundle by updating the local Dag files from GCS."""
        self._reject_version_refresh()
        with self.lock():
            self._refresh_locked()

    def _refresh_locked(self) -> None:
        """Stage the bundle while the bundle lock is held."""
        raise NotImplementedError


class GCSDagBundle(_GCSDagBundleBase):
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
        self.prefix = prefix
        super().__init__(gcp_conn_id=gcp_conn_id, bucket_name=bucket_name, **kwargs)

    def _get_logger(self):
        """Return a logger bound with this prefix bundle's context."""
        return structlog.get_logger(__name__).bind(**self._log_bindings(), prefix=self.prefix)

    def _console_url(self, domain: str) -> str:
        # https://console.cloud.google.com/storage/browser/<bucket-name>/<prefix>
        url = f"https://console.cloud.{domain}/storage/browser/{self.bucket_name}"
        if self.prefix:
            url += f"/{self.prefix}"
        return url

    def _validate_remote(self) -> None:
        if self.prefix:
            # don't check when prefix is ""
            if not self.gcs_hook.list(bucket_name=self.bucket_name, prefix=self.prefix):
                raise ValueError(f"GCS prefix 'gs://{self.bucket_name}/{self.prefix}' does not exist.")

    def __repr__(self):
        return (
            f"<GCSDagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def _refresh_locked(self) -> None:
        self._log.debug(
            "Downloading Dags from gs://%s/%s to %s", self.bucket_name, self.prefix, self.gcs_dags_dir
        )
        self.gcs_hook.sync_to_local_dir(
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            local_dir=self.gcs_dags_dir,
            delete_stale=True,
        )


class GCSArchiveDagBundle(_GCSDagBundleBase):
    """
    GCS Dag bundle staged from a single ``.tar.gz`` object.

    The archive is the only source of the Dags. Members are extracted relative to the
    bundle root, for example ``tar -C <dags_dir> -czf dags.tar.gz .``. Requires Python
    3.11.4 or later.

    :param gcp_conn_id: Airflow connection ID for GCS.  Defaults to GoogleBaseHook.default_conn_name.
    :param bucket_name: The name of the GCS bucket containing the archive.
    :param archive_object: GCS object name of the ``.tar.gz`` archive holding the Dag files.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        bucket_name: str,
        archive_object: str,
        **kwargs,
    ) -> None:
        _require_tar_data_filter()
        self.archive_object = archive_object
        super().__init__(gcp_conn_id=gcp_conn_id, bucket_name=bucket_name, **kwargs)

    def _get_logger(self):
        """Return a logger bound with this archive bundle's context."""
        return structlog.get_logger(__name__).bind(**self._log_bindings(), archive_object=self.archive_object)

    def _console_url(self, domain: str) -> str:
        # https://console.cloud.google.com/storage/browser/_details/<bucket>/<object>
        return (
            f"https://console.cloud.{domain}/storage/browser/_details/"
            f"{self.bucket_name}/{self.archive_object}"
        )

    def __repr__(self):
        return (
            f"<GCSArchiveDagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"archive_object={self.archive_object!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def _archive_state_dir(self) -> Path:
        return self.gcs_dags_dir.parent / f".{self.name}"

    def _archive_etag_path(self) -> Path:
        """Return the ETag marker path inside the staged Dag tree."""
        return self.gcs_dags_dir / ".archive_etag"

    def _bundle_dir_has_files(self) -> bool:
        """Return whether the staged tree has files other than the ETag marker."""
        marker = self._archive_etag_path()
        return self.gcs_dags_dir.is_dir() and any(
            path.is_file() and path != marker for path in self.gcs_dags_dir.rglob("*")
        )

    def _remove_stale_archive_staging_dirs(self) -> None:
        state_dir = self._archive_state_dir()
        if not state_dir.is_dir():
            return
        for leftover in state_dir.iterdir():
            if leftover.is_dir() and leftover.name.startswith("archive-staging-"):
                shutil.rmtree(leftover, ignore_errors=True)

    def _refresh_locked(self) -> None:
        self._remove_stale_archive_staging_dirs()
        blob = self.gcs_hook.get_conn().bucket(self.bucket_name).blob(self.archive_object)
        try:
            blob.reload()
        except NotFound:
            raise ValueError(f"GCS archive 'gs://{self.bucket_name}/{self.archive_object}' does not exist.")
        etag = blob.etag or ""

        marker = self._archive_etag_path()
        if etag and marker.is_file() and marker.read_text() == etag and self._bundle_dir_has_files():
            self._log.debug(
                "Dag bundle archive 'gs://%s/%s' is unchanged (ETag %s), skipping staging",
                self.bucket_name,
                self.archive_object,
                etag,
            )
            return

        state_dir = self._archive_state_dir()
        state_dir.mkdir(parents=True, exist_ok=True)
        staging_dir = Path(tempfile.mkdtemp(dir=state_dir, prefix="archive-staging-"))
        try:
            archive_path = staging_dir / "_bundle_archive"
            download_kwargs = {}
            if blob.generation is not None:
                download_kwargs["if_generation_match"] = blob.generation
            blob.download_to_filename(os.fspath(archive_path), **download_kwargs)

            unpack_dir = staging_dir / "unpacked"
            unpack_dir.mkdir()
            _extract_archive(archive_path, unpack_dir)
            archive_path.unlink()

            backup_dir = staging_dir / "previous"
            if self.gcs_dags_dir.exists():
                self.gcs_dags_dir.rename(backup_dir)
            try:
                unpack_dir.rename(self.gcs_dags_dir)
            except Exception:
                if backup_dir.exists():
                    backup_dir.rename(self.gcs_dags_dir)
                raise

            if etag:
                self._archive_etag_path().write_text(etag)

            self._log.debug(
                "Staged Dag bundle from archive 'gs://%s/%s' (%s bytes) to %s",
                self.bucket_name,
                self.archive_object,
                blob.size,
                self.gcs_dags_dir,
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
