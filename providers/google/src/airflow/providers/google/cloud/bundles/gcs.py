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


class GCSDagBundle(BaseDagBundle):
    """
    GCS Dag bundle - exposes a directory in GCS as a Dag bundle.

    This allows Airflow to load Dags directly from a GCS bucket.

    :param gcp_conn_id: Airflow connection ID for GCS.  Defaults to GoogleBaseHook.default_conn_name.
    :param bucket_name: The name of the GCS bucket containing the Dag files.
    :param prefix:  Optional subdirectory within the GCS bucket where the Dags are stored.
                    If None, Dags are assumed to be at the root of the bucket. Mutually exclusive
                    with ``archive_object`` (Optional).
    :param archive_object: Optional GCS object name of a ``.tar.gz`` archive holding the Dag files.
                    When set, the bundle is staged by downloading this single object and unpacking it
                    locally, instead of downloading every object under a prefix one at a time.
                    The archive is then the only source of the Dags: ``archive_object`` is mutually
                    exclusive with ``prefix``, so there is no second copy to keep in sync and no
                    fallback to a per-object sync. Members are extracted relative to the bundle
                    root, e.g. created with ``tar -C <dags_dir> -czf dags.tar.gz .`` (Optional).
    """

    supports_versioning = False

    archive_etag_marker = ".airflow_bundle_archive_etag"

    def __init__(
        self,
        *,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str = "",
        archive_object: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if prefix and archive_object:
            raise ValueError(
                "GCSDagBundle accepts either 'prefix' or 'archive_object', not both. When "
                "'archive_object' is set the archive is the only staging source."
            )
        self.gcp_conn_id = gcp_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.archive_object = archive_object
        # Local path where GCS Dags are downloaded
        self.gcs_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            archive_object=self.archive_object,
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
            f"archive_object={self.archive_object!r}, "
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
            if self.archive_object:
                self._refresh_from_archive(self.archive_object)
                return
            self._log.debug(
                "Downloading Dags from gs://%s/%s to %s", self.bucket_name, self.prefix, self.gcs_dags_dir
            )
            self.gcs_hook.sync_to_local_dir(
                bucket_name=self.bucket_name,
                prefix=self.prefix,
                local_dir=self.gcs_dags_dir,
                delete_stale=True,
            )

    def _refresh_from_archive(self, archive_object: str) -> None:
        """Stage the Dag bundle by downloading and unpacking the single archive object."""
        blob = self.gcs_hook.get_conn().bucket(self.bucket_name).blob(archive_object)
        try:
            blob.reload()
        except NotFound:
            raise ValueError(f"GCS archive 'gs://{self.bucket_name}/{archive_object}' does not exist.")
        etag = blob.etag or ""

        marker = self.gcs_dags_dir / self.archive_etag_marker
        if etag and marker.is_file() and marker.read_text() == etag:
            self._log.debug(
                "Dag bundle archive 'gs://%s/%s' is unchanged (ETag %s), skipping staging",
                self.bucket_name,
                archive_object,
                etag,
            )
            return

        staging_dir = Path(tempfile.mkdtemp(dir=self.gcs_dags_dir.parent, prefix=".gcs-archive-staging-"))
        try:
            archive_path = staging_dir / "_bundle_archive"
            # Pin the bytes to the generation just read. An overwrite between reload()
            # and the download then fails, instead of storing the new bytes under the
            # old ETag. GCSHook.download cannot pass if_generation_match.
            download_kwargs = {}
            if blob.generation is not None:
                download_kwargs["if_generation_match"] = blob.generation
            blob.download_to_filename(os.fspath(archive_path), **download_kwargs)

            unpack_dir = staging_dir / "unpacked"
            unpack_dir.mkdir()
            with tarfile.open(archive_path, "r:*") as tar:
                tar.extractall(unpack_dir, filter="data")
            archive_path.unlink()

            if etag:
                (unpack_dir / self.archive_etag_marker).write_text(etag)

            # Swap the unpacked tree into place. The previous tree is kept inside the
            # staging directory and renamed back if the second rename fails, so a
            # half-unpacked tree is never left at self.gcs_dags_dir. There is a short
            # gap between the two renames where the directory does not exist. Readers
            # do not take self.lock(), so a scan during that gap sees nothing.
            backup_dir = staging_dir / "previous"
            if self.gcs_dags_dir.exists():
                self.gcs_dags_dir.rename(backup_dir)
            try:
                unpack_dir.rename(self.gcs_dags_dir)
            except Exception:
                if backup_dir.exists():
                    backup_dir.rename(self.gcs_dags_dir)
                raise

            self._log.debug(
                "Staged Dag bundle from archive 'gs://%s/%s' (%s bytes) to %s",
                self.bucket_name,
                archive_object,
                blob.size,
                self.gcs_dags_dir,
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

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
        domain = GoogleBaseHook.get_high_value_cookie_domain()
        if self.archive_object:
            # https://console.cloud.google.com/storage/browser/_details/<bucket>/<object>
            return (
                f"https://console.cloud.{domain}/storage/browser/_details/"
                f"{self.bucket_name}/{self.archive_object}"
            )
        # https://console.cloud.google.com/storage/browser/<bucket-name>/<prefix>
        url = f"https://console.cloud.{domain}/storage/browser/{self.bucket_name}"
        if self.prefix:
            url += f"/{self.prefix}"

        return url
