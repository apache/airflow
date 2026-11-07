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

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import AirflowException


def _require_tar_data_filter() -> None:
    if hasattr(tarfile, "data_filter"):
        return
    raise RuntimeError("Archive Dag bundles require Python 3.11.4 or later (tarfile filter='data').")


def _extract_archive(archive_path: Path, unpack_dir: Path) -> None:
    """Unpack a bundle archive without writing outside ``unpack_dir``."""
    with tarfile.open(archive_path, "r:*") as tar:
        tar.extractall(unpack_dir, filter="data")


class _S3DagBundleBase(BaseDagBundle):
    """
    S3 connection, local directory, and console URL shared by the S3 bundle classes.

    :meta private:
    """

    def __init__(
        self,
        *,
        aws_conn_id: str = AwsBaseHook.default_conn_name,
        bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        # Local path where S3 Dags are downloaded
        self.s3_dags_dir: Path = self.base_dir
        self._log = self._get_logger()
        self._s3_hook: S3Hook | None = None

    def _log_bindings(self) -> dict[str, str | None]:
        """Return log context shared by every S3 Dag bundle."""
        return {
            "bundle_name": self.name,
            "version": self.version,
            "bucket_name": self.bucket_name,
            "aws_conn_id": self.aws_conn_id,
        }

    def _get_logger(self):
        """Return a logger bound with the context common to every S3 Dag bundle."""
        return structlog.get_logger(__name__).bind(**self._log_bindings())

    def _view_url_key(self) -> str:
        return ""

    def _validate_remote(self) -> None:
        """Check the remote source before the first refresh."""

    def _initialize(self):
        with self.lock():
            if not self.s3_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.s3_dags_dir)
                os.makedirs(self.s3_dags_dir)

            if not self.s3_dags_dir.is_dir():
                raise AirflowException(f"Local Dags path: {self.s3_dags_dir} is not a directory.")

            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise AirflowException(f"S3 bucket '{self.bucket_name}' does not exist.")

            self._validate_remote()
            self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    @property
    def s3_hook(self):
        if self._s3_hook is None:
            try:
                self._s3_hook: S3Hook = S3Hook(aws_conn_id=self.aws_conn_id)  # Initialize S3 hook.
            except AirflowException as e:
                self._log.warning("Could not create S3Hook for connection %s: %s", self.aws_conn_id, e)
        return self._s3_hook

    def get_current_version(self) -> str | None:
        """Return the current version of the Dag bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the Dag files."""
        return self.s3_dags_dir  # Path where Dags are downloaded.

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the Dags in S3. Currently, versioning is not supported.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        return self.view_url_template()

    def view_url_template(self) -> str | None:
        """Return a URL for viewing the Dags in S3. Currently, versioning is not supported."""
        if self.version:
            raise AirflowException("S3 url with version is not supported")
        if hasattr(self, "_view_url_template") and self._view_url_template:
            # Because we use this method in the view_url method, we need to handle
            # backward compatibility for Airflow versions that doesn't have the
            # _view_url_template attribute. Should be removed when we drop support for Airflow 3.0
            return self._view_url_template
        # https://<bucket-name>.s3.<region>.amazonaws.com/<object-key>
        url = f"https://{self.bucket_name}.s3"
        if self.s3_hook.region_name:
            url += f".{self.s3_hook.region_name}"
        url += ".amazonaws.com"
        key = self._view_url_key()
        if key:
            url += f"/{key}"

        return url

    def _reject_version_refresh(self) -> None:
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

    def refresh(self) -> None:
        """Refresh the Dag bundle by updating the local Dag files from S3."""
        self._reject_version_refresh()
        with self.lock():
            self._refresh_locked()

    def _refresh_locked(self) -> None:
        """Stage the bundle while the bundle lock is held."""
        raise NotImplementedError


class S3DagBundle(_S3DagBundleBase):
    """
    S3 Dag bundle - exposes a directory in S3 as a Dag bundle.

    This allows Airflow to load Dags directly from an S3 bucket.

    :param aws_conn_id: Airflow connection ID for AWS.  Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the Dag files.
    :param prefix:  Optional subdirectory within the S3 bucket where the Dags are stored.
                    A trailing slash is optional.
                    If None, Dags are assumed to be at the root of the bucket (Optional).
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        aws_conn_id: str = AwsBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str = "",
        **kwargs,
    ) -> None:
        self.prefix = prefix
        super().__init__(aws_conn_id=aws_conn_id, bucket_name=bucket_name, **kwargs)

    def _get_logger(self):
        """Return a logger bound with this prefix bundle's context."""
        return structlog.get_logger(__name__).bind(**self._log_bindings(), prefix=self.prefix)

    def _view_url_key(self) -> str:
        return self.prefix

    def _validate_remote(self) -> None:
        if self.prefix:
            # don't check when prefix is ""
            if not self.s3_hook.check_for_prefix(
                bucket_name=self.bucket_name, prefix=self.prefix, delimiter="/"
            ):
                raise AirflowException(f"S3 prefix 's3://{self.bucket_name}/{self.prefix}' does not exist.")

    def __repr__(self):
        return (
            f"<S3DagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def _refresh_locked(self) -> None:
        self._log.debug(
            "Downloading Dags from s3://%s/%s to %s", self.bucket_name, self.prefix, self.s3_dags_dir
        )
        sync_prefix = self.prefix
        if sync_prefix and not sync_prefix.endswith("/"):
            sync_prefix += "/"
        self.s3_hook.sync_to_local_dir(
            bucket_name=self.bucket_name,
            s3_prefix=sync_prefix,
            local_dir=self.s3_dags_dir,
            delete_stale=True,
        )


class S3ArchiveDagBundle(_S3DagBundleBase):
    """
    S3 Dag bundle staged from a single ``.tar.gz`` object.

    The archive is the only source of the Dags. Members are extracted relative to the
    bundle root, for example ``tar -C <dags_dir> -czf dags.tar.gz .``. Requires Python
    3.11.4 or later.

    :param aws_conn_id: Airflow connection ID for AWS.  Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the archive.
    :param archive_key: S3 key of the ``.tar.gz`` archive holding the Dag files.
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        aws_conn_id: str = AwsBaseHook.default_conn_name,
        bucket_name: str,
        archive_key: str,
        **kwargs,
    ) -> None:
        _require_tar_data_filter()
        self.archive_key = archive_key
        super().__init__(aws_conn_id=aws_conn_id, bucket_name=bucket_name, **kwargs)

    def _get_logger(self):
        """Return a logger bound with this archive bundle's context."""
        return structlog.get_logger(__name__).bind(**self._log_bindings(), archive_key=self.archive_key)

    def _view_url_key(self) -> str:
        return self.archive_key

    def __repr__(self):
        return (
            f"<S3ArchiveDagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"archive_key={self.archive_key!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def _get_archive_state_dir(self) -> Path:
        return self.s3_dags_dir.parent / f".{self.name}"

    def _get_archive_etag_path(self) -> Path:
        """Return the ETag marker path inside the staged Dag tree."""
        return self.s3_dags_dir / ".archive_etag"

    def _bundle_dir_has_files(self) -> bool:
        """Return whether the staged tree has files other than the ETag marker."""
        marker = self._get_archive_etag_path()
        return self.s3_dags_dir.is_dir() and any(
            path.is_file() and path != marker for path in self.s3_dags_dir.rglob("*")
        )

    def _remove_stale_archive_staging_dirs(self) -> None:
        state_dir = self._get_archive_state_dir()
        if not state_dir.is_dir():
            return
        for leftover in state_dir.iterdir():
            if leftover.is_dir() and leftover.name.startswith("archive-staging-"):
                shutil.rmtree(leftover, ignore_errors=True)

    def _download_archive(self, client, archive_path: Path, etag: str) -> str:
        """Download the archive object named by ``etag``."""
        request = {"Bucket": self.bucket_name, "Key": self.archive_key}
        if etag:
            request["IfMatch"] = etag
        response = client.get_object(**request)
        body = response["Body"]
        try:
            with archive_path.open("wb") as handle:
                shutil.copyfileobj(body, handle, 1024 * 1024)
        finally:
            body.close()
        return response.get("ETag", "")

    def _refresh_locked(self) -> None:
        self._remove_stale_archive_staging_dirs()
        client = self.s3_hook.get_conn()
        head = client.head_object(Bucket=self.bucket_name, Key=self.archive_key)
        etag: str = head.get("ETag", "")

        marker = self._get_archive_etag_path()
        if etag and marker.is_file() and marker.read_text() == etag and self._bundle_dir_has_files():
            self._log.debug(
                "Dag bundle archive 's3://%s/%s' is unchanged (ETag %s), skipping staging",
                self.bucket_name,
                self.archive_key,
                etag,
            )
            return

        state_dir = self._get_archive_state_dir()
        state_dir.mkdir(parents=True, exist_ok=True)
        staging_dir = Path(tempfile.mkdtemp(dir=state_dir, prefix="archive-staging-"))
        try:
            archive_path = staging_dir / "_bundle_archive"
            downloaded_etag = self._download_archive(client, archive_path, etag)

            unpack_dir = staging_dir / "unpacked"
            unpack_dir.mkdir()
            _extract_archive(archive_path, unpack_dir)
            archive_path.unlink()

            backup_dir = staging_dir / "previous"
            if self.s3_dags_dir.exists():
                self.s3_dags_dir.rename(backup_dir)
            try:
                unpack_dir.rename(self.s3_dags_dir)
            except Exception:
                if backup_dir.exists():
                    backup_dir.rename(self.s3_dags_dir)
                raise

            marker_etag = downloaded_etag or etag
            if marker_etag:
                self._get_archive_etag_path().write_text(marker_etag)

            self._log.debug(
                "Staged Dag bundle from archive 's3://%s/%s' (%s bytes) to %s",
                self.bucket_name,
                self.archive_key,
                head.get("ContentLength"),
                self.s3_dags_dir,
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
