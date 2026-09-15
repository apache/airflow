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
import uuid
from pathlib import Path

import structlog

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.compat.sdk import AirflowException


class S3DagBundle(BaseDagBundle):
    """
    S3 Dag bundle - exposes a directory in S3 as a Dag bundle.

    This allows Airflow to load Dags directly from an S3 bucket.

    :param aws_conn_id: Airflow connection ID for AWS.  Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the Dag files.
    :param prefix:  Optional subdirectory within the S3 bucket where the Dags are stored.
                    If None, Dags are assumed to be at the root of the bucket (Optional).
    :param archive_key: Optional S3 key of a ``.tar.gz`` archive containing the same files as
                    ``prefix``. When set, the bundle is staged by downloading this single object and
                    unpacking it locally, instead of downloading the prefix one object at a time.
                    Staging falls back to the per-object sync of ``prefix`` if the archive cannot be
                    fetched or unpacked. The archive members must be laid out exactly as the objects
                    under ``prefix`` (e.g. created with ``tar -C <dags_dir> -czf dags.tar.gz .``) so
                    both staging strategies produce the same tree (Optional).
    """

    supports_versioning = False

    archive_etag_marker = ".airflow_bundle_archive_etag"

    def __init__(
        self,
        *,
        aws_conn_id: str = AwsBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str = "",
        archive_key: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.archive_key = archive_key
        # Local path where S3 Dags are downloaded
        self.s3_dags_dir: Path = self.base_dir

        log = structlog.get_logger(__name__)
        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            archive_key=self.archive_key,
            aws_conn_id=self.aws_conn_id,
        )
        self._s3_hook: S3Hook | None = None

    def _initialize(self):
        with self.lock():
            if not self.s3_dags_dir.exists():
                self._log.info("Creating local Dags directory: %s", self.s3_dags_dir)
                os.makedirs(self.s3_dags_dir)

            if not self.s3_dags_dir.is_dir():
                raise AirflowException(f"Local Dags path: {self.s3_dags_dir} is not a directory.")

            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise AirflowException(f"S3 bucket '{self.bucket_name}' does not exist.")

            archive_exists = self.archive_key is not None and self.s3_hook.check_for_key(
                key=self.archive_key, bucket_name=self.bucket_name
            )
            if self.archive_key and not archive_exists:
                self._log.warning(
                    "S3 archive 's3://%s/%s' does not exist. Falling back to syncing "
                    "'s3://%s/%s' object by object.",
                    self.bucket_name,
                    self.archive_key,
                    self.bucket_name,
                    self.prefix,
                )
            if self.prefix and not archive_exists:
                # don't check when prefix is "", or when staging will use the archive anyway
                if not self.s3_hook.check_for_prefix(
                    bucket_name=self.bucket_name, prefix=self.prefix, delimiter="/"
                ):
                    raise AirflowException(
                        f"S3 prefix 's3://{self.bucket_name}/{self.prefix}' does not exist."
                    )
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

    def __repr__(self):
        return (
            f"<S3DagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, "
            f"archive_key={self.archive_key!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> str | None:
        """Return the current version of the Dag bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the Dag files."""
        return self.s3_dags_dir  # Path where Dags are downloaded.

    def refresh(self) -> None:
        """Refresh the Dag bundle by re-downloading the Dags from S3."""
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            if self.archive_key:
                try:
                    self._refresh_from_archive()
                    return
                except Exception:
                    self._log.warning(
                        "Downloading Dag bundle archive 's3://%s/%s' failed. Falling back to "
                        "syncing 's3://%s/%s' object by object.",
                        self.bucket_name,
                        self.archive_key,
                        self.bucket_name,
                        self.prefix,
                        exc_info=True,
                    )
            self._log.debug(
                "Downloading Dags from s3://%s/%s to %s", self.bucket_name, self.prefix, self.s3_dags_dir
            )
            self.s3_hook.sync_to_local_dir(
                bucket_name=self.bucket_name,
                s3_prefix=self.prefix,
                local_dir=self.s3_dags_dir,
                delete_stale=True,
            )

    def _refresh_from_archive(self) -> None:
        """Stage the Dag bundle by downloading and unpacking the single archive object."""
        client = self.s3_hook.get_conn()
        head = client.head_object(Bucket=self.bucket_name, Key=self.archive_key)
        etag: str = head.get("ETag", "")

        marker = self.s3_dags_dir / self.archive_etag_marker
        if etag and marker.is_file() and marker.read_text() == etag:
            self._log.debug(
                "Dag bundle archive 's3://%s/%s' is unchanged (ETag %s), skipping staging",
                self.bucket_name,
                self.archive_key,
                etag,
            )
            return

        staging_dir = Path(tempfile.mkdtemp(dir=self.s3_dags_dir.parent, prefix=".s3-archive-staging-"))
        try:
            archive_path = staging_dir / "_bundle_archive"
            client.download_file(self.bucket_name, self.archive_key, os.fspath(archive_path))

            unpack_dir = staging_dir / "unpacked"
            unpack_dir.mkdir()
            with tarfile.open(archive_path, "r:*") as tar:
                tar.extractall(unpack_dir, filter="data")
            archive_path.unlink()

            if etag:
                (unpack_dir / self.archive_etag_marker).write_text(etag)

            # Swap the freshly unpacked tree into place so a partially staged
            # bundle is never observable at self.s3_dags_dir.
            old_dir = self.s3_dags_dir.parent / f".s3-archive-old-{uuid.uuid4().hex}"
            if self.s3_dags_dir.exists():
                self.s3_dags_dir.rename(old_dir)
            unpack_dir.rename(self.s3_dags_dir)
            shutil.rmtree(old_dir, ignore_errors=True)

            self._log.debug(
                "Staged Dag bundle from archive 's3://%s/%s' (%s bytes) to %s",
                self.bucket_name,
                self.archive_key,
                head.get("ContentLength"),
                self.s3_dags_dir,
            )
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)

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
        if self.prefix:
            url += f"/{self.prefix}"

        return url
