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

log = structlog.get_logger(__name__)

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3DagBundle(BaseDagBundle):
    """
    S3 DAG bundle - exposes a directory in S3 as a DAG bundle.

    This allows Airflow to load DAGs directly from an S3 bucket.

    :param aws_conn_id: Airflow connection ID for AWS.  Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the DAG files.
    :param prefix:  Optional subdirectory within the S3 bucket where the DAGs are stored.
                    If None, DAGs are assumed to be at the root of the bucket (Optional).
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
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        # Local path where S3 DAGs are downloaded.
        self.s3_dags_root_dir: Path = self.base_dir.joinpath("s3")
        # Local path where S3 DAGs are downloaded for current config.
        self.s3_dags_dir: Path = self.s3_dags_root_dir.joinpath(self.name)

        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bucket_name=self.bucket_name,
            prefix=self.prefix,
            aws_conn_id=self.aws_conn_id,
        )

        try:
            self.s3_hook: S3Hook = S3Hook(aws_conn_id=self.aws_conn_id)  # Initialize S3 hook.
        except AirflowException as e:
            self._log.warning("Could not create S3Hook for connection %s: %s", self.aws_conn_id, e)

    def _initialize(self):
        with self.lock():
            if not self.s3_dags_dir.exists():
                self._log.info("Creating local DAGs directory: %s", self.s3_dags_dir)
                os.makedirs(self.s3_dags_dir)

            if not self.s3_dags_dir.is_dir():
                raise AirflowException(f"Local DAGs path: {self.s3_dags_dir} is not a directory.")

            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise AirflowException(f"S3 bucket '{self.bucket_name}' does not exist.")

            if self.prefix:
                # don't check when prefix is ""
                if not self.s3_hook.check_for_prefix(
                    bucket_name=self.bucket_name, prefix=self.prefix, delimiter="/"
                ):
                    raise AirflowException(
                        f"S3 prefix 's3://{self.bucket_name}/{self.prefix}' does not exist."
                    )

            self._download_s3_dags()
        self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    def _delete_stale_local_files(self, current_s3_objects: list[Path]):
        current_s3_keys = {key for key in current_s3_objects}

        for item in self.s3_dags_dir.iterdir():
            item: Path  # type: ignore[no-redef]
            absolute_item_path = item.resolve()

            if absolute_item_path not in current_s3_keys:
                try:
                    if item.is_file():
                        item.unlink(missing_ok=True)
                        self._log.debug("Deleted stale local file: %s", item)
                    elif item.is_dir():
                        # delete only when the folder is empty
                        if not os.listdir(item):
                            item.rmdir()
                            self._log.debug("Deleted stale empty directory: %s", item)
                    else:
                        self._log.debug("Skipping stale item of unknown type: %s", item)
                except OSError as e:
                    self._log.error("Error deleting stale item %s: %s", item, e)
                    raise e

    def _download_s3_object_if_changed(self, s3_bucket, s3_object, local_target_path: Path):
        should_download = False
        download_msg = ""
        if not local_target_path.exists():
            should_download = True
            download_msg = f"Local file {local_target_path} does not exist."
        else:
            local_stats = local_target_path.stat()

            if s3_object.size != local_stats.st_size:
                should_download = True
                download_msg = (
                    f"S3 object size ({s3_object.size}) and local file size ({local_stats.st_size}) differ."
                )

            s3_last_modified = s3_object.last_modified
            if local_stats.st_mtime < s3_last_modified.microsecond:
                should_download = True
                download_msg = f"S3 object last modified ({s3_last_modified.microsecond}) and local file last modified ({local_stats.st_mtime}) differ."

        if should_download:
            s3_bucket.download_file(s3_object.key, local_target_path)
            self._log.debug(
                "%s Downloaded %s to %s", download_msg, s3_object.key, local_target_path.as_posix()
            )
        else:
            self._log.debug(
                "Local file %s is up-to-date with S3 object %s. Skipping download.",
                local_target_path.as_posix(),
                s3_object.key,
            )

    def _download_s3_dags(self):
        """Download DAG files from the S3 bucket to the local directory."""
        self._log.debug(
            "Downloading DAGs from s3://%s/%s to %s", self.bucket_name, self.prefix, self.s3_dags_dir
        )
        local_s3_objects = []
        s3_bucket = self.s3_hook.get_bucket(self.bucket_name)
        for obj in s3_bucket.objects.filter(Prefix=self.prefix):
            obj_path = Path(obj.key)
            local_target_path = self.s3_dags_dir.joinpath(obj_path.relative_to(self.prefix))
            if not local_target_path.parent.exists():
                local_target_path.parent.mkdir(parents=True, exist_ok=True)
                self._log.debug("Created local directory: %s", local_target_path.parent)
            self._download_s3_object_if_changed(
                s3_bucket=s3_bucket, s3_object=obj, local_target_path=local_target_path
            )
            local_s3_objects.append(local_target_path)

        self._delete_stale_local_files(current_s3_objects=local_s3_objects)

    def __repr__(self):
        return (
            f"<S3DagBundle("
            f"name={self.name!r}, "
            f"bucket_name={self.bucket_name!r}, "
            f"prefix={self.prefix!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> str | None:
        """Return the current version of the DAG bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Return the local path to the DAG files."""
        return self.s3_dags_dir  # Path where DAGs are downloaded.

    def refresh(self) -> None:
        """Refresh the DAG bundle by re-downloading the DAGs from S3."""
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            self._download_s3_dags()

    def view_url(self, version: str | None = None) -> str | None:
        """Return a URL for viewing the DAGs in S3. Currently, versioning is not supported."""
        if self.version:
            raise AirflowException("S3 url with version is not supported")

        presigned_url = self.s3_hook.generate_presigned_url(
            client_method="get_object", params={"Bucket": self.bucket_name, "Key": self.prefix}
        )
        return presigned_url
