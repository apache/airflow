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

import os
from datetime import timezone, datetime
from pathlib import Path
from typing import List

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.types import ArgNotSet

try:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except ImportError:
    raise ImportError(
        "Airflow S3DagBundle requires airflow.providers.amazon.* library, but it is not installed. "
        "Please install Apache Airflow `amazon` provider package. "
        "pip install apache-airflow-providers-amazon"
    )


class S3DagBundle(BaseDagBundle, LoggingMixin):
    """
    S3 DAG bundle - exposes a directory in S3 as a DAG bundle.  This allows
    Airflow to load DAGs directly from an S3 bucket.

    :param aws_conn_id: Airflow connection ID for AWS.  Defaults to AwsBaseHook.default_conn_name.
    :param bucket_name: The name of the S3 bucket containing the DAG files.
    :param prefix:  Optional subdirectory within the S3 bucket where the DAGs are stored.
                    If None, DAGs are assumed to be at the root of the bucket (Optional).
    """

    supports_versioning = False

    def __init__(
        self,
        *,
        aws_conn_id: str | None | ArgNotSet = AwsBaseHook.default_conn_name,
        bucket_name: str,
        prefix: str | None = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.bare_repo_path: Path = (
            Path(self._dag_bundle_root_storage_path).joinpath("s3").joinpath(self.name)
        )  # Local path where DAGs are downloaded.
        try:
            self.s3_hook: S3Hook = S3Hook(aws_conn_id=self.aws_conn_id, extra_args={})  # Initialize S3 hook.
        except AirflowException as e:
            self.log.warning("Could not create S3Hook for connection %s: %s", self.aws_conn_id, e)

    def _initialize(self):
        with self.lock():
            if not self.bare_repo_path.exists():
                self.log.info("Creating local DAGs directory: %s", self.bare_repo_path)
                os.makedirs(self.bare_repo_path, exist_ok=True)

            if not self.bare_repo_path.is_dir():
                raise AirflowException(f"Local DAGs path: {self.bare_repo_path} is not a directory.")

            if not self.s3_hook.check_for_bucket(bucket_name=self.bucket_name):
                raise AirflowException(f"S3 bucket '{self.bucket_name}' does not exist.")

            if not self.s3_hook.check_for_prefix(bucket_name=self.bucket_name, prefix=self.prefix, delimiter="/"):
                raise AirflowException(f"S3 prefix 's3://{self.bucket_name}/{self.prefix}' does not exist.")

            self._download_s3_dags()
        self.refresh()

    def initialize(self) -> None:
        self._initialize()
        super().initialize()

    def _delete_stale_local_files(self, current_s3_objects: List[Path]):
        current_s3_keys = {key for key in current_s3_objects}

        for item in self.bare_repo_path.iterdir():
            item: Path
            absolute_item_path = item.resolve()

            if absolute_item_path not in current_s3_keys:
                try:
                    if item.is_file():
                        item.unlink(missing_ok=True)
                        self.log.debug(f"Deleted stale local file: {item}")
                    elif item.is_dir():
                        # delete only when the folder is empty
                        if not os.listdir(item):
                            item.rmdir()
                            self.log.debug(f"Deleted stale empty directory: {item}")
                    else:
                        self.log.debug(f"Skipping stale item of unknown type: {item}")
                except OSError as e:
                    self.log.error(f"Error deleting stale item {item}: {e}")
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
                download_msg = f"S3 object size ({s3_object.size}) and local file size ({local_stats.st_size}) differ."

            s3_last_modified = s3_object.last_modified
            local_last_modified = datetime.fromtimestamp(local_stats.st_mtime, tz=timezone.utc)
            if s3_last_modified.replace(microsecond=0) != local_last_modified.replace(microsecond=0):
                should_download = True
                download_msg = f"S3 object last modified ({s3_last_modified}) and local file last modified ({local_last_modified}) differ."

        if should_download:
            s3_bucket.download_file(s3_object.key, local_target_path)
            self.log.debug(f"{download_msg} Downloaded {s3_object.key} to {local_target_path.as_posix()}")
        else:
            self.log.debug(
                f"Local file {local_target_path.as_posix()} is up-to-date with S3 object {s3_object.key}. Skipping download."
            )

    def _download_s3_dags(self):
        """Downloads DAG files from the S3 bucket to the local directory."""
        self.log.debug(
            f"Downloading DAGs from s3://{self.bucket_name}/{self.prefix} to {self.bare_repo_path}"
        )
        local_s3_objects = []
        s3_bucket = self.s3_hook.get_bucket(self.bucket_name)
        for obj in s3_bucket.objects.filter(Prefix=self.prefix):
            obj_path = Path(obj.key)
            local_target_path = self.bare_repo_path.joinpath(obj_path.relative_to(self.prefix))
            if not local_target_path.parent.exists():
                local_target_path.parent.mkdir(parents=True, exist_ok=True)
                self.log.debug(f"Created local directory: {local_target_path.parent}")
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
        """Returns the current version of the DAG bundle. Currently not supported."""
        return None

    @property
    def path(self) -> Path:
        """Returns the local path to the DAG files."""
        return self.bare_repo_path  # Path where DAGs are downloaded.

    def refresh(self) -> None:
        """Refreshes the DAG bundle by re-downloading the DAGs from S3."""
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            self._download_s3_dags()

    def view_url(self, version: str | None = None) -> str | None:
        """Returns a URL for viewing the DAGs in S3. Currently, doesn't support versioning."""
        if self.version:
            raise AirflowException("S3 url with version is not supported")

        presigned_url = self.s3_hook.generate_presigned_url(
            client_method="get_object", params={"Bucket": self.bucket_name, "Key": self.prefix}
        )
        return presigned_url
