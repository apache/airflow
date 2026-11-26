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

import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

from airflow.providers.apache.ozone.hooks.ozone import (
    FAST_TIMEOUT_SECONDS,
    RETRY_ATTEMPTS,
    SLOW_TIMEOUT_SECONDS,
    ExistingTargetPolicy,
    OzoneAdminHook,
    OzoneFsHook,
)
from airflow.providers.apache.ozone.utils.errors import OzoneProviderError
from airflow.providers.apache.ozone.utils.helpers import FileHelper, TypeNormalizationHelper
from airflow.providers.apache.ozone.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.apache.ozone.version_compat import Context


class OzoneCreateVolumeOperator(BaseOperator):
    """Create an Ozone Volume using Native Admin CLI."""

    template_fields = ("volume_name", "quota", "ozone_conn_id")

    def __init__(
        self,
        volume_name: str,
        quota: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.IGNORE,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.volume_name = volume_name
        self.quota = quota
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.if_exists = if_exists

    def execute(self, context: Context) -> None:
        """Create the target volume if it does not exist."""
        quota = TypeNormalizationHelper.require_optional_non_empty(
            self.quota, "quota parameter cannot be an empty string (use None for unlimited)"
        )
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.create_volume(self.volume_name, quota, timeout=self.timeout, if_exists=self.if_exists)


class OzoneCreateBucketOperator(BaseOperator):
    """Create an Ozone Bucket using Native Admin CLI."""

    template_fields = ("volume_name", "bucket_name", "quota", "ozone_conn_id")

    def __init__(
        self,
        volume_name: str,
        bucket_name: str,
        quota: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.IGNORE,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.volume_name = volume_name
        self.bucket_name = bucket_name
        self.quota = quota
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.if_exists = if_exists

    def execute(self, context: Context) -> None:
        """Create the target bucket if it does not exist."""
        quota = TypeNormalizationHelper.require_optional_non_empty(
            self.quota, "quota parameter cannot be an empty string (use None for unlimited)"
        )
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.create_bucket(
            self.volume_name,
            self.bucket_name,
            quota,
            timeout=self.timeout,
            if_exists=self.if_exists,
        )


class OzoneSetQuotaOperator(BaseOperator):
    """Dynamically adjust volume or bucket quotas."""

    template_fields = ("volume", "quota", "bucket", "ozone_conn_id")

    def __init__(
        self,
        volume: str,
        quota: str,
        bucket: str | None = None,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = FAST_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.volume = volume
        self.quota = quota
        self.bucket = bucket
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context) -> None:
        """Apply a quota to a volume or bucket."""
        bucket = TypeNormalizationHelper.require_optional_non_empty(
            self.bucket, "bucket parameter cannot be an empty string (use None for volume quota)"
        )
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.set_quota(
            volume=self.volume,
            quota=self.quota,
            bucket=bucket,
            timeout=self.timeout,
        )


class OzoneDeleteVolumeOperator(BaseOperator):
    """Delete an Ozone Volume using Native Admin CLI."""

    template_fields = ("volume_name", "ozone_conn_id")

    def __init__(
        self,
        volume_name: str,
        recursive: bool = False,
        force: bool = False,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.volume_name = volume_name
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        if force and not recursive:
            raise ValueError("force=True requires recursive=True")
        self.recursive = recursive
        self.force = force

    def execute(self, context: Context) -> None:
        """Delete a volume with optional recursive mode."""
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.delete_volume(
            self.volume_name,
            self.recursive,
            self.force,
            timeout=self.timeout,
        )


class OzoneDeleteBucketOperator(BaseOperator):
    """Delete an Ozone Bucket using Native Admin CLI."""

    template_fields = ("volume_name", "bucket_name", "ozone_conn_id")

    def __init__(
        self,
        volume_name: str,
        bucket_name: str,
        recursive: bool = False,
        force: bool = False,
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.volume_name = volume_name
        self.bucket_name = bucket_name
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        if force and not recursive:
            raise ValueError("force=True requires recursive=True")
        self.recursive = recursive
        self.force = force

    def execute(self, context: Context) -> None:
        """Delete a bucket with optional recursive mode."""
        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.delete_bucket(
            self.volume_name,
            self.bucket_name,
            self.recursive,
            self.force,
            timeout=self.timeout,
        )


class OzoneCreatePathOperator(BaseOperator):
    """Create directory path via FS interface."""

    template_fields = ("path", "ozone_conn_id")

    def __init__(
        self,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = FAST_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.IGNORE,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.if_exists = if_exists

    def execute(self, context: Context) -> None:
        """Create a directory path in Ozone FS."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.make_path(self.path, timeout=self.timeout, if_exists=self.if_exists)


class OzoneUploadContentOperator(BaseOperator):
    """Put content string to a file via FS interface."""

    template_fields = ("content", "remote_path", "ozone_conn_id")

    def __init__(
        self,
        content: str,
        remote_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        max_content_size_bytes: int | None = None,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.ERROR,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.remote_path = remote_path
        self.ozone_conn_id = ozone_conn_id
        self.content = content
        self.max_content_size_bytes = TypeNormalizationHelper.require_optional_positive_int(
            max_content_size_bytes,
            field_name="max_content_size_bytes",
        )
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.if_exists = if_exists

    def execute(self, context: Context) -> None:
        """Write string content to a temporary file and upload it."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        size_limit_bytes = self.max_content_size_bytes or hook.connection_snapshot.max_content_size_bytes
        with tempfile.TemporaryDirectory(prefix="ozone_fs_put_") as tmp_dir:
            tmp_path = Path(tmp_dir) / "payload.txt"
            tmp_path.write_text(self.content, encoding="utf-8")
            content_size_bytes = FileHelper.get_file_size_bytes(tmp_path)
            if content_size_bytes <= 0:
                raise OzoneProviderError(
                    "Unable to determine payload size for Ozone upload "
                    f"({content_size_bytes} bytes) from temporary file: {tmp_path}"
                )
            if content_size_bytes > size_limit_bytes:
                raise OzoneProviderError(
                    f"Content size ({content_size_bytes} bytes) exceeds configured limit "
                    f"({size_limit_bytes} bytes) for Ozone upload."
                )
            hook.upload_key(
                str(tmp_path),
                self.remote_path,
                timeout=self.timeout,
                if_exists=self.if_exists,
            )


class OzoneDeleteKeyOperator(BaseOperator):
    """Deletes a key (file) from Ozone FS."""

    template_fields = ("path", "ozone_conn_id")

    def __init__(
        self,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context) -> None:
        """Delete a single key or a wildcard-matched group of keys."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.delete_key(self.path, timeout=self.timeout)


class OzoneDeletePathOperator(BaseOperator):
    """Delete file/directory path from Ozone FS."""

    template_fields = ("path", "ozone_conn_id")

    def __init__(
        self,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        recursive: bool = True,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.recursive = recursive
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context) -> None:
        """Delete file/directory path with optional recursive mode."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.delete_path(self.path, recursive=self.recursive, timeout=self.timeout)


class OzonePathExistsOperator(BaseOperator):
    """Check whether Ozone path exists and return boolean via XCom."""

    template_fields = ("path", "ozone_conn_id")

    def __init__(
        self,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = FAST_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context) -> bool:
        """Return True when path exists, False otherwise."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        return hook.path_exists(self.path, timeout=self.timeout)


class OzoneListOperator(BaseOperator):
    """Lists keys/prefixes in an Ozone path and returns them via XCom."""

    template_fields = ("path", "ozone_conn_id")

    def __init__(
        self,
        *,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = FAST_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context) -> list[str]:
        """Return a list of keys, optionally filtered by wildcard pattern."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        return hook.list_keys(self.path, timeout=self.timeout)


class OzoneUploadFileOperator(BaseOperator):
    """Upload a file from the local filesystem to Ozone."""

    template_fields = ("local_path", "remote_path", "ozone_conn_id")

    def __init__(
        self,
        local_path: str,
        remote_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        overwrite: bool = False,
        max_content_size_bytes: int | None = None,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.local_path = local_path
        self.remote_path = remote_path
        self.ozone_conn_id = ozone_conn_id
        self.overwrite = overwrite
        self.max_content_size_bytes = TypeNormalizationHelper.require_optional_positive_int(
            max_content_size_bytes,
            field_name="max_content_size_bytes",
        )
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context) -> None:
        """Upload a local file to Ozone, with optional overwrite."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        local_path_obj = Path(self.local_path)
        if not FileHelper.is_readable_file(local_path_obj):
            raise OzoneProviderError(f"Local file not found or is not readable: {self.local_path}")

        size_limit_bytes = self.max_content_size_bytes or hook.connection_snapshot.max_content_size_bytes
        file_size_bytes = FileHelper.get_file_size_bytes(local_path_obj)
        if file_size_bytes <= 0:
            raise OzoneProviderError(
                f"Local file size is unavailable or non-positive ({file_size_bytes} bytes): {self.local_path}"
            )
        if file_size_bytes > size_limit_bytes:
            raise OzoneProviderError(
                f"Local file size ({file_size_bytes} bytes) exceeds configured limit "
                f"({size_limit_bytes} bytes) for Ozone upload: {self.local_path}"
            )

        hook.upload_key(
            str(local_path_obj),
            self.remote_path,
            timeout=self.timeout,
            if_exists="overwrite" if self.overwrite else "error",
        )


class OzoneMoveOperator(BaseOperator):
    """Move or rename a key within the same Ozone cluster."""

    template_fields = ("source_path", "dest_path", "ozone_conn_id")

    def __init__(
        self,
        *,
        source_path: str,
        dest_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.ERROR,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_path = source_path
        self.dest_path = dest_path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.if_exists = if_exists

    def execute(self, context: Context) -> None:
        """Move one key or a wildcard-selected batch within Ozone."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.move(self.source_path, self.dest_path, timeout=self.timeout, if_exists=self.if_exists)


class OzoneCopyOperator(BaseOperator):
    """Copy one key or wildcard-selected keys within Ozone."""

    template_fields = ("source_path", "dest_path", "ozone_conn_id")

    def __init__(
        self,
        *,
        source_path: str,
        dest_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        if_exists: ExistingTargetPolicy | str = ExistingTargetPolicy.ERROR,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.source_path = source_path
        self.dest_path = dest_path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout
        self.if_exists = if_exists

    def execute(self, context: Context) -> None:
        """Copy one key or a wildcard-selected batch within Ozone."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        hook.copy_path(self.source_path, self.dest_path, timeout=self.timeout, if_exists=self.if_exists)


class OzoneDownloadFileOperator(BaseOperator):
    """Download a file from Ozone to local filesystem."""

    template_fields = ("remote_path", "local_path", "ozone_conn_id")

    def __init__(
        self,
        *,
        remote_path: str,
        local_path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        overwrite: bool = False,
        max_content_size_bytes: int | None = None,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.remote_path = remote_path
        self.local_path = local_path
        self.ozone_conn_id = ozone_conn_id
        self.overwrite = overwrite
        self.max_content_size_bytes = TypeNormalizationHelper.require_optional_positive_int(
            max_content_size_bytes,
            field_name="max_content_size_bytes",
        )
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context) -> None:
        """Download remote file to local path."""
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        size_limit_bytes = self.max_content_size_bytes or hook.connection_snapshot.max_content_size_bytes
        key_properties = hook.get_key_property(self.remote_path, timeout=self.timeout)
        remote_size_bytes = TypeNormalizationHelper.parse_non_negative_int(key_properties.get("data_size"))
        if remote_size_bytes is None:
            raise OzoneProviderError(
                f"Unable to determine remote key size for {self.remote_path}; "
                "missing or invalid 'data_size' in key properties."
            )
        if remote_size_bytes > size_limit_bytes:
            raise OzoneProviderError(
                f"Remote file size ({remote_size_bytes} bytes) exceeds configured limit "
                f"({size_limit_bytes} bytes) for Ozone download: {self.remote_path}"
            )

        hook.download_key(
            self.remote_path,
            self.local_path,
            overwrite=self.overwrite,
            timeout=self.timeout,
        )
