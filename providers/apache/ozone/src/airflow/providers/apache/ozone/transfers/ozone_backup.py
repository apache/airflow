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

from typing import TYPE_CHECKING

from airflow.providers.apache.ozone.hooks.ozone import (
    RETRY_ATTEMPTS,
    SLOW_TIMEOUT_SECONDS,
    OzoneAdminHook,
)
from airflow.providers.apache.ozone.utils.errors import (
    SNAPSHOT_ALREADY_EXISTS_MARKERS,
    OzoneCliError,
    OzoneProviderError,
)
from airflow.providers.apache.ozone.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.apache.ozone.version_compat import Context


class OzoneBackupOperator(BaseOperator):
    """Creates a snapshot of a bucket for backup and disaster recovery."""

    template_fields = ("volume", "bucket", "snapshot_name", "ozone_conn_id")

    def __init__(
        self,
        *,
        volume: str,
        bucket: str,
        snapshot_name: str,  # Please, don't forget add date and time to the snapshot name.
        ozone_conn_id: str = OzoneAdminHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = SLOW_TIMEOUT_SECONDS,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.volume = volume
        self.bucket = bucket
        self.snapshot_name = snapshot_name
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        self.timeout = timeout

    def execute(self, context: Context) -> None:
        """Create snapshot and treat existing snapshot as success."""
        path = f"/{self.volume}/{self.bucket}"
        cmd = ["ozone", "sh", "snapshot", "create", path, self.snapshot_name]

        hook = OzoneAdminHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        try:
            hook.run_cli(cmd, timeout=self.timeout)
            self.log.info("Snapshot created: %s (path=%s)", self.snapshot_name, path)
        except OzoneCliError as e:
            error_stderr = (e.stderr or "").strip()
            stderr_upper = error_stderr.upper()
            if any(marker in stderr_upper for marker in SNAPSHOT_ALREADY_EXISTS_MARKERS):
                self.log.info(
                    "Snapshot %s already exists, treating as success (idempotent operation).",
                    self.snapshot_name,
                )
                return
            raise OzoneProviderError(str(e)) from e
