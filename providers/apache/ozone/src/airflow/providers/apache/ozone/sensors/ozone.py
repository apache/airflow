#
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
    FAST_TIMEOUT_SECONDS,
    RETRY_ATTEMPTS,
    OzoneFsHook,
)
from airflow.providers.apache.ozone.utils.errors import OzoneCliError
from airflow.providers.apache.ozone.version_compat import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.providers.apache.ozone.version_compat import Context


class OzoneKeySensor(BaseSensorOperator):
    """
    Wait for a file or directory (key) to exist on Ozone FS (ofs:// or o3fs://).

    Uses OzoneFsHook; retryable CLI errors are treated as "not yet" and trigger retry.
    """

    template_fields = ("path", "ozone_conn_id")

    def __init__(
        self,
        path: str,
        ozone_conn_id: str = OzoneFsHook.default_conn_name,
        retry_attempts: int = RETRY_ATTEMPTS,
        timeout: int = FAST_TIMEOUT_SECONDS,  # Timeout for object existence check operation in Ozone CLI
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.path = path
        self.ozone_conn_id = ozone_conn_id
        self.retry_attempts = retry_attempts
        # Keep the Ozone CLI timeout separate from BaseSensorOperator.timeout.
        self.cli_timeout = timeout
        self.log.debug("OzoneKeySensor initialized (path=%s, conn_id=%s)", self.path, self.ozone_conn_id)

    def poke(self, context: Context) -> bool:
        """Return True when the target path appears in Ozone."""
        self.log.debug("Checking if key exists in Ozone: %s", self.path)
        hook = OzoneFsHook(
            ozone_conn_id=self.ozone_conn_id,
            retry_attempts=self.retry_attempts,
        )
        try:
            if hook.key_exists(self.path, timeout=self.cli_timeout):
                self.log.info("Key found in Ozone: %s", self.path)
                return True
            return False
        except OzoneCliError as e:
            if e.retryable:
                self.log.warning("Retryable error while checking key existence (will retry): %s", str(e))
                return False
            raise
