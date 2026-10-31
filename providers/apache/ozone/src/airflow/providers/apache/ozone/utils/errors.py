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

from dataclasses import dataclass

from airflow.providers.apache.ozone.version_compat import AirflowException

SNAPSHOT_ALREADY_EXISTS_MARKERS = ("FILE_ALREADY_EXISTS", "SNAPSHOT ALREADY EXISTS")


class OzoneProviderError(AirflowException):
    """Base exception for Ozone provider runtime validation errors."""


class OzoneCliError(AirflowException):
    """Ozone CLI error with retryable marker."""

    def __init__(
        self,
        message: str,
        *,
        command: list[str] | None = None,
        stderr: str | None = None,
        returncode: int | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.command = command
        self.stderr = stderr
        self.returncode = returncode
        self.retryable = retryable


class OzoneCliErrors:
    """Error classifier for Ozone CLI operations."""

    non_retryable_return_codes = (
        2,  # CLI usage / invalid arguments
        126,  # command invoked cannot execute
        127,  # command not found
        130,  # interrupted by user (SIGINT)
    )
    retryable_return_codes = (
        143,  # terminated (often timeout/orchestration kill)
        255,  # common transport/session failures in CLI tools
    )

    non_retryable_errors = (
        "already_exists",
        "not_found",
        "does not exist",
        "accessdenied",
        "access denied",
        "permission denied",
        "invalid",
        "illegalargumentexception",
        "authentication failed",
        "unauthorized",
    )

    retryable_errors = (
        "timed out",
        "timeout",
        "connection refused",
        "connection reset",
        "connection closed",
        "no route to host",
        "temporarily unavailable",
        "service unavailable",
        "unknownhostexception",
        "could not resolve",
        "sockettimeoutexception",
        "connectexception",
        "eofexception",
        "broken pipe",
        "too many requests",
        "throttl",
        "503",
    )

    @classmethod
    def is_retryable_failure(cls, return_code: int | None, stderr: str) -> bool:
        """Return True when CLI failure matches retryable return code or markers."""
        normalized = (stderr or "").lower()
        if any(marker in normalized for marker in cls.non_retryable_errors):
            return False
        if return_code in cls.non_retryable_return_codes:
            return False
        if return_code in cls.retryable_return_codes:
            return True
        return any(marker in normalized for marker in cls.retryable_errors)


@dataclass(frozen=True)
class AdminResourceSpec:
    """CLI markers used for idempotent admin operations."""

    already_exists_marker: str
    not_found_markers: tuple[str, ...]


ADMIN_RESOURCE_SPECS: dict[str, AdminResourceSpec] = {
    "volume": AdminResourceSpec(
        already_exists_marker="VOLUME_ALREADY_EXISTS",
        not_found_markers=("VOLUME_NOT_FOUND",),
    ),
    "bucket": AdminResourceSpec(
        already_exists_marker="BUCKET_ALREADY_EXISTS",
        not_found_markers=("BUCKET_NOT_FOUND",),
    ),
}
