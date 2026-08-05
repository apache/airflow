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
"""Validated values for the Docker Sandboxes scratch protocol."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar

from airflow.providers.common.sandbox.exceptions import SandboxInvalidHandleError
from airflow.providers.common.sandbox.models import SandboxHandle, is_preassigned_executor_id
from airflow.providers.docker.sandbox.exceptions import (
    DockerSandboxConfigurationError,
    DockerSandboxProtocolError,
)

SPEC_FILENAME = "launch.json"
STATUS_FILENAME = "status.json"
METADATA_FILENAME = "metadata.json"
SUPERVISOR_MODULE = "airflow.providers.docker.sandbox.supervisor"
SCRATCH_SCHEMA_VERSION = 1

_MEMORY_PATTERN = re.compile(r"^[1-9][0-9]*(?:[kmgt]i?b?)?$", re.IGNORECASE)
_MAX_STATUS_MESSAGE_LENGTH = 4096


def sandbox_name_from_request_id(request_id: str) -> str:
    """Return the deterministic Docker Sandbox name for one task attempt."""
    if not is_preassigned_executor_id(request_id):
        raise DockerSandboxConfigurationError("Docker Sandbox request ID must be a canonical UUID")
    return f"airflow-{request_id}"


def _require_non_empty_string(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise DockerSandboxConfigurationError(f"{field_name} must be a non-empty string")
    return value


def _require_protocol_string(value: Any, *, field_name: str) -> str:
    try:
        return _require_non_empty_string(value, field_name=field_name)
    except DockerSandboxConfigurationError as error:
        raise DockerSandboxProtocolError(str(error)) from error


@dataclass(frozen=True)
class DockerSandboxDriverConfig:
    """Host-side configuration for the development/e2e Docker Sandbox driver."""

    scratch_root: str
    sbx_binary: str = "sbx"
    acceptance_timeout_seconds: float = 30.0
    command_timeout_seconds: float = 60.0
    acceptance_poll_interval: float = 0.1

    def __post_init__(self) -> None:
        root = Path(_require_non_empty_string(self.scratch_root, field_name="scratch_root"))
        if not root.is_absolute() or ".." in root.parts:
            raise DockerSandboxConfigurationError("scratch_root must be an absolute normalized path")
        _require_non_empty_string(self.sbx_binary, field_name="sbx_binary")
        for name in (
            "acceptance_timeout_seconds",
            "command_timeout_seconds",
            "acceptance_poll_interval",
        ):
            value = getattr(self, name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
                or value <= 0
            ):
                raise DockerSandboxConfigurationError(f"{name} must be a finite positive number")

    @property
    def root_path(self) -> Path:
        """Return the validated scratch root as a path."""
        return Path(self.scratch_root)


@dataclass(frozen=True)
class DockerSandboxLaunchConfig:
    """Docker Sandbox options accepted from an executor launch request."""

    template: str
    cpus: int | None = None
    memory: str | None = None

    def __post_init__(self) -> None:
        _require_non_empty_string(self.template, field_name="template")
        if self.cpus is not None and (
            isinstance(self.cpus, bool) or not isinstance(self.cpus, int) or self.cpus <= 0
        ):
            raise DockerSandboxConfigurationError("cpus must be a positive integer or null")
        if self.memory is not None and (
            not isinstance(self.memory, str) or _MEMORY_PATTERN.fullmatch(self.memory) is None
        ):
            raise DockerSandboxConfigurationError(
                "memory must use a positive binary size such as '1024m' or '8g'"
            )

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> DockerSandboxLaunchConfig:
        """Parse a strict provider-specific launch object."""
        if not isinstance(value, dict):
            raise DockerSandboxConfigurationError("Docker Sandbox launch configuration must be an object")
        allowed = {"template", "cpus", "memory"}
        if unknown := sorted(set(value) - allowed):
            raise DockerSandboxConfigurationError(
                f"unsupported Docker Sandbox launch fields: {', '.join(unknown)}"
            )
        try:
            return cls(**value)
        except TypeError as error:
            raise DockerSandboxConfigurationError(
                "Docker Sandbox launch configuration needs template"
            ) from error


@dataclass(frozen=True)
class DockerSandboxHandle:
    """Durable identity for one sandbox created by ``sbx``."""

    request_id: str
    sandbox_name: str
    sandbox_id: str

    _SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        expected_name = sandbox_name_from_request_id(self.request_id)
        if self.sandbox_name != expected_name:
            raise DockerSandboxConfigurationError("Docker Sandbox handle name does not match its request ID")
        _require_non_empty_string(self.sandbox_id, field_name="sandbox_id")

    def to_common(self) -> SandboxHandle:
        """Convert this value to the common opaque handle."""
        return SandboxHandle(
            data={
                "request_id": self.request_id,
                "sandbox_id": self.sandbox_id,
                "sandbox_name": self.sandbox_name,
                "schema_version": self._SCHEMA_VERSION,
            },
            display_name=self.sandbox_name,
        )

    @classmethod
    def from_common(cls, handle: SandboxHandle) -> DockerSandboxHandle:
        """Parse and validate a common opaque handle."""
        expected_keys = {"request_id", "sandbox_id", "sandbox_name", "schema_version"}
        if set(handle.data) != expected_keys:
            raise SandboxInvalidHandleError("Docker Sandbox handle fields are invalid")
        schema_version = handle.data["schema_version"]
        if isinstance(schema_version, bool) or schema_version != cls._SCHEMA_VERSION:
            raise SandboxInvalidHandleError(
                f"unsupported Docker Sandbox handle schema version: {schema_version!r}"
            )
        values = {name: handle.data[name] for name in ("request_id", "sandbox_name", "sandbox_id")}
        if not all(isinstance(value, str) for value in values.values()):
            raise SandboxInvalidHandleError("Docker Sandbox handle fields must be strings")
        try:
            parsed = cls(**values)
        except DockerSandboxConfigurationError as error:
            raise SandboxInvalidHandleError(str(error)) from error
        if handle.display_name is not None and handle.display_name != parsed.sandbox_name:
            raise SandboxInvalidHandleError("Docker Sandbox handle display name is inconsistent")
        return parsed


@dataclass(frozen=True)
class DockerSandboxMetadata:
    """Host-persisted identity used for exact recovery after a scheduler restart."""

    request_id: str
    sandbox_name: str
    sandbox_id: str

    def __post_init__(self) -> None:
        try:
            DockerSandboxHandle(self.request_id, self.sandbox_name, self.sandbox_id)
        except DockerSandboxConfigurationError as error:
            raise DockerSandboxProtocolError(str(error)) from error

    def to_json(self) -> dict[str, Any]:
        """Return the versioned scratch representation."""
        return {
            "request_id": self.request_id,
            "sandbox_id": self.sandbox_id,
            "sandbox_name": self.sandbox_name,
            "schema_version": SCRATCH_SCHEMA_VERSION,
        }

    @classmethod
    def from_json(cls, value: Any) -> DockerSandboxMetadata:
        """Parse strict scratch metadata."""
        expected = {"request_id", "sandbox_id", "sandbox_name", "schema_version"}
        if not isinstance(value, dict) or set(value) != expected:
            raise DockerSandboxProtocolError("Docker Sandbox metadata fields are invalid")
        schema_version = value["schema_version"]
        if isinstance(schema_version, bool) or schema_version != SCRATCH_SCHEMA_VERSION:
            raise DockerSandboxProtocolError(
                f"unsupported Docker Sandbox metadata schema: {schema_version!r}"
            )
        for name in ("request_id", "sandbox_id", "sandbox_name"):
            _require_protocol_string(value[name], field_name=f"metadata {name}")
        return cls(
            request_id=value["request_id"],
            sandbox_name=value["sandbox_name"],
            sandbox_id=value["sandbox_id"],
        )


class DockerSandboxStatusState(str, Enum):
    """States written by the in-sandbox supervisor."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


@dataclass(frozen=True)
class DockerSandboxStatus:
    """One validated observation written atomically by the supervisor."""

    request_id: str
    state: DockerSandboxStatusState
    exit_code: int | None
    message: str | None

    def __post_init__(self) -> None:
        if not is_preassigned_executor_id(self.request_id):
            raise DockerSandboxProtocolError("status request_id must be a canonical UUID")
        if not isinstance(self.state, DockerSandboxStatusState):
            raise DockerSandboxProtocolError("status state is invalid")
        if isinstance(self.exit_code, bool) or (
            self.exit_code is not None and not isinstance(self.exit_code, int)
        ):
            raise DockerSandboxProtocolError("status exit_code must be an integer or null")
        if self.state is DockerSandboxStatusState.RUNNING and self.exit_code is not None:
            raise DockerSandboxProtocolError("running status cannot have an exit code")
        if self.state is DockerSandboxStatusState.SUCCEEDED and self.exit_code != 0:
            raise DockerSandboxProtocolError("successful status must have exit code zero")
        if self.state is DockerSandboxStatusState.FAILED and self.exit_code in {None, 0}:
            raise DockerSandboxProtocolError("failed status must have a non-zero exit code")
        if self.message is not None and (
            not isinstance(self.message, str) or len(self.message) > _MAX_STATUS_MESSAGE_LENGTH
        ):
            raise DockerSandboxProtocolError(
                f"status message must be a string of at most {_MAX_STATUS_MESSAGE_LENGTH} characters or null"
            )

    @classmethod
    def from_json(cls, value: Any, *, expected_request_id: str) -> DockerSandboxStatus:
        """Parse a strict, versioned status document."""
        expected = {"exit_code", "message", "request_id", "schema_version", "state"}
        if not isinstance(value, dict) or set(value) != expected:
            raise DockerSandboxProtocolError("Docker Sandbox status fields are invalid")
        schema_version = value["schema_version"]
        if isinstance(schema_version, bool) or schema_version != SCRATCH_SCHEMA_VERSION:
            raise DockerSandboxProtocolError(f"unsupported Docker Sandbox status schema: {schema_version!r}")
        if value["request_id"] != expected_request_id:
            raise DockerSandboxProtocolError("Docker Sandbox status request ID does not match")
        try:
            state = DockerSandboxStatusState(value["state"])
        except (TypeError, ValueError) as error:
            raise DockerSandboxProtocolError(
                f"unknown Docker Sandbox status state: {value['state']!r}"
            ) from error
        return cls(
            request_id=value["request_id"],
            state=state,
            exit_code=value["exit_code"],
            message=value["message"],
        )
