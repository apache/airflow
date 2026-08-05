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
"""Islo-specific configuration and API value objects."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, ClassVar

from airflow.providers.common.sandbox.models import SandboxHandle, is_preassigned_executor_id
from airflow.providers.islo.exceptions import IsloConfigurationError


class IsloExecutionState(str, Enum):
    """State returned by the Islo execution API."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    GONE = "gone"


@dataclass(frozen=True)
class IsloExecutionResult:
    """Complete observation of one Islo command."""

    state: IsloExecutionState
    exit_code: int | None = None
    stdout: str = ""
    stderr: str = ""
    truncated: bool = False


@dataclass(frozen=True)
class IsloExecutionStart:
    """Identity returned when Islo accepts an asynchronous command."""

    execution_id: str
    sandbox_id: str


@dataclass(frozen=True)
class IsloSandboxConfig:
    """Islo-only launch options stored inside a portable sandbox request."""

    image: str | None = None
    snapshot_name: str | None = None
    vcpus: int | None = None
    memory_mb: int | None = None
    disk_gb: int | None = None
    gateway_profile: str | None = None
    internet_enabled: bool = True

    def __post_init__(self) -> None:
        sources = (self.image, self.snapshot_name)
        if sum(source is not None for source in sources) != 1:
            raise IsloConfigurationError("exactly one of image or snapshot_name must be configured")
        if any(not isinstance(source, str) or not source.strip() for source in sources if source is not None):
            raise IsloConfigurationError("image and snapshot sources must be non-empty strings")
        for name in ("vcpus", "memory_mb", "disk_gb"):
            value = getattr(self, name)
            if value is not None and (isinstance(value, bool) or not isinstance(value, int) or value <= 0):
                raise IsloConfigurationError(f"{name} must be a positive integer or null")
        if self.gateway_profile is not None and (
            not isinstance(self.gateway_profile, str) or not self.gateway_profile.strip()
        ):
            raise IsloConfigurationError("gateway_profile must be a non-empty string or null")
        if not isinstance(self.internet_enabled, bool):
            raise IsloConfigurationError("internet_enabled must be a boolean")

    def to_json(self) -> dict[str, Any]:
        return {
            "disk_gb": self.disk_gb,
            "gateway_profile": self.gateway_profile,
            "image": self.image,
            "internet_enabled": self.internet_enabled,
            "memory_mb": self.memory_mb,
            "snapshot_name": self.snapshot_name,
            "vcpus": self.vcpus,
        }

    @classmethod
    def from_json(cls, value: dict[str, Any]) -> IsloSandboxConfig:
        allowed = {
            "disk_gb",
            "gateway_profile",
            "image",
            "internet_enabled",
            "memory_mb",
            "snapshot_name",
            "vcpus",
        }
        if unknown := sorted(set(value) - allowed):
            raise IsloConfigurationError(f"unsupported Islo launch fields: {', '.join(unknown)}")
        return cls(**value)


@dataclass(frozen=True)
class IsloSandboxSpec:
    """Request body inputs for creating one Islo sandbox."""

    name: str
    request_id: str
    config: IsloSandboxConfig
    ttl_seconds: int

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name.strip():
            raise IsloConfigurationError("sandbox name must be a non-empty string")
        if not is_preassigned_executor_id(self.request_id):
            raise IsloConfigurationError("request ID must be an Airflow pre-assigned UUID")
        if (
            isinstance(self.ttl_seconds, bool)
            or not isinstance(self.ttl_seconds, int)
            or self.ttl_seconds <= 0
        ):
            raise IsloConfigurationError("ttl_seconds must be a positive integer")


@dataclass(frozen=True)
class IsloSandboxHandle:
    """Validated Islo fields stored in a common opaque sandbox handle."""

    request_id: str
    sandbox_name: str
    sandbox_id: str
    execution_id: str

    _SCHEMA_VERSION: ClassVar[int] = 1

    def __post_init__(self) -> None:
        if not is_preassigned_executor_id(self.request_id):
            raise IsloConfigurationError("Islo handle request_id must be a canonical UUID")
        if self.sandbox_name != sandbox_name_from_request_id(self.request_id):
            raise IsloConfigurationError("Islo handle sandbox name does not match its request ID")
        for name in ("sandbox_id", "execution_id"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value:
                raise IsloConfigurationError(f"Islo handle {name} must be a non-empty string")

    def to_common(self) -> SandboxHandle:
        return SandboxHandle(
            data={
                "execution_id": self.execution_id,
                "schema_version": self._SCHEMA_VERSION,
                "request_id": self.request_id,
                "sandbox_id": self.sandbox_id,
                "sandbox_name": self.sandbox_name,
            },
            display_name=self.sandbox_name,
        )

    @classmethod
    def from_common(cls, handle: SandboxHandle) -> IsloSandboxHandle:
        expected_keys = {"execution_id", "request_id", "sandbox_id", "sandbox_name", "schema_version"}
        if set(handle.data) != expected_keys:
            raise IsloConfigurationError("Islo handle fields are invalid")
        try:
            schema_version = handle.data["schema_version"]
            values = {
                name: handle.data[name]
                for name in ("request_id", "sandbox_name", "sandbox_id", "execution_id")
            }
        except KeyError as error:
            raise IsloConfigurationError(f"Islo handle is missing {error.args[0]}") from error
        if (
            isinstance(schema_version, bool)
            or not isinstance(schema_version, int)
            or schema_version != cls._SCHEMA_VERSION
        ):
            raise IsloConfigurationError(f"unsupported Islo handle schema version: {schema_version!r}")
        if not all(isinstance(value, str) for value in values.values()):
            raise IsloConfigurationError("Islo handle fields must be strings")
        parsed = cls(**values)
        if handle.display_name is not None and handle.display_name != parsed.sandbox_name:
            raise IsloConfigurationError("Islo handle display name is inconsistent")
        return parsed


def sandbox_name_from_request_id(request_id: str) -> str:
    """Build the deterministic Islo sandbox name for an Airflow task attempt."""
    if not is_preassigned_executor_id(request_id):
        raise IsloConfigurationError("IsloExecutor requires an Airflow pre-assigned external executor ID")
    return f"airflow-{request_id.lower()}"


def coerce_islo_executor_config(value: Any) -> dict[str, Any]:
    """Validate Islo-specific values from ``executor_config['islo']``."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise IsloConfigurationError("executor_config must be a mapping")
    override = value.get("islo", {})
    if not isinstance(override, dict):
        raise IsloConfigurationError("executor_config['islo'] must be a mapping")
    allowed = {
        "disk_gb",
        "image",
        "memory_mb",
        "snapshot_name",
        "vcpus",
    }
    if unknown := sorted(set(override) - allowed):
        raise IsloConfigurationError(f"unsupported Islo executor_config keys: {', '.join(unknown)}")
    result = dict(override)
    for key in {"disk_gb", "memory_mb", "vcpus"} & result.keys():
        item = result[key]
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise IsloConfigurationError(f"Islo {key} must be a positive integer")
    for key in {"image", "snapshot_name"} & result.keys():
        item = result[key]
        if item is not None and (not isinstance(item, str) or not item.strip()):
            raise IsloConfigurationError(f"Islo {key} must be a non-empty string or null")
    return result
