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
"""Value objects shared by the Islo hook and executor."""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeGuard
from uuid import UUID

from airflow.providers.islo.exceptions import IsloConfigurationError


class IsloExecutionState(str, Enum):
    """Normalized state of an Islo command execution."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    GONE = "gone"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class IsloExecutionResult:
    """Status of one remote command without captured output."""

    state: IsloExecutionState
    exit_code: int | None = None


@dataclass(frozen=True)
class IsloSandboxSpec:
    """Creation parameters for one task sandbox."""

    name: str
    request_id: str
    image: str | None = None
    snapshot_name: str | None = None
    snapshot_url: str | None = None
    vcpus: int | None = None
    memory_mb: int | None = None
    disk_gb: int | None = None
    timeout_seconds: int = 3600
    ttl_seconds: int = 86400
    env: dict[str, str] = field(default_factory=dict)
    workdir: str | None = None
    gateway_profile: str | None = None
    internet_enabled: bool = True
    keep: bool = False

    def __post_init__(self) -> None:
        if not self.name or not self.request_id:
            raise IsloConfigurationError("sandbox name and request ID must be non-empty")
        sources = (self.image, self.snapshot_name, self.snapshot_url)
        if sum(source is not None for source in sources) != 1:
            raise IsloConfigurationError(
                "exactly one of image, snapshot_name, or snapshot_url must be configured"
            )
        if any(not isinstance(source, str) or not source.strip() for source in sources if source is not None):
            raise IsloConfigurationError("image and snapshot sources must be non-empty strings")
        for field_name in ("vcpus", "memory_mb", "disk_gb", "timeout_seconds", "ttl_seconds"):
            value = getattr(self, field_name)
            if value is not None and value <= 0:
                raise IsloConfigurationError(f"{field_name} must be greater than zero")
        if self.ttl_seconds < self.timeout_seconds:
            raise IsloConfigurationError("ttl_seconds must be greater than or equal to timeout_seconds")


@dataclass(frozen=True)
class IsloExecutionRef:
    """Durable identity required to poll or adopt an Islo execution."""

    request_id: str
    sandbox_name: str
    sandbox_id: str
    execution_id: str
    keep: bool = False

    _PREFIX = "islo:v1:"

    def encode(self) -> str:
        payload = json.dumps(
            {
                "execution_id": self.execution_id,
                "request_id": self.request_id,
                "sandbox_id": self.sandbox_id,
                "sandbox_name": self.sandbox_name,
                "keep": self.keep,
            },
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        return f"{self._PREFIX}{base64.urlsafe_b64encode(payload).rstrip(b'=').decode()}"

    @classmethod
    def decode(cls, value: str | None) -> IsloExecutionRef | None:
        if not value or not value.startswith(cls._PREFIX):
            return None
        token = value.removeprefix(cls._PREFIX)
        token += "=" * (-len(token) % 4)
        try:
            payload = json.loads(base64.urlsafe_b64decode(token).decode())
            values = {
                key: payload[key] for key in ("request_id", "sandbox_name", "sandbox_id", "execution_id")
            }
        except (binascii.Error, json.JSONDecodeError, KeyError, TypeError, UnicodeDecodeError, ValueError):
            return None
        if not all(isinstance(value, str) and value for value in values.values()):
            return None
        keep = payload.get("keep", False)
        if not isinstance(keep, bool):
            return None
        return cls(**values, keep=keep)


@dataclass(frozen=True)
class RunningIsloSandbox:
    """Executor bookkeeping for a launched task."""

    ref: IsloExecutionRef

    @property
    def keep(self) -> bool:
        """Return whether the sandbox should remain after a terminal task state."""
        return self.ref.keep


def is_preassigned_executor_id(value: str | None) -> TypeGuard[str]:
    """Return whether ``value`` is the UUID pre-assigned by Airflow 3.3+."""
    if not value:
        return False
    try:
        UUID(value)
    except ValueError:
        return False
    return True


def sandbox_name_from_request_id(request_id: str) -> str:
    """Build a stable name from Airflow's pre-assigned UUID."""
    if not is_preassigned_executor_id(request_id):
        raise IsloConfigurationError("IsloExecutor requires an Airflow pre-assigned external executor ID")
    return f"airflow-{request_id.lower()}"


def coerce_islo_executor_config(value: Any) -> dict[str, Any]:
    """Validate and copy the Islo-specific part of a task's ``executor_config``."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise IsloConfigurationError("executor_config must be a mapping")
    override = value.get("islo", {})
    if not isinstance(override, dict):
        raise IsloConfigurationError("executor_config['islo'] must be a mapping")
    allowed = {
        "disk_gb",
        "env",
        "gateway_profile",
        "image",
        "internet_enabled",
        "keep",
        "memory_mb",
        "snapshot_name",
        "snapshot_url",
        "timeout_seconds",
        "ttl_seconds",
        "vcpus",
        "workdir",
    }
    if unknown := sorted(set(override) - allowed):
        raise IsloConfigurationError(f"unsupported Islo executor_config keys: {', '.join(unknown)}")
    result = dict(override)

    positive_integer_keys = {
        "disk_gb",
        "memory_mb",
        "timeout_seconds",
        "ttl_seconds",
        "vcpus",
    }
    for key in positive_integer_keys & result.keys():
        value = result[key]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise IsloConfigurationError(f"Islo {key} must be a positive integer")

    for key in {"internet_enabled", "keep"} & result.keys():
        if not isinstance(result[key], bool):
            raise IsloConfigurationError(f"Islo {key} must be a boolean")

    optional_string_keys = {
        "gateway_profile",
        "image",
        "snapshot_name",
        "snapshot_url",
        "workdir",
    }
    for key in optional_string_keys & result.keys():
        value = result[key]
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise IsloConfigurationError(f"Islo {key} must be a non-empty string or null")

    env = result.get("env", {})
    if not isinstance(env, dict) or not all(
        isinstance(k, str) and isinstance(v, str) for k, v in env.items()
    ):
        raise IsloConfigurationError("Islo env must be a string-to-string mapping")
    if reserved := sorted(key for key in env if key.upper().startswith("AIRFLOW_")):
        raise IsloConfigurationError(
            f"task overrides cannot set reserved Airflow env keys: {', '.join(reserved)}"
        )
    result["env"] = dict(env)
    return result
