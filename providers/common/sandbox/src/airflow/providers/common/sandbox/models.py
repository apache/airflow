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
"""Portable value objects used by sandbox executors and drivers."""

from __future__ import annotations

import base64
import binascii
import json
import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, TypeGuard
from uuid import UUID

from airflow.providers.common.sandbox.exceptions import (
    SandboxConfigurationError,
    SandboxInvalidHandleError,
)


def _copy_json_object(value: dict[str, Any], *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise SandboxConfigurationError(f"{field_name} must be a string-keyed JSON object")
    try:
        encoded = json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)
        copied = json.loads(encoded)
    except (RecursionError, TypeError, ValueError) as error:
        raise SandboxConfigurationError(f"{field_name} must contain only JSON values") from error
    if not isinstance(copied, dict):
        raise SandboxConfigurationError(f"{field_name} must be a string-keyed JSON object")
    return copied


def is_preassigned_executor_id(value: str | None) -> TypeGuard[str]:
    """Return whether ``value`` is a canonical UUID assigned by Airflow."""
    if not value:
        return False
    try:
        parsed = UUID(value)
    except (AttributeError, ValueError):
        return False
    return str(parsed) == value


class SandboxState(str, Enum):
    """Provider-neutral state of one sandbox workload."""

    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    GONE = "gone"


@dataclass(frozen=True)
class SandboxResult:
    """One normalized driver observation."""

    state: SandboxState
    exit_code: int | None = None
    message: str | None = None
    retry_after: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, SandboxState):
            raise SandboxConfigurationError("sandbox result state must be a SandboxState")
        if isinstance(self.exit_code, bool) or (
            self.exit_code is not None and not isinstance(self.exit_code, int)
        ):
            raise SandboxConfigurationError("sandbox result exit_code must be an integer or null")
        if self.message is not None and not isinstance(self.message, str):
            raise SandboxConfigurationError("sandbox result message must be a string or null")
        if self.retry_after is not None and (
            isinstance(self.retry_after, bool)
            or not isinstance(self.retry_after, (int, float))
            or not math.isfinite(self.retry_after)
            or self.retry_after <= 0
        ):
            raise SandboxConfigurationError("sandbox result retry_after must be a finite positive number")
        if self.state in {SandboxState.PENDING, SandboxState.RUNNING, SandboxState.GONE}:
            if self.exit_code is not None:
                raise SandboxConfigurationError(f"sandbox result {self.state.value} cannot have an exit code")
        if self.state is SandboxState.SUCCEEDED and self.exit_code not in {None, 0}:
            raise SandboxConfigurationError("a successful sandbox result cannot have a non-zero exit code")
        if self.state is SandboxState.FAILED and self.exit_code == 0:
            raise SandboxConfigurationError("a failed sandbox result cannot have a zero exit code")
        if self.state not in {SandboxState.PENDING, SandboxState.RUNNING} and self.retry_after is not None:
            raise SandboxConfigurationError("only non-terminal sandbox results can specify retry_after")


@dataclass(frozen=True)
class SandboxOutput:
    """Optional provider-captured diagnostic output."""

    stdout: str = ""
    stderr: str = ""
    truncated: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.stdout, str) or not isinstance(self.stderr, str):
            raise SandboxConfigurationError("sandbox output streams must be strings")
        if not isinstance(self.truncated, bool):
            raise SandboxConfigurationError("sandbox output truncated must be a boolean")


@dataclass(frozen=True)
class SandboxHandle:
    """Opaque, JSON-serializable identity returned by a sandbox driver."""

    data: dict[str, Any]
    display_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "data", _copy_json_object(self.data, field_name="sandbox handle"))
        if self.display_name is not None and (
            not isinstance(self.display_name, str) or not self.display_name.strip()
        ):
            raise SandboxConfigurationError("sandbox handle display_name must be a non-empty string or null")


@dataclass(frozen=True)
class SandboxLaunchConfig:
    """Configuration prepared by one concrete provider executor for a task attempt."""

    provider_config: dict[str, Any]
    env: dict[str, str] = field(default_factory=dict)
    workdir: str | None = None
    timeout_seconds: int = 3600
    ttl_seconds: int = 86400
    keep: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "provider_config",
            _copy_json_object(self.provider_config, field_name="provider_config"),
        )
        if not isinstance(self.env, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in self.env.items()
        ):
            raise SandboxConfigurationError("sandbox env must be a string-to-string mapping")
        if reserved := sorted(key for key in self.env if key.upper().startswith("AIRFLOW_")):
            raise SandboxConfigurationError(
                f"task overrides cannot set reserved Airflow env keys: {', '.join(reserved)}"
            )
        object.__setattr__(self, "env", dict(self.env))
        if self.workdir is not None and (not isinstance(self.workdir, str) or not self.workdir.strip()):
            raise SandboxConfigurationError("sandbox workdir must be a non-empty string or null")
        for name in ("timeout_seconds", "ttl_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise SandboxConfigurationError(f"sandbox {name} must be a positive integer")
        if self.ttl_seconds < self.timeout_seconds:
            raise SandboxConfigurationError("ttl_seconds must be greater than or equal to timeout_seconds")
        if not isinstance(self.keep, bool):
            raise SandboxConfigurationError("sandbox keep must be a boolean")


@dataclass(frozen=True)
class SandboxLaunchRequest:
    """Complete Task SDK workload request passed to a sandbox driver."""

    request_id: str
    command: tuple[str, ...]
    env: dict[str, str]
    provider_config: dict[str, Any]
    workdir: str | None
    timeout_seconds: int
    ttl_seconds: int
    keep: bool = False

    def __post_init__(self) -> None:
        if not is_preassigned_executor_id(self.request_id):
            raise SandboxConfigurationError("sandbox request_id must be an Airflow pre-assigned UUID")
        if not self.command or not all(isinstance(part, str) and part for part in self.command):
            raise SandboxConfigurationError("sandbox command must contain non-empty argv strings")
        object.__setattr__(self, "command", tuple(self.command))
        if not isinstance(self.env, dict) or not all(
            isinstance(key, str) and isinstance(value, str) for key, value in self.env.items()
        ):
            raise SandboxConfigurationError("sandbox request env must be a string-to-string mapping")
        object.__setattr__(self, "env", dict(self.env))
        object.__setattr__(
            self,
            "provider_config",
            _copy_json_object(self.provider_config, field_name="provider_config"),
        )
        if self.workdir is not None and (not isinstance(self.workdir, str) or not self.workdir.strip()):
            raise SandboxConfigurationError("sandbox workdir must be a non-empty string or null")
        for name in ("timeout_seconds", "ttl_seconds"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise SandboxConfigurationError(f"sandbox {name} must be a positive integer")
        if self.ttl_seconds < self.timeout_seconds:
            raise SandboxConfigurationError("ttl_seconds must be greater than or equal to timeout_seconds")
        if not isinstance(self.keep, bool):
            raise SandboxConfigurationError("sandbox keep must be a boolean")


@dataclass(frozen=True)
class RecoveredSandbox:
    """Handle and lifecycle policy reconstructed by a driver after scheduler restart."""

    handle: SandboxHandle
    keep: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.handle, SandboxHandle):
            raise SandboxConfigurationError("recovered sandbox handle must be a SandboxHandle")
        if not isinstance(self.keep, bool):
            raise SandboxConfigurationError("recovered sandbox keep must be a boolean")


@dataclass(frozen=True)
class SandboxExecutionRef:
    """Durable provider-neutral identity persisted in ``external_executor_id``."""

    driver: str
    request_id: str
    handle: SandboxHandle
    keep: bool = False

    _PREFIX = "sandbox:v1:"
    _MAX_ENCODED_LENGTH = 65536

    def __post_init__(self) -> None:
        if not isinstance(self.driver, str) or not self.driver.strip():
            raise SandboxConfigurationError("sandbox driver id must be a non-empty string")
        if not is_preassigned_executor_id(self.request_id):
            raise SandboxConfigurationError("sandbox execution request_id must be a canonical UUID")
        if not isinstance(self.handle, SandboxHandle):
            raise SandboxConfigurationError("sandbox execution handle must be a SandboxHandle")
        if not isinstance(self.keep, bool):
            raise SandboxConfigurationError("sandbox execution keep must be a boolean")

    def encode(self) -> str:
        payload = json.dumps(
            {
                "display_name": self.handle.display_name,
                "driver": self.driver,
                "handle": self.handle.data,
                "keep": self.keep,
                "request_id": self.request_id,
            },
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
        encoded = f"{self._PREFIX}{base64.urlsafe_b64encode(payload).rstrip(b'=').decode()}"
        if len(encoded) > self._MAX_ENCODED_LENGTH:
            raise SandboxConfigurationError("sandbox execution reference is too large")
        return encoded

    @classmethod
    def decode(
        cls,
        value: str | None,
        *,
        expected_driver: str | None = None,
        strict: bool = False,
    ) -> SandboxExecutionRef | None:
        if not value or len(value) > cls._MAX_ENCODED_LENGTH or not value.startswith(cls._PREFIX):
            return None
        try:
            token = value.removeprefix(cls._PREFIX).encode("ascii")
            padded_token = token + b"=" * (-len(token) % 4)
            decoded = base64.b64decode(padded_token, altchars=b"-_", validate=True)
            if base64.urlsafe_b64encode(decoded).rstrip(b"=") != token:
                raise binascii.Error("sandbox execution reference is not canonical base64")
            payload = json.loads(decoded.decode())
            expected_fields = {"display_name", "driver", "handle", "keep", "request_id"}
            if not isinstance(payload, dict) or set(payload) != expected_fields:
                raise SandboxConfigurationError("sandbox execution reference fields are invalid")
            driver = payload["driver"]
            request_id = payload["request_id"]
            handle_data = payload["handle"]
            display_name = payload["display_name"]
            keep = payload["keep"]
            if not isinstance(driver, str) or not isinstance(request_id, str):
                raise SandboxConfigurationError("sandbox execution reference identity is invalid")
            if expected_driver is not None and driver != expected_driver:
                raise SandboxInvalidHandleError(
                    f"sandbox execution reference belongs to driver {driver!r}, expected {expected_driver!r}"
                )
            if not isinstance(handle_data, dict) or not isinstance(keep, bool):
                raise SandboxConfigurationError("sandbox execution reference handle is invalid")
            return cls(
                driver=driver,
                request_id=request_id,
                handle=SandboxHandle(data=handle_data, display_name=display_name),
                keep=keep,
            )
        except SandboxInvalidHandleError:
            if strict:
                raise
            return None
        except (
            binascii.Error,
            json.JSONDecodeError,
            SandboxConfigurationError,
            KeyError,
            TypeError,
            UnicodeEncodeError,
            UnicodeDecodeError,
            ValueError,
            RecursionError,
        ) as error:
            if strict:
                raise SandboxInvalidHandleError("sandbox execution reference is malformed") from error
            return None

    @classmethod
    def has_envelope(cls, value: str | None) -> bool:
        """Return whether ``value`` claims to be a common sandbox reference."""
        return bool(value and value.startswith(cls._PREFIX))


@dataclass(frozen=True)
class SandboxLaunchOutcome:
    """Validated launch identity ready to persist in Airflow."""

    ref: SandboxExecutionRef
    external_executor_id: str = field(init=False)

    def __post_init__(self) -> None:
        if not isinstance(self.ref, SandboxExecutionRef):
            raise SandboxConfigurationError("sandbox launch outcome ref must be a SandboxExecutionRef")
        object.__setattr__(self, "external_executor_id", self.ref.encode())


def coerce_sandbox_executor_config(value: Any) -> dict[str, Any]:
    """Validate portable values from ``executor_config['sandbox']``."""
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise SandboxConfigurationError("executor_config must be a mapping")
    override = value.get("sandbox", {})
    if not isinstance(override, dict):
        raise SandboxConfigurationError("executor_config['sandbox'] must be a mapping")
    allowed = {"env", "keep", "timeout_seconds", "ttl_seconds", "workdir"}
    if unknown := sorted(set(override) - allowed):
        raise SandboxConfigurationError(f"unsupported sandbox executor_config keys: {', '.join(unknown)}")
    result = dict(override)
    for key in {"timeout_seconds", "ttl_seconds"} & result.keys():
        item = result[key]
        if isinstance(item, bool) or not isinstance(item, int) or item <= 0:
            raise SandboxConfigurationError(f"sandbox {key} must be a positive integer")
    if "keep" in result and not isinstance(result["keep"], bool):
        raise SandboxConfigurationError("sandbox keep must be a boolean")
    if "workdir" in result:
        item = result["workdir"]
        if item is not None and (not isinstance(item, str) or not item.strip()):
            raise SandboxConfigurationError("sandbox workdir must be a non-empty string or null")
    env = result.get("env", {})
    if not isinstance(env, dict) or not all(
        isinstance(key, str) and isinstance(item, str) for key, item in env.items()
    ):
        raise SandboxConfigurationError("sandbox env must be a string-to-string mapping")
    if reserved := sorted(key for key in env if key.upper().startswith("AIRFLOW_")):
        raise SandboxConfigurationError(
            f"task overrides cannot set reserved Airflow env keys: {', '.join(reserved)}"
        )
    result["env"] = dict(env)
    return result
