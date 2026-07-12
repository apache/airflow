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

import base64
import json
from uuid import uuid4

import pytest

from airflow.providers.common.sandbox.exceptions import (
    SandboxConfigurationError,
    SandboxInvalidHandleError,
)
from airflow.providers.common.sandbox.models import (
    SandboxExecutionRef,
    SandboxHandle,
    SandboxLaunchConfig,
    SandboxResult,
    SandboxState,
    coerce_sandbox_executor_config,
)


def test_execution_reference_round_trip_and_driver_guard() -> None:
    ref = SandboxExecutionRef(
        driver="vendor",
        request_id=str(uuid4()),
        handle=SandboxHandle({"job": "123"}, "job-123"),
        keep=True,
    )

    assert SandboxExecutionRef.decode(ref.encode(), expected_driver="vendor") == ref
    assert SandboxExecutionRef.decode(ref.encode(), expected_driver="other") is None
    assert SandboxExecutionRef.decode("sandbox:v1:not-base64") is None
    corrupted = ref.encode().replace("sandbox:v1:", "sandbox:v1:!!!!", 1)
    assert SandboxExecutionRef.decode(corrupted) is None
    with pytest.raises(SandboxInvalidHandleError, match="other"):
        SandboxExecutionRef.decode(ref.encode(), expected_driver="other", strict=True)
    with pytest.raises(SandboxInvalidHandleError, match="malformed"):
        SandboxExecutionRef.decode("sandbox:v1:not-base64", strict=True)
    with pytest.raises(SandboxInvalidHandleError, match="malformed"):
        SandboxExecutionRef.decode(corrupted, strict=True)

    payload_with_extra_field = {
        "display_name": ref.handle.display_name,
        "driver": ref.driver,
        "handle": ref.handle.data,
        "keep": ref.keep,
        "request_id": ref.request_id,
        "unexpected": True,
    }
    encoded_payload = base64.urlsafe_b64encode(
        json.dumps(payload_with_extra_field, separators=(",", ":"), sort_keys=True).encode()
    ).rstrip(b"=")
    extra_field = f"sandbox:v1:{encoded_payload.decode()}"
    assert SandboxExecutionRef.decode(extra_field) is None
    with pytest.raises(SandboxInvalidHandleError, match="malformed"):
        SandboxExecutionRef.decode(extra_field, strict=True)

    assert SandboxExecutionRef.decode("sandbox:v1:\u00e9") is None
    with pytest.raises(SandboxInvalidHandleError, match="malformed"):
        SandboxExecutionRef.decode("sandbox:v1:\u00e9", strict=True)

    nested_json = "[" * 20000 + "null" + "]" * 20000
    payload = (
        '{"display_name":null,"driver":"vendor","handle":{"nested":'
        + nested_json
        + f'}},"keep":false,"request_id":"{ref.request_id}"}}'
    )
    deeply_nested = "sandbox:v1:" + base64.urlsafe_b64encode(payload.encode()).rstrip(b"=").decode()
    assert SandboxExecutionRef.decode(deeply_nested) is None
    with pytest.raises(SandboxInvalidHandleError, match="malformed"):
        SandboxExecutionRef.decode(deeply_nested, strict=True)


@pytest.mark.parametrize(
    ("state", "exit_code", "retry_after"),
    [
        (SandboxState.PENDING, 0, None),
        (SandboxState.RUNNING, 0, None),
        (SandboxState.GONE, 0, None),
        (SandboxState.SUCCEEDED, 1, None),
        (SandboxState.FAILED, 0, None),
        (SandboxState.FAILED, 1, 1.0),
    ],
)
def test_result_rejects_inconsistent_provider_observations(
    state: SandboxState,
    exit_code: int,
    retry_after: float | None,
) -> None:
    with pytest.raises(SandboxConfigurationError):
        SandboxResult(state, exit_code=exit_code, retry_after=retry_after)


def test_handles_must_be_json_serializable() -> None:
    with pytest.raises(SandboxConfigurationError, match="JSON"):
        SandboxHandle({"bad": object()})


def test_launch_config_protects_runtime_environment_and_lifecycle() -> None:
    with pytest.raises(SandboxConfigurationError, match="reserved"):
        SandboxLaunchConfig(
            provider_config={},
            env={"AIRFLOW__CORE__FERNET_KEY": "secret"},
        )
    with pytest.raises(SandboxConfigurationError, match="greater than or equal"):
        SandboxLaunchConfig(
            provider_config={},
            timeout_seconds=20,
            ttl_seconds=10,
        )


def test_portable_executor_config_is_allowlisted() -> None:
    assert coerce_sandbox_executor_config({"sandbox": {"env": {"MODEL": "small"}, "keep": True}}) == {
        "env": {"MODEL": "small"},
        "keep": True,
    }
    with pytest.raises(SandboxConfigurationError, match="unsupported"):
        coerce_sandbox_executor_config({"sandbox": {"command": ["rm", "-rf", "/"]}})
