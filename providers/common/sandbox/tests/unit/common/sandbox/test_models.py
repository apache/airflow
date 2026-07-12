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

from uuid import uuid4

import pytest

from airflow.providers.common.sandbox.exceptions import SandboxConfigurationError
from airflow.providers.common.sandbox.models import (
    SandboxExecutionRef,
    SandboxHandle,
    SandboxLaunchConfig,
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
