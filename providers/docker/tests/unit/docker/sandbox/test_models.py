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

from airflow.providers.common.sandbox.exceptions import SandboxInvalidHandleError
from airflow.providers.common.sandbox.models import SandboxHandle
from airflow.providers.docker.sandbox.exceptions import (
    DockerSandboxConfigurationError,
    DockerSandboxProtocolError,
)
from airflow.providers.docker.sandbox.models import (
    DockerSandboxDriverConfig,
    DockerSandboxHandle,
    DockerSandboxLaunchConfig,
    DockerSandboxMetadata,
    DockerSandboxStatus,
    sandbox_name_from_request_id,
)


@pytest.mark.parametrize("scratch_root", ["relative", "/tmp/../scratch", ""])
def test_driver_config_requires_absolute_normalized_scratch_root(scratch_root: str) -> None:
    with pytest.raises(DockerSandboxConfigurationError, match="scratch_root"):
        DockerSandboxDriverConfig(scratch_root=scratch_root)


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({}, "needs template"),
        ({"template": "python", "unknown": 1}, "unsupported"),
        ({"template": "python", "cpus": True}, "cpus"),
        ({"template": "python", "memory": "lots"}, "memory"),
    ],
)
def test_launch_config_is_strict(value: dict, message: str) -> None:
    with pytest.raises(DockerSandboxConfigurationError, match=message):
        DockerSandboxLaunchConfig.from_json(value)


def test_handle_round_trip_and_schema_validation() -> None:
    request_id = str(uuid4())
    parsed = DockerSandboxHandle(
        request_id=request_id,
        sandbox_name=sandbox_name_from_request_id(request_id),
        sandbox_id="stable-id",
    )

    assert DockerSandboxHandle.from_common(parsed.to_common()) == parsed

    with pytest.raises(SandboxInvalidHandleError, match="schema"):
        DockerSandboxHandle.from_common(
            SandboxHandle(
                {
                    "request_id": request_id,
                    "sandbox_name": sandbox_name_from_request_id(request_id),
                    "sandbox_id": "stable-id",
                    "schema_version": True,
                }
            )
        )


def test_metadata_rejects_identity_that_does_not_match_request() -> None:
    request_id = str(uuid4())
    value = {
        "request_id": request_id,
        "sandbox_id": "stable-id",
        "sandbox_name": "airflow-other",
        "schema_version": 1,
    }

    with pytest.raises(DockerSandboxProtocolError, match="name does not match"):
        DockerSandboxMetadata.from_json(value)


@pytest.mark.parametrize(
    "change",
    [
        {"state": "unknown"},
        {"state": "running", "exit_code": 0},
        {"state": "succeeded", "exit_code": 7},
        {"state": "failed", "exit_code": 0},
        {"schema_version": True},
    ],
)
def test_status_rejects_inconsistent_or_unknown_values(change: dict) -> None:
    request_id = str(uuid4())
    value = {
        "exit_code": None,
        "message": None,
        "request_id": request_id,
        "schema_version": 1,
        "state": "running",
    }
    value.update(change)

    with pytest.raises(DockerSandboxProtocolError):
        DockerSandboxStatus.from_json(value, expected_request_id=request_id)
