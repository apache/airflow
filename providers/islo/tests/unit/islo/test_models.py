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

from airflow.providers.common.sandbox.models import SandboxHandle
from airflow.providers.islo.exceptions import IsloConfigurationError
from airflow.providers.islo.models import (
    IsloSandboxConfig,
    IsloSandboxHandle,
    IsloSandboxSpec,
    coerce_islo_executor_config,
    sandbox_name_from_request_id,
)


def test_islo_handle_round_trips_through_common_envelope() -> None:
    request_id = str(uuid4())
    handle = IsloSandboxHandle(
        request_id,
        sandbox_name_from_request_id(request_id),
        "sandbox-id",
        "exec-id",
    )
    assert IsloSandboxHandle.from_common(handle.to_common()) == handle

    invalid = SandboxHandle({**handle.to_common().data, "schema_version": 2})
    with pytest.raises(IsloConfigurationError, match="schema version"):
        IsloSandboxHandle.from_common(invalid)

    with pytest.raises(IsloConfigurationError, match="fields"):
        IsloSandboxHandle.from_common(SandboxHandle({**handle.to_common().data, "unexpected": True}))
    with pytest.raises(IsloConfigurationError, match="display name"):
        IsloSandboxHandle.from_common(SandboxHandle(handle.to_common().data, "another-sandbox"))


def test_sandbox_name_uses_preassigned_uuid() -> None:
    request_id = str(uuid4())
    assert sandbox_name_from_request_id(request_id) == f"airflow-{request_id}"


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"image": "image", "snapshot_name": "snapshot"},
        {"image": ""},
        {"image": "image", "vcpus": 0},
    ],
)
def test_invalid_islo_configs_are_rejected(kwargs) -> None:
    with pytest.raises(IsloConfigurationError):
        IsloSandboxConfig(**kwargs)


def test_sandbox_spec_requires_positive_ttl() -> None:
    request_id = str(uuid4())
    with pytest.raises(IsloConfigurationError, match="ttl_seconds"):
        IsloSandboxSpec(
            name=sandbox_name_from_request_id(request_id),
            request_id=request_id,
            config=IsloSandboxConfig(image="image"),
            ttl_seconds=0,
        )


def test_islo_executor_config_is_provider_specific_and_allowlisted() -> None:
    assert coerce_islo_executor_config({"islo": {"snapshot_name": "runtime", "vcpus": 8}}) == {
        "snapshot_name": "runtime",
        "vcpus": 8,
    }
    with pytest.raises(IsloConfigurationError, match="unsupported"):
        coerce_islo_executor_config({"islo": {"command": ["rm", "-rf", "/"]}})
    with pytest.raises(IsloConfigurationError, match="unsupported"):
        coerce_islo_executor_config({"islo": {"timeout_seconds": 10}})


@pytest.mark.parametrize("key", ["gateway_profile", "internet_enabled"])
def test_task_config_cannot_set_deployment_network_policy(key: str) -> None:
    with pytest.raises(IsloConfigurationError, match="unsupported"):
        coerce_islo_executor_config({"islo": {key: "value"}})


def test_snapshot_url_is_not_an_islo_launch_option() -> None:
    with pytest.raises(IsloConfigurationError, match="unsupported"):
        coerce_islo_executor_config({"islo": {"snapshot_url": "oci://runtime"}})

    with pytest.raises(IsloConfigurationError, match="unsupported"):
        IsloSandboxConfig.from_json({"image": "runtime", "snapshot_url": "oci://runtime"})
