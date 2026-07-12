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

from airflow.providers.islo.exceptions import IsloConfigurationError
from airflow.providers.islo.models import (
    IsloExecutionRef,
    IsloSandboxSpec,
    coerce_islo_executor_config,
    sandbox_name_from_request_id,
)


def test_execution_ref_round_trip() -> None:
    ref = IsloExecutionRef("request", "sandbox", "sandbox-id", "exec-id", keep=True)
    assert IsloExecutionRef.decode(ref.encode()) == ref
    assert IsloExecutionRef.decode("islo:v1:not-base64") is None


def test_execution_ref_decodes_legacy_value_without_keep() -> None:
    ref = IsloExecutionRef("request", "sandbox", "sandbox-id", "exec-id")
    encoded = ref.encode()
    decoded = IsloExecutionRef.decode(encoded)
    assert decoded is not None
    assert decoded.keep is False


def test_sandbox_name_uses_preassigned_uuid() -> None:
    request_id = str(uuid4())
    assert sandbox_name_from_request_id(request_id) == f"airflow-{request_id}"


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"image": "image", "snapshot_name": "snapshot"},
        {"image": "image", "timeout_seconds": 0},
        {"image": "image", "timeout_seconds": 10, "ttl_seconds": 9},
    ],
)
def test_invalid_sandbox_specs_are_rejected(kwargs) -> None:
    with pytest.raises(IsloConfigurationError):
        IsloSandboxSpec(name="sandbox", request_id=str(uuid4()), **kwargs)


def test_executor_config_rejects_unknown_and_reserved_values() -> None:
    with pytest.raises(IsloConfigurationError, match="unsupported"):
        coerce_islo_executor_config({"islo": {"command": ["rm", "-rf", "/"]}})
    with pytest.raises(IsloConfigurationError, match="reserved"):
        coerce_islo_executor_config({"islo": {"env": {"AIRFLOW__CORE__FERNET_KEY": "secret"}}})
    with pytest.raises(IsloConfigurationError, match="positive integer"):
        coerce_islo_executor_config({"islo": {"vcpus": True}})
    with pytest.raises(IsloConfigurationError, match="boolean"):
        coerce_islo_executor_config({"islo": {"internet_enabled": "false"}})
