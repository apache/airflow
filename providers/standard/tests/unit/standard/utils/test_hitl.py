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

import pytest

from airflow.providers.common.compat.sdk import Param, ParamValidationError
from airflow.providers.standard.exceptions import HITLTimeoutError, HITLTriggerEventError
from airflow.providers.standard.utils.hitl import (
    params_from_serialized,
    raise_for_error_event,
    validate_params_input,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_3_PLUS

SERIALIZED_PARAMS = {
    "reason": {"value": "", "description": "Why", "schema": {"type": ["string", "null"]}, "source": "task"},
    "amount": {"value": 1, "description": None, "schema": {"type": "integer", "minimum": 1}, "source": None},
}


@pytest.mark.skipif(not AIRFLOW_V_3_1_3_PLUS, reason="Param.serialize() form is 3.1.3+")
def test_params_from_serialized_round_trips_param_serialize():
    original = {"reason": Param("", type=["string", "null"], description="Why")}

    rebuilt = params_from_serialized({key: param.serialize() for key, param in original.items()})

    assert rebuilt.get_param("reason").serialize() == original["reason"].serialize()


def test_params_from_serialized_keeps_each_schema():
    params = params_from_serialized(SERIALIZED_PARAMS)

    assert params.get_param("amount").schema == {"type": "integer", "minimum": 1}
    assert params.get_param("reason").description == "Why"
    assert params["reason"] == ""


def test_validate_params_input_writes_valid_values():
    params = params_from_serialized(SERIALIZED_PARAMS)

    validate_params_input({"reason": "over limit", "amount": 5}, params)

    assert params["reason"] == "over limit"
    assert params["amount"] == 5


@pytest.mark.parametrize(
    ("params_input", "error", "match"),
    [
        pytest.param({"reason": "x"}, ValueError, "does not match params", id="missing-field"),
        pytest.param(
            {"reason": "x", "amount": 1, "note": "y"}, ValueError, "does not match params", id="unknown-field"
        ),
        pytest.param(
            {"reason": "x", "amount": 0},
            ParamValidationError,
            "Invalid input for param amount",
            id="fails-schema",
        ),
    ],
)
def test_validate_params_input_refuses_input_that_does_not_fit(params_input, error, match):
    params = params_from_serialized(SERIALIZED_PARAMS)

    with pytest.raises(error, match=match):
        validate_params_input(params_input, params)


@pytest.mark.parametrize(
    ("event", "error"),
    [
        pytest.param({"error": "no answer", "error_type": "timeout"}, HITLTimeoutError, id="timeout"),
        pytest.param({"error": "boom", "error_type": "unknown"}, HITLTriggerEventError, id="other"),
    ],
)
def test_raise_for_error_event_maps_the_error_type(event, error):
    with pytest.raises(error) as exc_info:
        raise_for_error_event(event)

    # HITLTimeoutError subclasses HITLTriggerEventError, so check the exact class.
    assert type(exc_info.value) is error
