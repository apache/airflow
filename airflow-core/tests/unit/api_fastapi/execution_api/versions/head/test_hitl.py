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

from datetime import datetime

import pytest
import time_machine
from uuid6 import uuid7

from tests_common.test_utils.db import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop public API compatible with Airflow >= 3.0.1", allow_module_level=True)

from typing import TYPE_CHECKING, Any

from airflow.models.hitl import HITLResponseModel

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance

pytestmark = pytest.mark.db_test
TI_ID = uuid7()


@pytest.fixture
def sample_ti(create_task_instance) -> TaskInstance:
    return create_task_instance()


@pytest.fixture
def sample_hitl_response(session, sample_ti) -> HITLResponseModel:
    hitl_response_model = HITLResponseModel(
        ti_id=sample_ti.id,
        options=["Approve", "Reject"],
        subject="This is subject",
        body="this is body",
        default=["Approve"],
        multiple=False,
        params={"input_1": 1},
    )
    session.add(hitl_response_model)
    session.commit()

    return hitl_response_model


@pytest.fixture
def expected_sample_hitl_response_dict(sample_ti) -> dict[str, Any]:
    return {
        "body": "this is body",
        "default": ["Approve"],
        "multiple": False,
        "options": ["Approve", "Reject"],
        "params": {"input_1": 1},
        "params_input": {},
        "response_at": None,
        "response_content": None,
        "response_received": False,
        "subject": "This is subject",
        "ti_id": sample_ti.id,
        "user_id": None,
    }


def test_add_hitl_response(client, create_task_instance, session) -> None:
    ti = create_task_instance()
    session.commit()

    response = client.post(
        f"/execution/hitl-responses/{ti.id}",
        json={
            "ti_id": ti.id,
            "options": ["Approve", "Reject"],
            "subject": "This is subject",
            "body": "this is body",
            "default": ["Approve"],
            "multiple": False,
            "params": {"input_1": 1},
        },
    )
    assert response.status_code == 201
    assert response.json() == {
        "ti_id": ti.id,
        "options": ["Approve", "Reject"],
        "subject": "This is subject",
        "body": "this is body",
        "default": ["Approve"],
        "multiple": False,
        "params": {"input_1": 1},
    }


@time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
@pytest.mark.usefixtures("sample_hitl_response")
def test_update_hitl_response(client, sample_ti) -> None:
    response = client.patch(
        f"/execution/hitl-responses/{sample_ti.id}",
        json={
            "ti_id": sample_ti.id,
            "response_content": ["Reject"],
            "params_input": {"input_1": 2},
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "params_input": {"input_1": 2},
        "response_at": "2025-07-03T00:00:00Z",
        "response_content": ["Reject"],
        "response_received": True,
        "user_id": "Fallback to default",
    }


@pytest.mark.usefixtures("sample_hitl_response")
def test_get_hitl_response(client, sample_ti) -> None:
    response = client.get(f"/execution/hitl-responses/{sample_ti.id}")
    assert response.status_code == 200
    assert response.json() == {
        "params_input": {},
        "response_at": None,
        "response_content": None,
        "response_received": False,
        "user_id": None,
    }
