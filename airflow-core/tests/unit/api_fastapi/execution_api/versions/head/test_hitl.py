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
from typing import TYPE_CHECKING, Any

import pytest
import time_machine
from httpx import Client
from uuid6 import uuid7

from airflow._shared.timezones.timezone import convert_to_utc
from airflow.models.hitl import HITLDetail

if TYPE_CHECKING:
    from fastapi.testclient import TestClient
    from sqlalchemy.orm import Session

    from airflow.models.taskinstance import TaskInstance

    from tests_common.pytest_plugin import CreateTaskInstance

pytestmark = pytest.mark.db_test

default_hitl_detail_request_kwargs: dict[str, Any] = {
    # ti_id decided at a later stage
    "subject": "This is subject",
    "body": "this is body",
    "options": ["Approve", "Reject"],
    "defaults": ["Approve"],
    "multiple": False,
    "params": {"input_1": 1},
    "assignees": None,
}
expected_empty_hitl_detail_response_part: dict[str, Any] = {
    "responded_at": None,
    "chosen_options": None,
    "responded_by_user": None,
    "params_input": {},
    "response_received": False,
}


@pytest.fixture
def sample_ti(create_task_instance: CreateTaskInstance) -> TaskInstance:
    return create_task_instance()


@pytest.fixture
def sample_hitl_detail(session: Session, sample_ti: TaskInstance) -> HITLDetail:
    hitl_detail_model = HITLDetail(
        ti_id=sample_ti.id,
        **default_hitl_detail_request_kwargs,
    )
    session.add(hitl_detail_model)
    session.commit()

    return hitl_detail_model


@pytest.fixture
def expected_sample_hitl_detail_dict(sample_ti: TaskInstance) -> dict[str, Any]:
    return {
        "ti_id": sample_ti.id,
        **default_hitl_detail_request_kwargs,
        **expected_empty_hitl_detail_response_part,
    }


@pytest.mark.parametrize(
    "existing_hitl_detail_args",
    [
        None,
        default_hitl_detail_request_kwargs,
        {
            **default_hitl_detail_request_kwargs,
            **{
                "params_input": {"input_1": 2},
                "responded_at": convert_to_utc(datetime(2025, 7, 3, 0, 0, 0)),
                "chosen_options": ["Reject"],
                "responded_by": None,
            },
        },
    ],
    ids=[
        "no existing hitl detail",
        "existing hitl detail without response",
        "existing hitl detail with response",
    ],
)
def test_upsert_hitl_detail(
    client: TestClient,
    create_task_instance: CreateTaskInstance,
    session: Session,
    existing_hitl_detail_args: dict[str, Any],
) -> None:
    ti = create_task_instance()
    session.commit()

    if existing_hitl_detail_args:
        session.add(HITLDetail(ti_id=ti.id, **existing_hitl_detail_args))
        session.commit()

    response = client.post(
        f"/execution/hitlDetails/{ti.id}",
        json={
            "ti_id": ti.id,
            **default_hitl_detail_request_kwargs,
        },
    )

    expected_json = {
        "ti_id": ti.id,
        **default_hitl_detail_request_kwargs,
    }
    expected_json["assigned_users"] = expected_json.pop("assignees") or []

    assert response.status_code == 201
    assert response.json() == expected_json


def test_upsert_hitl_detail_with_empty_option(
    client: TestClient,
    create_task_instance: CreateTaskInstance,
    session: Session,
) -> None:
    ti = create_task_instance()
    session.commit()

    response = client.post(
        f"/execution/hitlDetails/{ti.id}",
        json={
            "ti_id": ti.id,
            "subject": "This is subject",
            "body": "this is body",
            "options": [],
            "defaults": ["Approve"],
            "multiple": False,
            "params": {"input_1": 1},
        },
    )
    assert response.status_code == 422


@time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
@pytest.mark.usefixtures("sample_hitl_detail")
def test_update_hitl_detail(client: Client, sample_ti: TaskInstance) -> None:
    response = client.patch(
        f"/execution/hitlDetails/{sample_ti.id}",
        json={
            "ti_id": sample_ti.id,
            "chosen_options": ["Reject"],
            "params_input": {"input_1": 2},
        },
    )
    assert response.status_code == 200
    assert response.json() == {
        "params_input": {"input_1": 2},
        "responded_at": "2025-07-03T00:00:00Z",
        "chosen_options": ["Reject"],
        "response_received": True,
        "responded_by_user": None,
    }


def test_update_hitl_detail_without_option(client: Client, sample_ti: TaskInstance) -> None:
    response = client.patch(
        f"/execution/hitlDetails/{sample_ti.id}",
        json={
            "ti_id": sample_ti.id,
            "chosen_options": [],
            "params_input": {"input_1": 2},
        },
    )
    assert response.status_code == 422


def test_update_hitl_detail_without_ti(client: Client) -> None:
    ti_id = str(uuid7())
    response = client.patch(
        f"/execution/hitlDetails/{ti_id}",
        json={
            "ti_id": ti_id,
            "chosen_options": ["Reject"],
            "params_input": {"input_1": 2},
        },
    )
    assert response.status_code == 404
    assert response.json() == {
        "detail": {
            "message": "HITLDetail not found. This happens most likely due to clearing task instance before receiving response.",
            "reason": "not_found",
        },
    }


@pytest.mark.usefixtures("sample_hitl_detail")
def test_get_hitl_detail(client: Client, sample_ti: TaskInstance) -> None:
    response = client.get(f"/execution/hitlDetails/{sample_ti.id}")
    assert response.status_code == 200
    assert response.json() == expected_empty_hitl_detail_response_part


def test_get_hitl_detail_without_ti(client: Client) -> None:
    response = client.get(f"/execution/hitlDetails/{uuid7()}")
    assert response.status_code == 404
    assert response.json() == {
        "detail": {
            "message": "HITLDetail not found. This happens most likely due to clearing task instance before receiving response.",
            "reason": "not_found",
        },
    }
