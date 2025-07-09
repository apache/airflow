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

from tests_common.test_utils.db import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop public API compatible with Airflow >= 3.0.1", allow_module_level=True)

from datetime import datetime
from typing import TYPE_CHECKING, Any

import time_machine
from uuid6 import uuid7

from airflow.models.hitl import HITLDetail

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance


pytestmark = pytest.mark.db_test
TI_ID = uuid7()


@pytest.fixture
def sample_ti(create_task_instance) -> TaskInstance:
    return create_task_instance()


@pytest.fixture
def sample_hitl_detail(session, sample_ti) -> HITLDetail:
    hitl_detail_model = HITLDetail(
        ti_id=sample_ti.id,
        options=["Approve", "Reject"],
        subject="This is subject",
        body="this is body",
        default=["Approve"],
        multiple=False,
        params={"input_1": 1},
    )
    session.add(hitl_detail_model)
    session.commit()

    return hitl_detail_model


@pytest.fixture
def expected_sample_hitl_detail_dict(sample_ti) -> dict[str, Any]:
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


class TestUpdateHITLDetailEndpoint:
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(self, test_client, sample_ti):
        response = test_client.patch(
            f"/hitl-details/{sample_ti.id}",
            json={"response_content": ["Approve"], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "response_content": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }

    def test_should_respond_404(self, test_client, sample_ti):
        response = test_client.get(f"/hitl-details/{sample_ti.id}")
        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Human-in-the-loop detail not found",
                "reason": "not_found",
            },
        }

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_409(self, test_client, sample_ti, expected_sample_hitl_detail_dict):
        response = test_client.patch(
            f"/hitl-details/{sample_ti.id}",
            json={"response_content": ["Approve"], "params_input": {"input_1": 2}},
        )

        expected_response = {
            "params_input": {"input_1": 2},
            "response_content": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }
        assert response.status_code == 200
        assert response.json() == expected_response

        response = test_client.patch(
            f"/hitl-details/{sample_ti.id}",
            json={"response_content": ["Approve"], "params_input": {"input_1": 2}},
        )
        assert response.status_code == 409
        assert response.json() == {
            "detail": (
                "Human-in-the-loop detail has already been updated for Task Instance "
                f"with id {sample_ti.id} "
                "and is not allowed to write again."
            )
        }

    def test_should_respond_401(self, unauthenticated_test_client, sample_ti):
        response = unauthenticated_test_client.get(f"/hitl-details/{sample_ti.id}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client, sample_ti):
        response = unauthorized_test_client.get(f"/hitl-details/{sample_ti.id}")
        assert response.status_code == 403


class TestGetHITLDetailEndpoint:
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self, test_client, sample_ti, expected_sample_hitl_detail_dict
    ):
        response = test_client.get(f"/hitl-details/{sample_ti.id}")
        assert response.status_code == 200
        assert response.json() == expected_sample_hitl_detail_dict

    def test_should_respond_404(self, test_client, sample_ti):
        response = test_client.get(f"/hitl-details/{sample_ti.id}")
        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Human-in-the-loop detail not found",
                "reason": "not_found",
            },
        }

    def test_should_respond_401(self, unauthenticated_test_client, sample_ti):
        response = unauthenticated_test_client.get(f"/hitl-details/{sample_ti.id}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client, sample_ti):
        response = unauthorized_test_client.get(f"/hitl-details/{sample_ti.id}")
        assert response.status_code == 403


class TestGetHITLDetailsEndpoint:
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self, test_client, sample_ti, expected_sample_hitl_detail_dict
    ):
        response = test_client.get("/hitl-details/")
        assert response.status_code == 200
        assert response.json() == {
            "hitl_details": [expected_sample_hitl_detail_dict],
            "total_entries": 1,
        }

    def test_should_respond_200_without_response(self, test_client):
        response = test_client.get("/hitl-details/")
        assert response.status_code == 200
        assert response.json() == {
            "hitl_details": [],
            "total_entries": 0,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/hitl-details/")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/hitl-details/")
        assert response.status_code == 403
