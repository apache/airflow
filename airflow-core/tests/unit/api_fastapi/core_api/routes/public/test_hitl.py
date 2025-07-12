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
from sqlalchemy.orm import Session

from tests_common.test_utils.db import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop public API compatible with Airflow >= 3.0.1", allow_module_level=True)

from datetime import datetime
from typing import TYPE_CHECKING, Any

import time_machine

from airflow.models.hitl import HITLDetail

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

    from airflow.models.taskinstance import TaskInstance

    from tests_common.pytest_plugin import CreateTaskInstance


pytestmark = pytest.mark.db_test

DAG_ID = "test_hitl_dag"


@pytest.fixture
def sample_ti(create_task_instance: CreateTaskInstance) -> TaskInstance:
    return create_task_instance()


@pytest.fixture
def sample_ti_url_identifier(sample_ti: TaskInstance) -> str:
    if TYPE_CHECKING:
        assert sample_ti.task

    return f"{sample_ti.dag_id}/{sample_ti.run_id}/{sample_ti.task.task_id}"


@pytest.fixture
def sample_hitl_detail(sample_ti: TaskInstance, session: Session) -> HITLDetail:
    hitl_detail_model = HITLDetail(
        ti_id=sample_ti.id,
        options=["Approve", "Reject"],
        subject="This is subject",
        body="this is body",
        defaults=["Approve"],
        multiple=False,
        params={"input_1": 1},
    )
    session.add(hitl_detail_model)
    session.commit()

    return hitl_detail_model


@pytest.fixture
def expected_ti_not_found_error_msg(sample_ti: TaskInstance) -> str:
    if TYPE_CHECKING:
        assert sample_ti.task

    return (
        f"The Task Instance with dag_id: `{sample_ti.dag_id}`,"
        f" run_id: `{sample_ti.run_id}`, task_id: `{sample_ti.task.task_id}`"
        " and map_index: `None` was not found"
    )


@pytest.fixture
def expected_mapped_ti_not_found_error_msg(sample_ti: TaskInstance) -> str:
    if TYPE_CHECKING:
        assert sample_ti.task

    return (
        f"The Task Instance with dag_id: `{sample_ti.dag_id}`,"
        f" run_id: `{sample_ti.run_id}`, task_id: `{sample_ti.task.task_id}`"
        " and map_index: `-1` was not found"
    )


@pytest.fixture
def expected_sample_hitl_detail_dict(sample_ti: TaskInstance) -> dict[str, Any]:
    return {
        "body": "this is body",
        "defaults": ["Approve"],
        "multiple": False,
        "options": ["Approve", "Reject"],
        "params": {"input_1": 1},
        "params_input": {},
        "response_at": None,
        "chosen_options": None,
        "response_received": False,
        "subject": "This is subject",
        "ti_id": sample_ti.id,
        "user_id": None,
    }


class TestUpdateHITLDetailEndpoint:
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }

    def test_should_respond_404(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_ti_not_found_error_msg: str,
    ) -> None:
        response = test_client.get(f"/hitl-details/{sample_ti_url_identifier}")
        assert response.status_code == 404
        assert response.json() == {"detail": expected_ti_not_found_error_msg}

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_409(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_ti: TaskInstance,
    ) -> None:
        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        expected_response = {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }
        assert response.status_code == 200
        assert response.json() == expected_response

        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )
        assert response.status_code == 409
        assert response.json() == {
            "detail": (
                "Human-in-the-loop detail has already been updated for Task Instance "
                f"with id {sample_ti.id} "
                "and is not allowed to write again."
            )
        }

    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitl-details/{sample_ti_url_identifier}")
        assert response.status_code == 401

    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitl-details/{sample_ti_url_identifier}")
        assert response.status_code == 403


class TestUpdateMappedTIHITLDetail:
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }

    def test_should_respond_404(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_mapped_ti_not_found_error_msg: str,
    ) -> None:
        response = test_client.get(f"/hitl-details/{sample_ti_url_identifier}/-1")
        assert response.status_code == 404
        assert response.json() == {"detail": expected_mapped_ti_not_found_error_msg}

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_409(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_ti: TaskInstance,
    ) -> None:
        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        expected_response = {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }
        assert response.status_code == 200
        assert response.json() == expected_response

        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )
        assert response.status_code == 409
        assert response.json() == {
            "detail": (
                "Human-in-the-loop detail has already been updated for Task Instance "
                f"with id {sample_ti.id} "
                "and is not allowed to write again."
            )
        }

    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitl-details/{sample_ti_url_identifier}/-1")
        assert response.status_code == 401

    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitl-details/{sample_ti_url_identifier}/-1")
        assert response.status_code == 403


class TestGetHITLDetailEndpoint:
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_sample_hitl_detail_dict: dict[str, Any],
    ) -> None:
        response = test_client.get(f"/hitl-details/{sample_ti_url_identifier}")
        assert response.status_code == 200
        assert response.json() == expected_sample_hitl_detail_dict

    def test_should_respond_404(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_ti_not_found_error_msg: str,
    ) -> None:
        response = test_client.get(f"/hitl-details/{sample_ti_url_identifier}")
        assert response.status_code == 404
        assert response.json() == {"detail": expected_ti_not_found_error_msg}

    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitl-details/{sample_ti_url_identifier}")
        assert response.status_code == 401

    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitl-details/{sample_ti_url_identifier}")
        assert response.status_code == 403


class TestGetMappedTIHITLDetail:
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_sample_hitl_detail_dict: dict[str, Any],
    ) -> None:
        response = test_client.get(f"/hitl-details/{sample_ti_url_identifier}/-1")
        assert response.status_code == 200
        assert response.json() == expected_sample_hitl_detail_dict

    def test_should_respond_404(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_mapped_ti_not_found_error_msg: str,
    ) -> None:
        response = test_client.get(f"/hitl-details/{sample_ti_url_identifier}/-1")
        assert response.status_code == 404
        assert response.json() == {"detail": expected_mapped_ti_not_found_error_msg}

    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitl-details/{sample_ti_url_identifier}/-1")
        assert response.status_code == 401

    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitl-details/{sample_ti_url_identifier}/-1")
        assert response.status_code == 403


class TestGetHITLDetailsEndpoint:
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        expected_sample_hitl_detail_dict: dict[str, Any],
    ) -> None:
        response = test_client.get("/hitl-details/")
        assert response.status_code == 200
        assert response.json() == {
            "hitl_details": [expected_sample_hitl_detail_dict],
            "total_entries": 1,
        }

    def test_should_respond_200_without_response(self, test_client: TestClient) -> None:
        response = test_client.get("/hitl-details/")
        assert response.status_code == 200
        assert response.json() == {
            "hitl_details": [],
            "total_entries": 0,
        }

    def test_should_respond_401(self, unauthenticated_test_client: TestClient) -> None:
        response = unauthenticated_test_client.get("/hitl-details/")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client: TestClient) -> None:
        response = unauthorized_test_client.get("/hitl-details/")
        assert response.status_code == 403
