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

from unittest import mock

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
        " was not found"
    )


@pytest.fixture
def expected_mapped_ti_not_found_error_msg(sample_ti: TaskInstance) -> str:
    if TYPE_CHECKING:
        assert sample_ti.task

    return (
        f"The Task Instance with dag_id: `{sample_ti.dag_id}`,"
        f" run_id: `{sample_ti.run_id}`, task_id: `{sample_ti.task.task_id}`, map_index:"
        " `-1` was not found"
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
        "user_id": None,
        "task_instance": {
            "dag_display_name": "dag",
            "dag_id": "dag",
            "dag_run_id": "test",
            "dag_version": {
                "bundle_name": "dag_maker",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": mock.ANY,
                "dag_display_name": "dag",
                "dag_id": "dag",
                "id": mock.ANY,
                "version_number": 1,
            },
            "duration": None,
            "end_date": None,
            "executor": None,
            "executor_config": "{}",
            "hostname": "",
            "id": sample_ti.id,
            "logical_date": mock.ANY,
            "map_index": -1,
            "max_tries": 0,
            "note": None,
            "operator": "EmptyOperator",
            "pid": None,
            "pool": "default_pool",
            "pool_slots": 1,
            "priority_weight": 1,
            "queue": "default",
            "queued_when": None,
            "rendered_fields": {},
            "rendered_map_index": None,
            "run_after": mock.ANY,
            "scheduled_when": None,
            "start_date": None,
            "state": None,
            "task_display_name": "op1",
            "task_id": "op1",
            "trigger": None,
            "triggerer_job": None,
            "try_number": 0,
            "unixname": "root",
        },
        # Shared link fields (None for regular HITL operations)
        "action": None,
        "expires_at": None,
        "link_type": "action",
        "link_url": None,
    }


class TestUpdateHITLDetailEndpoint:
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_ti: TaskInstance,
        sample_hitl_detail: HITLDetail,
    ) -> None:
        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}, "try_number": 0},
        )

        assert response.status_code == 200
        expected = {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
            "task_instance_id": None,
            "link_url": None,
            "expires_at": None,
            "action": None,
            "link_type": "action",
        }
        assert response.json() == expected

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
    def test_should_respond_409(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_ti: TaskInstance,
        sample_hitl_detail: HITLDetail,
    ) -> None:
        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}, "try_number": 0},
        )

        expected_response = {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
            "task_instance_id": None,
            "link_url": None,
            "expires_at": None,
            "action": None,
            "link_type": "action",
        }
        assert response.status_code == 200
        assert response.json() == expected_response

        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}, "try_number": 0},
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
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_ti: TaskInstance,
        sample_hitl_detail: HITLDetail,
    ) -> None:
        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}, "try_number": 0},
        )

        assert response.status_code == 200
        expected = {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
            "task_instance_id": None,
            "link_url": None,
            "expires_at": None,
            "action": None,
            "link_type": "action",
        }
        assert response.json() == expected

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
    def test_should_respond_409(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_ti: TaskInstance,
        sample_hitl_detail: HITLDetail,
    ) -> None:
        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}, "try_number": 0},
        )

        expected_response = {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
            "task_instance_id": None,
            "link_url": None,
            "expires_at": None,
            "action": None,
            "link_type": "action",
        }
        assert response.status_code == 200
        assert response.json() == expected_response

        response = test_client.patch(
            f"/hitl-details/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}, "try_number": 0},
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
        expected: dict[str, Any] = expected_sample_hitl_detail_dict
        if "expires_in_hours" in expected:
            expected.pop("expires_in_hours")
        assert response.json() == expected

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
        expected: dict[str, Any] = expected_sample_hitl_detail_dict
        if "expires_in_hours" in expected:
            expected.pop("expires_in_hours")
        assert response.json() == expected

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
        expected: dict[str, Any] = {
            "chosen_options": None,
            "hitl_details": [expected_sample_hitl_detail_dict],
            "params_input": {},
            "total_entries": 1,
        }
        if "response_content" in expected:
            expected.pop("response_content")
        assert response.json() == expected

    def test_should_respond_200_without_response(self, test_client: TestClient) -> None:
        response = test_client.get("/hitl-details/")
        assert response.status_code == 200
        expected: dict[str, Any] = {
            "chosen_options": None,
            "hitl_details": [],
            "params_input": {},
            "total_entries": 0,
        }
        if "response_content" in expected:
            expected.pop("response_content")
        assert response.json() == expected

    def test_should_respond_401(self, unauthenticated_test_client: TestClient) -> None:
        response = unauthenticated_test_client.get("/hitl-details/")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client: TestClient) -> None:
        response = unauthorized_test_client.get("/hitl-details/")
        assert response.status_code == 403
