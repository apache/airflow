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
from unittest import mock

import pytest
import time_machine
from sqlalchemy.orm import Session

from airflow._shared.timezones.timezone import utcnow
from airflow.models.hitl import HITLDetail
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from fastapi.testclient import TestClient

    from airflow.models.taskinstance import TaskInstance

    from tests_common.pytest_plugin import CreateTaskInstance


pytestmark = pytest.mark.db_test

DAG_ID = "test_hitl_dag"
ANOTHER_DAG_ID = "another_hitl_dag"
TASK_ID = "sample_task_hitl"


@pytest.fixture
def sample_ti(create_task_instance: CreateTaskInstance) -> TaskInstance:
    return create_task_instance(dag_id=DAG_ID, task_id=TASK_ID)


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
        respondents=None,
    )
    session.add(hitl_detail_model)
    session.commit()

    return hitl_detail_model


@pytest.fixture
def sample_hitl_detail_non_respondent(sample_ti: TaskInstance, session: Session) -> HITLDetail:
    hitl_detail_model = HITLDetail(
        ti_id=sample_ti.id,
        options=["Approve", "Reject"],
        subject="This is subject",
        body="this is body",
        defaults=["Approve"],
        multiple=False,
        params={"input_1": 1},
        respondents=["non_test"],
    )
    session.add(hitl_detail_model)
    session.commit()

    return hitl_detail_model


@pytest.fixture
def sample_hitl_detail_respondent(sample_ti: TaskInstance, session: Session) -> HITLDetail:
    hitl_detail_model = HITLDetail(
        ti_id=sample_ti.id,
        options=["Approve", "Reject"],
        subject="This is subject",
        body="this is body",
        defaults=["Approve"],
        multiple=False,
        params={"input_1": 1},
        respondents=["test"],
    )
    session.add(hitl_detail_model)
    session.commit()

    return hitl_detail_model


@pytest.fixture
def sample_tis(create_task_instance: CreateTaskInstance) -> list[TaskInstance]:
    tis = [
        create_task_instance(
            dag_id=f"hitl_dag_{i}",
            run_id=f"hitl_run_{i}",
            task_id=f"hitl_task_{i}",
            state=TaskInstanceState.RUNNING,
        )
        for i in range(5)
    ]
    tis.extend(
        [
            create_task_instance(
                dag_id=f"other_Dag_{i}",
                run_id=f"another_hitl_run_{i}",
                task_id=f"another_hitl_task_{i}",
                state=TaskInstanceState.SUCCESS,
            )
            for i in range(3)
        ]
    )
    return tis


@pytest.fixture
def sample_hitl_details(sample_tis: list[TaskInstance], session: Session) -> list[HITLDetail]:
    hitl_detail_models = [
        HITLDetail(
            ti_id=ti.id,
            options=["Approve", "Reject"],
            subject=f"This is subject {i}",
            body=f"this is body {i}",
            defaults=["Approve"],
            multiple=False,
            params={"input_1": 1},
        )
        for i, ti in enumerate(sample_tis[:5])
    ]
    hitl_detail_models.extend(
        [
            HITLDetail(
                ti_id=ti.id,
                options=["1", "2", "3"],
                subject=f"Subject {i} this is",
                body=f"Body {i} this is",
                defaults=["1"],
                multiple=False,
                params={"input": 1},
                response_at=utcnow(),
                chosen_options=[str(i)],
                params_input={"input": i},
                user_id="test",
            )
            for i, ti in enumerate(sample_tis[5:])
        ]
    )

    session.add_all(hitl_detail_models)
    session.commit()

    return hitl_detail_models


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
        "respondents": None,
        "params_input": {},
        "response_at": None,
        "chosen_options": None,
        "response_received": False,
        "subject": "This is subject",
        "user_id": None,
        "task_instance": {
            "dag_display_name": DAG_ID,
            "dag_id": DAG_ID,
            "dag_run_id": "test",
            "dag_version": {
                "bundle_name": "dag_maker",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": mock.ANY,
                "dag_display_name": DAG_ID,
                "dag_id": DAG_ID,
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
            "task_display_name": "sample_task_hitl",
            "task_id": TASK_ID,
            "trigger": None,
            "triggerer_job": None,
            "try_number": 0,
            "unixname": "root",
        },
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
            f"/hitlDetails/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail_respondent")
    def test_should_respond_200_to_respondent_user(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ):
        """Test with an authorized user and the user is a respondent to the task."""
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }

    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}")
        assert response.status_code == 401

    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}")
        assert response.status_code == 403

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail_non_respondent")
    def test_should_respond_403_to_non_respondent_user(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ):
        """Test with an authorized user but the user is not a respondent to the task."""
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )
        assert response.status_code == 403

    def test_should_respond_404(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_ti_not_found_error_msg: str,
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}")
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
            f"/hitlDetails/{sample_ti_url_identifier}",
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
            f"/hitlDetails/{sample_ti_url_identifier}",
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

    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_422_with_empty_option(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}",
            json={"chosen_options": [], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 422


class TestUpdateMappedTIHITLDetail:
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail_respondent")
    def test_should_respond_200_to_respondent_user(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ):
        """Test with an authorized user and the user is a respondent to the task."""
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "user_id": "test",
            "response_at": "2025-07-03T00:00:00Z",
        }

    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}/-1")
        assert response.status_code == 401

    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}/-1")
        assert response.status_code == 403

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail_non_respondent")
    def test_should_respond_403_to_non_respondent_user(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ):
        """Test with an authorized user but the user is not a respondent to the task."""
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}/-1",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )
        assert response.status_code == 403

    def test_should_respond_404(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_mapped_ti_not_found_error_msg: str,
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}/-1")
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
            f"/hitlDetails/{sample_ti_url_identifier}/-1",
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
            f"/hitlDetails/{sample_ti_url_identifier}/-1",
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

    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_422_with_empty_option(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}/-1",
            json={"chosen_options": [], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 422


class TestGetHITLDetailEndpoint:
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_sample_hitl_detail_dict: dict[str, Any],
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}")
        assert response.status_code == 200
        assert response.json() == expected_sample_hitl_detail_dict

    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}")
        assert response.status_code == 401

    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}")
        assert response.status_code == 403

    def test_should_respond_404(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_ti_not_found_error_msg: str,
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}")
        assert response.status_code == 404
        assert response.json() == {"detail": expected_ti_not_found_error_msg}


class TestGetMappedTIHITLDetail:
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_sample_hitl_detail_dict: dict[str, Any],
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}/-1")
        assert response.status_code == 200
        assert response.json() == expected_sample_hitl_detail_dict

    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}/-1")
        assert response.status_code == 401

    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}/-1")
        assert response.status_code == 403


class TestGetHITLDetailsEndpoint:
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        expected_sample_hitl_detail_dict: dict[str, Any],
    ) -> None:
        response = test_client.get("/hitlDetails/")
        assert response.status_code == 200
        assert response.json() == {
            "hitl_details": [expected_sample_hitl_detail_dict],
            "total_entries": 1,
        }

    @pytest.mark.usefixtures("sample_hitl_details")
    @pytest.mark.parametrize(
        "params, expected_ti_count",
        [
            # ti related filter
            ({"dag_id_pattern": "hitl_dag"}, 5),
            ({"dag_id_pattern": "other_Dag_"}, 3),
            ({"dag_id": "hitl_dag_0"}, 1),
            ({"dag_run_id": "hitl_run_0"}, 1),
            ({"task_id": "hitl_task_0"}, 1),
            ({"task_id_pattern": "another_hitl"}, 3),
            ({"state": "running"}, 5),
            ({"state": "success"}, 3),
            # hitl detail related filter
            ({"subject_search": "This is subject"}, 5),
            ({"body_search": "this is"}, 8),
            ({"response_received": False}, 5),
            ({"response_received": True}, 3),
            ({"user_id": ["test"]}, 3),
        ],
        ids=[
            "dag_id_pattern_hitl_dag",
            "dag_id_pattern_other_dag",
            "dag_id",
            "dag_run_id",
            "task_id_pattern",
            "task_id",
            "ti_state_running",
            "ti_state_success",
            "subject",
            "body",
            "response_not_received",
            "response_received",
            "user_id",
        ],
    )
    def test_should_respond_200_with_existing_response_and_query(
        self,
        test_client: TestClient,
        params: dict[str, Any],
        expected_ti_count: int,
    ) -> None:
        response = test_client.get("/hitlDetails/", params=params)
        assert response.status_code == 200
        assert response.json()["total_entries"] == expected_ti_count
        assert len(response.json()["hitl_details"]) == expected_ti_count

    def test_should_respond_200_without_response(self, test_client: TestClient) -> None:
        response = test_client.get("/hitlDetails/")
        assert response.status_code == 200
        assert response.json() == {
            "hitl_details": [],
            "total_entries": 0,
        }

    def test_should_respond_401(self, unauthenticated_test_client: TestClient) -> None:
        response = unauthenticated_test_client.get("/hitlDetails/")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client: TestClient) -> None:
        response = unauthorized_test_client.get("/hitlDetails/")
        assert response.status_code == 403

    def test_should_respond_404(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_mapped_ti_not_found_error_msg: str,
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}/-1")
        assert response.status_code == 404
        assert response.json() == {"detail": expected_mapped_ti_not_found_error_msg}
