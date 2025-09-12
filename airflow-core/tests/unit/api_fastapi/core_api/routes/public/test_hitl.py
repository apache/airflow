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

import json
from collections.abc import Callable
from datetime import datetime
from operator import itemgetter
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
import time_machine
from sqlalchemy import select
from sqlalchemy.orm import Session

from airflow._shared.timezones.timezone import utcnow
from airflow.models.hitl import HITLDetail
from airflow.models.log import Log
from airflow.sdk.execution_time.hitl import HITLUser
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
def sample_ti(
    create_task_instance: CreateTaskInstance,
    session: Session,
) -> TaskInstance:
    ti = create_task_instance(
        dag_id=DAG_ID,
        task_id=TASK_ID,
        session=session,
    )
    session.commit()
    return ti


@pytest.fixture
def sample_ti_url_identifier() -> str:
    return f"{DAG_ID}/test/{TASK_ID}"


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
        assignees=None,
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
        assignees=[HITLUser(id="non_test", name="non_test")],
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
        assignees=[HITLUser(id="test", name="test")],
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
                responded_by={"id": "test", "name": "test"},
            )
            for i, ti in enumerate(sample_tis[5:])
        ]
    )

    session.add_all(hitl_detail_models)
    session.commit()

    return hitl_detail_models


expected_ti_not_found_error_msg = (
    f"The Task Instance with dag_id: `{DAG_ID}`,"
    f" run_id: `test`, task_id: `{TASK_ID}` and map_index: `-1` was not found"
)


@pytest.fixture
def expected_hitl_detail_not_found_error_msg(sample_ti: TaskInstance) -> str:
    if TYPE_CHECKING:
        assert sample_ti.task

    return f"Human-in-the-loop detail does not exist for Task Instance with id {sample_ti.id}"


@pytest.fixture
def expected_sample_hitl_detail_dict(sample_ti: TaskInstance) -> dict[str, Any]:
    return {
        "body": "this is body",
        "defaults": ["Approve"],
        "multiple": False,
        "options": ["Approve", "Reject"],
        "params": {"input_1": 1},
        "assigned_users": [],
        "params_input": {},
        "response_at": None,
        "chosen_options": None,
        "response_received": False,
        "subject": "This is subject",
        "responded_by_user": None,
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
            "operator_name": "EmptyOperator",
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


@pytest.fixture(autouse=True)
def cleanup_audit_log(session: Session) -> None:
    session.query(Log).delete()
    session.commit()


def _assert_sample_audit_log(audit_log: Log, map_index: int | None) -> None:
    assert audit_log.dag_id == DAG_ID
    assert audit_log.task_id == TASK_ID
    assert audit_log.run_id == "test"
    assert audit_log.map_index is None
    assert audit_log.try_number is None
    assert audit_log.owner == "test"
    assert audit_log.owner_display_name == "test"
    assert audit_log.event == "update_hitl_detail"

    if TYPE_CHECKING:
        assert isinstance(audit_log.extra, str)

    expected_extra = {
        "chosen_options": ["Approve"],
        "params_input": {"input_1": 2},
        "method": "PATCH",
    }
    if map_index is not None:
        expected_extra["map_index"] = str(map_index)

    assert json.loads(audit_log.extra) == expected_extra


@pytest.fixture
def sample_update_payload() -> dict[str, Any]:
    return {"chosen_options": ["Approve"], "params_input": {"input_1": 2}}


class TestUpdateHITLDetailEndpoint:
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    @pytest.mark.parametrize("map_index", [None, -1])
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        map_index: int | None,
        sample_update_payload: dict[str, Any],
        session: Session,
    ) -> None:
        query_param = "" if map_index is None else f"?map_index={map_index}"
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json=sample_update_payload,
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "responded_by": {"id": "test", "name": "test"},
            "response_at": "2025-07-03T00:00:00Z",
        }

        audit_log = session.scalar(select(Log))
        _assert_sample_audit_log(audit_log, map_index=map_index)

    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail_respondent")
    @pytest.mark.parametrize("map_index", [None, -1])
    def test_should_respond_200_to_assigned_users(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        map_index: int | None,
        sample_update_payload: dict[str, Any],
        session: Session,
    ):
        """Test with an authorized user and the user is a respondent to the task."""
        query_param = "" if map_index is None else f"?map_index={map_index}"
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json=sample_update_payload,
        )

        assert response.status_code == 200
        assert response.json() == {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "responded_by": {"id": "test", "name": "test"},
            "response_at": "2025-07-03T00:00:00Z",
        }

        audit_log = session.scalar(select(Log))
        _assert_sample_audit_log(audit_log, map_index=map_index)

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_update_payload: dict[str, Any],
        query_param: str,
    ) -> None:
        response = unauthenticated_test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json=sample_update_payload,
        )
        assert response.status_code == 401

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_update_payload: dict[str, Any],
        query_param: str,
    ) -> None:
        response = unauthorized_test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json=sample_update_payload,
        )
        assert response.status_code == 403

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail_non_respondent")
    def test_should_respond_403_to_non_respondent_user(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_update_payload: dict[str, Any],
        query_param: str,
    ):
        """Test with an authorized user but the user is not a respondent to the task."""
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json=sample_update_payload,
        )
        assert response.status_code == 403

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    def test_should_respond_404_without_ti(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        query_param: str,
    ) -> None:
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )
        assert response.status_code == 404
        assert response.json() == {"detail": expected_ti_not_found_error_msg}

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    def test_should_respond_404_without_hitl_detail(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        sample_update_payload: dict[str, Any],
        query_param: str,
        expected_hitl_detail_not_found_error_msg: str,
    ) -> None:
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json=sample_update_payload,
        )

        assert response.status_code == 404
        assert response.json() == {"detail": expected_hitl_detail_not_found_error_msg}

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    @time_machine.travel(datetime(2025, 7, 3, 0, 0, 0), tick=False)
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_409(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        query_param: str,
        sample_ti: TaskInstance,
    ) -> None:
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 2}},
        )

        expected_response = {
            "params_input": {"input_1": 2},
            "chosen_options": ["Approve"],
            "responded_by": {"id": "test", "name": "test"},
            "response_at": "2025-07-03T00:00:00Z",
        }
        assert response.status_code == 200
        assert response.json() == expected_response

        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json={"chosen_options": ["Approve"], "params_input": {"input_1": 3}},
        )
        assert response.status_code == 409
        assert response.json() == {
            "detail": (
                "Human-in-the-loop detail has already been updated for Task Instance "
                f"with id {sample_ti.id} "
                "and is not allowed to write again."
            )
        }

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_422_with_empty_option(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        query_param: str,
    ) -> None:
        response = test_client.patch(
            f"/hitlDetails/{sample_ti_url_identifier}{query_param}",
            json={"chosen_options": [], "params_input": {"input_1": 2}},
        )

        assert response.status_code == 422


class TestGetHITLDetailEndpoint:
    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    @pytest.mark.usefixtures("sample_hitl_detail")
    def test_should_respond_200_with_existing_response(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_sample_hitl_detail_dict: dict[str, Any],
        query_param: str,
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}{query_param}")
        assert response.status_code == 200
        assert response.json() == expected_sample_hitl_detail_dict

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    def test_should_respond_401(
        self,
        unauthenticated_test_client: TestClient,
        sample_ti_url_identifier: str,
        query_param: str,
    ) -> None:
        response = unauthenticated_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}{query_param}")
        assert response.status_code == 401

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    def test_should_respond_403(
        self,
        unauthorized_test_client: TestClient,
        sample_ti_url_identifier: str,
        query_param: str,
    ) -> None:
        response = unauthorized_test_client.get(f"/hitlDetails/{sample_ti_url_identifier}{query_param}")
        assert response.status_code == 403

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    def test_should_respond_404_without_ti(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        query_param: str,
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}{query_param}")
        assert response.status_code == 404
        assert response.json() == {"detail": expected_ti_not_found_error_msg}

    @pytest.mark.parametrize("query_param", ["", "?map_index=-1"])
    def test_should_respond_404_without_hitl_detail(
        self,
        test_client: TestClient,
        sample_ti_url_identifier: str,
        expected_hitl_detail_not_found_error_msg: str,
        query_param: str,
    ) -> None:
        response = test_client.get(f"/hitlDetails/{sample_ti_url_identifier}{query_param}")
        assert response.status_code == 404
        assert response.json() == {"detail": expected_hitl_detail_not_found_error_msg}


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
            ({"responded_by_user_id": ["test"]}, 3),
            ({"responded_by_user_name": ["test"]}, 3),
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
            "responded_user_id",
            "responded_user_name",
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

    @pytest.mark.usefixtures("sample_hitl_details")
    @pytest.mark.parametrize("asc_desc_mark", ["", "-"], ids=["asc", "desc"])
    @pytest.mark.parametrize(
        "key, get_key_lambda",
        [
            # ti key
            ("ti_id", lambda x: x["task_instance"]["id"]),
            ("dag_id", lambda x: x["task_instance"]["dag_id"]),
            ("run_id", lambda x: x["task_instance"]["dag_run_id"]),
            ("run_after", lambda x: x["task_instance"]["run_after"]),
            ("rendered_map_index", lambda x: x["task_instance"]["rendered_map_index"]),
            ("task_instance_operator", lambda x: x["task_instance"]["operator_name"]),
            # htil key
            ("subject", itemgetter("subject")),
            ("response_at", itemgetter("response_at")),
        ],
        ids=[
            "ti_id",
            "dag_id",
            "run_id",
            "run_after",
            "rendered_map_index",
            "task_instance_operator",
            "subject",
            "response_at",
        ],
    )
    def test_should_respond_200_with_existing_response_and_order_by(
        self,
        test_client: TestClient,
        asc_desc_mark: str,
        key: str,
        get_key_lambda: Callable,
    ) -> None:
        reverse = asc_desc_mark == "-"

        response = test_client.get("/hitlDetails/", params={"order_by": f"{asc_desc_mark}{key}"})
        data = response.json()
        hitl_details = data["hitl_details"]

        assert response.status_code == 200
        assert data["total_entries"] == 8
        assert len(hitl_details) == 8

        sorted_hitl_details = sorted(
            hitl_details,
            key=lambda x: (
                # pull none to the last no matter it's asc or desc
                (get_key_lambda(x) is not None) if reverse else (get_key_lambda(x) is None),
                get_key_lambda(x),
                x["task_instance"]["id"],
            ),
            reverse=reverse,
        )

        assert hitl_details == sorted_hitl_details

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
