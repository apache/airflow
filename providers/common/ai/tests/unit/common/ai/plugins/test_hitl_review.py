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

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from airflow._shared.timezones import timezone
from airflow.models.dagrun import DagRun
from airflow.models.xcom import XComModel
from airflow.providers.common.ai.plugins.hitl_review import (
    HITLReviewPlugin,
    _get_map_index,
    _parse_model,
    _read_xcom_by_prefix,
    hitl_review_app,
)
from airflow.providers.common.ai.utils.hitl_review import (
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_AGENT_SESSION,
    AgentSessionData,
    HumanActionData,
    SessionStatus,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_xcom,
)
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("HITL Review is only compatible with Airflow >= 3.1.0", allow_module_level=True)


# --- Non-DB tests (pure functions, no database) ---


class TestGetMapIndex:
    def test_valid_int(self):
        assert _get_map_index("0") == 0
        assert _get_map_index("1") == 1
        assert _get_map_index("-1") == -1

    def test_invalid_returns_minus_one(self):
        assert _get_map_index("{MAP_INDEX}") == -1
        assert _get_map_index("") == -1


class TestParseModel:
    def test_from_dict(self):
        data = {"action": "approve", "iteration": 1}
        obj = _parse_model(HumanActionData, data)
        assert obj.action == "approve"
        assert obj.iteration == 1

    def test_from_json_string(self):
        data = '{"action": "reject", "iteration": 2}'
        obj = _parse_model(HumanActionData, data)
        assert obj.action == "reject"
        assert obj.iteration == 2


class TestReadXcomByPrefix:
    def test_extracts_iteration_suffix(self):
        mock_result = [
            (f"{XCOM_AGENT_OUTPUT_PREFIX}1", "output1"),
            (f"{XCOM_AGENT_OUTPUT_PREFIX}2", "output2"),
        ]
        mock_session = MagicMock()
        mock_session.execute.return_value.all.return_value = mock_result

        result = _read_xcom_by_prefix(
            mock_session,
            dag_id="d",
            run_id="r",
            task_id="t",
            map_index=-1,
            prefix=XCOM_AGENT_OUTPUT_PREFIX,
        )
        assert result == {1: "output1", 2: "output2"}

    def test_ignores_non_numeric_suffix(self):
        mock_result = [
            (f"{XCOM_AGENT_OUTPUT_PREFIX}1", "v1"),
            (f"{XCOM_AGENT_OUTPUT_PREFIX}bad", "v2"),
        ]
        mock_session = MagicMock()
        mock_session.execute.return_value.all.return_value = mock_result

        result = _read_xcom_by_prefix(
            mock_session,
            dag_id="d",
            run_id="r",
            task_id="t",
            map_index=-1,
            prefix=XCOM_AGENT_OUTPUT_PREFIX,
        )
        assert result == {1: "v1"}


class TestHealthEndpoint:
    """Health endpoint does not use database."""

    def test_health_returns_ok(self):
        client = TestClient(hitl_review_app)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "ok"}


class TestHITLReviewPlugin:
    def test_plugin_name(self):
        assert HITLReviewPlugin.name == "hitl_review"

    def test_fastapi_apps_registered(self):
        assert len(HITLReviewPlugin.fastapi_apps) == 1
        assert HITLReviewPlugin.fastapi_apps[0]["name"] == "hitl-review"
        assert "url_prefix" in HITLReviewPlugin.fastapi_apps[0]


# --- DB-dependent API tests ---

DAG_ID = "hitl_test_dag"
TASK_ID = "hitl_test_task"
LOGICAL_DATE_STR = "2025-01-01T00:00:00+00:00"
logical_date = timezone.parse(LOGICAL_DATE_STR)
RUN_ID = DagRun.generate_run_id(
    run_type=DagRunType.MANUAL,
    logical_date=logical_date,
    run_after=logical_date,
)
MAP_INDEX = -1


def _clear_db():
    clear_db_dags()
    clear_db_runs()
    clear_db_dag_bundles()
    clear_db_xcom()


@provide_session
def _create_hitl_session(
    session=None,
    *,
    dag_id=DAG_ID,
    run_id=RUN_ID,
    task_id=TASK_ID,
    map_index=MAP_INDEX,
    status=SessionStatus.PENDING_REVIEW,
    iteration=1,
    prompt="Summarize",
    current_output="Initial output",
):
    """Create HITL session and output XCom entries in the database."""
    sess = AgentSessionData(
        status=status,
        iteration=iteration,
        prompt=prompt,
        current_output=current_output,
    )
    XComModel.set(
        key=XCOM_AGENT_SESSION,
        value=sess.model_dump(mode="json"),
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
        serialize=False,
        session=session,
    )
    XComModel.set(
        key=f"{XCOM_AGENT_OUTPUT_PREFIX}{iteration}",
        value=current_output,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
        serialize=False,
        session=session,
    )
    session.commit()


@pytest.mark.db_test
class TestFindSessionEndpoint:
    """Test the /sessions/find endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TASK_ID)
        dag_maker.create_dagrun(
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        _create_hitl_session()
        yield
        _clear_db()

    def test_returns_session_when_found(self):
        client = TestClient(hitl_review_app)
        response = client.get(
            "/sessions/find",
            params={"dag_id": DAG_ID, "run_id": RUN_ID, "task_id": TASK_ID},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["dag_id"] == DAG_ID
        assert data["status"] == "pending_review"
        assert data["iteration"] == 1
        assert data["current_output"] == "Initial output"

    def test_404_when_session_not_found(self):
        client = TestClient(hitl_review_app)
        response = client.get(
            "/sessions/find",
            params={
                "dag_id": DAG_ID,
                "run_id": "nonexistent_run",
                "task_id": TASK_ID,
            },
        )
        assert response.status_code == 404
        assert "task_active" in response.json()["detail"]


@pytest.mark.db_test
class TestSubmitFeedbackEndpoint:
    """Test the /sessions/feedback endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TASK_ID)
        dag_maker.create_dagrun(
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        _create_hitl_session()
        yield
        _clear_db()

    def test_submit_feedback_returns_updated_session(self):
        client = TestClient(hitl_review_app)
        response = client.post(
            "/sessions/feedback",
            params={"dag_id": DAG_ID, "run_id": RUN_ID, "task_id": TASK_ID},
            json={"feedback": "Add more detail"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "changes_requested"
        assert data["iteration"] == 1
        assert any(e["role"] == "human" and "Add more detail" in e["content"] for e in data["conversation"])

    def test_submit_feedback_404_when_no_session(self):
        client = TestClient(hitl_review_app)
        response = client.post(
            "/sessions/feedback",
            params={
                "dag_id": DAG_ID,
                "run_id": "nonexistent_run",
                "task_id": TASK_ID,
            },
            json={"feedback": "Add more"},
        )
        assert response.status_code == 404


@pytest.mark.db_test
class TestApproveEndpoint:
    """Test the /sessions/approve endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TASK_ID)
        dag_maker.create_dagrun(
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        _create_hitl_session()
        yield
        _clear_db()

    def test_approve_returns_session(self):
        client = TestClient(hitl_review_app)
        response = client.post(
            "/sessions/approve",
            params={"dag_id": DAG_ID, "run_id": RUN_ID, "task_id": TASK_ID},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "approved"


@pytest.mark.db_test
class TestRejectEndpoint:
    """Test the /sessions/reject endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TASK_ID)
        dag_maker.create_dagrun(
            run_id=RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        _create_hitl_session()
        yield
        _clear_db()

    def test_reject_returns_session(self):
        client = TestClient(hitl_review_app)
        response = client.post(
            "/sessions/reject",
            params={"dag_id": DAG_ID, "run_id": RUN_ID, "task_id": TASK_ID},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "rejected"
