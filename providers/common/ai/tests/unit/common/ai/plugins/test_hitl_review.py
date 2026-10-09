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

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS, AIRFLOW_V_3_4_PLUS

if not AIRFLOW_V_3_1_PLUS:
    pytest.skip("Human in the loop is only compatible with Airflow >= 3.1.0", allow_module_level=True)

import datetime
from typing import TYPE_CHECKING
from unittest import mock
from uuid import UUID

import time_machine
from fastapi.testclient import TestClient
from sqlalchemy import select

from airflow.api_fastapi.app import create_app, purge_cached_app
from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.models.xcom import XComModel
from airflow.providers.common.ai.plugins.hitl_review import (
    HITLReviewPlugin,
    _build_session_response,
    _get_base_url_path,
    _get_map_index,
    _is_task_completed,
    _read_xcom,
    _read_xcom_by_prefix,
    _write_xcom,
    hitl_review_app,
)
from airflow.providers.common.ai.utils.hitl_review import (
    XCOM_AGENT_OUTPUT_PREFIX,
    XCOM_AGENT_SESSION,
    XCOM_HUMAN_FEEDBACK_PREFIX,
    AgentSessionData,
    SessionStatus,
)
from airflow.providers.common.compat.sdk import timezone
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType

if AIRFLOW_V_3_4_PLUS:
    from airflow.models.dynamic_region import DynamicRegion
    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import task_group
    from airflow.sdk.definitions._internal.loop import create_loop

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_xcom,
)

if TYPE_CHECKING:
    from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager

pytestmark = pytest.mark.db_test

BASE_URL = "http://testserver"

TEST_DAG_ID = "test_dag"
TEST_TASK_ID = "test_task"
TEST_RUN_ID = "test_run_id"
LOGICAL_DATE_STR = "2025-01-01T00:00:00+00:00"
logical_date = timezone.parse(LOGICAL_DATE_STR)
MAP_INDEX = -1
NO_REGION = UUID(int=0) if AIRFLOW_V_3_4_PLUS else None


def _clear_db():
    clear_db_dags()
    clear_db_runs()
    clear_db_dag_bundles()
    clear_db_xcom()


@provide_session
def _create_hitl_session(
    *,
    dag_id=TEST_DAG_ID,
    run_id=TEST_RUN_ID,
    task_id=TEST_TASK_ID,
    map_index=MAP_INDEX,
    status=SessionStatus.PENDING_REVIEW,
    iteration=1,
    prompt="Summarize",
    current_output="Initial output",
    session=None,
):
    """Create HITL session and output XCom entries in the database."""
    sess = AgentSessionData(
        status=status,
        iteration=iteration,
        max_iterations=5,
        prompt=prompt,
        current_output=current_output,
    )
    _write_xcom(
        session,
        key=XCOM_AGENT_SESSION,
        value=sess.model_dump(mode="json"),
        region_id=NO_REGION,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
    )
    _write_xcom(
        session,
        key=f"{XCOM_AGENT_OUTPUT_PREFIX}{iteration}",
        value=current_output,
        region_id=NO_REGION,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        map_index=map_index,
    )


@pytest.fixture
def test_client():
    """Test client for HITL Review plugin endpoints. Use full paths like /hitl-review/sessions/find."""
    with (
        conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
                ("core", "lazy_discover_providers"): "false",
            }
        ),
        mock.patch("airflow.settings.LAZY_LOAD_PROVIDERS", False),
    ):
        purge_cached_app()
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager
        time_very_before = datetime.datetime(2014, 1, 1, 0, 0, 0)
        time_after = datetime.datetime.now() + datetime.timedelta(days=1)
        with time_machine.travel(time_very_before, tick=False):
            token = auth_manager._get_token_signer(
                expiration_time_in_seconds=(time_after - time_very_before).total_seconds()
            ).generate(
                auth_manager.serialize_user(SimpleAuthManagerUser(username="test", role="admin")),
            )

        yield TestClient(
            app,
            headers={"Authorization": f"Bearer {token}"},
            base_url=BASE_URL,
        )


@pytest.fixture
def unauthenticated_test_client():
    """Test client with no Authorization header."""
    with (
        conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
                ("core", "lazy_discover_providers"): "false",
            }
        ),
        mock.patch("airflow.settings.LAZY_LOAD_PROVIDERS", False),
    ):
        purge_cached_app()
        app = create_app()
        yield TestClient(app, base_url=BASE_URL)


@pytest.fixture
def unauthorized_test_client():
    """Test client with a user lacking DAG access (role=None)."""
    with (
        conf_vars(
            {
                (
                    "core",
                    "auth_manager",
                ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
                ("core", "lazy_discover_providers"): "false",
            }
        ),
        mock.patch("airflow.settings.LAZY_LOAD_PROVIDERS", False),
    ):
        purge_cached_app()
        app = create_app()
        auth_manager: SimpleAuthManager = app.state.auth_manager
        token = auth_manager._get_token_signer().generate(
            auth_manager.serialize_user(SimpleAuthManagerUser(username="dummy", role=None))
        )
        yield TestClient(
            app,
            headers={"Authorization": f"Bearer {token}"},
            base_url=BASE_URL,
        )


class TestEndpointAuthorization:
    """Test 401 and 403 for each protected HITL endpoint."""

    PARAMS = {"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID}

    def test_sessions_find_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/hitl-review/sessions/find", params=self.PARAMS)
        assert response.status_code == 401

    def test_sessions_find_403_forbidden(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/hitl-review/sessions/find", params=self.PARAMS)
        assert response.status_code == 403

    def test_sessions_feedback_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            "/hitl-review/sessions/feedback",
            params=self.PARAMS,
            json={"feedback": "fix this"},
        )
        assert response.status_code == 401

    def test_sessions_feedback_403_forbidden(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            "/hitl-review/sessions/feedback",
            params=self.PARAMS,
            json={"feedback": "fix this"},
        )
        assert response.status_code == 403

    def test_sessions_approve_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/hitl-review/sessions/approve", params=self.PARAMS)
        assert response.status_code == 401

    def test_sessions_approve_403_forbidden(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/hitl-review/sessions/approve", params=self.PARAMS)
        assert response.status_code == 403

    def test_sessions_reject_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/hitl-review/sessions/reject", params=self.PARAMS)
        assert response.status_code == 401

    def test_sessions_reject_403_forbidden(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/hitl-review/sessions/reject", params=self.PARAMS)
        assert response.status_code == 403


class TestGetMapIndex:
    def test_valid_int(self):
        assert _get_map_index("0") == 0
        assert _get_map_index("1") == 1
        assert _get_map_index("-1") == -1

    def test_invalid_returns_minus_one(self):
        assert _get_map_index("{MAP_INDEX}") == -1
        assert _get_map_index("") == -1


class TestReadXcomByPrefix:
    """Test _read_xcom_by_prefix."""

    @pytest.mark.parametrize(
        ("xcom_entries", "prefix", "expected"),
        [
            # ([(key_suffix, value), ...], prefix, expected_result)
            (
                [("1", "output1"), ("2", "output2")],
                XCOM_AGENT_OUTPUT_PREFIX,
                {1: "output1", 2: "output2"},
            ),
            (
                [("1", "v1")],
                XCOM_AGENT_OUTPUT_PREFIX,
                {1: "v1"},
            ),
            (
                [("1", "v1"), ("bad", "v2")],
                XCOM_AGENT_OUTPUT_PREFIX,
                {1: "v1"},
            ),
            (
                [("1", "a"), ("2", "b"), ("x", "c"), ("99", "d")],
                XCOM_AGENT_OUTPUT_PREFIX,
                {1: "a", 2: "b", 99: "d"},
            ),
        ],
        ids=[
            "two_iterations",
            "single_iteration",
            "numeric_and_non_numeric",
            "mixed_with_high_iteration",
        ],
    )
    def test_read_xcom_by_prefix_output_combinations(
        self, session, dag_maker, xcom_entries, prefix, expected
    ):
        _clear_db()
        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()

        for suffix, val in xcom_entries:
            _write_xcom(
                session,
                key=f"{prefix}{suffix}",
                value=val,
                region_id=NO_REGION,
                dag_id="d",
                task_id="t",
                run_id="r",
                map_index=-1,
            )
        session.commit()

        result = _read_xcom_by_prefix(
            session, region_id=NO_REGION, dag_id="d", run_id="r", task_id="t", map_index=-1, prefix=prefix
        )
        assert result == expected
        _clear_db()

    @pytest.mark.parametrize(
        ("output_value", "expected"),
        [
            # SQL
            (
                "SELECT id, name FROM users WHERE active = 1",
                "SELECT id, name FROM users WHERE active = 1",
            ),
            (
                "CREATE TABLE logs (\n  id INT PRIMARY KEY,\n  msg TEXT\n);",
                "CREATE TABLE logs (\n  id INT PRIMARY KEY,\n  msg TEXT\n);",
            ),
            (
                "INSERT INTO orders (user_id, total) VALUES (1, 99.99);",
                "INSERT INTO orders (user_id, total) VALUES (1, 99.99);",
            ),
            # JSON string (deserialize_value parses to dict)
            (
                '{"query": "SELECT * FROM t", "params": []}',
                '{"query": "SELECT * FROM t", "params": []}',
            ),
            (
                '{"rows": [{"id": 1, "name": "a"}]}',
                '{"rows": [{"id": 1, "name": "a"}]}',
            ),
            (
                {"query": "SELECT 1", "result": "ok"},
                {"query": "SELECT 1", "result": "ok"},
            ),
            (
                [{"id": 1}, {"id": 2}],
                [{"id": 1}, {"id": 2}],
            ),
            ("Summary: The report shows 3 items.", "Summary: The report shows 3 items."),
            ("Markdown: **bold** and `code`", "Markdown: **bold** and `code`"),
        ],
        ids=[
            "sql_select",
            "sql_create_table",
            "sql_insert",
            "json_string_query",
            "json_string_rows",
            "json_object",
            "json_array",
            "plain_text",
            "markdown_like",
        ],
    )
    def test_read_xcom_by_prefix_agent_output_formats(self, session, dag_maker, output_value, expected):
        _clear_db()
        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()

        _write_xcom(
            session,
            key=f"{XCOM_AGENT_OUTPUT_PREFIX}1",
            value=output_value,
            region_id=NO_REGION,
            dag_id="d",
            task_id="t",
            run_id="r",
            map_index=-1,
        )
        session.commit()

        if AIRFLOW_V_3_4_PLUS:
            read = XComModel.get_many(
                run_id="r",
                dag_ids="d",
                task_ids="t",
                key=f"{XCOM_AGENT_OUTPUT_PREFIX}1",
            )
            entity = read.column_descriptions[0]["entity"]
            stored = session.scalar(read.with_only_columns(entity.value))
        else:
            stored = session.scalar(
                select(XComModel.value).where(
                    XComModel.dag_id == "d",
                    XComModel.task_id == "t",
                    XComModel.run_id == "r",
                    XComModel.key == f"{XCOM_AGENT_OUTPUT_PREFIX}1",
                )
            )
        assert stored == output_value

        result = _read_xcom_by_prefix(
            session,
            region_id=NO_REGION,
            dag_id="d",
            run_id="r",
            task_id="t",
            map_index=-1,
            prefix=XCOM_AGENT_OUTPUT_PREFIX,
        )
        assert result == {1: expected}
        _clear_db()

    def test_read_xcom_by_prefix_mixed_formats_across_iterations(self, session, dag_maker):
        """Agent returns different formats per iteration: SQL, JSON, text."""
        _clear_db()
        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()

        entries = [
            (1, "SELECT * FROM users"),
            (2, '{"rows": [{"id": 1}]}'),
            (3, {"result": "ok", "count": 42}),
            (4, "Final summary text."),
        ]
        for i, val in entries:
            _write_xcom(
                session,
                key=f"{XCOM_AGENT_OUTPUT_PREFIX}{i}",
                value=val,
                region_id=NO_REGION,
                dag_id="d",
                task_id="t",
                run_id="r",
                map_index=-1,
            )
        session.commit()

        result = _read_xcom_by_prefix(
            session,
            region_id=NO_REGION,
            dag_id="d",
            run_id="r",
            task_id="t",
            map_index=-1,
            prefix=XCOM_AGENT_OUTPUT_PREFIX,
        )
        assert result == {
            1: "SELECT * FROM users",
            2: '{"rows": [{"id": 1}]}',
            3: {"result": "ok", "count": 42},
            4: "Final summary text.",
        }
        _clear_db()


class TestReadXcom:
    """Test _read_xcom."""

    @pytest.mark.parametrize(
        ("key", "value", "expected"),
        [
            (
                XCOM_AGENT_SESSION,
                {"status": "pending_review", "iteration": 1},
                {"status": "pending_review", "iteration": 1},
            ),
            (
                XCOM_AGENT_SESSION,
                {"status": "approved", "iteration": 2},
                {"status": "approved", "iteration": 2},
            ),
            (
                XCOM_AGENT_SESSION,
                {
                    "status": "changes_requested",
                    "iteration": 1,
                    "current_output": "SELECT id FROM t",
                },
                {
                    "status": "changes_requested",
                    "iteration": 1,
                    "current_output": "SELECT id FROM t",
                },
            ),
            (
                XCOM_AGENT_SESSION,
                {
                    "status": "pending_review",
                    "iteration": 2,
                    "current_output": '{"rows": [1, 2, 3]}',
                },
                {
                    "status": "pending_review",
                    "iteration": 2,
                    "current_output": '{"rows": [1, 2, 3]}',
                },
            ),
            (XCOM_AGENT_SESSION, None, None),
        ],
        ids=[
            "pending_review",
            "approved",
            "session_with_sql_output",
            "session_with_json_string_output",
            "not_found",
        ],
    )
    def test_read_xcom_key_value_combinations(self, session, dag_maker, key, value, expected):
        _clear_db()
        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()

        if value is not None:
            _write_xcom(
                session,
                key=key,
                value=value,
                region_id=NO_REGION,
                dag_id="d",
                task_id="t",
                run_id="r",
                map_index=-1,
            )
        session.commit()

        result = _read_xcom(
            session, region_id=NO_REGION, dag_id="d", run_id="r", task_id="t", map_index=-1, key=key
        )
        if expected is None:
            assert result is None
        else:
            assert result == expected
        _clear_db()


class TestWriteXcom:
    """Test _write_xcom."""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            (
                {"status": "approved", "iteration": 1},
                {"status": "approved", "iteration": 1},
            ),
            (
                {"status": "pending_review", "iteration": 2},
                {"status": "pending_review", "iteration": 2},
            ),
            (
                {"status": "rejected", "iteration": 1, "prompt": "p"},
                {"status": "rejected", "iteration": 1, "prompt": "p"},
            ),
            (
                {
                    "status": "changes_requested",
                    "iteration": 1,
                    "current_output": "SELECT * FROM orders",
                },
                {
                    "status": "changes_requested",
                    "iteration": 1,
                    "current_output": "SELECT * FROM orders",
                },
            ),
            (
                {
                    "status": "pending_review",
                    "iteration": 1,
                    "current_output": '{"data": [{"id": 1}]}',
                },
                {
                    "status": "pending_review",
                    "iteration": 1,
                    "current_output": '{"data": [{"id": 1}]}',
                },
            ),
        ],
        ids=[
            "approved",
            "pending_review",
            "rejected_with_prompt",
            "session_with_sql",
            "session_with_json_string",
        ],
    )
    def test_writes_and_reads_back_combinations(self, session, dag_maker, value, expected):
        _clear_db()
        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()
        session.commit()

        _write_xcom(
            session,
            region_id=NO_REGION,
            dag_id="d",
            run_id="r",
            task_id="t",
            map_index=-1,
            key=XCOM_AGENT_SESSION,
            value=value,
        )
        session.commit()

        result = _read_xcom(
            session,
            region_id=NO_REGION,
            dag_id="d",
            run_id="r",
            task_id="t",
            map_index=-1,
            key=XCOM_AGENT_SESSION,
        )
        assert result == expected
        _clear_db()

    @pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Attempt ownership starts in Airflow 3.4")
    def test_write_uses_current_attempt_after_retry(self, session, dag_maker):
        from airflow.models.xcom import XComModelV2
        from airflow.utils.state import TaskInstanceState

        _clear_db()
        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        run = dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()
        old = run.get_task_instance("t", session=session)
        old.state = TaskInstanceState.RUNNING
        _write_xcom(
            session, region_id=NO_REGION, dag_id="d", run_id="r", task_id="t", key="owner", value="old"
        )

        current = old.prepare_db_for_next_try(session)
        _write_xcom(
            session, region_id=NO_REGION, dag_id="d", run_id="r", task_id="t", key="owner", value="current"
        )

        assert XComModelV2.get_for_attempt(old.id, "owner", session=session).value == "old"
        assert XComModelV2.get_for_attempt(current.id, "owner", session=session).value == "current"
        assert (
            _read_xcom(session, region_id=NO_REGION, dag_id="d", run_id="r", task_id="t", key="owner")
            == "current"
        )
        _clear_db()


class TestGetBaseUrlPath:
    def test_default_base_url(self):
        with conf_vars({("api", "base_url"): "/"}):
            assert _get_base_url_path("/hitl-review") == "/hitl-review"

    def test_http_base_url_extracts_path(self):
        with conf_vars({("api", "base_url"): "http://example.com/airflow/"}):
            assert _get_base_url_path("/hitl-review") == "/airflow/hitl-review"


class TestIsTaskCompleted:
    """Test _is_task_completed."""

    @pytest.fixture(autouse=True)
    def setup(self):
        _clear_db()
        yield
        _clear_db()

    def test_returns_true_when_no_ti(self, session, dag_maker):
        result = _is_task_completed(
            session, region_id=NO_REGION, dag_id="x", run_id="y", task_id="z", map_index=-1
        )
        assert result is True

    def test_returns_false_when_ti_running(self, session, dag_maker):
        from airflow.utils.state import TaskInstanceState

        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()
        ti = dr.get_task_instance("t")
        ti.state = TaskInstanceState.RUNNING
        session.merge(ti)
        session.commit()

        result = _is_task_completed(
            session, region_id=NO_REGION, dag_id="d", run_id="r", task_id="t", map_index=-1
        )
        assert result is False

    @pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Attempt ownership starts in Airflow 3.4")
    @pytest.mark.parametrize(
        ("old_state", "archived_state"),
        [
            pytest.param("success", "success", id="archived-success"),
            pytest.param("running", "failed", id="archived-failed-retry"),
        ],
    )
    def test_ignores_historical_attempt_state(self, session, dag_maker, old_state, archived_state):
        from airflow.utils.state import TaskInstanceState

        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        run = dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        old = run.get_task_instance("t", session=session)
        old.state = TaskInstanceState(old_state)
        current = old.prepare_db_for_next_try(session)
        assert old.state == TaskInstanceState(archived_state)
        current.state = TaskInstanceState.RUNNING
        session.flush()

        assert _is_task_completed(session, region_id=NO_REGION, dag_id="d", run_id="r", task_id="t") is False


class TestBuildSessionResponse:
    """Test _build_session_response."""

    @pytest.fixture(autouse=True)
    def setup(self):
        _clear_db()
        yield
        _clear_db()

    def test_returns_none_when_no_session(self, session, dag_maker):
        with dag_maker("d", schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun(run_id="r", run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()
        session.commit()

        result = _build_session_response(
            session, region_id=NO_REGION, dag_id="d", run_id="r", task_id="t", map_index=-1
        )
        assert result is None

    @pytest.mark.parametrize(
        ("status", "iteration", "prompt", "current_output", "expected_status", "expected_output"),
        [
            (
                SessionStatus.PENDING_REVIEW,
                1,
                "Summarize",
                "Initial output",
                "pending_review",
                "Initial output",
            ),
            (
                SessionStatus.CHANGES_REQUESTED,
                2,
                "Fix it",
                "Revised text",
                "changes_requested",
                "Revised text",
            ),
            (
                SessionStatus.APPROVED,
                1,
                "",
                "Done",
                "approved",
                "Done",
            ),
            (
                SessionStatus.MAX_ITERATIONS_EXCEEDED,
                5,
                "p",
                "output",
                "max_iterations_exceeded",
                "output",
            ),
            (
                SessionStatus.TIMEOUT_EXCEEDED,
                2,
                "p",
                "output",
                "timeout_exceeded",
                "output",
            ),
        ],
        ids=[
            "pending_review",
            "changes_requested",
            "approved",
            "max_iterations_exceeded",
            "timeout_exceeded",
        ],
    )
    def test_build_session_response_combinations(
        self,
        session,
        dag_maker,
        status,
        iteration,
        prompt,
        current_output,
        expected_status,
        expected_output,
    ):
        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(run_id=TEST_RUN_ID, run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()
        _create_hitl_session(
            status=status,
            iteration=iteration,
            prompt=prompt,
            current_output=current_output,
            session=session,
        )
        session.commit()

        result = _build_session_response(
            session,
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            run_id=TEST_RUN_ID,
            task_id=TEST_TASK_ID,
            map_index=-1,
        )
        assert result is not None
        assert result.dag_id == TEST_DAG_ID
        assert result.status == expected_status
        assert result.current_output == expected_output

    def test_build_session_response_single_conversation(self, session, dag_maker):
        """Response conversation has one assistant entry when only agent output exists."""
        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(run_id=TEST_RUN_ID, run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()
        _create_hitl_session(
            status=SessionStatus.PENDING_REVIEW,
            iteration=1,
            prompt="Summarize",
            current_output="First output",
            session=session,
        )
        session.commit()

        result = _build_session_response(
            session,
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            run_id=TEST_RUN_ID,
            task_id=TEST_TASK_ID,
            map_index=-1,
        )
        assert result is not None
        assert len(result.conversation) == 1
        assert result.conversation[0].role == "assistant"
        assert result.conversation[0].content == "First output"
        assert result.conversation[0].iteration == 1

    def test_build_session_response_single_conversation_with_human_feedback(self, session, dag_maker):
        """Response conversation order: assistant then human for one iteration."""
        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(run_id=TEST_RUN_ID, run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()
        _create_hitl_session(
            status=SessionStatus.PENDING_REVIEW,
            iteration=1,
            prompt="p",
            current_output="Initial",
            session=session,
        )
        _write_xcom(
            session,
            key=f"{XCOM_HUMAN_FEEDBACK_PREFIX}1",
            value="Please add more detail",
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        session.commit()

        result = _build_session_response(
            session,
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            run_id=TEST_RUN_ID,
            task_id=TEST_TASK_ID,
            map_index=-1,
        )
        assert result is not None
        assert len(result.conversation) == 2
        assert result.conversation[0].role == "assistant"
        assert result.conversation[0].content == "Initial"
        assert result.conversation[0].iteration == 1
        assert result.conversation[1].role == "human"
        assert result.conversation[1].content == "Please add more detail"
        assert result.conversation[1].iteration == 1

    def test_build_session_response_multiple_conversation_order(self, session, dag_maker):
        """Response conversation order: assistant 1, human 1, assistant 2, human 2."""

        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(run_id=TEST_RUN_ID, run_type=DagRunType.MANUAL, logical_date=logical_date)
        dag_maker.sync_dagbag_to_db()
        sess = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=2,
            max_iterations=5,
            prompt="p",
            current_output="Output 2",
        )
        _write_xcom(
            session,
            key=XCOM_AGENT_SESSION,
            value=sess.model_dump(mode="json"),
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        _write_xcom(
            session,
            key=f"{XCOM_AGENT_OUTPUT_PREFIX}1",
            value="Output 1",
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        _write_xcom(
            session,
            key=f"{XCOM_AGENT_OUTPUT_PREFIX}2",
            value="Output 2",
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        _write_xcom(
            session,
            key=f"{XCOM_HUMAN_FEEDBACK_PREFIX}1",
            value="Feedback 1",
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        _write_xcom(
            session,
            key=f"{XCOM_HUMAN_FEEDBACK_PREFIX}2",
            value="Feedback 2",
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        session.commit()

        result = _build_session_response(
            session,
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            run_id=TEST_RUN_ID,
            task_id=TEST_TASK_ID,
            map_index=-1,
        )
        assert result is not None
        assert result.iteration == 2
        assert len(result.conversation) == 4
        # Order: assistant 1, human 1, assistant 2, human 2
        assert result.conversation[0].role == "assistant"
        assert result.conversation[0].content == "Output 1"
        assert result.conversation[0].iteration == 1
        assert result.conversation[1].role == "human"
        assert result.conversation[1].content == "Feedback 1"
        assert result.conversation[1].iteration == 1
        assert result.conversation[2].role == "assistant"
        assert result.conversation[2].content == "Output 2"
        assert result.conversation[2].iteration == 2
        assert result.conversation[3].role == "human"
        assert result.conversation[3].content == "Feedback 2"
        assert result.conversation[3].iteration == 2


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

    def test_react_apps_registered(self):
        assert len(HITLReviewPlugin.react_apps) == 1
        app = HITLReviewPlugin.react_apps[0]
        assert app["name"] == "HITL Review"
        assert app["url_route"] == "hitl-review"
        assert app["destination"] == "task_instance"
        assert "main.umd.cjs" in app["bundle_url"]


@pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Regional task identity requires Airflow 3.4+")
class TestRegionalReview:
    @pytest.fixture
    def regional_review(self, dag_maker, session):
        _clear_db()

        @task_group
        def body():
            EmptyOperator(task_id="review")

        with dag_maker(TEST_DAG_ID, serialized=True):
            create_loop(body, max_iterations=2)
        dr = dag_maker.create_dagrun(run_id=TEST_RUN_ID)
        first = next(ti for ti in dr.task_instances if ti.task_id == "body.review")
        first.state = "success"
        second = TaskInstance(
            task=dag_maker.serialized_dag.get_task(first.task_id),
            run_id=dr.run_id,
            dag_version_id=first.dag_version_id,
            region_id=first.region_id,
            region_index=1,
            state="deferred",
        )
        session.add(second)
        for ti in (first, second):
            output = f"pass {ti.region_index}"
            values = {
                XCOM_AGENT_SESSION: AgentSessionData(
                    status=SessionStatus.PENDING_REVIEW,
                    iteration=1,
                    max_iterations=5,
                    current_output=output,
                ).model_dump(mode="json"),
                f"{XCOM_AGENT_OUTPUT_PREFIX}1": output,
            }
            session.flush()
            for key, value in values.items():
                XComModel.set_for_attempt(
                    task_instance_id=ti.id, key=key, value=value, serialize=False, session=session
                )
        session.commit()
        yield second
        _clear_db()

    @pytest.mark.parametrize(
        ("action", "expected_status"),
        [
            ("find", "pending_review"),
            ("feedback", "changes_requested"),
            ("approve", "approved"),
            ("reject", "rejected"),
        ],
    )
    def test_exact_scope_preserves_other_loop_pass(
        self, regional_review, test_client, session, action, expected_status
    ):
        ti = regional_review
        params = {
            "dag_id": ti.dag_id,
            "run_id": ti.run_id,
            "task_id": ti.task_id,
            "map_index": -1,
            "region_id": str(ti.region_id),
            "region_index": ti.region_index,
        }
        if action == "find":
            response = test_client.get("/hitl-review/sessions/find", params=params)
        else:
            response = test_client.post(
                f"/hitl-review/sessions/{action}", params=params, json={"feedback": "revise"}
            )

        assert response.status_code == 200, response.text
        data = response.json()
        assert data["status"] == expected_status
        assert data["current_output"] == "pass 1"
        assert data["conversation"][0]["content"] == "pass 1"
        assert data["task_completed"] is False
        session.expire_all()
        earlier = session.scalar(
            select(XComModel.value).where(
                XComModel.key == XCOM_AGENT_SESSION,
                XComModel.region_id == ti.region_id,
                XComModel.region_index == 0,
            )
        )
        assert earlier["status"] == "pending_review"
        assert earlier["current_output"] == "pass 0"

    @mock.patch("airflow.providers.common.ai.plugins.hitl_review.AIRFLOW_V_3_4_PLUS", False)
    def test_region_selectors_are_rejected_on_hosts_without_regions(self, regional_review, test_client):
        ti = regional_review
        response = test_client.get(
            "/hitl-review/sessions/find",
            params={
                "dag_id": ti.dag_id,
                "run_id": ti.run_id,
                "task_id": ti.task_id,
                "region_id": str(ti.region_id),
                "region_index": ti.region_index,
            },
        )
        assert response.status_code == 400
        assert response.json()["detail"] == "Region selectors require Airflow 3.4 or later"

    def test_loop_scope_is_required(self, regional_review, test_client):
        ti = regional_review
        response = test_client.get(
            "/hitl-review/sessions/find",
            params={"dag_id": ti.dag_id, "run_id": ti.run_id, "task_id": ti.task_id},
        )
        assert response.status_code == 400
        assert "loop producer requires" in response.json()["detail"]


@pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Regional task identity requires Airflow 3.4+")
class TestForkedRegionReview:
    # A live row beyond a fork's cut should be impossible; this is a defensive test for a clear racing the scheduler.
    @pytest.fixture
    def forked_passes(self, dag_maker, session):
        _clear_db()

        @task_group
        def body():
            EmptyOperator(task_id="review")

        with dag_maker(TEST_DAG_ID, serialized=True):
            create_loop(body, max_iterations=2)
        dr = dag_maker.create_dagrun(run_id=TEST_RUN_ID)
        first = next(ti for ti in dr.task_instances if ti.task_id == "body.review")
        region = session.get(DynamicRegion, first.region_id)
        fork = DynamicRegion(
            dag_id=region.dag_id,
            run_id=region.run_id,
            node_id=region.node_id,
            forked_from_region_id=region.id,
        )
        session.add(fork)
        session.flush()
        task = dag_maker.serialized_dag.get_task(first.task_id)
        beyond_cut = TaskInstance(
            task=task,
            run_id=dr.run_id,
            dag_version_id=first.dag_version_id,
            region_id=region.id,
            region_index=1,
            state="failed",
        )
        forked = TaskInstance(
            task=task,
            run_id=dr.run_id,
            dag_version_id=first.dag_version_id,
            region_id=fork.id,
            region_index=1,
            state="running",
        )
        session.add_all([beyond_cut, forked])
        for ti, output in ((beyond_cut, "beyond cut"), (forked, "forked")):
            values = {
                XCOM_AGENT_SESSION: AgentSessionData(
                    status=SessionStatus.PENDING_REVIEW,
                    iteration=1,
                    max_iterations=5,
                    current_output=output,
                ).model_dump(mode="json"),
                f"{XCOM_AGENT_OUTPUT_PREFIX}1": output,
            }
            session.flush()
            for key, value in values.items():
                XComModel.set_for_attempt(
                    task_instance_id=ti.id, key=key, value=value, serialize=False, session=session
                )
        session.commit()
        yield beyond_cut, forked
        _clear_db()

    @staticmethod
    def _params(ti):
        return {
            "dag_id": ti.dag_id,
            "run_id": ti.run_id,
            "task_id": ti.task_id,
            "region_id": str(ti.region_id),
            "region_index": ti.region_index,
        }

    @pytest.mark.parametrize(
        ("action", "expected_status"),
        [
            ("feedback", "changes_requested"),
            ("approve", "approved"),
            ("reject", "rejected"),
        ],
    )
    def test_response_ignores_live_row_left_beyond_a_fork_cut(
        self, forked_passes, test_client, action, expected_status
    ):
        beyond_cut, forked = forked_passes

        response = test_client.post(
            f"/hitl-review/sessions/{action}", params=self._params(forked), json={"feedback": "revise"}
        )

        assert response.status_code == 200, response.text
        assert response.json()["status"] == expected_status
        assert response.json()["current_output"] == "forked"
        other = test_client.get("/hitl-review/sessions/find", params=self._params(beyond_cut))
        assert other.status_code == 200, other.text
        assert other.json()["status"] == "pending_review"
        assert other.json()["current_output"] == "beyond cut"

    @pytest.mark.parametrize(("pass_number", "expected_completed"), [(0, True), (1, False)])
    def test_completion_ignores_live_row_left_beyond_a_fork_cut(
        self, forked_passes, test_client, pass_number, expected_completed
    ):
        response = test_client.get(
            "/hitl-review/sessions/find", params=self._params(forked_passes[pass_number])
        )

        assert response.status_code == 200, response.text
        assert response.json()["task_completed"] is expected_completed


@pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Regional task identity requires Airflow 3.4+")
class TestMappedReview:
    @pytest.fixture
    def mapped_review(self, dag_maker, session):
        _clear_db()
        with dag_maker(TEST_DAG_ID, serialized=True):
            PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1], [2]])
        dr = dag_maker.create_dagrun(run_id=TEST_RUN_ID)
        slots = sorted(
            (ti for ti in dr.task_instances if ti.task_id == "mapped"), key=lambda ti: ti.region_index
        )
        assert [ti.region_index for ti in slots] == [0, 1]
        for ti in slots:
            ti.state = "deferred"
            output = f"slot {ti.region_index}"
            values = {
                XCOM_AGENT_SESSION: AgentSessionData(
                    status=SessionStatus.PENDING_REVIEW,
                    iteration=1,
                    max_iterations=5,
                    current_output=output,
                ).model_dump(mode="json"),
                f"{XCOM_AGENT_OUTPUT_PREFIX}1": output,
            }
            session.flush()
            for key, value in values.items():
                XComModel.set_for_attempt(
                    task_instance_id=ti.id, key=key, value=value, serialize=False, session=session
                )
        session.commit()
        yield slots
        _clear_db()

    @pytest.mark.parametrize("approved", [0, 1])
    def test_approve_addressed_by_map_index_only_touches_that_slot(
        self, approved, mapped_review, test_client
    ):
        ti = mapped_review[approved]
        params = {"dag_id": ti.dag_id, "run_id": ti.run_id, "task_id": ti.task_id}

        response = test_client.post(
            "/hitl-review/sessions/approve", params={**params, "map_index": approved}, json={}
        )

        assert response.status_code == 200, response.text
        assert response.json()["current_output"] == f"slot {approved}"
        sessions = [
            test_client.get("/hitl-review/sessions/find", params={**params, "map_index": index}).json()
            for index in range(len(mapped_review))
        ]
        assert [(s["status"], s["current_output"]) for s in sessions] == [
            ("approved" if index == approved else "pending_review", f"slot {index}")
            for index in range(len(mapped_review))
        ]

    def test_explicit_sentinel_region_does_not_read_the_node_region_session(self, mapped_review, test_client):
        slot = mapped_review[1]
        response = test_client.get(
            "/hitl-review/sessions/find",
            params={
                "dag_id": slot.dag_id,
                "run_id": slot.run_id,
                "task_id": slot.task_id,
                "map_index": 1,
                "region_id": str(NO_REGION),
            },
        )

        assert response.status_code == 404


@pytest.mark.skipif(not AIRFLOW_V_3_4_PLUS, reason="Regional task identity requires Airflow 3.4+")
class TestMappedReviewInLoop:
    @pytest.fixture
    def loop_passes(self, dag_maker, session):
        _clear_db()

        @task_group
        def body():
            PythonOperator.partial(task_id="mapped", python_callable=str).expand(op_args=[[1]])

        with dag_maker(TEST_DAG_ID, serialized=True):
            create_loop(body, max_iterations=2)
        dr = dag_maker.create_dagrun(run_id=TEST_RUN_ID)
        first_pass = next(ti for ti in dr.task_instances if ti.task_id == "body.mapped")
        first_region = session.get(DynamicRegion, first_pass.region_id)
        second_region = DynamicRegion.get_or_create(
            dag_id=first_region.dag_id,
            run_id=first_region.run_id,
            node_id=first_region.node_id,
            parent_region_id=first_region.parent_region_id,
            parent_region_index=1,
            session=session,
        )
        session.flush()
        second_pass = TaskInstance(
            task=dag_maker.serialized_dag.get_task(first_pass.task_id),
            run_id=dr.run_id,
            dag_version_id=first_pass.dag_version_id,
            region_id=second_region.id,
            region_index=first_pass.region_index,
            state="running",
        )
        session.add(second_pass)
        first_pass.state = "failed"
        for ti, output in ((first_pass, "first pass"), (second_pass, "second pass")):
            values = {
                XCOM_AGENT_SESSION: AgentSessionData(
                    status=SessionStatus.PENDING_REVIEW,
                    iteration=1,
                    max_iterations=5,
                    current_output=output,
                ).model_dump(mode="json"),
                f"{XCOM_AGENT_OUTPUT_PREFIX}1": output,
            }
            session.flush()
            for key, value in values.items():
                XComModel.set_for_attempt(
                    task_instance_id=ti.id, key=key, value=value, serialize=False, session=session
                )
        session.commit()
        yield first_pass, second_pass
        _clear_db()

    @staticmethod
    def _params(ti):
        return {
            "dag_id": ti.dag_id,
            "run_id": ti.run_id,
            "task_id": ti.task_id,
            "region_id": str(ti.region_id),
            "region_index": ti.region_index,
        }

    @pytest.mark.parametrize(
        ("action", "expected_status"),
        [
            ("feedback", "changes_requested"),
            ("approve", "approved"),
            ("reject", "rejected"),
        ],
    )
    def test_response_for_one_pass_leaves_the_same_map_index_in_the_other_pass(
        self, loop_passes, test_client, session, action, expected_status
    ):
        first_pass, second_pass = loop_passes
        assert first_pass.region_id != second_pass.region_id
        assert first_pass.region_index == second_pass.region_index

        response = test_client.post(
            f"/hitl-review/sessions/{action}", params=self._params(second_pass), json={"feedback": "revise"}
        )

        assert response.status_code == 200, response.text
        assert response.json()["status"] == expected_status
        assert response.json()["current_output"] == "second pass"
        other = test_client.get("/hitl-review/sessions/find", params=self._params(first_pass))
        assert other.status_code == 200, other.text
        assert other.json()["status"] == "pending_review"
        assert other.json()["current_output"] == "first pass"

    @pytest.mark.parametrize(("pass_number", "expected_completed"), [(0, True), (1, False)])
    def test_task_completion_is_reported_for_the_selected_pass(
        self, loop_passes, test_client, pass_number, expected_completed
    ):
        response = test_client.get(
            "/hitl-review/sessions/find", params=self._params(loop_passes[pass_number])
        )

        assert response.status_code == 200, response.text
        assert response.json()["task_completed"] is expected_completed


class TestFindSessionEndpoint:
    """Test the /sessions/find endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(
            run_id=TEST_RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        yield
        _clear_db()

    def test_returns_session_when_found(self, test_client):
        _create_hitl_session()
        response = test_client.get(
            "/hitl-review/sessions/find",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["dag_id"] == TEST_DAG_ID
        assert data["run_id"] == TEST_RUN_ID
        assert data["task_id"] == TEST_TASK_ID
        assert data["status"] == "pending_review"
        assert data["iteration"] == 1
        assert data["max_iterations"] == 5
        assert data["prompt"] == "Summarize"
        assert data["current_output"] == "Initial output"
        assert data["task_completed"] is True
        assert len(data["conversation"]) == 1
        assert data["conversation"][0]["role"] == "assistant"
        assert data["conversation"][0]["content"] == "Initial output"
        assert data["conversation"][0]["iteration"] == 1

    def test_404_when_session_not_found(self, test_client):
        response = test_client.get(
            "/hitl-review/sessions/find",
            params={
                "dag_id": TEST_DAG_ID,
                "run_id": "nonexistent_run",
                "task_id": TEST_TASK_ID,
            },
        )
        assert response.status_code == 404
        assert "task_active" in response.json()["detail"]

    def test_returns_max_iterations_exceeded_status(self, test_client, session, dag_maker):
        """Find endpoint returns max_iterations_exceeded when session has that status."""
        _create_hitl_session(
            status=SessionStatus.MAX_ITERATIONS_EXCEEDED,
            iteration=5,
            prompt="p",
            current_output="output",
            session=session,
        )
        session.commit()

        response = test_client.get(
            "/hitl-review/sessions/find",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "max_iterations_exceeded"
        assert data["iteration"] == 5
        assert data["max_iterations"] == 5
        assert data["prompt"] == "p"
        assert data["current_output"] == "output"
        assert len(data["conversation"]) == 1
        assert data["conversation"][0]["role"] == "assistant"
        assert data["conversation"][0]["content"] == "output"

    def test_returns_timeout_exceeded_status(self, test_client, session, dag_maker):
        """Find endpoint returns timeout_exceeded when session has that status."""
        _create_hitl_session(
            status=SessionStatus.TIMEOUT_EXCEEDED,
            iteration=2,
            prompt="p",
            current_output="output",
            session=session,
        )
        session.commit()

        response = test_client.get(
            "/hitl-review/sessions/find",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "timeout_exceeded"
        assert data["iteration"] == 2
        assert data["prompt"] == "p"
        assert data["current_output"] == "output"
        assert len(data["conversation"]) == 1
        assert data["conversation"][0]["content"] == "output"

    def test_find_returns_full_response_content_multiple_conversation(self, test_client, session, dag_maker):
        """Find endpoint returns ordered conversation (assistant 1, human 1, assistant 2)."""
        sess = AgentSessionData(
            status=SessionStatus.PENDING_REVIEW,
            iteration=2,
            max_iterations=5,
            prompt="Summarize",
            current_output="Revised output",
        )
        _write_xcom(
            session,
            key=XCOM_AGENT_SESSION,
            value=sess.model_dump(mode="json"),
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        _write_xcom(
            session,
            key=f"{XCOM_AGENT_OUTPUT_PREFIX}1",
            value="First output",
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        _write_xcom(
            session,
            key=f"{XCOM_AGENT_OUTPUT_PREFIX}2",
            value="Revised output",
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        _write_xcom(
            session,
            key=f"{XCOM_HUMAN_FEEDBACK_PREFIX}1",
            value="Add more detail",
            region_id=NO_REGION,
            dag_id=TEST_DAG_ID,
            task_id=TEST_TASK_ID,
            run_id=TEST_RUN_ID,
            map_index=-1,
        )
        session.commit()

        response = test_client.get(
            "/hitl-review/sessions/find",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["dag_id"] == TEST_DAG_ID
        assert data["status"] == "pending_review"
        assert data["iteration"] == 2
        assert data["max_iterations"] == 5
        assert data["prompt"] == "Summarize"
        assert data["current_output"] == "Revised output"
        assert len(data["conversation"]) == 3
        assert data["conversation"][0]["role"] == "assistant"
        assert data["conversation"][0]["content"] == "First output"
        assert data["conversation"][0]["iteration"] == 1
        assert data["conversation"][1]["role"] == "human"
        assert data["conversation"][1]["content"] == "Add more detail"
        assert data["conversation"][1]["iteration"] == 1
        assert data["conversation"][2]["role"] == "assistant"
        assert data["conversation"][2]["content"] == "Revised output"
        assert data["conversation"][2]["iteration"] == 2


class TestSubmitFeedbackEndpoint:
    """Test the /sessions/feedback endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(
            run_id=TEST_RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        yield
        _clear_db()

    def test_submit_feedback_returns_updated_session(self, test_client):
        _create_hitl_session()
        response = test_client.post(
            "/hitl-review/sessions/feedback",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
            json={"feedback": "Add more detail"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "changes_requested"
        assert data["iteration"] == 1
        assert data["current_output"] == "Initial output"
        assert len(data["conversation"]) == 2
        assert data["conversation"][0]["role"] == "assistant"
        assert data["conversation"][0]["content"] == "Initial output"
        assert data["conversation"][0]["iteration"] == 1
        assert data["conversation"][1]["role"] == "human"
        assert data["conversation"][1]["content"] == "Add more detail"
        assert data["conversation"][1]["iteration"] == 1

    def test_submit_feedback_404_when_no_session(self, test_client):
        response = test_client.post(
            "/hitl-review/sessions/feedback",
            params={
                "dag_id": TEST_DAG_ID,
                "run_id": "nonexistent_run",
                "task_id": TEST_TASK_ID,
            },
            json={"feedback": "Add more"},
        )
        assert response.status_code == 404

    def test_submit_feedback_400_when_empty_feedback(self, test_client):
        _create_hitl_session()
        for feedback in ("", "   ", "\t\n"):
            response = test_client.post(
                "/hitl-review/sessions/feedback",
                params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
                json={"feedback": feedback},
            )
            assert response.status_code == 400
            assert "Feedback is required" in response.json()["detail"]


class TestApproveEndpoint:
    """Test the /sessions/approve endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(
            run_id=TEST_RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        yield
        _clear_db()

    def test_approve_returns_session(self, test_client):
        _create_hitl_session()
        response = test_client.post(
            "/hitl-review/sessions/approve",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "approved"


class TestRejectEndpoint:
    """Test the /sessions/reject endpoint."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(
            run_id=TEST_RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        yield
        _clear_db()

    def test_reject_returns_session(self, test_client):
        _create_hitl_session()
        response = test_client.post(
            "/hitl-review/sessions/reject",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
        )
        assert response.status_code == 200
        assert response.json()["status"] == "rejected"


class TestConflictOnWrongStatus:
    """Test 409 when session is not pending_review."""

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        _clear_db()
        with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date, serialized=True):
            EmptyOperator(task_id=TEST_TASK_ID)
        dag_maker.create_dagrun(
            run_id=TEST_RUN_ID,
            run_type=DagRunType.MANUAL,
            logical_date=logical_date,
        )
        dag_maker.sync_dagbag_to_db()
        _create_hitl_session(status=SessionStatus.APPROVED)
        yield
        _clear_db()

    def test_submit_feedback_409_when_already_approved(self, test_client):
        response = test_client.post(
            "/hitl-review/sessions/feedback",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
            json={"feedback": "Too late"},
        )
        assert response.status_code == 409
        assert "approved" in response.json()["detail"]

    def test_approve_409_when_already_approved(self, test_client):
        response = test_client.post(
            "/hitl-review/sessions/approve",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
        )
        assert response.status_code == 409

    def test_reject_409_when_already_approved(self, test_client):
        response = test_client.post(
            "/hitl-review/sessions/reject",
            params={"dag_id": TEST_DAG_ID, "run_id": TEST_RUN_ID, "task_id": TEST_TASK_ID},
        )
        assert response.status_code == 409
