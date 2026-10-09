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

import re
from datetime import UTC, datetime
from unittest import mock
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.orm import Session

from airflow.api_fastapi.auth.managers.models.resource_details import (
    AccessView,
    DagAccessEntity,
    DagDetails,
)
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.utils.session import NEW_SESSION, provide_session

from tests_common.test_utils.asserts import assert_queries_count, capture_orm_selects
from tests_common.test_utils.db import clear_db_logs, clear_db_runs
from tests_common.test_utils.format_datetime import from_datetime_to_zulu, from_datetime_to_zulu_without_ms

pytestmark = pytest.mark.db_test

DAG_ID = "TEST_DAG_ID"
DAG_DISPLAY_NAME = "TEST_DAG_ID"
DAG_RUN_ID = "TEST_DAG_RUN_ID"
TASK_ID = "TEST_TASK_ID"
TASK_DISPLAY_NAME = "TEST_TASK_ID"
DAG_EXECUTION_DATE = datetime(2024, 6, 15, 0, 0, tzinfo=UTC)
OWNER = "TEST_OWNER"
OWNER_DISPLAY_NAME = "Test Owner"
OWNER_AIRFLOW = "airflow"
TASK_INSTANCE_EVENT = "TASK_INSTANCE_EVENT"


EVENT_NORMAL = "NORMAL_EVENT"
EVENT_WITH_OWNER = "EVENT_WITH_OWNER"
EVENT_WITH_TASK_INSTANCE = "EVENT_WITH_TASK_INSTANCE"
EVENT_WITH_OWNER_AND_TASK_INSTANCE = "EVENT_WITH_OWNER_AND_TASK_INSTANCE"
EVENT_WITHOUT_DTTM = "EVENT_WITHOUT_DTTM"
EVENT_NON_EXISTED_ID = 9999
TEAM_EVENT = "TEAM_EVENT"
TEAM_NAME = "TEST_TEAM"


def _assert_selects_only_display_name_columns(statements: list[str]) -> None:
    (sql,) = [sql for sql in statements if "task_instance_1" in sql]
    select_clause = sql.split(" FROM log ", 1)[0]
    assert set(re.findall(r"\bdag_1\.(\w+)", select_clause)) == {"dag_id", "dag_display_name"}
    assert set(re.findall(r"\btask_instance_1\.(\w+)", select_clause)) == {
        "id",
        "task_id",
        "task_display_name",
    }
    assert "dag_run" not in sql


class TestEventLogsEndpoint:
    """Common class for /eventLogs related unit tests."""

    @staticmethod
    def _clear_db():
        clear_db_logs()
        clear_db_runs()

    @pytest.fixture(autouse=True)
    @provide_session
    def setup(self, create_task_instance, *, session: Session = NEW_SESSION) -> dict[str, Log]:
        """
        Setup event logs for testing.
        :return: Dictionary with event log keys and their corresponding IDs.
        """
        self._clear_db()
        # create task instances for testing
        task_instance = create_task_instance(
            session=session,
            dag_id=DAG_ID,
            task_id=TASK_ID,
            run_id=DAG_RUN_ID,
            logical_date=DAG_EXECUTION_DATE,
        )
        normal_log = Log(
            event=EVENT_NORMAL,
        )
        log_with_owner = Log(
            event=EVENT_WITH_OWNER,
            owner=OWNER,
            owner_display_name=OWNER_DISPLAY_NAME,
        )
        log_with_task_instance = Log(
            event=TASK_INSTANCE_EVENT,
            task_instance=task_instance,
        )
        log_with_owner_and_task_instance = Log(
            event=EVENT_WITH_OWNER_AND_TASK_INSTANCE,
            owner=OWNER,
            owner_display_name=OWNER_DISPLAY_NAME,
            task_instance=task_instance,
        )
        session.add_all(
            [normal_log, log_with_owner, log_with_task_instance, log_with_owner_and_task_instance]
        )
        session.commit()
        return {
            EVENT_NORMAL: normal_log,
            EVENT_WITH_OWNER: log_with_owner,
            TASK_INSTANCE_EVENT: log_with_task_instance,
            EVENT_WITH_OWNER_AND_TASK_INSTANCE: log_with_owner_and_task_instance,
        }

    def teardown_method(self) -> None:
        self._clear_db()


class TestGetEventLog(TestEventLogsEndpoint):
    @pytest.mark.parametrize(
        ("event_log_key", "expected_status_code", "expected_body"),
        [
            (
                EVENT_NORMAL,
                200,
                {
                    "event": EVENT_NORMAL,
                },
            ),
            (
                EVENT_WITH_OWNER,
                200,
                {
                    "event": EVENT_WITH_OWNER,
                    "owner": OWNER,
                    "owner_display_name": OWNER_DISPLAY_NAME,
                },
            ),
            (
                TASK_INSTANCE_EVENT,
                200,
                {
                    "dag_id": DAG_ID,
                    "dag_display_name": DAG_DISPLAY_NAME,
                    "event": TASK_INSTANCE_EVENT,
                    "map_index": -1,
                    "owner": OWNER_AIRFLOW,
                    "owner_display_name": OWNER_AIRFLOW,
                    "run_id": DAG_RUN_ID,
                    "task_id": TASK_ID,
                    "task_display_name": TASK_DISPLAY_NAME,
                },
            ),
            (
                EVENT_WITH_OWNER_AND_TASK_INSTANCE,
                200,
                {
                    "dag_id": DAG_ID,
                    "dag_display_name": DAG_DISPLAY_NAME,
                    "event": EVENT_WITH_OWNER_AND_TASK_INSTANCE,
                    "map_index": -1,
                    "owner": OWNER,
                    "owner_display_name": OWNER_DISPLAY_NAME,
                    "run_id": DAG_RUN_ID,
                    "task_id": TASK_ID,
                    "task_display_name": TASK_DISPLAY_NAME,
                    "try_number": 0,
                },
            ),
            ("not_existed_event_log_key", 404, {}),
        ],
    )
    def test_get_event_log(self, test_client, setup, event_log_key, expected_status_code, expected_body):
        event_log: Log | None = setup.get(event_log_key, None)
        event_log_id = event_log.id if event_log else EVENT_NON_EXISTED_ID
        response = test_client.get(f"/eventLogs/{event_log_id}")
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        expected_json = {
            "event_log_id": event_log_id,
            "when": from_datetime_to_zulu(event_log.dttm) if event_log.dttm else None,
            "dag_display_name": expected_body.get("dag_display_name"),
            "dag_id": expected_body.get("dag_id"),
            "task_id": expected_body.get("task_id"),
            "task_display_name": expected_body.get("task_display_name"),
            "run_id": expected_body.get("run_id"),
            "map_index": event_log.map_index,
            "try_number": event_log.try_number,
            "event": expected_body.get("event"),
            "logical_date": from_datetime_to_zulu_without_ms(event_log.logical_date)
            if event_log.logical_date
            else None,
            "owner": expected_body.get("owner"),
            "owner_display_name": expected_body.get("owner_display_name"),
            "extra": expected_body.get("extra"),
            "team_name": None,
            "task_instance_id": str(event_log.task_instance_id) if event_log.task_instance_id else None,
        }

        assert response.json() == expected_json

    def test_get_event_log_selects_only_display_name_columns(self, test_client, setup):
        with capture_orm_selects("log") as statements:
            response = test_client.get(f"/eventLogs/{setup[TASK_INSTANCE_EVENT].id}")

        assert response.status_code == 200
        _assert_selects_only_display_name_columns(statements)

    def test_get_event_log_returns_the_recorded_team(self, test_client, session):
        event_log = Log(event="cli_triggerer", team_name=TEAM_NAME)
        session.add(event_log)
        session.commit()

        response = test_client.get(f"/eventLogs/{event_log.id}")

        assert response.status_code == 200
        assert response.json()["team_name"] == TEAM_NAME

    def test_should_raises_401_unauthenticated(self, unauthenticated_test_client, setup):
        event_log_id = setup[EVENT_NORMAL].id
        response = unauthenticated_test_client.get(f"/eventLogs/{event_log_id}")
        assert response.status_code == 401

    def test_should_raises_403_forbidden(self, unauthorized_test_client, setup):
        event_log_id = setup[EVENT_NORMAL].id
        response = unauthorized_test_client.get(f"/eventLogs/{event_log_id}")
        assert response.status_code == 403

    def test_should_respond_403_when_user_lacks_dag_audit_log_permission(self, test_client, setup):
        """The detail endpoint must enforce the per-DAG audit log permission of the event log's dag_id."""
        event_log_id = setup[TASK_INSTANCE_EVENT].id
        with mock.patch(
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.is_authorized_dag",
            return_value=False,
        ) as mock_is_authorized_dag:
            response = test_client.get(f"/eventLogs/{event_log_id}")

        assert response.status_code == 403
        mock_is_authorized_dag.assert_called_once_with(
            method="GET",
            access_entity=DagAccessEntity.AUDIT_LOG,
            details=DagDetails(id=DAG_ID, team_name=None),
            user=mock.ANY,
        )

    def test_should_authorize_with_event_log_dag_id(self, test_client, setup):
        """When the event log is bound to a DAG, authorization must scope to that DAG id."""
        event_log_id = setup[TASK_INSTANCE_EVENT].id
        with mock.patch(
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.is_authorized_dag",
            return_value=True,
        ) as mock_is_authorized_dag:
            response = test_client.get(f"/eventLogs/{event_log_id}")

        assert response.status_code == 200
        mock_is_authorized_dag.assert_called_once_with(
            method="GET",
            access_entity=DagAccessEntity.AUDIT_LOG,
            details=DagDetails(id=DAG_ID, team_name=None),
            user=mock.ANY,
        )

    @pytest.mark.parametrize(
        ("can_view_all_audit_logs", "expected_status_code"),
        [
            pytest.param(True, 200, id="with-AUDIT_LOGS_ALL-sees-row"),
            pytest.param(False, 403, id="without-AUDIT_LOGS_ALL-forbidden"),
        ],
    )
    def test_non_dag_row_is_gated_on_audit_logs_all(
        self, test_client, setup, can_view_all_audit_logs, expected_status_code
    ):
        """
        A row with a NULL dag_id records an operation that is not tied to a Dag -- a
        Connection, Variable or Pool change -- so it has no per-Dag key to authorize on.
        Visibility is gated on the dedicated ``AUDIT_LOGS_ALL`` view rather than riding on
        Dag-level audit log access, which every viewer holds.
        """
        event_log_id = setup[EVENT_NORMAL].id
        with mock.patch(
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.is_authorized_view",
            return_value=can_view_all_audit_logs,
        ) as mock_is_authorized_view:
            response = test_client.get(f"/eventLogs/{event_log_id}")

        assert response.status_code == expected_status_code
        mock_is_authorized_view.assert_called_once_with(
            access_view=AccessView.AUDIT_LOGS_ALL, user=mock.ANY, team_name=None
        )

    def test_unknown_id_stays_404_and_does_not_consult_audit_logs_all(self, test_client, setup):
        """
        An id that matches no row must answer 404, not 403.

        A missing row and a NULL dag_id both read back as ``None``, so the guard has to tell
        them apart: turning an unknown id into a permission error would change the documented
        contract of the endpoint for callers that are allowed to use it.
        """
        with mock.patch(
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.is_authorized_view",
            return_value=False,
        ) as mock_is_authorized_view:
            response = test_client.get(f"/eventLogs/{EVENT_NON_EXISTED_ID}")

        assert response.status_code == 404
        mock_is_authorized_view.assert_not_called()

    @provide_session
    def test_should_return_404_for_log_without_dttm(self, test_client, *, session: Session = NEW_SESSION):  # noqa: PT028
        event_log = Log(event=EVENT_WITHOUT_DTTM)
        session.add(event_log)
        session.flush()
        event_log_id = event_log.id
        event_log.dttm = None
        session.commit()

        response = test_client.get(f"/eventLogs/{event_log_id}")

        assert response.status_code == 404


class TestGetEventLogs(TestEventLogsEndpoint):
    @pytest.mark.parametrize("fate", ["live", "archived", "purged"])
    @pytest.mark.parametrize("mapped", [False, True])
    def test_projects_public_mapping_index_for_exact_execution(
        self, test_client, dag_maker, session, fate, mapped
    ):
        @task_group
        def body():
            if mapped:
                PythonOperator.partial(task_id="work", python_callable=list).expand(op_kwargs=[{}, {}])
            else:
                EmptyOperator(task_id="work")

        with dag_maker(dag_id="audit_loop", serialized=True) as dag:
            loop = create_loop(body, max_iterations=4)
        run = dag_maker.create_dagrun()
        if mapped:
            ti = next(ti for ti in run.task_instances if ti.task_id == "body.work" and ti.region_index == 1)
        else:
            region = DynamicRegion.get_or_create(
                dag_id=run.dag_id, run_id=run.run_id, node_id=loop.group_id, session=session
            )
            session.add(region)
            session.flush()
            ti = TaskInstance(
                task=dag.get_task("body.work"),
                run_id=run.run_id,
                dag_version_id=run.created_dag_version_id,
                region_id=region.id,
                region_index=2,
            )
            session.add(ti)
        ti.try_number = 1
        ti.state = "success"
        session.flush()
        identity = ti.id
        event = Log(event="success", task_instance=ti)
        session.add(event)
        session.flush()
        if fate == "archived":
            ti.prepare_db_for_next_try(session=session)
        elif fate == "purged":
            session.execute(delete(TaskInstance.__table__).where(TaskInstance.__table__.c.id == identity))
        session.commit()
        expected_index = 1 if mapped else -1

        detail = test_client.get(f"/eventLogs/{event.id}")
        assert detail.status_code == 200, detail.text
        assert detail.json()["map_index"] == expected_index
        listed = test_client.get(
            "/eventLogs", params={"task_instance_id": str(identity), "map_index": expected_index}
        )
        assert listed.status_code == 200, listed.text
        assert listed.json()["total_entries"] == 1
        assert listed.json()["event_logs"][0]["map_index"] == expected_index

    @pytest.mark.parametrize("mapped", [False, True])
    def test_row_without_attempt_takes_the_display_name_of_its_live_task_instance(
        self, test_client, session, dag_maker, mapped
    ):
        with dag_maker(dag_id="legacy_audit", serialized=True):
            if mapped:
                PythonOperator.partial(task_id="work", python_callable=list).expand(op_kwargs=[{}, {}])
            else:
                EmptyOperator(task_id="work")
        run = dag_maker.create_dagrun()
        index = 1 if mapped else -1
        task_instance = next(ti for ti in run.task_instances if ti.region_index == index)
        task_instance._task_display_property_value = "Shown name"
        before_upgrade = Log(
            event="success",
            dag_id="legacy_audit",
            task_id="work",
            run_id=run.run_id,
            map_index=index,
        )
        other_task = Log(
            event="success", dag_id="legacy_audit", task_id="other", run_id=run.run_id, map_index=index
        )
        session.add_all([before_upgrade, other_task])
        session.commit()

        shown = test_client.get(f"/eventLogs/{before_upgrade.id}").json()
        unmatched = test_client.get(f"/eventLogs/{other_task.id}").json()
        listed = {
            entry["event_log_id"]: entry
            for entry in test_client.get("/eventLogs", params={"dag_id": "legacy_audit"}).json()["event_logs"]
        }

        assert shown["task_display_name"] == "Shown name"
        assert shown["task_instance_id"] is None
        assert unmatched["task_display_name"] is None
        assert listed[before_upgrade.id]["task_display_name"] == "Shown name"

    def test_row_with_attempt_resolves_by_attempt_and_exposes_its_id(self, test_client, session, setup):
        row = setup[TASK_INSTANCE_EVENT]
        by_attempt = test_client.get(f"/eventLogs/{row.id}").json()
        unattributed = test_client.get(f"/eventLogs/{setup[EVENT_NORMAL].id}").json()

        assert by_attempt["task_instance_id"] == str(row.task_instance_id)
        assert by_attempt["task_display_name"] == TASK_DISPLAY_NAME
        assert unattributed["task_instance_id"] is None

    @pytest.mark.parametrize("unknown", [False, True])
    def test_filters_exact_execution_before_pagination(self, test_client, session, unknown):
        selected = uuid4()
        events = [
            Log(
                event="execution_event",
                dag_id=DAG_ID,
                task_id=TASK_ID,
                run_id=DAG_RUN_ID,
                map_index=-1,
                task_instance_id=identity,
            )
            for identity in (selected, selected, uuid4(), None)
        ]
        session.add_all(events)
        session.commit()
        response = test_client.get(
            "/eventLogs",
            params={
                "task_instance_id": str(uuid4() if unknown else selected),
                "limit": 1,
                "offset": 1,
                "order_by": "event_log_id",
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["total_entries"] == (0 if unknown else 2)
        assert [row["event_log_id"] for row in response.json()["event_logs"]] == (
            [] if unknown else [events[1].id]
        )
        assert all(row["map_index"] == -1 for row in response.json()["event_logs"])

    @pytest.mark.parametrize(
        ("query_params", "expected_status_code", "expected_total_entries", "expected_events"),
        [
            (
                {},
                200,
                4,
                [EVENT_NORMAL, EVENT_WITH_OWNER, TASK_INSTANCE_EVENT, EVENT_WITH_OWNER_AND_TASK_INSTANCE],
            ),
            # offset, limit
            (
                {"offset": 1, "limit": 2},
                200,
                4,
                [EVENT_WITH_OWNER, TASK_INSTANCE_EVENT],
            ),
            # equal filter
            (
                {"event": EVENT_NORMAL},
                200,
                1,
                [EVENT_NORMAL],
            ),
            (
                {"event": EVENT_WITH_OWNER},
                200,
                1,
                [EVENT_WITH_OWNER],
            ),
            (
                {"task_id": TASK_ID},
                200,
                2,
                [TASK_INSTANCE_EVENT, EVENT_WITH_OWNER_AND_TASK_INSTANCE],
            ),
            # multiple equal filters
            (
                {"event": EVENT_WITH_OWNER, "owner": OWNER},
                200,
                1,
                [EVENT_WITH_OWNER],
            ),
            (
                {"event": EVENT_WITH_OWNER_AND_TASK_INSTANCE, "task_id": TASK_ID, "run_id": DAG_RUN_ID},
                200,
                1,
                [EVENT_WITH_OWNER_AND_TASK_INSTANCE],
            ),
            # list filter
            (
                {"excluded_events": [EVENT_NORMAL, EVENT_WITH_OWNER]},
                200,
                2,
                [TASK_INSTANCE_EVENT, EVENT_WITH_OWNER_AND_TASK_INSTANCE],
            ),
            (
                {"included_events": [EVENT_NORMAL, EVENT_WITH_OWNER]},
                200,
                2,
                [EVENT_NORMAL, EVENT_WITH_OWNER],
            ),
            # multiple list filters
            (
                {"excluded_events": [EVENT_NORMAL], "included_events": [EVENT_WITH_OWNER]},
                200,
                1,
                [EVENT_WITH_OWNER],
            ),
            # before, after filters
            (
                {"before": "2024-06-15T00:00:00Z"},
                200,
                0,
                [],
            ),
            (
                {"after": "2024-06-15T00:00:00Z"},
                200,
                4,
                [EVENT_NORMAL, EVENT_WITH_OWNER, TASK_INSTANCE_EVENT, EVENT_WITH_OWNER_AND_TASK_INSTANCE],
            ),
            (
                {"offset": 1, "excluded_events": ["non_existed_event"], "order_by": "event"},
                200,
                4,
                [EVENT_WITH_OWNER_AND_TASK_INSTANCE, EVENT_NORMAL, TASK_INSTANCE_EVENT],
            ),
            (
                {"excluded_events": [EVENT_NORMAL], "included_events": [EVENT_WITH_OWNER], "order_by": "-id"},
                200,
                1,
                [EVENT_WITH_OWNER],
            ),
            (
                {"map_index": -1, "try_number": 0, "order_by": "event", "limit": 1},
                200,
                2,
                [EVENT_WITH_OWNER_AND_TASK_INSTANCE],
            ),
        ],
    )
    def test_get_event_logs(
        self, test_client, query_params, expected_status_code, expected_total_entries, expected_events
    ):
        with assert_queries_count(3):
            response = test_client.get("/eventLogs", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        resp_json = response.json()
        assert resp_json["total_entries"] == expected_total_entries
        for event_log, expected_event in zip(resp_json["event_logs"], expected_events):
            assert event_log["event"] == expected_event

    def test_get_event_logs_selects_only_display_name_columns(self, test_client):
        with capture_orm_selects("log") as statements:
            response = test_client.get("/eventLogs")

        assert response.status_code == 200
        _assert_selects_only_display_name_columns(statements)

    @provide_session
    def test_get_event_logs_excludes_logs_without_dttm(
        self,
        test_client,
        *,
        session: Session = NEW_SESSION,  # noqa: PT028
    ):
        event_log = Log(event=EVENT_WITHOUT_DTTM)
        session.add(event_log)
        session.flush()
        event_log.dttm = None
        session.commit()

        with assert_queries_count(3):
            response = test_client.get("/eventLogs", params={"order_by": "-when"})

        assert response.status_code == 200
        resp_json = response.json()
        assert resp_json["total_entries"] == 4
        assert EVENT_WITHOUT_DTTM not in {event_log["event"] for event_log in resp_json["event_logs"]}

    def test_get_event_logs_includes_owner_display_name(self, test_client):
        response = test_client.get("/eventLogs", params={"event": EVENT_WITH_OWNER})
        assert response.status_code == 200

        event_log = response.json()["event_logs"][0]
        assert event_log["owner"] == OWNER
        assert event_log["owner_display_name"] == OWNER_DISPLAY_NAME

    def test_get_event_logs_falls_back_to_owner_when_display_name_is_unavailable(self, test_client):
        response = test_client.get("/eventLogs", params={"event": TASK_INSTANCE_EVENT})

        assert response.status_code == 200
        event_log = response.json()["event_logs"][0]
        assert event_log["owner"] == OWNER_AIRFLOW
        assert event_log["owner_display_name"] == OWNER_AIRFLOW

    def test_get_event_logs_returns_the_recorded_team(self, test_client, session):
        session.add(Log(event=TEAM_EVENT, dag_id=DAG_ID, team_name=TEAM_NAME))
        session.commit()

        with assert_queries_count(4):
            response = test_client.get("/eventLogs")

        assert response.status_code == 200
        teams_by_event = {
            event_log["event"]: event_log["team_name"] for event_log in response.json()["event_logs"]
        }
        assert teams_by_event == {
            EVENT_NORMAL: None,
            EVENT_WITH_OWNER: None,
            TASK_INSTANCE_EVENT: None,
            EVENT_WITH_OWNER_AND_TASK_INSTANCE: None,
            TEAM_EVENT: TEAM_NAME,
        }

    def test_get_event_logs_filtered_by_team(self, test_client, session):
        session.add_all(
            [
                Log(event=TEAM_EVENT, dag_id=DAG_ID, team_name=TEAM_NAME),
                Log(event="cli_triggerer", team_name=TEAM_NAME),
                Log(event="cli_triggerer", team_name="other-team"),
            ]
        )
        session.commit()

        with assert_queries_count(4):
            response = test_client.get("/eventLogs", params={"teams": [TEAM_NAME]})

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 2
        assert {event_log["event"] for event_log in body["event_logs"]} == {TEAM_EVENT, "cli_triggerer"}

        # A team no event was recorded for returns nothing.
        response = test_client.get("/eventLogs", params={"teams": ["nonexistent-team"]})
        assert response.status_code == 200
        assert response.json()["total_entries"] == 0

    def test_get_event_logs_filters_by_owner_display_name_pattern(self, test_client):
        response = test_client.get("/eventLogs", params={"owner_display_name_pattern": "est Own"})

        assert response.status_code == 200
        events = {event_log["event"] for event_log in response.json()["event_logs"]}
        assert events == {EVENT_WITH_OWNER, EVENT_WITH_OWNER_AND_TASK_INSTANCE}

    def test_get_event_logs_filters_by_owner_display_name_prefix_pattern(self, test_client):
        response = test_client.get("/eventLogs", params={"owner_display_name_prefix_pattern": "Test"})

        assert response.status_code == 200
        events = {event_log["event"] for event_log in response.json()["event_logs"]}
        assert events == {EVENT_WITH_OWNER, EVENT_WITH_OWNER_AND_TASK_INSTANCE}

    # Ordering of nulls values is DB specific.
    @pytest.mark.backend("sqlite")
    @pytest.mark.parametrize(
        ("query_params", "expected_status_code", "expected_total_entries", "expected_events"),
        [
            (
                {"order_by": "-id"},
                200,
                4,
                [EVENT_WITH_OWNER_AND_TASK_INSTANCE, TASK_INSTANCE_EVENT, EVENT_WITH_OWNER, EVENT_NORMAL],
            ),
            (
                {"order_by": "logical_date"},
                200,
                4,
                [EVENT_NORMAL, EVENT_WITH_OWNER, TASK_INSTANCE_EVENT, EVENT_WITH_OWNER_AND_TASK_INSTANCE],
            ),
            (
                {"order_by": "-logical_date"},
                200,
                4,
                [EVENT_WITH_OWNER_AND_TASK_INSTANCE, TASK_INSTANCE_EVENT, EVENT_WITH_OWNER, EVENT_NORMAL],
            ),
        ],
    )
    def test_get_event_logs_order_by(
        self, test_client, query_params, expected_status_code, expected_total_entries, expected_events
    ):
        with assert_queries_count(3):
            response = test_client.get("/eventLogs", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        resp_json = response.json()
        assert resp_json["total_entries"] == expected_total_entries
        for event_log, expected_event in zip(resp_json["event_logs"], expected_events):
            assert event_log["event"] == expected_event

    def test_should_raises_401_unauthenticated(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/eventLogs")
        assert response.status_code == 401

    def test_should_raises_403_forbidden(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/eventLogs")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("can_view_all_audit_logs", "expected_events"),
        [
            pytest.param(
                True,
                [EVENT_NORMAL, EVENT_WITH_OWNER, TASK_INSTANCE_EVENT, EVENT_WITH_OWNER_AND_TASK_INSTANCE],
                id="with-AUDIT_LOGS_ALL-sees-non-dag-rows",
            ),
            pytest.param(
                False,
                [TASK_INSTANCE_EVENT, EVENT_WITH_OWNER_AND_TASK_INSTANCE],
                id="without-AUDIT_LOGS_ALL-only-dag-rows",
            ),
        ],
    )
    def test_non_dag_rows_are_gated_on_audit_logs_all(
        self, test_client, can_view_all_audit_logs, expected_events
    ):
        """
        Rows with a NULL dag_id are returned only to callers holding ``AUDIT_LOGS_ALL``.

        Before this gate every caller that could read event logs at all received them, which
        for the default auth manager is any viewer. ``EVENT_NORMAL`` and ``EVENT_WITH_OWNER``
        carry no dag_id; the other two are bound to a Dag and stay visible either way.
        """
        with mock.patch(
            "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager.is_authorized_view",
            return_value=can_view_all_audit_logs,
        ) as mock_is_authorized_view:
            response = test_client.get("/eventLogs")

        assert response.status_code == 200
        resp_json = response.json()
        # Filtered in the query, so the excluded rows are absent from the count and
        # pagination too -- their existence does not leak through total_entries.
        assert resp_json["total_entries"] == len(expected_events)
        assert {event_log["event"] for event_log in resp_json["event_logs"]} == set(expected_events)
        mock_is_authorized_view.assert_called_once_with(
            access_view=AccessView.AUDIT_LOGS_ALL, user=mock.ANY, team_name=None
        )
