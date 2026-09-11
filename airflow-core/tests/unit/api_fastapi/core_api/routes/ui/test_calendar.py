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

import pendulum
import pytest
from sqlalchemy.orm import Session

from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.services.ui.calendar import CalendarService
from airflow.models.deadline import Deadline
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import CronPartitionTimetable, CronTriggerTimetable
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_dags, clear_db_deadline, clear_db_runs

pytestmark = pytest.mark.db_test


class TestCalendar:
    DAG_NAME = "test_dag1"

    @pytest.fixture(autouse=True)
    @provide_session
    def setup_dag_runs(self, dag_maker, *, session: Session = NEW_SESSION) -> None:
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            self.DAG_NAME,
            schedule="0 0,1 * * *",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 3, 2),
            catchup=True,
            serialized=True,
            session=session,
        ):
            EmptyOperator(task_id="test_task1")
        dag_maker.create_dagrun(run_id="run_1", state=DagRunState.FAILED, logical_date=datetime(2025, 1, 1))
        dag_maker.create_dagrun(
            run_id="run_2",
            state=DagRunState.SUCCESS,
            logical_date=datetime(2025, 1, 1, 1),
        )
        dag_maker.create_dagrun(run_id="run_3", state=DagRunState.RUNNING, logical_date=datetime(2025, 1, 2))

        dag_maker.sync_dagbag_to_db()

        session.commit()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()

    @pytest.mark.parametrize(
        ("query_params", "result"),
        [
            (
                {},
                {
                    "total_entries": 5,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-01T00:00:00Z", "state": "success", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "planned", "count": 1},
                        {"date": "2025-01-03T00:00:00Z", "state": "planned", "count": 2},
                    ],
                },
            ),
            (
                {"logical_date_gte": "2025-01-01T00:00:00Z", "logical_date_lte": "2025-01-01T23:23:59Z"},
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-01T00:00:00Z", "state": "success", "count": 1},
                    ],
                },
            ),
            (
                {"logical_date_gte": "2025-01-02T00:00:00Z", "logical_date_lte": "2025-01-02T23:23:59Z"},
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "planned", "count": 1},
                    ],
                },
            ),
        ],
    )
    def test_daily_calendar(self, test_client, query_params, result):
        with assert_queries_count(4):
            response = test_client.get(f"/calendar/{self.DAG_NAME}", params=query_params)
        assert response.status_code == 200
        body = response.json()
        print(body)

        assert body == result

    @pytest.mark.parametrize(
        ("query_params", "result"),
        [
            (
                {"granularity": "hourly"},
                {
                    "total_entries": 6,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-01T01:00:00Z", "state": "success", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T01:00:00Z", "state": "planned", "count": 1},
                        {"date": "2025-01-03T00:00:00Z", "state": "planned", "count": 1},
                        {"date": "2025-01-03T01:00:00Z", "state": "planned", "count": 1},
                    ],
                },
            ),
            (
                {
                    "granularity": "hourly",
                    "logical_date_gte": "2025-01-02T00:00:00Z",
                    "logical_date_lte": "2025-01-02T23:23:59Z",
                },
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T01:00:00Z", "state": "planned", "count": 1},
                    ],
                },
            ),
            (
                {
                    "granularity": "hourly",
                    "logical_date_gte": "2025-01-02T00:00:00Z",
                    "logical_date_lte": "2025-01-02T23:23:59Z",
                    "logical_date_gt": "2025-01-02T00:00:00Z",
                    "logical_date_lt": "2025-01-02T23:23:59Z",
                },
                {
                    "total_entries": 0,
                    "dag_runs": [],
                },
            ),
            (
                {
                    "granularity": "hourly",
                    "logical_date_gte": "2025-01-02T00:00:00Z",
                    "logical_date_lte": "2025-01-02T23:23:59Z",
                    "logical_date_gt": "2025-01-01T23:00:00Z",
                    "logical_date_lt": "2025-01-03T00:00:00Z",
                },
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T01:00:00Z", "state": "planned", "count": 1},
                    ],
                },
            ),
        ],
    )
    def test_hourly_calendar(self, setup_dag_runs, test_client, query_params, result):
        with assert_queries_count(4):
            response = test_client.get(f"/calendar/{self.DAG_NAME}", params=query_params)
        assert response.status_code == 200
        body = response.json()

        assert body == result


class TestCalendarCronNonUTCTimezone:
    """Planned runs for a cron timetable must be computed in the timetable's own timezone, not UTC."""

    DAG_NAME = "test_dag_non_utc_tz"

    @pytest.fixture(autouse=True)
    @provide_session
    def setup_dag_runs(self, dag_maker, *, session: Session = NEW_SESSION) -> None:
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            self.DAG_NAME,
            schedule=CronTriggerTimetable("0 8 * * *", timezone="Asia/Seoul"),
            start_date=datetime(2025, 1, 1),
            catchup=True,
            serialized=True,
            session=session,
        ):
            EmptyOperator(task_id="test_task1")
        dag_maker.create_dagrun(
            run_id="run_1",
            state=DagRunState.SUCCESS,
            logical_date=pendulum.datetime(2025, 1, 1, 23, 0, 0, tz="UTC"),
        )
        dag_maker.sync_dagbag_to_db()

        session.commit()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()

    def test_planned_runs_use_timetable_timezone_not_utc(self, test_client):
        response = test_client.get(f"/calendar/{self.DAG_NAME}", params={"granularity": "hourly"})
        assert response.status_code == 200
        body = response.json()

        planned = [r for r in body["dag_runs"] if r["state"] == "planned"]
        # Daily 08:00 Asia/Seoul is 23:00Z the previous day; the last run's data interval
        # ends 2025-01-01T23:00Z, so planned runs are one per remaining day of 2025.
        assert len(planned) == 364
        assert min(r["date"] for r in planned) == "2025-01-02T23:00:00Z"
        assert all(r["date"].endswith("T23:00:00Z") for r in planned), planned
        assert all(r["count"] == 1 for r in planned)


class CalendarEveryHourCronDstBase:
    """Every-hour crons must plan exactly one run per UTC hour across a DST transition, like the scheduler."""

    DAG_NAME: str
    START_DATE: datetime
    LAST_RUN_UTC: pendulum.DateTime
    EXPECTED_HOURS: list[str]

    @pytest.fixture(autouse=True)
    @provide_session
    def setup_dag_runs(self, dag_maker, *, session: Session = NEW_SESSION) -> None:
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            self.DAG_NAME,
            schedule=CronTriggerTimetable("0 * * * *", timezone="Europe/Zurich"),
            start_date=self.START_DATE,
            catchup=True,
            serialized=True,
            session=session,
        ):
            EmptyOperator(task_id="test_task1")
        dag_maker.create_dagrun(
            run_id="run_1",
            state=DagRunState.SUCCESS,
            logical_date=self.LAST_RUN_UTC,
        )
        dag_maker.sync_dagbag_to_db()

        session.commit()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()

    def test_one_planned_run_per_utc_hour(self, test_client):
        response = test_client.get(f"/calendar/{self.DAG_NAME}", params={"granularity": "hourly"})
        assert response.status_code == 200

        planned = {r["date"]: r["count"] for r in response.json()["dag_runs"] if r["state"] == "planned"}
        assert {h: planned.get(h) for h in self.EXPECTED_HOURS} == dict.fromkeys(self.EXPECTED_HOURS, 1)


class TestCalendarEveryHourCronDstFold(CalendarEveryHourCronDstBase):
    """Fall-back (2025-10-26 03:00 CEST -> 02:00 CET): the repeated hour keeps its planned run."""

    DAG_NAME = "test_dag_every_hour_dst_fold"
    START_DATE = datetime(2025, 10, 1)
    LAST_RUN_UTC = pendulum.datetime(2025, 10, 25, 22, 0, 0, tz="UTC")
    EXPECTED_HOURS = [
        "2025-10-25T23:00:00Z",
        "2025-10-26T00:00:00Z",
        "2025-10-26T01:00:00Z",
        "2025-10-26T02:00:00Z",
        "2025-10-26T03:00:00Z",
    ]


class TestCalendarEveryHourCronDstGap(CalendarEveryHourCronDstBase):
    """Spring-forward (2026-03-29 02:00 CET -> 03:00 CEST): the skipped hour is not double-counted."""

    DAG_NAME = "test_dag_every_hour_dst_gap"
    START_DATE = datetime(2026, 3, 1)
    LAST_RUN_UTC = pendulum.datetime(2026, 3, 28, 22, 0, 0, tz="UTC")
    EXPECTED_HOURS = [
        "2026-03-28T23:00:00Z",
        "2026-03-29T00:00:00Z",
        "2026-03-29T01:00:00Z",
        "2026-03-29T02:00:00Z",
        "2026-03-29T03:00:00Z",
    ]


class TestPartitionedCalendar:
    """Calendar tests for partitioned Dags (AIP-76) which use partition_date instead of logical_date."""

    DAG_NAME = "test_partitioned_dag"

    @pytest.fixture(autouse=True)
    @provide_session
    def setup_dag_runs(self, dag_maker, *, session: Session = NEW_SESSION) -> None:
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            self.DAG_NAME,
            schedule=CronPartitionTimetable("0 0 * * *", timezone=pendulum.UTC),
            start_date=datetime(2025, 1, 1),
            catchup=True,
            serialized=True,
            session=session,
        ):
            EmptyOperator(task_id="test_task1")
        dag_maker.create_dagrun(
            run_id="part_run_1",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2025, 1, 1, tzinfo=pendulum.UTC),
            partition_key="2025-01-01T00:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_run_2",
            state=DagRunState.FAILED,
            logical_date=None,
            partition_date=datetime(2025, 1, 2, tzinfo=pendulum.UTC),
            partition_key="2025-01-02T00:00:00",
        )
        dag_maker.create_dagrun(
            run_id="part_run_3",
            state=DagRunState.SUCCESS,
            logical_date=None,
            partition_date=datetime(2025, 1, 3, tzinfo=pendulum.UTC),
            partition_key="2025-01-03T00:00:00",
        )
        # Run without partition_date
        dag_maker.create_dagrun(
            run_id="non_part_run",
            state=DagRunState.RUNNING,
            logical_date=datetime(2025, 1, 4, tzinfo=pendulum.UTC),
            partition_date=None,
        )

        dag_maker.sync_dagbag_to_db()
        session.commit()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()

    @pytest.mark.parametrize(
        ("query_params", "result"),
        [
            (
                {},
                {
                    "total_entries": 4,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "success", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-03T00:00:00Z", "state": "success", "count": 1},
                        {"date": "2025-01-04T00:00:00Z", "state": "running", "count": 1},
                    ],
                },
            ),
            (
                {"partition_date_gte": "2025-01-02T00:00:00Z", "partition_date_lte": "2025-01-03T23:59:59Z"},
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-02T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-03T00:00:00Z", "state": "success", "count": 1},
                    ],
                },
            ),
            (
                {"logical_date_gte": "2025-01-01T00:00:00Z", "logical_date_lte": "2025-01-04T23:59:59Z"},
                {
                    "total_entries": 1,
                    "dag_runs": [
                        {"date": "2025-01-04T00:00:00Z", "state": "running", "count": 1},
                    ],
                },
            ),
        ],
    )
    def test_daily_calendar_partitioned(self, test_client, query_params, result):
        with assert_queries_count(4):
            response = test_client.get(f"/calendar/{self.DAG_NAME}", params=query_params)
        assert response.status_code == 200
        body = response.json()
        assert body == result

    @pytest.mark.parametrize(
        ("query_params", "result"),
        [
            (
                {"granularity": "hourly"},
                {
                    "total_entries": 4,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "success", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-03T00:00:00Z", "state": "success", "count": 1},
                        {"date": "2025-01-04T00:00:00Z", "state": "running", "count": 1},
                    ],
                },
            ),
            (
                {
                    "granularity": "hourly",
                    "partition_date_gte": "2025-01-01T00:00:00Z",
                    "partition_date_lte": "2025-01-01T23:59:59Z",
                },
                {
                    "total_entries": 1,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "success", "count": 1},
                    ],
                },
            ),
        ],
    )
    def test_hourly_calendar_partitioned(self, test_client, query_params, result):
        with assert_queries_count(4):
            response = test_client.get(f"/calendar/{self.DAG_NAME}", params=query_params)
        assert response.status_code == 200
        body = response.json()
        assert body == result


class TestCalendarPlannedRunsCap:
    """A high-frequency cron must stop at MAX_PLANNED_RUNS instead of iterating to the year boundary."""

    DAG_NAME = "test_minutely_dag"

    @pytest.fixture(autouse=True)
    @provide_session
    def setup_dag_runs(self, dag_maker, *, session: Session = NEW_SESSION) -> None:
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            self.DAG_NAME,
            schedule="* * * * *",
            start_date=datetime(2025, 6, 1),
            catchup=False,
            serialized=True,
            session=session,
        ):
            EmptyOperator(task_id="test_task1")
        dag_maker.create_dagrun(
            run_id="run_1",
            state=DagRunState.SUCCESS,
            logical_date=datetime(2025, 6, 1),
        )
        dag_maker.sync_dagbag_to_db()
        session.commit()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()

    def test_planned_runs_capped_for_high_frequency_cron(self, test_client):
        response = test_client.get(f"/calendar/{self.DAG_NAME}")
        assert response.status_code == 200
        planned = [r for r in response.json()["dag_runs"] if r["state"] == "planned"]
        assert sum(r["count"] for r in planned) == CalendarService.MAX_PLANNED_RUNS


_CALLBACK_PATH = "tests.unit.api_fastapi.core_api.routes.ui.test_calendar._noop_callback"


async def _noop_callback(**kwargs):
    """No-op callback used to satisfy Deadline creation in tests."""


def _cb() -> AsyncCallback:
    return AsyncCallback(_CALLBACK_PATH)


class TestCalendarDeadlines:
    """Tests for the GET /calendar/{dag_id}/deadlines endpoint."""

    DAG_NAME = "test_deadlines_calendar_dag"

    @pytest.fixture(autouse=True)
    def setup_deadlines(self, dag_maker, session) -> None:
        clear_db_deadline()
        clear_db_runs()
        clear_db_dags()

        with dag_maker(self.DAG_NAME, serialized=True, session=session):
            EmptyOperator(task_id="task")

        run1 = dag_maker.create_dagrun(
            run_id="run_active",
            state=DagRunState.SUCCESS,
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.datetime(2025, 1, 1),
            triggered_by=DagRunTriggeredByType.TEST,
        )
        session.add(
            Deadline(
                deadline_time=timezone.datetime(2025, 1, 1, 12, 0, 0),
                callback=_cb(),
                dagrun_id=run1.id,
                deadline_alert_id=None,
            )
        )

        run2 = dag_maker.create_dagrun(
            run_id="run_missed",
            state=DagRunState.SUCCESS,
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.datetime(2025, 1, 2),
            triggered_by=DagRunTriggeredByType.TEST,
        )
        missed_dl = Deadline(
            deadline_time=timezone.datetime(2025, 1, 2, 8, 0, 0),
            callback=_cb(),
            dagrun_id=run2.id,
            deadline_alert_id=None,
        )
        missed_dl.missed = True
        session.add(missed_dl)

        run3 = dag_maker.create_dagrun(
            run_id="run_multi",
            state=DagRunState.RUNNING,
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.datetime(2025, 1, 3),
            triggered_by=DagRunTriggeredByType.TEST,
        )
        for hour in (6, 18):
            session.add(
                Deadline(
                    deadline_time=timezone.datetime(2025, 1, 1, hour, 0, 0),
                    callback=_cb(),
                    dagrun_id=run3.id,
                    deadline_alert_id=None,
                )
            )

        run4 = dag_maker.create_dagrun(
            run_id="run_missed_jan1",
            state=DagRunState.FAILED,
            run_type=DagRunType.SCHEDULED,
            logical_date=timezone.datetime(2025, 1, 4),
            triggered_by=DagRunTriggeredByType.TEST,
        )
        missed_dl_jan1 = Deadline(
            deadline_time=timezone.datetime(2025, 1, 1, 20, 0, 0),
            callback=_cb(),
            dagrun_id=run4.id,
            deadline_alert_id=None,
        )
        missed_dl_jan1.missed = True
        session.add(missed_dl_jan1)

        dag_maker.sync_dagbag_to_db()
        session.commit()

    def teardown_method(self) -> None:
        clear_db_deadline()
        clear_db_runs()
        clear_db_dags()

    @pytest.mark.parametrize(
        ("query_params", "expected"),
        [
            (
                {},
                {
                    "total_entries": 3,
                    "deadlines": [
                        {"date": "2025-01-01T00:00:00Z", "missed": False, "count": 3},
                        {"date": "2025-01-01T00:00:00Z", "missed": True, "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "missed": True, "count": 1},
                    ],
                },
            ),
            (
                {
                    "deadline_time_gte": "2025-01-01T00:00:00Z",
                    "deadline_time_lte": "2025-01-01T23:59:59Z",
                },
                {
                    "total_entries": 2,
                    "deadlines": [
                        {"date": "2025-01-01T00:00:00Z", "missed": False, "count": 3},
                        {"date": "2025-01-01T00:00:00Z", "missed": True, "count": 1},
                    ],
                },
            ),
            (
                {
                    "deadline_time_gte": "2025-01-02T00:00:00Z",
                    "deadline_time_lte": "2025-01-02T23:59:59Z",
                },
                {
                    "total_entries": 1,
                    "deadlines": [
                        {"date": "2025-01-02T00:00:00Z", "missed": True, "count": 1},
                    ],
                },
            ),
            (
                {"deadline_time_gte": "2025-06-01T00:00:00Z"},
                {
                    "total_entries": 0,
                    "deadlines": [],
                },
            ),
        ],
    )
    def test_daily_deadlines(self, test_client, query_params, expected):
        response = test_client.get(f"/calendar/{self.DAG_NAME}/deadlines", params=query_params)
        assert response.status_code == 200
        assert response.json() == expected

    @pytest.mark.parametrize(
        ("query_params", "expected"),
        [
            (
                {"granularity": "hourly"},
                {
                    "total_entries": 5,
                    "deadlines": [
                        {"date": "2025-01-01T06:00:00Z", "missed": False, "count": 1},
                        {"date": "2025-01-01T12:00:00Z", "missed": False, "count": 1},
                        {"date": "2025-01-01T18:00:00Z", "missed": False, "count": 1},
                        {"date": "2025-01-01T20:00:00Z", "missed": True, "count": 1},
                        {"date": "2025-01-02T08:00:00Z", "missed": True, "count": 1},
                    ],
                },
            ),
            (
                {
                    "granularity": "hourly",
                    "deadline_time_gte": "2025-01-01T10:00:00Z",
                    "deadline_time_lte": "2025-01-01T20:00:00Z",
                },
                {
                    "total_entries": 3,
                    "deadlines": [
                        {"date": "2025-01-01T12:00:00Z", "missed": False, "count": 1},
                        {"date": "2025-01-01T18:00:00Z", "missed": False, "count": 1},
                        {"date": "2025-01-01T20:00:00Z", "missed": True, "count": 1},
                    ],
                },
            ),
        ],
    )
    def test_hourly_deadlines(self, test_client, query_params, expected):
        response = test_client.get(f"/calendar/{self.DAG_NAME}/deadlines", params=query_params)
        assert response.status_code == 200
        assert response.json() == expected

    def test_unknown_dag_returns_empty(self, test_client):
        response = test_client.get("/calendar/nonexistent_dag/deadlines")
        assert response.status_code == 200
        body = response.json()
        assert body == {"total_entries": 0, "deadlines": []}
