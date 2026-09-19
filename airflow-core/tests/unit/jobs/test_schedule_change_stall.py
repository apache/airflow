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
"""
Regression test for https://github.com/apache/airflow/issues/66754: changing a Dag's
cron to a coarser one (e.g. hourly to daily) must not permanently stall DagRun creation.
Calls the DagRun-creation and TI-scheduling code paths directly
(``SchedulerJobRunner._create_dag_runs``, ``DagRun.update_state``,
``DagRun.schedule_tis``) against a real Dag re-sync (``SerializedDAG.bulk_write_to_db``
via ``dag_maker``) and real ``DagRun``/``TaskInstance`` rows, not just
``CronDataIntervalTimetable`` in isolation, to prove the stall reproduces at the
database level and that the guard in ``timetables/interval.py`` resolves it.
"""

from __future__ import annotations

import datetime

import pytest
import time_machine
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models import DagRun
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.state import DagRunState, TaskInstanceState

from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_backfills,
    clear_db_callbacks,
    clear_db_dags,
    clear_db_deadline,
    clear_db_import_errors,
    clear_db_jobs,
    clear_db_pools,
    clear_db_runs,
    clear_db_triggers,
)
from tests_common.test_utils.mock_executor import MockExecutor

pytestmark = pytest.mark.db_test

DAG_ID = "schedule_change_stall_coarser_cron"
START_DATE = timezone.datetime(2026, 5, 4)


def _clean_db():
    clear_db_dags()
    clear_db_runs()
    clear_db_backfills()
    clear_db_pools()
    clear_db_import_errors()
    clear_db_jobs()
    clear_db_assets()
    clear_db_deadline()
    clear_db_callbacks()
    clear_db_triggers()


@pytest.fixture(autouse=True)
def clean_db():
    _clean_db()
    yield
    _clean_db()


def _make_runner():
    return SchedulerJobRunner(job=Job(), executors=[MockExecutor()])


@time_machine.travel(START_DATE, tick=False)
def test_coarser_schedule_change_does_not_stall_dagrun_creation(dag_maker, session):
    # 1. First Dag scheduling: hourly, catchup=True.
    with dag_maker(
        dag_id=DAG_ID,
        schedule="0 * * * *",
        start_date=START_DATE,
        catchup=True,
        max_active_runs=1,
        session=session,
    ):
        BashOperator(task_id="do_something", bash_command="true")

    dag_model = dag_maker.dag_model
    assert dag_model.next_dagrun == START_DATE
    assert dag_model.next_dagrun_create_after == START_DATE + datetime.timedelta(hours=1)

    runner = _make_runner()

    # Tick once the hourly run becomes due.
    with time_machine.travel(START_DATE + datetime.timedelta(hours=1), tick=False):
        runner._create_dag_runs([dag_model], session)
        session.flush()

    runs = session.scalars(select(DagRun).where(DagRun.dag_id == DAG_ID)).all()
    assert len(runs) == 1, f"expected exactly one DagRun after the first tick, got {runs}"
    hourly_run = runs[0]
    assert hourly_run.logical_date == START_DATE
    assert hourly_run.data_interval_end == START_DATE + datetime.timedelta(hours=1)

    # 2. The first TI of the first Dag scheduling already ran.
    ti = hourly_run.get_task_instances(session=session)[0]
    ti.state = TaskInstanceState.SUCCESS
    session.merge(ti)
    session.flush()
    hourly_run.dag = dag_maker.serialized_dag
    hourly_run.update_state(session=session)
    session.flush()
    assert hourly_run.state == DagRunState.SUCCESS

    # 3. Change the same Dag's schedule: hourly to daily, drop end_date.
    with dag_maker(
        dag_id=DAG_ID,
        schedule="0 0 * * *",
        start_date=START_DATE,
        catchup=True,
        max_active_runs=1,
        session=session,
    ):
        BashOperator(task_id="do_something", bash_command="true")

    session.expire_all()
    dag_model = dag_maker.dag_model

    # 4. Advance past the next expected (daily) boundary and tick again.
    with time_machine.travel(START_DATE + datetime.timedelta(days=1, minutes=5), tick=False):
        runner._create_dag_runs([dag_model], session)
        session.flush()

    session.expire_all()
    runs = session.scalars(select(DagRun).where(DagRun.dag_id == DAG_ID).order_by(DagRun.logical_date)).all()

    expected_logical_date = START_DATE + datetime.timedelta(days=1)
    new_runs = [r for r in runs if r.logical_date == expected_logical_date]
    assert len(new_runs) == 1, (
        f"expected a new DagRun at logical_date={expected_logical_date} (the next day's slot); "
        f"instead the scheduler produced these runs: "
        f"{[(r.run_id, r.logical_date, r.state) for r in runs]}. "
        "This is the 'run already exists; skipping dagrun creation' stall: the scheduler is stuck "
        "proposing the pre-existing hourly run's logical_date forever instead of advancing."
    )

    new_run = new_runs[0]
    new_ti = new_run.get_task_instances(session=session)[0]
    assert new_ti.state != TaskInstanceState.REMOVED

    # Drive scheduling decisions for the new run and confirm its TI reaches SCHEDULED,
    # i.e. it is not stuck in a not-yet-scheduled/None/queued limbo.
    new_run.dag = dag_maker.serialized_dag
    schedulable_tis, _ = new_run.update_state(session=session)
    new_run.schedule_tis(schedulable_tis, session=session)
    session.flush()
    session.expire_all()
    new_ti = new_run.get_task_instances(session=session)[0]
    assert new_ti.state == TaskInstanceState.SCHEDULED, (
        f"new run's TaskInstance should reach SCHEDULED, got {new_ti.state!r}, it is stuck."
    )
