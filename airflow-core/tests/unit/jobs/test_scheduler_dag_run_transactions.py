#
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

from datetime import timedelta
from unittest import mock

import pytest
from sqlalchemy import delete, func, inspect, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session
from tenacity import Retrying, retry_if_exception_type, stop_after_attempt, wait_none

from airflow._shared.timezones import timezone
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks
from airflow.utils.state import DagRunState

from tests_common.test_utils.mock_executor import MockExecutor

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

DECISION_DATE = timezone.datetime(2026, 9, 9)


@pytest.fixture
def dag_runs(dag_maker, session):
    runs = []
    for index in range(3):
        with dag_maker(f"transaction_dag_{index}", session=session, tags=["env:test"]):
            EmptyOperator(task_id="task")
        runs.append(dag_maker.create_dagrun(state=DagRunState.RUNNING))
    session.commit()
    return runs


@pytest.fixture
def runner():
    return SchedulerJobRunner(job=Job(), executors=[MockExecutor(do_update=False)])


@pytest.fixture
def immediate_db_retries():
    def get_retries(**kwargs):
        return Retrying(
            retry=retry_if_exception_type(DBAPIError),
            stop=stop_after_attempt(2),
            wait=wait_none(),
            reraise=True,
        )

    with mock.patch("airflow.utils.retries.run_with_db_retries", autospec=True, side_effect=get_retries):
        yield


@pytest.mark.parametrize("eager_tags", [False, True])
@mock.patch.object(SchedulerJobRunner, "_schedule_dag_run", autospec=True)
def test_schedule_locks_only_current_dag_run(schedule, runner, dag_runs, session, monkeypatch, eager_tags):
    if session.bind.dialect.name == "sqlite":
        pytest.skip("SQLite does not support row locks")
    run_ids = {run.id for run in dag_runs}
    monkeypatch.setattr(runner, "_dag_tags_in_metrics", eager_tags)
    candidates = list(
        DagRun.get_running_dag_runs_to_examine(
            session=session, eagerly_load_dag_tags=eager_tags, lock_rows=False
        )
    )
    assert {run.id for run in candidates} == run_ids

    def get_available_ids():
        with Session(bind=session.get_bind()) as other:
            return set(
                other.scalars(
                    with_row_locks(
                        select(DagRun.id).where(DagRun.id.in_(run_ids)),
                        session=other,
                        of=DagRun,
                        skip_locked=True,
                    )
                )
            )

    assert get_available_ids() == run_ids

    def schedule_run(self, run, *, session):
        assert get_available_ids() == run_ids - {run.id}
        if eager_tags:
            assert "tags" not in inspect(run.dag_model).unloaded
        run.last_scheduling_decision = DECISION_DATE

    schedule.side_effect = schedule_run
    completed = []
    for run, callback in runner._schedule_all_dag_runs(candidates, session=session):
        completed.append(run.id)
        assert callback is None
        assert get_available_ids() == run_ids
    assert set(completed) == run_ids


@mock.patch.object(SchedulerJobRunner, "_schedule_dag_run", autospec=True)
def test_schedule_skips_run_claimed_by_another_scheduler(schedule, runner, dag_runs, session):
    if session.bind.dialect.name == "sqlite":
        pytest.skip("SQLite does not support row locks")
    run_id = dag_runs[0].id
    with Session(bind=session.get_bind()) as other:
        other.scalar(with_row_locks(select(DagRun.id).where(DagRun.id == run_id), session=other, of=DagRun))
        assert list(runner._schedule_all_dag_runs(dag_runs[:1], session=session)) == []
    schedule.assert_not_called()


@pytest.mark.parametrize("change", ["paused", "stale", "finished", "scheduled", "deleted", "future"])
@mock.patch.object(SchedulerJobRunner, "_schedule_dag_run", autospec=True)
def test_schedule_revalidates_candidate(schedule, runner, dag_runs, session, change):
    run = dag_runs[0]
    with Session(bind=session.get_bind()) as other:
        if change in {"paused", "stale"}:
            other.execute(
                update(DagModel).where(DagModel.dag_id == run.dag_id).values({f"is_{change}": True})
            )
        elif change == "deleted":
            other.execute(delete(DagRun).where(DagRun.id == run.id))
        elif change == "future":
            other.execute(
                update(DagRun)
                .where(DagRun.id == run.id)
                .values(run_after=other.scalar(select(func.now(type_=UtcDateTime))) + timedelta(days=1))
            )
        else:
            values = {
                "finished": {"state": DagRunState.SUCCESS},
                "scheduled": {"last_scheduling_decision": DECISION_DATE},
            }[change]
            other.execute(update(DagRun).where(DagRun.id == run.id).values(**values))
        other.commit()

    assert list(runner._schedule_all_dag_runs([run], session=session)) == []
    schedule.assert_not_called()


@mock.patch.object(SchedulerJobRunner, "_schedule_dag_run", autospec=True)
def test_schedule_refreshes_candidate_before_scheduling(schedule, runner, dag_runs, session):
    run = dag_runs[0]
    max_active_tasks = run.dag_model.max_active_tasks + 1
    with Session(bind=session.get_bind()) as other:
        other.execute(update(DagRun).where(DagRun.id == run.id).values(conf={"updated": True}))
        other.execute(
            update(DagModel).where(DagModel.dag_id == run.dag_id).values(max_active_tasks=max_active_tasks)
        )
        other.commit()

    def schedule_run(self, run, *, session):
        assert run.conf == {"updated": True}
        assert "dag_model" not in inspect(run).unloaded
        assert run.dag_model.max_active_tasks == max_active_tasks

    schedule.side_effect = schedule_run
    assert len(list(runner._schedule_all_dag_runs([run], session=session))) == 1
    schedule.assert_called_once()


@pytest.mark.parametrize("failures", [1, 2])
@mock.patch.object(SchedulerJobRunner, "_schedule_dag_run", autospec=True)
def test_retry_does_not_repeat_committed_runs(
    schedule, runner, dag_runs, session, immediate_db_retries, failures
):
    first_id, second_id = (run.id for run in dag_runs[:2])
    attempts = []
    error = DBAPIError("update dag_run", None, Exception("retry transaction"))

    def schedule_run(self, run, *, session):
        attempts.append(run.id)
        run.last_scheduling_decision = DECISION_DATE
        session.flush()
        if run.id == second_id and attempts.count(second_id) <= failures:
            raise error

    schedule.side_effect = schedule_run
    results = runner._schedule_all_dag_runs(dag_runs[:2], session=session)
    first, callback = next(results)
    assert first.id == first_id
    assert callback is None
    with Session(bind=session.get_bind()) as other:
        assert other.get(DagRun, first_id).last_scheduling_decision == DECISION_DATE

    if failures == 1:
        assert [run.id for run, _ in results] == [second_id]
    else:
        with pytest.raises(DBAPIError):
            next(results)
        with Session(bind=session.get_bind()) as other:
            assert other.get(DagRun, second_id).last_scheduling_decision is None
    assert attempts == [first_id, second_id, second_id]


@mock.patch.object(SchedulerJobRunner, "_schedule_dag_run", autospec=True)
def test_unexpected_error_rolls_back_only_failed_run(schedule, runner, dag_runs, session):
    failed_id = dag_runs[0].id

    def schedule_run(self, run, *, session):
        run.state = DagRunState.SUCCESS
        session.flush()
        if run.id == failed_id:
            raise ValueError("invalid scheduling state")

    schedule.side_effect = schedule_run
    assert [run.id for run, _ in runner._schedule_all_dag_runs(dag_runs, session=session)] == [
        run.id for run in dag_runs[1:]
    ]
    with Session(bind=session.get_bind()) as other:
        assert other.get(DagRun, failed_id).state == DagRunState.RUNNING
        assert all(other.get(DagRun, run.id).state == DagRunState.SUCCESS for run in dag_runs[1:])


@mock.patch.object(SchedulerJobRunner, "_send_dag_callbacks_to_processor", autospec=True)
@mock.patch.object(SchedulerJobRunner, "_schedule_dag_run", autospec=True)
def test_committed_callback_is_sent_before_later_run_fails(
    schedule, send_callback, runner, dag_runs, session, immediate_db_retries, monkeypatch
):
    monkeypatch.setattr(runner, "_scheduler_use_job_schedule", False)
    callback = mock.Mock(spec=DagCallbackRequest)
    scheduled_ids = []

    def schedule_run(self, run, *, session):
        scheduled_ids.append(run.id)
        if len(scheduled_ids) == 1:
            run.last_scheduling_decision = DECISION_DATE
            return callback
        send_callback.assert_called_once()
        assert send_callback.call_args.args[2] is callback
        raise DBAPIError("update dag_run", None, Exception("retry transaction"))

    schedule.side_effect = schedule_run
    with pytest.raises(DBAPIError):
        runner._do_scheduling(session)
    assert len(scheduled_ids) == 3
    assert scheduled_ids[0] != scheduled_ids[1] == scheduled_ids[2]
