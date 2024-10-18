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

from contextlib import nullcontext
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

import pendulum
import pytest
from sqlalchemy import select

from airflow.models import DagRun, TaskInstance
from airflow.models.backfill import (
    AlreadyRunningBackfill,
    Backfill,
    BackfillDagRun,
    _cancel_backfill,
    _create_backfill,
)
from airflow.operators.python import PythonOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.utils import timezone
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import (
    clear_db_backfills,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


def _clean_db():
    clear_db_backfills()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


@pytest.fixture(autouse=True)
def clean_db():
    _clean_db()
    yield
    _clean_db()


@pytest.mark.parametrize("dep_on_past", [True, False])
def test_reverse_and_depends_on_past_fails(dep_on_past, dag_maker, session):
    with dag_maker() as dag:
        PythonOperator(task_id="hi", python_callable=print, depends_on_past=dep_on_past)
    session.commit()
    cm = nullcontext()
    if dep_on_past:
        cm = pytest.raises(ValueError, match="cannot be run in reverse")
    b = None
    with cm:
        b = _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2021-01-01"),
            to_date=pendulum.parse("2021-01-05"),
            max_active_runs=2,
            reverse=True,
            dag_run_conf={},
        )
    if dep_on_past:
        assert b is None
    else:
        assert b is not None


@pytest.mark.parametrize("reverse", [True, False])
def test_create_backfill_simple(reverse, dag_maker, session):
    """
    Verify simple case behavior.

    This test verifies that runs in the range are created according
    to schedule intervals, and the sort ordinal is correct. Also verifies
    that dag runs are created in the queued state.
    """
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    expected_run_conf = {"param1": "valABC"}
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=2,
        reverse=reverse,
        dag_run_conf=expected_run_conf,
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    dates = [str(x.logical_date.date()) for x in dag_runs]
    expected_dates = ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"]
    if reverse:
        expected_dates = list(reversed(expected_dates))
    assert dates == expected_dates
    assert all(x.state == DagRunState.QUEUED for x in dag_runs)
    assert all(x.conf == expected_run_conf for x in dag_runs)


def test_params_stored_correctly(dag_maker, session):
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=263,
        reverse=False,
        dag_run_conf={"this": "param"},
    )
    session.expunge_all()
    b_stored = session.get(Backfill, b.id)
    assert all(
        (
            b_stored.dag_id == b.dag_id,
            b_stored.from_date == b.from_date,
            b_stored.to_date == b.to_date,
            b_stored.max_active_runs == b.max_active_runs,
            b_stored.dag_run_conf == b.dag_run_conf,
        )
    )


def test_active_dag_run(dag_maker, session):
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()
    b1 = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=10,
        reverse=False,
        dag_run_conf={"this": "param"},
    )
    assert b1 is not None
    with pytest.raises(AlreadyRunningBackfill, match="Another backfill is running for dag"):
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2021-02-01"),
            to_date=pendulum.parse("2021-02-05"),
            max_active_runs=10,
            reverse=False,
            dag_run_conf={"this": "param"},
        )


def test_cancel_backfill(dag_maker, session):
    """
    Queued runs should be marked *failed*.
    Every other dag run should be left alone.
    """
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=timezone.datetime(2021, 1, 1),
        to_date=timezone.datetime(2021, 1, 5),
        max_active_runs=2,
        reverse=False,
        dag_run_conf={},
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    dates = [str(x.logical_date.date()) for x in dag_runs]
    expected_dates = ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"]
    assert dates == expected_dates
    assert all(x.state == DagRunState.QUEUED for x in dag_runs)
    dag_runs[0].state = "running"
    session.commit()
    _cancel_backfill(backfill_id=b.id)
    session.expunge_all()
    dag_runs = session.scalars(query).all()
    states = [x.state for x in dag_runs]
    assert states == ["running", "failed", "failed", "failed", "failed"]


def create_next_run(*, is_backfill: bool, next_date: datetime, dag_id: str, dag_maker, session: Session):
    """Used in test_ignore_first_depends_on_past to create the next run after a failed run."""
    if is_backfill:
        b = _create_backfill(
            dag_id=dag_id,
            from_date=next_date,
            to_date=next_date + timedelta(days=1),
            max_active_runs=2,
            reverse=False,
            dag_run_conf=None,
        )
        assert b
        # and grab the first dag run from this backfill run
        next_run = session.scalar(
            select(DagRun)
            .join(BackfillDagRun.dag_run)
            .where(DagRun.backfill_id == b.id)
            .order_by(BackfillDagRun.sort_ordinal)
            .limit(1)
        )
        return next_run
    else:
        dr = dag_maker.create_dagrun(execution_date=next_date, run_id="second_run")
        return dr


@pytest.mark.parametrize("is_backfill", [True, False])
@pytest.mark.parametrize("catchup", [True, False])
@pytest.mark.parametrize("days_between", [1, 10])
@pytest.mark.parametrize("first_run_type", [DagRunType.SCHEDULED, DagRunType.MANUAL])
def test_ignore_first_depends_on_past(first_run_type, days_between, catchup, is_backfill, dag_maker, session):
    """When creating a backfill, should ignore depends_on_past task attr for the first run in a backfill."""
    base_date = timezone.datetime(2021, 1, 1)
    from_date = base_date + timedelta(days=days_between)
    with dag_maker(dag_id="abc123", serialized=True, catchup=catchup) as dag:
        op = PythonOperator(task_id="dep_on_past", python_callable=lambda: print, depends_on_past=True)
    dr = dag_maker.create_dagrun(execution_date=base_date, run_type=first_run_type)
    dr.state = DagRunState.FAILED
    for ti in dr.task_instances:
        ti.state = TaskInstanceState.FAILED
        session.merge(ti)
    session.commit()

    # let's verify all is as expected
    session.expunge_all()
    first_run = session.scalar(select(DagRun).order_by(DagRun.execution_date).limit(1))
    assert first_run.state == DagRunState.FAILED
    tis = first_run.get_task_instances(session=session)
    assert len(tis) > 0
    assert all(x.state == TaskInstanceState.FAILED for x in tis)

    next_run = create_next_run(
        is_backfill=is_backfill,
        next_date=from_date,
        dag_id=dag.dag_id,
        dag_maker=dag_maker,
        session=session,
    )

    # check that it's immediately after the other dag run
    prior_runs = session.scalars(
        select(DagRun.execution_date).where(DagRun.execution_date < next_run.execution_date)
    ).all()
    assert len(prior_runs) == 1
    assert prior_runs[0] == first_run.execution_date
    assert prior_runs[0] + timedelta(days=days_between) == next_run.execution_date

    # so now the first backfill dag run follows the other one immediately

    ti: TaskInstance = next_run.get_task_instances(session=session)[0]
    ti.task = op

    dep_statuses = ti.get_failed_dep_statuses(dep_context=DepContext(), session=session)
    if is_backfill:
        expect_pass = True
    elif catchup and first_run_type is DagRunType.MANUAL:
        # this one is a bit weird
        # if not catchup and first run is manual then it will *not* pass
        # this is because if it is not catchup it looks at the absolute prior run
        # but if it is catchup it looks at only the scheduled prior run
        # could be a bug ¯\_(ツ)_/¯
        expect_pass = True
    else:
        expect_pass = False
    if expect_pass:
        with pytest.raises(StopIteration):
            next(dep_statuses)
        assert ti.are_dependencies_met(session=session) is True
    else:
        status = next(dep_statuses)
        assert status.reason == (
            "depends_on_past is true for this task, "
            "but 1 previous task instance(s) are not in a successful state."
        )
        assert ti.are_dependencies_met(session=session) is False
