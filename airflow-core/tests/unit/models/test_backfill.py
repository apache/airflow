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

from airflow._shared.timezones import timezone
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.models.backfill import (
    AlreadyRunningBackfill,
    Backfill,
    BackfillDagRun,
    BackfillDagRunExceptionReason,
    InvalidBackfillDirection,
    InvalidReprocessBehavior,
    ReprocessBehavior,
    _create_backfill,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.ti_deps.dep_context import DepContext
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

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
        cm = pytest.raises(
            InvalidBackfillDirection,
            match="Backfill cannot be run in reverse when the DAG has tasks where depends_on_past=True.",
        )
    b = None
    with cm:
        b = _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2021-01-01"),
            to_date=pendulum.parse("2021-01-05"),
            max_active_runs=2,
            reverse=True,
            triggering_user_name="pytest",
            dag_run_conf={},
        )
    if dep_on_past:
        assert b is None
    else:
        assert b is not None


@pytest.mark.parametrize("reverse", [True, False])
@pytest.mark.parametrize("existing", [["2021-01-02", "2021-01-03"], []])
def test_create_backfill_simple(reverse, existing, dag_maker, session):
    """
    Verify simple case behavior.

    This test verifies that runs in the range are created according
    to schedule intervals, and the sort ordinal is correct. Also verifies
    that dag runs are created in the queued state.
    """
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)

    for date in existing:
        dag_maker.create_dagrun(
            run_id=f"scheduled_{date}",
            logical_date=timezone.parse(date),
            session=session,
        )
        session.commit()

    expected_run_conf = {"param1": "valABC"}
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=2,
        reverse=reverse,
        triggering_user_name="pytest",
        dag_run_conf=expected_run_conf,
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    total_dates = ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"]
    backfill_dates = [str(x.logical_date.date()) for x in dag_runs]
    expected_dates = [x for x in total_dates if x not in existing]
    if reverse:
        expected_dates = list(reversed(expected_dates))

    for date in existing:
        bdr = session.scalar(
            select(BackfillDagRun).where(
                BackfillDagRun.backfill_id == b.id,
                BackfillDagRun.logical_date == timezone.parse(date),
            )
        )
        assert bdr.exception_reason == BackfillDagRunExceptionReason.IN_FLIGHT
    assert backfill_dates == expected_dates
    assert all(x.state == DagRunState.QUEUED for x in dag_runs)
    assert all(x.conf == expected_run_conf for x in dag_runs)


@pytest.mark.parametrize("run_on_latest_version", [True, False])
def test_create_backfill_clear_existing_bundle_version(dag_maker, session, run_on_latest_version):
    """
    Verify that when backfill clears an existing dag run, bundle version is cleared.
    """
    # two that will be reprocessed, and an old one not to be processed by backfill
    existing = ["1985-01-01", "2021-01-02", "2021-01-03"]
    run_ids = {d: f"scheduled_{d}" for d in existing}
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)

    dag_model = session.scalar(select(DagModel).where(DagModel.dag_id == dag.dag_id))
    first_bundle_version = "bundle_VclmpcTdXv"
    dag_model.bundle_version = first_bundle_version
    session.commit()
    for date in existing:
        dag_maker.create_dagrun(
            run_id=run_ids[date], logical_date=timezone.parse(date), session=session, state="failed"
        )
        session.commit()

    # update bundle version
    new_bundle_version = "bundle_VclmpcTdXv-2"
    dag_model.bundle_version = new_bundle_version
    session.commit()

    # verify that existing dag runs still have the first bundle version
    dag_runs = list(session.scalars(select(DagRun).where(DagRun.dag_id == dag.dag_id)))
    assert [x.bundle_version for x in dag_runs] == 3 * [first_bundle_version]
    assert [x.state for x in dag_runs] == 3 * ["failed"]
    session.commit()
    _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=10,
        reverse=False,
        dag_run_conf=None,
        triggering_user_name="pytest",
        reprocess_behavior=ReprocessBehavior.FAILED,
        run_on_latest_version=run_on_latest_version,
    )
    session.commit()

    # verify that the old dag run (not included in backfill) still has first bundle version
    # but the latter 5, which are included in the backfill, have the latest bundle version if run_on_latest_version
    # is True, otherwise they have the first bundle version
    dag_runs = sorted(
        session.scalars(
            select(DagRun).where(
                DagRun.dag_id == dag.dag_id,
            ),
        ),
        key=lambda x: x.logical_date,
    )
    if run_on_latest_version:
        expected = [first_bundle_version] + 5 * [new_bundle_version]
    else:
        expected = (
            [first_bundle_version]
            + [new_bundle_version]
            + 2 * [first_bundle_version]
            + 2 * [new_bundle_version]
        )
    assert [x.bundle_version for x in dag_runs] == expected


@pytest.mark.parametrize(
    ("reprocess_behavior", "num_in_b", "exc_reasons"),
    [
        (
            ReprocessBehavior.NONE,
            4,
            {"2021-01-05": "already exists", "2021-01-06": "already exists", "2021-01-07": "in flight"},
        ),
        (
            ReprocessBehavior.FAILED,
            5,
            {"2021-01-05": "already exists", "2021-01-07": "in flight"},
        ),
        (ReprocessBehavior.COMPLETED, 6, {"2021-01-07": "in flight"}),
    ],
)
def test_reprocess_behavior(reprocess_behavior, num_in_b, exc_reasons, dag_maker, session):
    """
    We have two modes whereby when there's an existing run(s) in the range
    of the backfill, we will clear an existing run.
    """

    # introduce runs for a dag different from the test dag
    # so that we can verify that queries won't pick up runs from
    # other dags with same date
    with dag_maker(schedule="@daily", dag_id="noise-dag"):
        PythonOperator(task_id="hi", python_callable=print)
    date = "2021-01-06"
    dr = dag_maker.create_dagrun(
        run_id=f"scheduled_{date}",
        logical_date=timezone.parse(date),
        session=session,
        state="success",
    )
    # should appear more recent than next runs we'll create
    dr.start_date = timezone.parse(date) + timedelta(minutes=2)
    session.commit()

    # now the main part of the test
    # we insert some historical runs with various states and see
    # what the backfill behavior is depending on requested
    # reprocessing behavior
    dag_id = "backfill-test-reprocess-behavior"
    with dag_maker(schedule="@daily", dag_id=dag_id) as dag:
        PythonOperator(task_id="hi", python_callable=print)

    for date, state in [
        ("2021-01-05", "success"),
        ("2021-01-06", "failed"),
        ("2021-01-07", "running"),
    ]:
        dr = dag_maker.create_dagrun(
            run_id=f"scheduled_{date}",
            logical_date=timezone.parse(date),
            session=session,
            state=state,
        )
        # should sort just older than the noise dag with same logical date
        dr.start_date = timezone.parse(date)
        for ti in dr.get_task_instances(session=session):
            ti.state = state
        session.commit()

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-03"),
        to_date=pendulum.parse("2021-01-09"),
        max_active_runs=2,
        reprocess_behavior=reprocess_behavior,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf=None,
    )

    session.expunge_all()
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id, DagRun.dag_id == dag.dag_id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    # these are all the dag runs that are part of this backfill
    dag_runs_in_b = session.scalars(query).all()
    assert len(dag_runs_in_b) == num_in_b

    # verify they all have the right run type
    assert all(x.run_type == DagRunType.BACKFILL_JOB for x in dag_runs_in_b)
    # verify they all have the right triggered by type
    assert all(x.triggered_by == DagRunTriggeredByType.BACKFILL for x in dag_runs_in_b)
    # every run associated with the backfill should have the backfill id
    assert all(x.backfill_id == b.id for x in dag_runs_in_b)

    reasons = session.execute(
        select(BackfillDagRun.logical_date, BackfillDagRun.exception_reason).where(
            BackfillDagRun.backfill_id == b.id, BackfillDagRun.exception_reason.is_not(None)
        )
    ).all()
    actual = dict({str(d.date()): r for d, r in reasons})
    assert actual == exc_reasons
    # all the runs created by the backfill should have state queued
    assert all(x.state == DagRunState.QUEUED for x in dag_runs_in_b)


def test_params_stored_correctly(dag_maker, session):
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=263,
        reverse=False,
        triggering_user_name="pytest",
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
        triggering_user_name="pytest",
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
            triggering_user_name="pytest",
            dag_run_conf={"this": "param"},
        )


def create_next_run(
    *, is_backfill: bool, next_date: datetime, dag_id: str, dag_maker, reprocess=None, session: Session
):
    """Used in test_ignore_first_depends_on_past to create the next run after a failed run."""
    if is_backfill:
        b = _create_backfill(
            dag_id=dag_id,
            from_date=next_date,
            to_date=next_date + timedelta(days=1),
            max_active_runs=2,
            reverse=False,
            dag_run_conf=None,
            triggering_user_name="pytest",
            reprocess_behavior=reprocess,
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
    dr = dag_maker.create_dagrun(logical_date=next_date, run_id="second_run")
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
        PythonOperator(task_id="dep_on_past", python_callable=lambda: print, depends_on_past=True)
    dr = dag_maker.create_dagrun(logical_date=base_date, run_type=first_run_type)
    dr.state = DagRunState.FAILED
    for ti in dr.task_instances:
        ti.state = TaskInstanceState.FAILED
        session.merge(ti)
    session.commit()

    # let's verify all is as expected
    session.expunge_all()
    first_run = session.scalar(select(DagRun).order_by(DagRun.logical_date).limit(1))
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
        reprocess=ReprocessBehavior.FAILED,
    )

    # check that it's immediately after the other dag run
    prior_runs = session.scalars(
        select(DagRun.logical_date).where(DagRun.logical_date < next_run.logical_date)
    ).all()
    assert len(prior_runs) == 1
    assert prior_runs[0] == first_run.logical_date
    assert prior_runs[0] + timedelta(days=days_between) == next_run.logical_date

    # so now the first backfill dag run follows the other one immediately

    ti: TaskInstance = next_run.get_task_instances(session=session)[0]
    ti.task = dag.task_dict[ti.task_id]

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


@pytest.mark.parametrize("behavior", [None, *ReprocessBehavior])
@pytest.mark.parametrize("dep_on_past", [True, False])
def test_depends_on_past_requires_reprocess_failed(dep_on_past, behavior, dag_maker):
    with dag_maker(dag_id="abc123", serialized=True, catchup=True) as dag:
        PythonOperator(
            task_id="dep_on_past",
            python_callable=lambda: print,
            depends_on_past=dep_on_past,
        )
    raises_cm = pytest.raises(
        InvalidReprocessBehavior,
        match="DAG has tasks for which depends_on_past=True. You must set reprocess behavior to reprocess completed or reprocess failed.",
    )
    null_cm = nullcontext()
    cm = null_cm
    if dep_on_past and behavior in (ReprocessBehavior.NONE, None):
        cm = raises_cm
    with cm:
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=timezone.parse("2021-01-01"),
            to_date=timezone.parse("2021-01-10"),
            max_active_runs=5,
            reverse=False,
            dag_run_conf={},
            triggering_user_name="pytest",
            reprocess_behavior=behavior,
        )
