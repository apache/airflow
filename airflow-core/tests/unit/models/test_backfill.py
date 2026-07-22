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
from unittest import mock

import pendulum
import pytest
from sqlalchemy import func, select

from airflow._shared.timezones import timezone
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.models.backfill import (
    AlreadyRunningBackfill,
    Backfill,
    BackfillDagRun,
    BackfillDagRunExceptionReason,
    DagNonPeriodicScheduleException,
    InvalidBackfillConf,
    InvalidBackfillDateRange,
    InvalidBackfillDirection,
    InvalidReprocessBehavior,
    NoBackfillRunsToCreate,
    ReprocessBehavior,
    _create_backfill,
    _do_dry_run,
    _get_latest_dag_run_row_query,
    _handle_clear_run,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset, CronPartitionTimetable, PartitionedAssetTimetable
from airflow.ti_deps.dep_context import DepContext
from airflow.timetables.base import DagRunInfo, DataInterval
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.strings import get_random_string
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
            match="Backfill cannot be run in reverse when the Dag has tasks where depends_on_past=True.",
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


@pytest.mark.parametrize("reverse", [True, False])
@pytest.mark.parametrize(
    "existing",
    [
        ["2026-02-23T00:00:00", "2026-02-24T00:00:00"],
        [],
    ],
)
@pytest.mark.parametrize(
    "start_date",
    [
        None,
        pendulum.parse("2026-02-20"),
    ],
)
def test_create_backfill_partitioned(reverse, existing, start_date, dag_maker, session):
    """Verify partitioned backfill creates new runs per partition."""
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)

    # Map each partition_key label to the partition_date (UTC instant) the timetable computes.
    # Bounds are passed tz-aware (timetable timezone) so they name the intended local partitions:
    # partition_date is now honored as an instant, not rounded to a calendar day.
    partition_date_by_key = {
        info.partition_key: info.partition_date
        for info in dag.iter_dagrun_infos_between(
            pendulum.datetime(2026, 2, 14, tz="Asia/Taipei"),
            pendulum.datetime(2026, 2, 25, tz="Asia/Taipei"),
        )
    }
    # Existing runs stand in for historical scheduled runs: they carry partition_date, which is
    # what deduplication keys on (see _get_latest_dag_run_row_query).
    for date in existing:
        dag_maker.create_dagrun(
            start_date=start_date,
            run_id=f"scheduled_{date}",
            logical_date=None,
            partition_key=date,
            partition_date=partition_date_by_key[date],
            session=session,
        )
        session.commit()

    expected_run_conf = {"param1": "valABC"}
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 15, tz="Asia/Taipei"),
        to_date=pendulum.datetime(2026, 2, 24, tz="Asia/Taipei"),
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
    # partition_key is a timetable-local label (e.g. "2026-02-15T00:00:00"), so its own date
    # is the partition's calendar date. to_date is inclusive, so the window is 2026-02-15..24;
    # the seeded existing partitions (reprocess_behavior=None) are skipped.
    partition_keys = [str(datetime.fromisoformat(x.partition_key).date()) for x in dag_runs]
    expected_dates = [f"2026-02-{d}" for d in range(15, 23 if existing else 25)]
    if reverse:
        expected_dates = list(reversed(expected_dates))

    assert partition_keys == expected_dates
    assert all(x.state == DagRunState.QUEUED for x in dag_runs)
    assert all(x.conf == expected_run_conf for x in dag_runs)
    # Calendar view filters partitioned Dags by partition_date, so the backfill
    # path must populate it alongside partition_key. Verify that backfill copies
    # info.partition_date faithfully — i.e. the stored value matches what the
    # timetable computed for each partition_key.
    # Asia/Taipei partitions have partition_date (UTC instant) one day earlier than the
    # calendar label: label "2026-02-15" → partition_date 2026-02-14T16:00:00Z.
    assert [x.partition_date for x in dag_runs] == [partition_date_by_key[x.partition_key] for x in dag_runs]


def test_create_backfill_partitioned_key_uses_timetable_timezone(dag_maker, session):
    """partition_key is labelled in the timetable timezone, not UTC.

    An Asia/Taipei midnight partition is the 16:00Z instant of the previous UTC day, so its
    key must read as the local date ("2026-02-18T00:00:00"), not "2026-02-17T16:00:00", while
    the stored partition_date keeps the underlying UTC instant.
    """
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        to_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        max_active_runs=1,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
    )
    query = select(DagRun).join(BackfillDagRun.dag_run).where(BackfillDagRun.backfill_id == b.id)
    dag_runs = session.scalars(query).all()

    assert [x.partition_key for x in dag_runs] == ["2026-02-18T00:00:00"]
    assert [x.partition_date for x in dag_runs] == [pendulum.datetime(2026, 2, 17, 16, tz="UTC")]


def test_backfill_partitioned_does_not_duplicate_legacy_utc_keyed_run(dag_maker, session):
    """A backfill must not duplicate a run that was keyed with the old UTC-instant label.

    CronPartitionTimetable shipped in 3.2.0/3.2.1 formatting partition_key off the UTC
    instant; it now labels the key in the timetable timezone. Deduplication keys on
    partition_date (the UTC instant), so a backfill over a window that already has a run
    skips it regardless of the key-string format — no duplicate partition run is created.
    """
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)

    # Asia/Taipei midnight 2026-02-18 is the UTC instant 2026-02-17T16:00:00Z. The historical
    # run carries the *old* UTC-instant key, not today's local label.
    partition_date = pendulum.datetime(2026, 2, 17, 16, tz="UTC")
    dag_maker.create_dagrun(
        run_id="scheduled_legacy",
        logical_date=None,
        run_type="scheduled",
        state=DagRunState.SUCCESS,
        partition_key="2026-02-17T16:00:00",
        partition_date=partition_date,
        session=session,
    )
    session.commit()

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        to_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        max_active_runs=1,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
    )

    # No new run created for this partition: still exactly one DagRun at that partition_date.
    runs_at_partition = session.scalars(
        select(DagRun).where(DagRun.dag_id == dag.dag_id, DagRun.partition_date == partition_date)
    ).all()
    assert [x.run_id for x in runs_at_partition] == ["scheduled_legacy"]
    # The backfill recorded the partition as already existing instead of creating a duplicate.
    bdr = session.scalars(select(BackfillDagRun).where(BackfillDagRun.backfill_id == b.id)).all()
    assert len(bdr) == 1
    assert bdr[0].dag_run_id is None
    assert bdr[0].exception_reason == BackfillDagRunExceptionReason.ALREADY_EXISTS


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


@pytest.mark.parametrize(
    "reprocess_behavior",
    [ReprocessBehavior.FAILED, ReprocessBehavior.COMPLETED],
)
def test_backfill_conf_overrides_existing_dag_run(reprocess_behavior, dag_maker, session):
    """When reprocessing an existing DagRun, the backfill's dag_run_conf should override the existing conf."""
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)

    existing_date = "2021-01-03"
    dag_maker.create_dagrun(
        run_id=f"scheduled_{existing_date}",
        logical_date=timezone.parse(existing_date),
        session=session,
        state=DagRunState.FAILED,
        conf={"old_key": "old_value"},
    )
    session.commit()

    new_conf = {"new_key": "new_value"}
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=10,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf=new_conf,
        reprocess_behavior=reprocess_behavior,
    )

    session.expunge_all()

    # The reprocessed existing run should have the new conf
    reprocessed_dr = session.scalar(
        select(DagRun).where(
            DagRun.dag_id == dag.dag_id,
            DagRun.logical_date == timezone.parse(existing_date),
        )
    )
    assert reprocessed_dr.conf == new_conf

    # New runs created by the backfill should also have the new conf
    all_backfill_runs = session.scalars(
        select(DagRun).join(BackfillDagRun.dag_run).where(BackfillDagRun.backfill_id == b.id)
    ).all()
    assert all(x.conf == new_conf for x in all_backfill_runs)


def test_backfill_none_conf_preserves_existing_dag_run_conf(dag_maker, session):
    """When backfill dag_run_conf is None, existing DagRun conf should be preserved."""
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)

    existing_date = "2021-01-03"
    original_conf = {"keep": "this"}
    dag_maker.create_dagrun(
        run_id=f"scheduled_{existing_date}",
        logical_date=timezone.parse(existing_date),
        session=session,
        state=DagRunState.FAILED,
        conf=original_conf,
    )
    session.commit()

    _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=10,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf=None,
        reprocess_behavior=ReprocessBehavior.FAILED,
    )

    session.expunge_all()

    reprocessed_dr = session.scalar(
        select(DagRun).where(
            DagRun.dag_id == dag.dag_id,
            DagRun.logical_date == timezone.parse(existing_date),
        )
    )
    assert reprocessed_dr.conf == original_conf


def test_backfill_empty_conf_overrides_existing_dag_run(dag_maker, session):
    """When backfill dag_run_conf is {}, existing DagRun conf should be updated to {}."""
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)

    existing_date = "2021-01-03"
    dag_maker.create_dagrun(
        run_id=f"scheduled_{existing_date}",
        logical_date=timezone.parse(existing_date),
        session=session,
        state=DagRunState.FAILED,
        conf={"old_key": "old_value"},
    )
    session.commit()

    _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=10,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.FAILED,
    )

    session.expunge_all()

    reprocessed_dr = session.scalar(
        select(DagRun).where(
            DagRun.dag_id == dag.dag_id,
            DagRun.logical_date == timezone.parse(existing_date),
        )
    )
    assert reprocessed_dr.conf == {}


def test_backfill_rejects_invalid_conf(dag_maker, session):
    """Backfill with invalid conf should fail validation before creating any runs."""
    from airflow.sdk import Param

    with dag_maker(
        schedule="@daily",
        params={"validated_number": Param(1, type="integer", minimum=1, maximum=10)},
    ) as dag:
        PythonOperator(task_id="hi", python_callable=print)

    with pytest.raises(InvalidBackfillConf, match="Invalid input for param validated_number"):
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2021-01-01"),
            to_date=pendulum.parse("2021-01-05"),
            max_active_runs=10,
            reverse=False,
            triggering_user_name="pytest",
            dag_run_conf={"validated_number": 99},
        )

    # No runs should have been created
    assert session.scalar(select(DagRun).where(DagRun.dag_id == dag.dag_id)) is None


def test_do_dry_run_rejects_invalid_conf(dag_maker, session):
    """Dry run with invalid conf should fail validation."""
    from airflow.sdk import Param

    with dag_maker(
        schedule="@daily",
        params={"validated_number": Param(1, type="integer", minimum=1, maximum=10)},
    ) as dag:
        PythonOperator(task_id="hi", python_callable=print)

    with pytest.raises(InvalidBackfillConf, match="Invalid input for param validated_number"):
        list(
            _do_dry_run(
                dag_id=dag.dag_id,
                from_date=pendulum.parse("2021-01-01"),
                to_date=pendulum.parse("2021-01-05"),
                reverse=False,
                reprocess_behavior=ReprocessBehavior.NONE,
                dag_run_conf={"validated_number": 99},
                session=session,
            )
        )


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
    with pytest.raises(AlreadyRunningBackfill, match="Another backfill is running for Dag"):
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
        match="Dag has tasks for which depends_on_past=True. You must set reprocess behavior to reprocess completed or reprocess failed.",
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


def test_get_latest_dag_run_row_partitioned(session: Session):
    """Deduplication matches on partition_date, independent of the partition_key string.

    The seeded runs stand in for historical scheduled runs keyed with the UTC-instant label
    that CronPartitionTimetable shipped in 3.2.0/3.2.1; the incoming info carries the same
    instant relabelled in the timetable timezone. The query must still find the existing run
    on partition_date so a backfill does not duplicate an already-scheduled partition run.
    """
    partition_date = timezone.parse("2026-02-22T16:00:00Z")
    legacy_utc_key = "2026-02-22T16:00:00"
    for start_date in [timezone.parse("2025-05-12"), None, timezone.parse("2026-02-23")]:
        session.add(
            DagRun(
                dag_id="test_dag_id",
                run_id=f"test_run_id_{get_random_string()}",
                start_date=start_date,
                run_type="scheduled",
                state=DagRunState.SUCCESS,
                partition_key=legacy_utc_key,
                partition_date=partition_date,
            )
        )
        session.commit()
    info = DagRunInfo(
        run_after=pendulum.now(),
        data_interval=None,
        partition_date=partition_date,
        partition_key="2026-02-23T00:00:00",  # same instant, relabelled in the timetable tz
    )
    stmt = _get_latest_dag_run_row_query(dag_id="test_dag_id", info=info)

    dr = session.scalar(stmt)
    assert dr is not None
    assert dr.start_date == timezone.parse("2026-02-23")
    # Matched despite the differing key string — deduplication is on partition_date.
    assert dr.partition_key == legacy_utc_key


@pytest.mark.parametrize(
    ("schedule", "dag_kwargs"),
    [
        pytest.param(None, {}, id="no-schedule"),
        pytest.param("@once", {}, id="once"),
        pytest.param("@continuous", {"max_active_runs": 1}, id="continuous"),
        pytest.param([Asset(uri="test://asset", name="test-asset")], {}, id="asset-scheduled"),
        pytest.param(
            PartitionedAssetTimetable(assets=Asset(uri="test://partitioned", name="test-partitioned")),
            {},
            id="partitioned-asset",
        ),
    ],
)
def test_create_backfill_non_periodic_schedule_rejected(schedule, dag_kwargs, dag_maker, session):
    with dag_maker(schedule=schedule, **dag_kwargs) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()
    with pytest.raises(
        DagNonPeriodicScheduleException,
        match="has a non-periodic schedule that does not support backfills",
    ):
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2021-01-01"),
            to_date=pendulum.parse("2021-01-05"),
            max_active_runs=2,
            reverse=False,
            triggering_user_name="pytest",
            dag_run_conf={},
        )


@pytest.mark.parametrize(
    ("schedule", "dag_kwargs"),
    [
        pytest.param(None, {}, id="no-schedule"),
        pytest.param("@once", {}, id="once"),
        pytest.param("@continuous", {"max_active_runs": 1}, id="continuous"),
        pytest.param([Asset(uri="test://asset", name="test-asset")], {}, id="asset-scheduled"),
        pytest.param(
            PartitionedAssetTimetable(assets=Asset(uri="test://partitioned", name="test-partitioned")),
            {},
            id="partitioned-asset",
        ),
    ],
)
def test_do_dry_run_non_periodic_schedule_rejected(schedule, dag_kwargs, dag_maker, session):
    with dag_maker(schedule=schedule, **dag_kwargs) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()
    with pytest.raises(
        DagNonPeriodicScheduleException,
        match="has a non-periodic schedule that does not support backfills",
    ):
        list(
            _do_dry_run(
                dag_id=dag.dag_id,
                from_date=pendulum.parse("2021-01-01"),
                to_date=pendulum.parse("2021-01-05"),
                reverse=False,
                reprocess_behavior=ReprocessBehavior.NONE,
                session=session,
            )
        )


def test_create_backfill_from_date_after_to_date_raises(dag_maker, session):
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    with pytest.raises(InvalidBackfillDateRange, match="must not be after to_date"):
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2026-05-13"),
            to_date=pendulum.parse("2026-05-12"),
            max_active_runs=2,
            reverse=False,
            triggering_user_name="pytest",
            dag_run_conf={},
        )


def test_create_backfill_partitioned_from_date_after_to_date_raises(dag_maker, session):
    """Partitioned Dag + from_date > to_date raises InvalidBackfillDateRange."""
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    with pytest.raises(InvalidBackfillDateRange, match="must not be after to_date"):
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2026-05-13"),
            to_date=pendulum.parse("2026-05-12"),
            max_active_runs=2,
            reverse=False,
            triggering_user_name="pytest",
            dag_run_conf={},
        )


@pytest.mark.parametrize("reverse", [False, True])
def test_backfill_partitioned_with_partition_window(reverse, dag_maker, session):
    """Partitioned Dag: from/to window of 3 days produces 3 runs (auto-detected)."""
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        to_date=pendulum.datetime(2026, 2, 20, tz="Asia/Taipei"),
        max_active_runs=2,
        reverse=reverse,
        triggering_user_name="pytest",
        dag_run_conf={},
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    partition_date_labels = [
        str(pendulum.instance(x.partition_date).in_timezone("Asia/Taipei").date()) for x in dag_runs
    ]
    expected = ["2026-02-18", "2026-02-19", "2026-02-20"]
    if reverse:
        expected = list(reversed(expected))
    assert partition_date_labels == expected


def test_backfill_partitioned_at_cap_single_day(dag_maker, session):
    """Partitioned Dag: from_date == to_date produces exactly one run (at-cap boundary, auto-detected)."""
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        to_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        max_active_runs=2,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
    )
    query = select(DagRun).join(BackfillDagRun.dag_run).where(BackfillDagRun.backfill_id == b.id)
    dag_runs = session.scalars(query).all()
    assert len(dag_runs) == 1
    assert (
        str(pendulum.instance(dag_runs[0].partition_date).in_timezone("Asia/Taipei").date()) == "2026-02-18"
    )


def test_backfill_orm_from_to_synthesised(dag_maker, session):
    """Partitioned Dag: Backfill.from_date / to_date match the provided from/to range."""
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    pds = pendulum.parse("2026-02-18")
    pde = pendulum.parse("2026-02-20")

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pds,
        to_date=pde,
        max_active_runs=2,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
    )

    br = session.get(Backfill, b.id)
    assert br is not None
    assert br.from_date.date() == pds.date()
    assert br.to_date.date() == pde.date()


def test_do_dry_run_with_partition_window(dag_maker, session):
    """_do_dry_run with partitioned Dag returns infos within the from/to window (auto-detected)."""
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    infos = list(
        _do_dry_run(
            dag_id=dag.dag_id,
            from_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
            to_date=pendulum.datetime(2026, 2, 20, tz="Asia/Taipei"),
            reverse=False,
            reprocess_behavior=ReprocessBehavior.NONE,
            session=session,
        )
    )
    dates = [str(pendulum.instance(i.partition_date).in_timezone("Asia/Taipei").date()) for i in infos]
    assert dates == ["2026-02-18", "2026-02-19", "2026-02-20"]


@pytest.mark.db_test
def test_create_backfill_partitioned_non_utc_boundary(dag_maker, session):
    """
    Guard: the production backfill path supplies UTC-midnight bounds; the first
    partition-day must not be dropped even when the timetable timezone is UTC+8.

    The CLI/API parses ``--from-date 2026-02-15`` as ``2026-02-15T00:00:00Z``
    (UTC midnight) because ``default_timezone`` is UTC on a standard deployment.
    Without wall-clock localization, ``_align_to_next`` would see this UTC instant and
    return the first Taipei-midnight tick *after* UTC midnight — skipping the
    2026-02-15 partition (Taipei midnight = 2026-02-14T16:00Z) and starting from
    2026-02-15T16:00Z instead. The localization step re-interprets the UTC-midnight
    wall-clock as a Taipei-midnight before alignment, so the first day is included.
    """
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 15, tz="UTC"),
        to_date=pendulum.datetime(2026, 2, 24, tz="UTC"),
        max_active_runs=10,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf=None,
    )

    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    partition_date_labels = [
        str(pendulum.instance(x.partition_date).in_timezone("Asia/Taipei").date()) for x in dag_runs
    ]

    # Independent oracle: all 10 calendar days from 2026-02-15 to 2026-02-24 inclusive.
    # Built without calling iter_dagrun_infos_between so the oracle cannot drift with
    # padding changes.
    expected = [f"2026-02-{d:02d}" for d in range(15, 25)]
    assert partition_date_labels == expected

    # Cap-boundary pair: padding days must be trimmed by the date-label filter.
    # at-cap: 2026-02-24 (end boundary) is included — already covered by full-sequence assert above.
    # over-cap: 2026-02-25 (one past end) and 2026-02-14 (one before start) must not appear.
    assert "2026-02-25" not in partition_date_labels
    assert "2026-02-14" not in partition_date_labels


@pytest.mark.db_test
def test_create_backfill_partitioned_sub_day_window_not_widened(dag_maker, session):
    """
    Guard: a narrow hourly window must not be widened to a full day after the
    wall-clock localization fix.

    The production backfill path supplies UTC-midnight-style bounds. For an
    hourly Taipei timetable (``0 * * * *``) the user's intent for a two-hour
    window is exactly two partitions. If localization discarded sub-day precision
    (e.g. by flooring to midnight), the result would be 24 partitions instead.

    Window: 2026-02-15 08:00 UTC → 2026-02-15 09:00 UTC (production-path form).
    Localized to Taipei these are 2026-02-15T08:00+08:00 and 2026-02-15T09:00+08:00,
    i.e. UTC instants 2026-02-15T00:00Z and 2026-02-15T01:00Z — exactly two ticks.
    """
    with dag_maker(schedule=CronPartitionTimetable("0 * * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)

    # Production-path bounds: UTC wall-clock (what CLI/API delivers on a UTC host).
    # The user intended "2026-02-15 08:00 Taipei" and "2026-02-15 09:00 Taipei"
    # — written as the corresponding UTC-wall-clock values a UTC machine produces.
    from_date = pendulum.datetime(2026, 2, 15, 8, 0, 0, tz="UTC")
    to_date = pendulum.datetime(2026, 2, 15, 9, 0, 0, tz="UTC")

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=from_date,
        to_date=to_date,
        max_active_runs=10,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf=None,
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    partition_hour_labels = [
        pendulum.instance(x.partition_date).in_timezone("Asia/Taipei").strftime("%Y-%m-%dT%H:%M")
        for x in dag_runs
    ]

    # Full-sequence equality: exactly the two requested partitions (in Taipei local time).
    assert partition_hour_labels == ["2026-02-15T08:00", "2026-02-15T09:00"]


@pytest.mark.db_test
@pytest.mark.parametrize("run_offset", [1, 2, -1, -2])
def test_backfill_partitioned_nonzero_offset_full_window(run_offset, dag_maker, session):
    """Non-zero run_offset: backfill returns exactly the requested partition window (no leaks, no gaps).

    Proves that direct partition-axis enumeration via timetable.iter_partition_dagrun_infos
    produces the correct set of runs for any integer run_offset.
    """
    with dag_maker(
        schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei", run_offset=run_offset)
    ) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        to_date=pendulum.datetime(2026, 2, 20, tz="Asia/Taipei"),
        max_active_runs=10,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    partition_date_labels = [
        str(pendulum.instance(x.partition_date).in_timezone("Asia/Taipei").date()) for x in dag_runs
    ]
    # Full-sequence equality: all three days, nothing more, nothing less.
    assert partition_date_labels == ["2026-02-18", "2026-02-19", "2026-02-20"]
    # Behavioral change: for offset != 0, run_after == partition_date (not the offset cron tick).
    for dr in dag_runs:
        assert dr.run_after == dr.partition_date, (
            f"run_offset={run_offset}: expected run_after == partition_date, "
            f"got run_after={dr.run_after!r}, partition_date={dr.partition_date!r}"
        )


@pytest.mark.db_test
@pytest.mark.parametrize(
    ("run_offset", "from_date", "to_date", "expected_labels"),
    [
        # at-cap: single-day window [2026-02-18, 2026-02-18] must produce exactly 1 run and
        # must NOT include the adjacent day (2026-02-17 or 2026-02-19).
        pytest.param(1, "2026-02-18", "2026-02-18", ["2026-02-18"], id="offset_1_at_cap"),
        pytest.param(-1, "2026-02-18", "2026-02-18", ["2026-02-18"], id="offset_minus1_at_cap"),
        # neighbor-pair: window [2026-02-18, 2026-02-19] — full-sequence equality proves
        # the enumeration produces both days and stops exactly at the boundary.
        pytest.param(1, "2026-02-18", "2026-02-19", ["2026-02-18", "2026-02-19"], id="offset_1_two_day_pair"),
        pytest.param(
            -1, "2026-02-18", "2026-02-19", ["2026-02-18", "2026-02-19"], id="offset_minus1_two_day_pair"
        ),
    ],
)
def test_backfill_partitioned_nonzero_offset_at_cap_single_day(
    run_offset, from_date, to_date, expected_labels, dag_maker, session
):
    """Non-zero run_offset: partition enumeration produces the exact requested window (auto-detected).

    at-cap cases ([2026-02-18, 2026-02-18]) prove exactly one run is produced and
    adjacent days are excluded.  neighbor-pair cases ([2026-02-18, 2026-02-19]) prove
    the enumeration includes both days and stops exactly at the upper boundary.
    Together they pin the ``>`` vs ``>=`` boundary: at-cap is allowed, one step over
    adds the next day without dropping the cap.

    Each parametrize case is an independent test run (separate DB state via autouse fixture),
    so there is no AlreadyRunningBackfill conflict between cases.
    """
    with dag_maker(
        schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei", run_offset=run_offset)
    ) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    def _parse_date_taipei(date_str: str) -> pendulum.DateTime:
        y, m, d = (int(p) for p in date_str.split("-"))
        return pendulum.datetime(y, m, d, tz="Asia/Taipei")

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=_parse_date_taipei(from_date),
        to_date=_parse_date_taipei(to_date),
        max_active_runs=2,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    labels = [
        str(pendulum.instance(x.partition_date).in_timezone("Asia/Taipei").date())
        for x in session.scalars(query).all()
    ]
    assert labels == expected_labels


@pytest.mark.db_test
def test_backfill_partitioned_offset_zero_behavior_unchanged(dag_maker, session):
    """offset==0: direct partition-axis enumeration produces the same result as the previous implementation.

    Regression guard that ensures offset==0 results are unchanged after the refactor to
    direct partition enumeration via timetable.iter_partition_dagrun_infos.
    """
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei", run_offset=0)) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        to_date=pendulum.datetime(2026, 2, 20, tz="Asia/Taipei"),
        max_active_runs=5,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    partition_date_labels = [
        str(pendulum.instance(x.partition_date).in_timezone("Asia/Taipei").date()) for x in dag_runs
    ]
    assert partition_date_labels == ["2026-02-18", "2026-02-19", "2026-02-20"]


def test_partitioned_backfill_reprocess_failed(dag_maker, session):
    """Partitioned backfill with reprocess_behavior=FAILED creates a new run, keeping the failed one.

    Unlike non-partitioned Dags (which clear and re-queue), partitioned backfills create a
    fresh run alongside the existing failed one. The prior run is kept as a historical record;
    the new backfill run becomes the active one (latest start_date wins in deduplication).
    """
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)

    # Determine the UTC partition_date for the 2026-02-18 Asia/Taipei partition.
    info = next(
        i
        for i in dag.iter_dagrun_infos_between(
            pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
            pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        )
    )
    expected_partition_date = info.partition_date

    # Simulate a previously-scheduled run that failed.
    dag_maker.create_dagrun(
        run_id="scheduled__2026-02-18",
        logical_date=None,
        run_type="scheduled",
        state=DagRunState.FAILED,
        partition_key=info.partition_key,
        partition_date=expected_partition_date,
        session=session,
    )
    session.commit()

    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        to_date=pendulum.datetime(2026, 2, 18, tz="Asia/Taipei"),
        max_active_runs=1,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.FAILED,
    )

    session.expunge_all()

    all_runs = session.scalars(select(DagRun).where(DagRun.dag_id == dag.dag_id)).all()
    # Two runs: original failed run kept as historical record + new backfill run.
    assert len(all_runs) == 2

    failed_run = next(r for r in all_runs if r.run_id == "scheduled__2026-02-18")
    assert failed_run.state == DagRunState.FAILED

    backfill_run = next(r for r in all_runs if r.run_id != "scheduled__2026-02-18")
    assert backfill_run.state == DagRunState.QUEUED
    assert backfill_run.run_type == DagRunType.BACKFILL_JOB
    assert backfill_run.partition_date == expected_partition_date

    # BackfillDagRun links to the new run, not the historical failed one.
    bdr = session.scalar(select(BackfillDagRun).where(BackfillDagRun.backfill_id == b.id))
    assert bdr is not None
    assert bdr.dag_run_id == backfill_run.id
    assert bdr.partition_key == info.partition_key


@mock.patch("airflow.models.backfill._get_info_list", autospec=True, return_value=[])
def test_create_backfill_empty_window_raises_no_runs_to_create(mock_get_info_list, dag_maker, session):
    """_create_backfill raises NoBackfillRunsToCreate when _get_info_list returns an empty list."""
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    with pytest.raises(NoBackfillRunsToCreate, match=dag.dag_id):
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2026-02-18"),
            to_date=pendulum.parse("2026-02-18"),
            max_active_runs=2,
            reverse=False,
            triggering_user_name="pytest",
            dag_run_conf={},
        )


@pytest.mark.parametrize("dag_run_conf", [None, {}])
@mock.patch("airflow.models.backfill._get_info_list", autospec=True, return_value=[])
def test_do_dry_run_empty_window_returns_empty_iterable(mock_get_info_list, dag_run_conf, dag_maker, session):
    """_do_dry_run on an empty window yields nothing (does not raise), with or without dag_run_conf."""
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * *", timezone="Asia/Taipei")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    infos = list(
        _do_dry_run(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2026-02-18"),
            to_date=pendulum.parse("2026-02-18"),
            reverse=False,
            reprocess_behavior=ReprocessBehavior.NONE,
            dag_run_conf=dag_run_conf,
            session=session,
        )
    )
    assert infos == []


def test_create_backfill_real_empty_window_no_orphan(dag_maker, session):
    """_create_backfill with a real empty window raises NoBackfillRunsToCreate without leaving an orphan.

    Uses a weekly-Monday timetable; 2026-02-18 is a Wednesday so _get_info_list returns [] for real.
    After the raise, no incomplete Backfill row must exist — proving #1 (orphan fix) holds.
    A second call for the same dag_id must succeed (not be blocked by AlreadyRunningBackfill).
    """
    with dag_maker(schedule=CronPartitionTimetable("0 0 * * 1", timezone="UTC")) as dag:
        PythonOperator(task_id="hi", python_callable=print)
    session.commit()

    wednesday = pendulum.datetime(2026, 2, 18, tz="UTC")  # not a Monday

    with pytest.raises(NoBackfillRunsToCreate, match=dag.dag_id):
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=wednesday,
            to_date=wednesday,
            max_active_runs=2,
            reverse=False,
            triggering_user_name="pytest",
            dag_run_conf=None,
        )

    # No orphan: zero incomplete Backfill rows for this dag
    orphan_count = session.scalar(
        select(func.count()).where(
            Backfill.dag_id == dag.dag_id,
            Backfill.completed_at.is_(None),
        )
    )
    assert orphan_count == 0, "An orphan Backfill row was left behind after NoBackfillRunsToCreate"

    # A valid (Monday) window must not be blocked by AlreadyRunningBackfill
    monday = pendulum.datetime(2026, 2, 23, tz="UTC")  # a Monday
    br = _create_backfill(
        dag_id=dag.dag_id,
        from_date=monday,
        to_date=monday,
        max_active_runs=2,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf=None,
    )
    assert br is not None


def test_handle_clear_run_preserves_partition_key(dag_maker, session):
    """BackfillDagRun created via the clear/reprocess path carries partition_key from info."""

    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)

    logical_date = timezone.parse("2026-01-10")
    dr = dag_maker.create_dagrun(
        run_id="scheduled_2026-01-10",
        logical_date=logical_date,
        session=session,
        state="failed",
    )
    session.commit()

    # Create a Backfill row so the foreign-key constraint is satisfied.
    backfill = Backfill(
        dag_id=dag.dag_id,
        from_date=logical_date,
        to_date=logical_date,
        dag_run_conf=None,
        max_active_runs=1,
        reprocess_behavior=ReprocessBehavior.FAILED,
    )
    session.add(backfill)
    session.flush()

    partition_key = "2026-01-10T00:00:00"
    info = DagRunInfo(
        run_after=logical_date,
        data_interval=DataInterval(logical_date, logical_date),
        partition_key=partition_key,
        partition_date=None,
    )

    _handle_clear_run(
        session=session,
        dag=dag,
        dr=dr,
        info=info,
        backfill_id=backfill.id,
        sort_ordinal=1,
    )
    session.flush()

    bdr = session.scalar(
        select(BackfillDagRun).where(
            BackfillDagRun.backfill_id == backfill.id,
            BackfillDagRun.dag_run_id == dr.id,
        )
    )
    assert bdr is not None
    assert bdr.partition_key == partition_key
