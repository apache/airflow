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
"""Backfill of a Dag scheduled on partitioned assets (AIP-129 proof of concept)."""

from __future__ import annotations

from datetime import timedelta

import pendulum
import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models import DagRun
from airflow.models.asset import AssetEvent, AssetModel, AssetPartitionDagRun, PartitionedAssetKeyLog
from airflow.models.backfill import (
    Backfill,
    BackfillDagRun,
    ConsumerBackfillNotSupported,
    DagNonPeriodicScheduleException,
    ReprocessBehavior,
    TooManyBackfillPartitions,
    _create_backfill,
    _do_dry_run,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import (
    Asset,
    DayWindow,
    HourWindow,
    MinimumCount,
    PartitionedAssetTimetable,
    RollupMapper,
    StartOfDayMapper,
    StartOfHourMapper,
    StartOfWeekMapper,
    WeekWindow,
)
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_apdr,
    clear_db_assets,
    clear_db_backfills,
    clear_db_dags,
    clear_db_pakl,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

SALES = Asset(uri="s3://lake/sales", name="sales")
W06, W07, W08, W09 = "2026-02-02 (W06)", "2026-02-09 (W07)", "2026-02-16 (W08)", "2026-02-23 (W09)"
FEB = [W06, W07, W08, W09]


def _clean_db():
    clear_db_pakl()
    clear_db_apdr()
    clear_db_backfills()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()
    clear_db_assets()


@pytest.fixture(autouse=True)
def clean_db():
    _clean_db()
    yield
    _clean_db()


def _weekly_mapper(**kwargs):
    return RollupMapper(
        upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d"), window=WeekWindow(), **kwargs
    )


def _consumer(dag_maker, session, schedule=None, dag_id="weekly_report"):
    schedule = schedule or PartitionedAssetTimetable(assets=SALES, default_partition_mapper=_weekly_mapper())
    with dag_maker(dag_id=dag_id, schedule=schedule, session=session):
        EmptyOperator(task_id="build_report")
    session.commit()


def _daily_events(
    session, first: str, last: str, *, skip: tuple[str, ...] = (), asset_name: str = "sales"
) -> None:
    """Existing asset events, one for each day, as a daily producer would have left them."""
    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.name == asset_name))
    day, end = timezone.parse(first), timezone.parse(last)
    while day <= end:
        key = day.strftime("%Y-%m-%d")
        if key not in skip:
            session.add(
                AssetEvent(
                    asset_id=asset_id, partition_key=key, source_dag_id="daily_sales", source_run_id=key
                )
            )
        day += timedelta(days=1)
    session.commit()


def _backfill(dag_id="weekly_report", from_date="2026-02-01", to_date="2026-02-28", **kwargs):
    return _create_backfill(
        dag_id=dag_id,
        from_date=pendulum.parse(from_date),
        to_date=pendulum.parse(to_date),
        max_active_runs=2,
        reverse=kwargs.pop("reverse", False),
        triggering_user_name="pytest",
        dag_run_conf={},
        **kwargs,
    )


def _runs(session, dag_id="weekly_report"):
    return session.scalars(
        select(DagRun).where(DagRun.dag_id == dag_id).order_by(DagRun.partition_date)
    ).all()


def _results(session, backfill):
    rows = session.scalars(
        select(BackfillDagRun)
        .where(BackfillDagRun.backfill_id == backfill.id)
        .order_by(BackfillDagRun.sort_ordinal)
    ).all()
    return [(row.partition_key, row.exception_reason, row.dag_run_id is not None) for row in rows]


def test_date_range_creates_one_run_for_each_week_from_existing_events(dag_maker, session):
    """A new consumer gets its history from events that exist. The producer does not run."""
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08")

    backfill = _backfill()

    runs = _runs(session)
    assert [r.partition_key for r in runs] == FEB
    assert [r.partition_date for r in runs] == [timezone.parse(k[:10]) for k in FEB]
    assert {(r.run_type, r.state, r.backfill_id) for r in runs} == {
        (DagRunType.BACKFILL_JOB, DagRunState.QUEUED, backfill.id)
    }
    assert len({r.run_id for r in runs}) == 4
    assert _results(session, backfill) == [(key, None, True) for key in FEB]
    # Each run reads exactly the seven days of its week.
    w09 = runs[-1]
    assert sorted(e.partition_key for e in w09.consumed_asset_events) == [
        "2026-02-23",
        "2026-02-24",
        "2026-02-25",
        "2026-02-26",
        "2026-02-27",
        "2026-02-28",
        "2026-03-01",
    ]
    assert {len(r.consumed_asset_events) for r in runs} == {7}
    assert session.scalars(select(DagRun).where(DagRun.dag_id == "daily_sales")).all() == []


def test_a_week_that_starts_before_the_range_is_not_selected(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08")

    _backfill(from_date="2026-02-03", to_date="2026-02-16")

    assert [r.partition_key for r in _runs(session)] == [W07, W08]


def test_bounds_are_instants_and_both_ends_are_included(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08")

    _backfill(from_date="2026-02-02T00:00:00+00:00", to_date="2026-02-09T09:00:00+09:00")

    # 2026-02-09T09:00+09:00 is 2026-02-09T00:00Z, so the week that starts then is included.
    assert [r.partition_key for r in _runs(session)] == [W06, W07]


def test_a_missing_input_is_recorded_and_the_other_weeks_run(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08", skip=("2026-02-10",))

    backfill = _backfill()

    assert [r.partition_key for r in _runs(session)] == [W06, W08, W09]
    assert _results(session, backfill) == [
        (W06, None, True),
        (W07, "missing input", False),
        (W08, None, True),
        (W09, None, True),
    ]


def test_dry_run_lists_only_the_weeks_it_would_create(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08", skip=("2026-02-10",))

    infos = list(
        _do_dry_run(
            dag_id="weekly_report",
            from_date=pendulum.parse("2026-02-01"),
            to_date=pendulum.parse("2026-02-28"),
            reverse=False,
            reprocess_behavior=ReprocessBehavior.NONE,
            session=session,
        )
    )

    assert [i.partition_key for i in infos] == [W06, W08, W09]
    assert _runs(session) == []
    assert session.scalars(select(Backfill)).all() == []


def test_the_newest_event_of_an_input_is_the_evidence(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-02-02", "2026-02-08")
    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.name == "sales"))
    newer = AssetEvent(
        asset_id=asset_id, partition_key="2026-02-03", source_dag_id="daily_sales", source_run_id="again"
    )
    session.add(newer)
    session.commit()

    _backfill(from_date="2026-02-02", to_date="2026-02-02")

    [run] = _runs(session)
    tuesday = [e for e in run.consumed_asset_events if e.partition_key == "2026-02-03"]
    assert [e.source_run_id for e in tuesday] == ["again"]
    assert len(run.consumed_asset_events) == 7


def test_an_existing_run_is_handled_as_partitioned_backfill_handles_it(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-02-02", "2026-02-15")
    first = _backfill(from_date="2026-02-02", to_date="2026-02-09")
    for run in _runs(session):
        run.state = DagRunState.SUCCESS
        run.start_date = timezone.utcnow()
    session.get(Backfill, first.id).completed_at = timezone.utcnow()
    session.commit()

    second = _backfill(from_date="2026-02-02", to_date="2026-02-09")
    assert _results(session, second) == [(W06, "already exists", False), (W07, "already exists", False)]
    session.get(Backfill, second.id).completed_at = timezone.utcnow()
    session.commit()

    # A repeated backfill of the same keys creates new runs with new run ids.
    third = _backfill(
        from_date="2026-02-02", to_date="2026-02-09", reprocess_behavior=ReprocessBehavior.COMPLETED
    )
    assert _results(session, third) == [(W06, None, True), (W07, None, True)]
    assert len(_runs(session)) == 4
    assert len({r.run_id for r in _runs(session)}) == 4


def test_the_run_is_linked_like_a_live_run_and_live_pending_rows_are_not_touched(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-02-02", "2026-02-08")
    pending = AssetPartitionDagRun(
        target_dag_id="weekly_report", partition_key=W06, pending_partition_key=W06
    )
    session.add(pending)
    session.commit()
    pending_id = pending.id

    _backfill(from_date="2026-02-02", to_date="2026-02-02")

    [run] = _runs(session)
    rows = session.scalars(
        select(AssetPartitionDagRun).where(AssetPartitionDagRun.target_dag_id == "weekly_report")
    ).all()
    linked = [r for r in rows if r.created_dag_run_id == run.id]
    assert len(linked) == 1
    assert linked[0].pending_partition_key is None
    assert linked[0].partition_date == run.partition_date
    logged = session.scalars(
        select(PartitionedAssetKeyLog).where(
            PartitionedAssetKeyLog.asset_partition_dag_run_id == linked[0].id
        )
    ).all()
    assert sorted(row.source_partition_key for row in logged) == [f"2026-02-0{d}" for d in range(2, 9)]
    assert {row.target_partition_key for row in logged} == {W06}
    still_pending = session.get(AssetPartitionDagRun, pending_id)
    assert still_pending.created_dag_run_id is None
    assert still_pending.pending_partition_key == W06


def test_reverse_order(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08")

    backfill = _backfill(reverse=True)

    assert [key for key, _, _ in _results(session, backfill)] == FEB[::-1]


def test_no_week_in_the_range_is_rejected_like_an_empty_periodic_range(dag_maker, session):
    from airflow.models.backfill import NoBackfillRunsToCreate

    _consumer(dag_maker, session)
    with pytest.raises(NoBackfillRunsToCreate):
        _backfill(from_date="2026-02-03", to_date="2026-02-08")
    assert session.scalars(select(Backfill)).all() == []


@conf_vars({("core", "multi_team"): "True"})
def test_refused_with_multi_team(dag_maker, session):
    _consumer(dag_maker, session)
    with pytest.raises(ConsumerBackfillNotSupported, match="multi_team"):
        _backfill()
    assert session.scalars(select(Backfill)).all() == []


@pytest.mark.parametrize(
    ("schedule", "reason"),
    [
        pytest.param(
            PartitionedAssetTimetable(assets=SALES), "RollupMapper", id="identity-not-in-this-slice"
        ),
        pytest.param(
            PartitionedAssetTimetable(
                assets=SALES,
                default_partition_mapper=RollupMapper(
                    upstream_mapper=StartOfHourMapper(), window=HourWindow()
                ),
            ),
            "StartOfHourMapper",
            id="hour-grain",
        ),
        pytest.param(
            PartitionedAssetTimetable(
                assets=SALES, default_partition_mapper=_weekly_mapper(wait_policy=MinimumCount(5))
            ),
            "WaitForAll",
            id="threshold-policy",
        ),
        pytest.param(
            PartitionedAssetTimetable(
                assets=SALES,
                default_partition_mapper=RollupMapper(
                    upstream_mapper=StartOfWeekMapper(input_format="%Y-%m-%d"),
                    window=WeekWindow(direction=WeekWindow.Direction.BACKWARD),
                ),
            ),
            "forward",
            id="backward-window",
        ),
        pytest.param(
            PartitionedAssetTimetable(
                assets=SALES,
                default_partition_mapper=RollupMapper(
                    upstream_mapper=StartOfDayMapper(input_format="%Y-%m-%dT%H", output_format="%Y-%m-%d%z"),
                    window=DayWindow(),
                ),
            ),
            "offset",
            id="key-format-with-offset",
        ),
        pytest.param(
            PartitionedAssetTimetable(
                assets=SALES | Asset(uri="s3://lake/returns", name="returns"),
                default_partition_mapper=_weekly_mapper(),
            ),
            "condition",
            id="or-condition",
        ),
    ],
)
def test_consumers_outside_the_supported_list_are_refused(dag_maker, session, schedule, reason):
    _consumer(dag_maker, session, schedule=schedule)
    with pytest.raises(ConsumerBackfillNotSupported, match=reason) as excinfo:
        _backfill()
    # The REST route already maps this exception class to a 422 response.
    assert isinstance(excinfo.value, DagNonPeriodicScheduleException)
    assert session.scalars(select(Backfill)).all() == []


def test_a_mapper_and_window_that_do_not_fit_are_refused(dag_maker, session):
    """StartOfDayMapper with WeekWindow: the inputs of a day key map to other days."""
    schedule = PartitionedAssetTimetable(
        assets=SALES,
        default_partition_mapper=RollupMapper(
            upstream_mapper=StartOfDayMapper(input_format="%Y-%m-%d"), window=WeekWindow()
        ),
    )
    _consumer(dag_maker, session, schedule=schedule)
    with pytest.raises(ConsumerBackfillNotSupported, match="maps to"):
        _backfill(from_date="2026-02-02", to_date="2026-02-03")
    assert session.scalars(select(Backfill)).all() == []


@conf_vars({("scheduler", "max_consumer_backfill_partitions"): "3"})
def test_a_request_over_the_key_limit_creates_nothing(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08")
    with pytest.raises(TooManyBackfillPartitions, match="3"):
        _backfill()
    assert session.scalars(select(Backfill)).all() == []
    assert _runs(session) == []


def test_two_assets_joined_with_and_need_evidence_in_both(dag_maker, session):
    returns = Asset(uri="s3://lake/returns", name="returns")
    schedule = PartitionedAssetTimetable(assets=SALES & returns, default_partition_mapper=_weekly_mapper())
    _consumer(dag_maker, session, schedule=schedule)
    _daily_events(session, "2026-02-02", "2026-02-15")
    _daily_events(session, "2026-02-02", "2026-02-08", asset_name="returns")

    backfill = _backfill(from_date="2026-02-02", to_date="2026-02-09")

    assert _results(session, backfill) == [(W06, None, True), (W07, "missing input", False)]
    [run] = _runs(session)
    assert len(run.consumed_asset_events) == 14
