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
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models import DagRun
from airflow.models.asset import AssetEvent, AssetModel, AssetPartitionDagRun, PartitionedAssetKeyLog
from airflow.models.backfill import (
    Backfill,
    BackfillDagRun,
    ConsumerBackfillNotSupported,
    DagNonPeriodicScheduleException,
    InvalidBackfillPartitionKeys,
    ReprocessBehavior,
    TooManyBackfillPartitions,
    _create_backfill,
    _do_dry_run,
)
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import (
    AllowedKeyMapper,
    Asset,
    CronPartitionTimetable,
    DayWindow,
    HourWindow,
    IdentityMapper,
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
from tests_common.test_utils.mock_executor import MockExecutor

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
            PartitionedAssetTimetable(assets=SALES, default_partition_mapper=AllowedKeyMapper(["us", "eu"])),
            "AllowedKeyMapper",
            id="allowed-key-mapper",
        ),
        pytest.param(
            PartitionedAssetTimetable(
                assets=SALES & Asset(uri="s3://lake/returns", name="returns"),
                default_partition_mapper=IdentityMapper(),
                partition_mapper_config={SALES: _weekly_mapper()},
            ),
            "must all use",
            id="identity-mixed-with-rollup",
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


def _backfill_keys(keys, dag_id="weekly_report", **kwargs):
    return _create_backfill(
        dag_id=dag_id,
        from_date=None,
        to_date=None,
        max_active_runs=2,
        reverse=kwargs.pop("reverse", False),
        triggering_user_name="pytest",
        dag_run_conf={},
        partition_keys=keys,
        **kwargs,
    )


def test_explicit_keys_create_runs_and_the_bounds_are_the_key_dates(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08", skip=("2026-02-18",))

    backfill = _backfill_keys([W09, W06, W08])

    # The runs are ordered by partition date, whatever the order of the request.
    assert _results(session, backfill) == [
        (W06, None, True),
        (W08, "missing input", False),
        (W09, None, True),
    ]
    assert [r.partition_key for r in _runs(session)] == [W06, W09]
    stored = session.get(Backfill, backfill.id)
    assert (stored.from_date, stored.to_date) == (timezone.parse("2026-02-02"), timezone.parse("2026-02-23"))


def test_explicit_keys_dry_run(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-02-02", "2026-02-08")

    infos = list(
        _do_dry_run(
            dag_id="weekly_report",
            from_date=None,
            to_date=None,
            reverse=False,
            reprocess_behavior=ReprocessBehavior.NONE,
            session=session,
            partition_keys=[W07, W06],
        )
    )

    assert [i.partition_key for i in infos] == [W06]


@pytest.mark.parametrize(
    ("keys", "reason"),
    [
        pytest.param(["2026-02-23 (W99)"], "canonical", id="wrong-week-number"),
        pytest.param(["2026-02-24 (W09)"], "canonical", id="not-a-monday"),
        pytest.param(["week six"], "not a partition key", id="malformed"),
        pytest.param([W06, W06], "more than once", id="duplicate"),
        pytest.param([], "empty", id="empty"),
    ],
)
def test_explicit_keys_that_are_not_valid_are_refused(dag_maker, session, keys, reason):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08")
    with pytest.raises(InvalidBackfillPartitionKeys, match=reason):
        _backfill_keys(keys)
    assert session.scalars(select(Backfill)).all() == []
    assert _runs(session) == []


def test_a_request_has_a_date_range_or_keys_not_both(dag_maker, session):
    _consumer(dag_maker, session)
    with pytest.raises(InvalidBackfillPartitionKeys, match="not both"):
        _backfill(partition_keys=[W06])


def test_explicit_keys_are_refused_for_a_periodic_dag(dag_maker, session):
    with dag_maker(dag_id="daily", schedule="@daily", session=session):
        EmptyOperator(task_id="t")
    session.commit()
    with pytest.raises(InvalidBackfillPartitionKeys, match="only accepted"):
        _backfill_keys(["2026-02-02"], dag_id="daily")


@conf_vars({("scheduler", "max_consumer_backfill_partitions"): "2"})
def test_explicit_keys_over_the_limit_create_nothing(dag_maker, session):
    _consumer(dag_maker, session)
    _daily_events(session, "2026-01-26", "2026-03-08")
    with pytest.raises(TooManyBackfillPartitions):
        _backfill_keys([W06, W07, W08])
    assert session.scalars(select(Backfill)).all() == []


# ---------------------------------------------------------------------------------------------
# Consumers whose inputs use IdentityMapper. A key has no date of its own: it takes the
# partition date of the run that wrote its evidence, as live scheduling does.
# ---------------------------------------------------------------------------------------------
ORDERS = Asset(uri="s3://lake/orders", name="orders")
REFUNDS = Asset(uri="s3://lake/refunds", name="refunds")


def _identity_consumer(dag_maker, session, assets=ORDERS):
    _consumer(dag_maker, session, schedule=PartitionedAssetTimetable(assets=assets), dag_id="orders_report")


def _produced(
    session, key: str, *, asset_name="orders", hour=1, producer="daily_orders", run_suffix=""
) -> None:
    """A producer run with a partition date, and the event it wrote."""
    partition_date = timezone.parse(f"{key}T{hour:02d}:00:00")
    run_id = f"scheduled__{key}_{asset_name}{run_suffix}"
    session.add(
        DagRun(
            dag_id=producer,
            run_id=run_id,
            run_type="scheduled",
            state=DagRunState.SUCCESS,
            partition_key=key,
            partition_date=partition_date,
        )
    )
    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.name == asset_name))
    session.add(
        AssetEvent(asset_id=asset_id, partition_key=key, source_dag_id=producer, source_run_id=run_id)
    )
    session.commit()


def _event_with_no_source_run(session, key: str, asset_name="orders") -> None:
    """An event as the REST API writes it: no source Dag and no source run."""
    asset_id = session.scalar(select(AssetModel.id).where(AssetModel.name == asset_name))
    session.add(AssetEvent(asset_id=asset_id, partition_key=key))
    session.commit()


def _identity_backfill(from_date="2026-02-01", to_date="2026-02-28T23:59:59", **kwargs):
    return _backfill(dag_id="orders_report", from_date=from_date, to_date=to_date, **kwargs)


def _identity_runs(session):
    return _runs(session, dag_id="orders_report")


def test_identity_keys_come_from_events_whose_source_run_is_in_the_range(dag_maker, session):
    _identity_consumer(dag_maker, session)
    for key in ("2026-01-31", "2026-02-01", "2026-02-02", "2026-02-03", "2026-03-01"):
        _produced(session, key)

    backfill = _identity_backfill(from_date="2026-02-01", to_date="2026-02-03T23:59:59")

    runs = _identity_runs(session)
    assert [r.partition_key for r in runs] == ["2026-02-01", "2026-02-02", "2026-02-03"]
    # The run gets the partition date of the producer run that wrote its evidence.
    assert [r.partition_date for r in runs] == [timezone.parse(f"2026-02-0{d}T01:00:00") for d in (1, 2, 3)]
    assert {(r.run_type, r.state, r.backfill_id) for r in runs} == {
        (DagRunType.BACKFILL_JOB, DagRunState.QUEUED, backfill.id)
    }
    assert [[e.partition_key for e in r.consumed_asset_events] for r in runs] == [
        ["2026-02-01"],
        ["2026-02-02"],
        ["2026-02-03"],
    ]
    # No producer run was created.
    assert len(session.scalars(select(DagRun).where(DagRun.dag_id == "daily_orders")).all()) == 5


def test_identity_bounds_are_instants_on_the_evidence_date(dag_maker, session):
    _identity_consumer(dag_maker, session)
    for key in ("2026-02-01", "2026-02-02", "2026-02-03"):
        _produced(session, key)

    # The producer's partitions are at 01:00, so a to-date at midnight leaves the last day out.
    _identity_backfill(from_date="2026-02-01", to_date="2026-02-03")

    assert [r.partition_key for r in _identity_runs(session)] == ["2026-02-01", "2026-02-02"]


def test_identity_key_whose_newest_event_has_no_source_run_gets_no_run(dag_maker, session):
    _identity_consumer(dag_maker, session)
    for key in ("2026-02-01", "2026-02-02"):
        _produced(session, key)
    _event_with_no_source_run(session, "2026-02-02")

    backfill = _identity_backfill()

    assert _results(session, backfill) == [
        ("2026-02-01", None, True),
        ("2026-02-02", "no single partition date", False),
    ]
    assert [r.partition_key for r in _identity_runs(session)] == ["2026-02-01"]


def test_identity_newest_event_is_the_evidence(dag_maker, session):
    _identity_consumer(dag_maker, session)
    _produced(session, "2026-02-01")
    _produced(session, "2026-02-01", run_suffix="_again")

    _identity_backfill()

    [run] = _identity_runs(session)
    assert [e.source_run_id for e in run.consumed_asset_events] == ["scheduled__2026-02-01_orders_again"]


def test_identity_two_assets_agree_disagree_or_miss(dag_maker, session):
    _identity_consumer(dag_maker, session, assets=ORDERS & REFUNDS)
    # 02-01: both assets, same partition time. 02-02: only orders. 02-03: the two times differ.
    _produced(session, "2026-02-01", asset_name="orders")
    _produced(session, "2026-02-01", asset_name="refunds")
    _produced(session, "2026-02-02", asset_name="orders")
    _produced(session, "2026-02-03", asset_name="orders", hour=1)
    _produced(session, "2026-02-03", asset_name="refunds", hour=3)

    backfill = _identity_backfill()

    assert _results(session, backfill) == [
        ("2026-02-01", None, True),
        ("2026-02-02", "missing input", False),
        ("2026-02-03", "no single partition date", False),
    ]
    [run] = _identity_runs(session)
    assert sorted(e.asset.name for e in run.consumed_asset_events) == ["orders", "refunds"]


def test_identity_existing_run_is_found_by_the_evidence_date(dag_maker, session):
    _identity_consumer(dag_maker, session)
    _produced(session, "2026-02-01")
    first = _identity_backfill()
    [run] = _identity_runs(session)
    run.state = DagRunState.SUCCESS
    run.start_date = timezone.utcnow()
    session.get(Backfill, first.id).completed_at = timezone.utcnow()
    session.commit()

    second = _identity_backfill()

    assert _results(session, second) == [("2026-02-01", "already exists", False)]
    assert len(_identity_runs(session)) == 1


def test_identity_dry_run(dag_maker, session):
    _identity_consumer(dag_maker, session)
    for key in ("2026-02-01", "2026-02-02"):
        _produced(session, key)
    _event_with_no_source_run(session, "2026-02-02")

    infos = list(
        _do_dry_run(
            dag_id="orders_report",
            from_date=pendulum.parse("2026-02-01"),
            to_date=pendulum.parse("2026-02-28"),
            reverse=False,
            reprocess_behavior=ReprocessBehavior.NONE,
            session=session,
        )
    )

    assert [(i.partition_key, i.partition_date) for i in infos] == [
        ("2026-02-01", timezone.parse("2026-02-01T01:00:00"))
    ]
    assert _identity_runs(session) == []


def test_identity_no_event_in_the_range_is_rejected(dag_maker, session):
    from airflow.models.backfill import NoBackfillRunsToCreate

    _identity_consumer(dag_maker, session)
    _produced(session, "2026-03-05")
    with pytest.raises(NoBackfillRunsToCreate):
        _identity_backfill()
    assert session.scalars(select(Backfill)).all() == []


def test_identity_explicit_keys_take_their_dates_from_the_evidence(dag_maker, session):
    _identity_consumer(dag_maker, session)
    for key in ("2026-02-01", "2026-02-02", "2026-02-05"):
        _produced(session, key)
    _event_with_no_source_run(session, "2026-02-02")

    backfill = _backfill_keys(
        ["2026-02-05", "2026-02-02", "2026-02-01", "2026-02-09"], dag_id="orders_report"
    )

    # Keys with a date come first, by date. Keys with no single date come after, in request order.
    assert _results(session, backfill) == [
        ("2026-02-01", None, True),
        ("2026-02-05", None, True),
        ("2026-02-02", "no single partition date", False),
        ("2026-02-09", "no single partition date", False),
    ]
    stored = session.get(Backfill, backfill.id)
    assert (stored.from_date, stored.to_date) == (
        timezone.parse("2026-02-01T01:00:00"),
        timezone.parse("2026-02-05T01:00:00"),
    )


def test_identity_explicit_keys_with_no_dated_key_are_refused(dag_maker, session):
    _identity_consumer(dag_maker, session)
    _event_with_no_source_run(session, "us")
    with pytest.raises(InvalidBackfillPartitionKeys, match="single partition date"):
        _backfill_keys(["us", "eu"], dag_id="orders_report")
    assert session.scalars(select(Backfill)).all() == []


@pytest.mark.parametrize(
    ("keys", "reason"),
    [
        pytest.param(["2026-02-01", "2026-02-01"], "more than once", id="duplicate"),
        pytest.param([], "empty", id="empty"),
    ],
)
def test_identity_explicit_keys_that_are_not_valid_are_refused(dag_maker, session, keys, reason):
    _identity_consumer(dag_maker, session)
    _produced(session, "2026-02-01")
    with pytest.raises(InvalidBackfillPartitionKeys, match=reason):
        _backfill_keys(keys, dag_id="orders_report")
    assert session.scalars(select(Backfill)).all() == []


@conf_vars({("scheduler", "max_consumer_backfill_partitions"): "2"})
def test_identity_range_over_the_key_limit_creates_nothing(dag_maker, session):
    _identity_consumer(dag_maker, session)
    for key in ("2026-02-01", "2026-02-02", "2026-02-03"):
        _produced(session, key)
    with pytest.raises(TooManyBackfillPartitions):
        _identity_backfill()
    assert session.scalars(select(Backfill)).all() == []
    assert _identity_runs(session) == []


def test_identity_reverse_order(dag_maker, session):
    _identity_consumer(dag_maker, session)
    for key in ("2026-02-01", "2026-02-02", "2026-02-03"):
        _produced(session, key)

    backfill = _identity_backfill(reverse=True)

    assert [key for key, _, _ in _results(session, backfill)] == ["2026-02-03", "2026-02-02", "2026-02-01"]


def test_identity_backfill_run_gets_the_same_partition_date_as_a_live_run(dag_maker, session):
    """A real producer run emits through the asset manager, and the real scheduler step creates the live run."""
    _identity_consumer(dag_maker, session)
    with dag_maker(
        dag_id="daily_orders",
        schedule=CronPartitionTimetable("0 1 * * *", timezone="UTC", key_format="%Y-%m-%d"),
        session=session,
    ) as producer:
        EmptyOperator(task_id="load", outlets=[ORDERS])
    producer_run = dag_maker.create_dagrun(
        partition_key="2026-02-01", partition_date=timezone.parse("2026-02-01T01:00:00"), session=session
    )
    [ti] = producer_run.get_task_instances(session=session)
    session.commit()
    TaskInstance.register_asset_changes_in_db(
        ti=ti,
        task_outlets=[outlet.asprofile() for outlet in producer.get_task("load").outlets],
        outlet_events=[],
        session=session,
    )
    session.commit()
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    assert runner._create_dagruns_for_partitioned_asset_dags(session=session) == {"orders_report"}
    [live] = _identity_runs(session)
    assert live.run_type == DagRunType.ASSET_TRIGGERED
    live_date = live.partition_date
    live_event_ids = [event.id for event in live.consumed_asset_events]
    live.state = DagRunState.SUCCESS
    live.start_date = timezone.utcnow()
    session.commit()

    _identity_backfill(reprocess_behavior=ReprocessBehavior.COMPLETED)

    backfilled = [r for r in _identity_runs(session) if r.run_type == DagRunType.BACKFILL_JOB]
    assert len(backfilled) == 1
    assert backfilled[0].partition_key == "2026-02-01"
    assert backfilled[0].partition_date == live_date == timezone.parse("2026-02-01T01:00:00")
    assert [event.id for event in backfilled[0].consumed_asset_events] == live_event_ids
    assert len(live_event_ids) == 1
