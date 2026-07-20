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

import concurrent.futures
import itertools
import logging
from collections import Counter
from typing import TYPE_CHECKING
from unittest import mock

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import Session

from airflow import settings
from airflow._shared.observability.metrics.base_stats_logger import StatsLogger
from airflow._shared.timezones import timezone
from airflow.assets.manager import AssetManager
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    DagScheduleAssetAliasReference,
    DagScheduleAssetReference,
)
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.log import Log
from airflow.models.team import Team
from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper
from airflow.partition_mappers.window import WeekWindow
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_apdr,
    clear_db_logs,
    clear_db_pakl,
)
from unit.listeners import asset_listener

pytestmark = pytest.mark.db_test


pytest.importorskip("pydantic", minversion="2.0.0")


@pytest.fixture
def clear_assets():
    from tests_common.test_utils.db import clear_db_assets

    clear_db_assets()
    yield
    clear_db_assets()


@pytest.fixture
def clear_teams():
    from tests_common.test_utils.db import clear_db_teams

    clear_db_teams()
    yield
    clear_db_teams()


@pytest.fixture
def mock_task_instance():
    # TODO: Fixme - some mock_task_instance is needed here
    return None


def create_mock_dag():
    for dag_id in itertools.count(1):
        mock_dag = mock.Mock(spec=DAG)
        mock_dag.dag_id = dag_id
        yield mock_dag


def _clear_partition_db() -> None:
    clear_db_apdr()
    clear_db_pakl()
    clear_db_logs()


class TestAssetManager:
    def test_register_asset_change_asset_doesnt_exist(self, mock_task_instance):
        mock_task_instance = mock.Mock()
        asset = Asset(uri="asset_doesnt_exist", name="not exist")

        mock_session = mock.Mock(spec=Session)
        # Gotta mock up the query results
        mock_session.scalar.return_value = None

        asset_manger = AssetManager()
        asset_manger.register_asset_change(
            task_instance=mock_task_instance, asset=asset, session=mock_session
        )

        # Ensure that we have ignored the asset and _not_ created an AssetEvent or
        # AssetDagRunQueue rows
        mock_session.add.assert_not_called()
        mock_session.merge.assert_not_called()

    @pytest.mark.usefixtures("dag_maker", "testing_dag_bundle")
    def test_register_asset_change(self, session, mock_task_instance):
        asset_manager = AssetManager()

        asset = Asset(uri="test://asset1", name="test_asset_uri", group="asset")
        bundle_name = "testing"

        dag1 = DagModel(dag_id="dag1", is_stale=False, bundle_name=bundle_name)
        dag2 = DagModel(dag_id="dag2", is_stale=False, bundle_name=bundle_name)
        session.add_all([dag1, dag2])

        asm = AssetModel(uri="test://asset1/", name="test_asset_uri", group="asset")
        session.add(asm)
        asm.scheduled_dags = [DagScheduleAssetReference(dag_id=dag.dag_id) for dag in (dag1, dag2)]
        session.execute(delete(AssetDagRunQueue))
        session.flush()

        asset_manager.register_asset_change(task_instance=mock_task_instance, asset=asset, session=session)
        session.flush()

        # Ensure we've created an asset
        assert (
            session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asm.id))
            == 1
        )
        assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 2

    @pytest.mark.usefixtures("clear_assets")
    def test_register_asset_change_with_alias(
        self, session, dag_maker, mock_task_instance, testing_dag_bundle
    ):
        bundle_name = "testing"

        consumer_dag_1 = DagModel(
            dag_id="conumser_1", bundle_name=bundle_name, is_stale=False, fileloc="dag1.py"
        )
        consumer_dag_2 = DagModel(
            dag_id="conumser_2", bundle_name=bundle_name, is_stale=False, fileloc="dag2.py"
        )
        session.add_all([consumer_dag_1, consumer_dag_2])

        asm = AssetModel(uri="test://asset1/", name="test_asset_uri", group="asset")
        session.add(asm)

        asam = AssetAliasModel(name="test_alias_name", group="test")
        session.add(asam)
        asam.scheduled_dags = [
            DagScheduleAssetAliasReference(alias_id=asam.id, dag_id=dag.dag_id)
            for dag in (consumer_dag_1, consumer_dag_2)
        ]
        session.execute(delete(AssetDagRunQueue))
        session.flush()

        asset = Asset(uri="test://asset1", name="test_asset_uri")
        asset_manager = AssetManager()
        asset_manager.register_asset_change(
            task_instance=mock_task_instance,
            asset=asset,
            source_alias_names=["test_alias_name"],
            session=session,
        )
        session.flush()

        # Ensure we've created an asset
        assert (
            session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asm.id))
            == 1
        )
        assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 2

    def test_register_asset_change_no_downstreams(self, session, mock_task_instance):
        asset_manager = AssetManager()

        asset = Asset(uri="test://asset1", name="never_consumed")
        asm = AssetModel(uri="test://asset1/", name="never_consumed", group="asset")
        session.add(asm)
        session.execute(delete(AssetDagRunQueue))
        session.flush()

        asset_manager.register_asset_change(task_instance=mock_task_instance, asset=asset, session=session)
        session.flush()

        # Ensure we've created an asset
        assert (
            session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asm.id))
            == 1
        )
        assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 0

    @pytest.mark.parametrize(
        ("dialect_name", "expected_helper"),
        [
            ("postgresql", "_queue_dagruns_nonpartitioned_postgres"),
            ("mysql", "_queue_dagruns_nonpartitioned_mysql"),
            ("sqlite", "_queue_dagruns_nonpartitioned_slow_path"),
        ],
    )
    def test_queue_dagruns_routes_by_dialect(self, dialect_name, expected_helper):
        """Test that _queue_dagruns routes to the dialect-appropriate queue helper."""
        dag = DagModel(dag_id="dag1")
        session = mock.MagicMock(spec=Session)
        with (
            mock.patch("airflow.assets.manager.get_dialect_name", return_value=dialect_name),
            mock.patch.object(AssetManager, "_queue_partitioned_dags"),
            mock.patch.object(AssetManager, expected_helper) as mock_helper,
        ):
            AssetManager._queue_dagruns(
                asset_id=1,
                dags_to_queue={dag},
                partition_key=None,
                partition_date=None,
                event=mock.MagicMock(),
                task_instance=None,
                session=session,
            )
        mock_helper.assert_called_once_with(1, {dag}, session)

    def test_queue_dagruns_nonpartitioned_mysql_builds_upsert(self):
        """Test that the MySQL queue path emits an INSERT ... ON DUPLICATE KEY UPDATE."""
        dag = DagModel(dag_id="dag1")
        session = mock.MagicMock(spec=Session)

        AssetManager._queue_dagruns_nonpartitioned_mysql(asset_id=1, dags_to_queue={dag}, session=session)

        stmt, values = session.execute.call_args.args
        compiled = str(stmt.compile(dialect=mysql.dialect())).upper()
        assert "ON DUPLICATE KEY UPDATE" in compiled
        assert values == [{"target_dag_id": "dag1"}]

    def test_register_asset_change_notifies_asset_listener(
        self, session, mock_task_instance, testing_dag_bundle, listener_manager
    ):
        asset_manager = AssetManager()
        asset_listener.clear()
        listener_manager(asset_listener)

        bundle_name = "testing"

        asset = Asset(uri="test://asset1", name="test_asset_1")
        dag1 = DagModel(dag_id="dag3", bundle_name=bundle_name)
        session.add(dag1)

        asm = AssetModel(uri="test://asset1/", name="test_asset_1", group="asset")
        session.add(asm)
        asm.scheduled_dags = [DagScheduleAssetReference(dag_id=dag1.dag_id)]
        session.flush()

        asset_manager.register_asset_change(task_instance=mock_task_instance, asset=asset, session=session)
        session.flush()

        # Ensure the listener was notified
        assert len(asset_listener.changed) == 1
        assert asset_listener.changed[0].uri == asset.uri

    def test_create_assets_notifies_asset_listener(self, session, listener_manager):
        asset_manager = AssetManager()
        asset_listener.clear()
        listener_manager(asset_listener)

        asset = Asset(uri="test://asset1", name="test_asset_1")

        asms = asset_manager.create_assets([asset], session=session)

        # Ensure the listener was notified
        assert len(asset_listener.created) == 1
        assert len(asms) == 1
        assert asset_listener.created[0].uri == asset.uri == asms[0].uri

    @pytest.mark.usefixtures("dag_maker", "testing_dag_bundle")
    def test_get_or_create_apdr_race_condition(self, session, caplog):
        asm = AssetModel(uri="test://asset1/", name="partition_asset", group="asset")
        testing_dag = DagModel(dag_id="testing_dag", is_stale=False, bundle_name="testing")
        session.add_all([asm, testing_dag])
        session.commit()
        session.flush()
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 0

        rollup_fingerprint = {"asset-1|test://asset1/": {"__type": "RollupMapper", "__var": {}}}

        def _get_or_create_apdr():
            if TYPE_CHECKING:
                assert settings.Session
                assert settings.Session.session_factory

            _session = settings.Session.session_factory()
            _session.begin()
            try:
                return AssetManager._get_or_create_apdr(
                    target_key="test_partition_key",
                    target_partition_date=None,
                    target_dag=testing_dag,
                    rollup_fingerprint=rollup_fingerprint,
                    asset_id=asm.id,
                    session=_session,
                ).id
            finally:
                _session.commit()
                _session.close()

        thread_count = 100
        with caplog.at_level(logging.DEBUG):
            with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as pool:
                ids = pool.map(lambda _: _get_or_create_apdr(), [None] * thread_count)

        assert Counter(r.msg for r in caplog.records) == {
            "Existing APDR found for key test_partition_key dag_id testing_dag": thread_count - 1,
            "No existing APDR found. Create APDR for key test_partition_key dag_id testing_dag": 1,
        }

        assert len(set(ids)) == 1
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 1

    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_get_or_create_apdr_suppresses_conflicting_partition_date(self, session):
        """Two events resolving the same target key to different dates → suppress to None.

        Rather than an order-dependent first-event-wins, conflicting carried dates produce a
        deterministic ``None`` so the consumer DagRun is not stamped with a wrong, unstable date.
        """
        asm = AssetModel(uri="test://asset1/", name="partition_asset", group="asset")
        testing_dag = DagModel(dag_id="testing_dag_pd_conflict", is_stale=False, bundle_name="testing")
        session.add_all([asm, testing_dag])
        session.commit()
        fp = {"asset-1|test://asset1/": {"__type": "IdentityMapper", "__var": {}}}

        first = AssetManager._get_or_create_apdr(
            target_key="2026-05-20",
            target_partition_date=timezone.parse("2026-05-20T00:00:00"),
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            asset_id=asm.id,
            session=session,
        )
        assert first.partition_date == timezone.parse("2026-05-20T00:00:00")

        # A second contributing event resolves the same key to a DIFFERENT date.
        second = AssetManager._get_or_create_apdr(
            target_key="2026-05-20",
            target_partition_date=timezone.parse("2026-05-21T00:00:00"),
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            asset_id=asm.id,
            session=session,
        )
        assert second.id == first.id  # same pending APDR
        assert second.partition_date is None  # conflict suppressed, deterministic

    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_get_or_create_apdr_keeps_agreeing_partition_date(self, session):
        """A later event carrying the same (or no) date does not trip the conflict suppression."""
        asm = AssetModel(uri="test://asset1/", name="partition_asset", group="asset")
        testing_dag = DagModel(dag_id="testing_dag_pd_agree", is_stale=False, bundle_name="testing")
        session.add_all([asm, testing_dag])
        session.commit()
        fp = {"asset-1|test://asset1/": {"__type": "IdentityMapper", "__var": {}}}
        source_date = timezone.parse("2026-05-20T00:00:00")

        kwargs = dict(
            target_key="2026-05-20",
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            asset_id=asm.id,
            session=session,
        )
        first = AssetManager._get_or_create_apdr(target_partition_date=source_date, **kwargs)
        # Same date agrees → kept.
        same = AssetManager._get_or_create_apdr(target_partition_date=source_date, **kwargs)
        assert same.id == first.id
        assert same.partition_date == source_date
        # A None-carrying event (e.g. a temporal mapper, resolved by the scheduler) is not a
        # conflict → the existing date is kept.
        with_none = AssetManager._get_or_create_apdr(target_partition_date=None, **kwargs)
        assert with_none.id == first.id
        assert with_none.partition_date == source_date

    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_get_or_create_apdr_adopts_date_when_existing_is_none(self, session):
        """An APDR created with no date adopts a later event's carried date (not dropped)."""
        asm = AssetModel(uri="test://asset1/", name="partition_asset", group="asset")
        testing_dag = DagModel(dag_id="testing_dag_pd_adopt", is_stale=False, bundle_name="testing")
        session.add_all([asm, testing_dag])
        session.commit()
        fp = {"asset-1|test://asset1/": {"__type": "IdentityMapper", "__var": {}}}
        source_date = timezone.parse("2026-05-20T00:00:00")

        kwargs = dict(
            target_key="2026-05-20",
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            asset_id=asm.id,
            session=session,
        )
        # First event carries no date (e.g. producer had no partition_date).
        first = AssetManager._get_or_create_apdr(target_partition_date=None, **kwargs)
        assert first.partition_date is None
        # A later identity event carries a real date → adopted, not silently dropped.
        adopted = AssetManager._get_or_create_apdr(target_partition_date=source_date, **kwargs)
        assert adopted.id == first.id
        assert adopted.partition_date == source_date

    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_get_or_create_apdr_recovers_after_conflict(self, session):
        """Once a conflict has suppressed the date to None, a later event re-adopts a date."""
        asm = AssetModel(uri="test://asset1/", name="partition_asset", group="asset")
        testing_dag = DagModel(dag_id="testing_dag_pd_recover", is_stale=False, bundle_name="testing")
        session.add_all([asm, testing_dag])
        session.commit()
        fp = {"asset-1|test://asset1/": {"__type": "IdentityMapper", "__var": {}}}
        date_1 = timezone.parse("2026-05-20T00:00:00")
        date_2 = timezone.parse("2026-05-21T00:00:00")

        kwargs = dict(
            target_key="2026-05-20",
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            asset_id=asm.id,
            session=session,
        )
        first = AssetManager._get_or_create_apdr(target_partition_date=date_1, **kwargs)
        assert first.partition_date == date_1
        # Conflicting date suppresses to None.
        conflicted = AssetManager._get_or_create_apdr(target_partition_date=date_2, **kwargs)
        assert conflicted.partition_date is None
        # A subsequent event re-adopts (suppression is not permanently sticky).
        recovered = AssetManager._get_or_create_apdr(target_partition_date=date_2, **kwargs)
        assert recovered.id == first.id
        assert recovered.partition_date == date_2

    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_carry_partition_date_failure_degrades_to_none(self, session, dag_maker, mock_task_instance):
        """A mapper whose carry_partition_date raises must not abort the write.

        The consumer is still queued via partition_key; only the carried partition_date is lost
        (set to None), mirroring how a to_downstream failure is caught and handled in the loop.
        """
        _clear_partition_db()

        asset_def = Asset(uri="s3://bucket/carry_raise", name="carry_raise")
        with dag_maker(
            dag_id="carry_raise_consumer",
            schedule=PartitionedAssetTimetable(
                assets=asset_def,
                partition_mapper_config={asset_def: IdentityMapper()},
            ),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        with (
            mock.patch.object(IdentityMapper, "carry_partition_date", side_effect=RuntimeError("boom")),
            mock.patch("airflow.assets.manager.log") as mock_log,
        ):
            AssetManager.register_asset_change(
                task_instance=mock_task_instance,
                asset=asset_def,
                session=session,
                partition_key="2026-05-20",
                partition_date=timezone.parse("2026-05-20T00:00:00"),
            )
            session.flush()

        # Write not aborted: the consumer is still queued...
        apdr = session.scalar(select(AssetPartitionDagRun))
        assert apdr is not None
        # ...but the failed carry degraded to None instead of propagating.
        assert apdr.partition_date is None
        mock_log.exception.assert_called_once()

    @pytest.mark.need_serialized_dag
    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_queue_partitioned_dags_stamps_rollup_fingerprint(self, session, dag_maker):
        """APDR created by _queue_partitioned_dags is stamped with the correct rollup fingerprint."""
        from airflow.sdk import Asset, HourWindow, RollupMapper, StartOfHourMapper
        from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
        from airflow.timetables.base import compute_rollup_fingerprint
        from airflow.timetables.simple import PartitionedAssetTimetable as CorePartitionedAssetTimetable

        asset_1 = Asset(name="asset-stamp-test")
        # sdk version — dag_maker serializes this to a core timetable.
        rollup_schedule = PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
            ),
        )
        # core version — compute_rollup_fingerprint requires ``.partitioned = True``.
        core_rollup_schedule = CorePartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
            ),
        )
        with dag_maker(
            dag_id="rollup-consumer-stamp",
            schedule=rollup_schedule,
            session=session,
        ):
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="t1")
        session.commit()

        expected_fp = compute_rollup_fingerprint(core_rollup_schedule)

        # Produce an asset event to trigger APDR creation. The producer is a
        # partition-at-runtime Dag so its run can carry a ``partition_key`` that
        # the emitted ``AssetEvent`` inherits.
        from airflow.models.taskinstance import TaskInstance
        from airflow.sdk import PartitionedAtRuntime

        with dag_maker(
            dag_id="stamp-producer", schedule=PartitionedAtRuntime(), session=session
        ) as producer_dag:
            from airflow.providers.standard.operators.empty import EmptyOperator

            EmptyOperator(task_id="hi", outlets=[asset_1])

        dr = dag_maker.create_dagrun(partition_key="2024-01-01T00:00:00", session=session)
        [ti] = dr.get_task_instances(session=session)
        session.commit()

        TaskInstance.register_asset_changes_in_db(
            ti=ti,
            task_outlets=[o.asprofile() for o in producer_dag.get_task("hi").outlets],
            outlet_events=[],
            session=session,
        )
        session.commit()

        apdr = session.scalar(
            select(AssetPartitionDagRun).where(
                AssetPartitionDagRun.target_dag_id == "rollup-consumer-stamp",
                AssetPartitionDagRun.created_dag_run_id.is_(None),
            )
        )
        assert apdr is not None, "APDR should have been created"
        assert apdr.rollup_fingerprint == expected_fp, (
            "APDR rollup_fingerprint must match compute_rollup_fingerprint(timetable)"
        )

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_register_asset_change_queues_stale_dag(self, session, mock_task_instance):
        asset_manager = AssetManager()
        bundle_name = "testing"

        # Setup an Asset
        asset_uri = "test://stale_asset/"
        asset_name = "test_stale_asset"
        asset_definition = Asset(uri=asset_uri, name=asset_name)

        asm = AssetModel(uri=asset_uri, name=asset_name, group="asset")
        session.add(asm)

        # Setup a Dag that is STALE but NOT PAUSED
        # We want stale Dags to still receive asset updates
        stale_dag = DagModel(dag_id="stale_dag", is_stale=True, is_paused=False, bundle_name=bundle_name)
        session.add(stale_dag)

        # Link the Stale Dag to the Asset
        asm.scheduled_dags = [DagScheduleAssetReference(dag_id=stale_dag.dag_id)]

        session.execute(delete(AssetDagRunQueue))
        session.flush()

        # Register the asset change
        asset_manager.register_asset_change(
            task_instance=mock_task_instance, asset=asset_definition, session=session
        )
        session.flush()

        # Verify the stale Dag was NOT ignored
        assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 1

        queued_id = session.scalar(select(AssetDagRunQueue.target_dag_id))
        assert queued_id == "stale_dag"

    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partitioned_asset_event_does_not_trigger_non_partitioned_dag(self, session, mock_task_instance):
        """partitioned asset events (events with partition key) must not queue non-partition-aware Dags."""
        asm = AssetModel(uri="test://asset/", name="test_asset", group="asset")
        session.add(asm)
        dag = DagModel(
            dag_id="consumer_dag", is_paused=False, bundle_name="testing", timetable_partitioned=False
        )
        session.add(dag)
        asm.scheduled_dags = [DagScheduleAssetReference(dag_id=dag.dag_id)]
        session.execute(delete(AssetDagRunQueue))
        session.flush()

        AssetManager.register_asset_change(
            task_instance=mock_task_instance,
            asset=Asset(uri="test://asset/", name="test_asset"),
            session=session,
            partition_key="2024-01-01T00:00:00+00:00",
        )
        session.flush()

        assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 0

    @pytest.mark.parametrize(
        ("cap", "expect_trip"),
        [
            # WeekWindow always fans a weekly upstream out into 7 daily keys.
            pytest.param(2, True, id="way_over_cap"),
            # at-cap (cap == 7) and one-over (cap == 6) pin the boundary at
            # ``>`` not ``>=``; a flipped comparison would still pass way_over.
            pytest.param(7, False, id="at_cap_allowed"),
            pytest.param(6, True, id="one_over_cap_trips"),
        ],
    )
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fan_out_cap(self, session, dag_maker, mock_task_instance, cap, expect_trip):
        """The ``[scheduler] partition_mapper_max_downstream_keys`` cap gates fan-out.

        A WeekWindow fan-out of 7 daily keys is either queued in full (cap >= 7)
        or skipped entirely — no APDR queued, ``log.error`` fired, and a Log row
        written — when it exceeds the cap.
        """
        _clear_partition_db()

        asset_def = Asset(uri=f"s3://bucket/weekly_{cap}", name=f"weekly_{cap}")
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        dag_id = f"fan_out_dag_cap_{cap}"
        with dag_maker(
            dag_id=dag_id,
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        with (
            conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): str(cap)}),
            mock.patch("airflow.assets.manager.log") as mock_log,
        ):
            AssetManager.register_asset_change(
                task_instance=mock_task_instance,
                asset=asset_def,
                session=session,
                partition_key="2024-06-03T00:00:00",
            )
            session.flush()

        apdr_count = session.scalar(select(func.count()).select_from(AssetPartitionDagRun))
        log_extras = session.scalars(select(Log.extra).where(Log.event == "partition fan-out exceeded")).all()
        if not expect_trip:
            assert apdr_count == 7
            assert log_extras == []
            mock_log.error.assert_not_called()
            return

        assert apdr_count == 0
        assert len(log_extras) == 1
        assert dag_id in log_extras[0]
        assert f"partition_mapper_max_downstream_keys={cap}" in log_extras[0]
        # The scheduler-log `log.error` line is a separate observable from the
        # DB Log row; pin its keyword fields so a rename / level flip is caught.
        mock_log.error.assert_called_once()
        error_call = mock_log.error.call_args
        assert error_call.kwargs["target_dag"] == dag_id
        assert error_call.kwargs["source_partition_key"] == "2024-06-03T00:00:00"
        assert error_call.kwargs["produced_keys"] == 7
        assert error_call.kwargs["max_downstream_keys"] == cap
        assert error_call.kwargs["cap_source"] == f"[scheduler] partition_mapper_max_downstream_keys={cap}"

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "100"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fanout_per_mapper_override_stricter_than_global_trips(
        self, session, dag_maker, mock_task_instance
    ):
        """Per-mapper max_downstream_keys=3 trips even when the global cap is 100.

        Proves the per-mapper override takes precedence over a more permissive global.
        The Log.extra must mention 'max_downstream_keys=3' and must NOT mention
        'partition_mapper_max_downstream_keys' (i.e. the global cap name is absent from the message).
        """
        clear_db_apdr()
        clear_db_pakl()
        clear_db_logs()

        asset_def = Asset(uri="s3://bucket/per_mapper_strict", name="per_mapper_strict")
        # WeekWindow produces 7 daily keys; per-mapper cap of 3 must trip first.
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow(), max_downstream_keys=3)
        with dag_maker(
            dag_id="per_mapper_strict_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        with mock.patch("airflow.assets.manager.log") as mock_log:
            AssetManager.register_asset_change(
                task_instance=mock_task_instance,
                asset=asset_def,
                session=session,
                partition_key="2024-06-03T00:00:00",
            )
            session.flush()

        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 0
        log_extras = session.scalars(select(Log.extra).where(Log.event == "partition fan-out exceeded")).all()
        assert len(log_extras) == 1
        assert "max_downstream_keys=3" in log_extras[0]
        assert "partition_mapper_max_downstream_keys" not in log_extras[0]
        # Pin the scheduler-log error kwargs for the per-mapper path symmetrically
        # with the global-cap path in test_partition_fan_out_cap.
        mock_log.error.assert_called_once()
        error_call = mock_log.error.call_args
        assert error_call.kwargs["cap_source"] == "max_downstream_keys=3"

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "3"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fanout_per_mapper_override_looser_than_global_permits(
        self, session, dag_maker, mock_task_instance
    ):
        """Per-mapper max_downstream_keys=10 permits 7 keys even when the global cap is 3.

        Proves the per-mapper override can relax, not just tighten, the cap.
        """
        clear_db_apdr()
        clear_db_pakl()
        clear_db_logs()

        asset_def = Asset(uri="s3://bucket/per_mapper_loose", name="per_mapper_loose")
        # Global cap of 3 would block the 7-key WeekWindow fanout, but per-mapper
        # max_downstream_keys=10 overrides it and all 7 rows must be queued.
        mapper = FanOutMapper(
            upstream_mapper=StartOfWeekMapper(), window=WeekWindow(), max_downstream_keys=10
        )
        with dag_maker(
            dag_id="per_mapper_loose_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        AssetManager.register_asset_change(
            task_instance=mock_task_instance,
            asset=asset_def,
            session=session,
            partition_key="2024-06-03T00:00:00",
        )
        session.flush()

        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 7
        assert (
            session.scalar(
                select(func.count()).select_from(Log).where(Log.event == "partition fan-out exceeded")
            )
            == 0
        )

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "1"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fanout_per_mapper_at_cap_is_allowed(self, session, dag_maker, mock_task_instance):
        """Per-mapper max_downstream_keys=7 with a 7-key fanout: exactly at cap is allowed.

        Pairs with test_partition_fanout_per_mapper_one_over_cap_trips to pin the
        boundary at '>' (not '>=') on the per-mapper branch.
        """
        clear_db_apdr()
        clear_db_pakl()
        clear_db_logs()

        asset_def = Asset(uri="s3://bucket/per_mapper_at_cap", name="per_mapper_at_cap")
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow(), max_downstream_keys=7)
        with dag_maker(
            dag_id="per_mapper_at_cap_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        AssetManager.register_asset_change(
            task_instance=mock_task_instance,
            asset=asset_def,
            session=session,
            partition_key="2024-06-03T00:00:00",
        )
        session.flush()

        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 7
        assert (
            session.scalar(
                select(func.count()).select_from(Log).where(Log.event == "partition fan-out exceeded")
            )
            == 0
        )

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "1"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fanout_per_mapper_one_over_cap_trips(self, session, dag_maker, mock_task_instance):
        """Per-mapper max_downstream_keys=6 with a 7-key fanout: one over cap trips the guard.

        Pairs with test_partition_fanout_per_mapper_at_cap_is_allowed to lock the
        boundary: 7 keys at cap=7 is allowed, but 7 keys at cap=6 is not.
        """
        clear_db_apdr()
        clear_db_pakl()
        clear_db_logs()

        asset_def = Asset(uri="s3://bucket/per_mapper_over_cap", name="per_mapper_over_cap")
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow(), max_downstream_keys=6)
        with dag_maker(
            dag_id="per_mapper_over_cap_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        AssetManager.register_asset_change(
            task_instance=mock_task_instance,
            asset=asset_def,
            session=session,
            partition_key="2024-06-03T00:00:00",
        )
        session.flush()

        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 0
        log_extras = session.scalars(select(Log.extra).where(Log.event == "partition fan-out exceeded")).all()
        assert len(log_extras) == 1
        assert "max_downstream_keys=6" in log_extras[0]


def _make_dag(dag_id: str) -> DagModel:
    dag = mock.Mock(spec=DagModel)
    dag.dag_id = dag_id
    return dag


def _make_asset_model(
    scheduled_dags: dict[str, list[str]] | None = None,
    allow_global: dict[str, bool] | None = None,
) -> AssetModel:
    """Create a mock AssetModel.

    :param scheduled_dags: mapping of dag_id -> allow_producer_teams for each consumer reference.
    :param allow_global: mapping of dag_id -> allow_global_producers for each consumer reference.
    """
    allow_global = allow_global or {}
    model = mock.Mock(spec=AssetModel)
    model.scheduled_dags = [
        mock.Mock(
            dag_id=dag_id,
            allow_producer_teams=teams,
            allow_global_producers=allow_global.get(dag_id, True),
        )
        for dag_id, teams in (scheduled_dags or {}).items()
    ]
    return model


class TestAssetMetricsTeamName:
    @pytest.mark.usefixtures("clear_teams")
    @pytest.mark.parametrize(
        ("multi_team", "expect_team_tag"),
        [
            pytest.param("true", True, id="with_team"),
            pytest.param("false", False, id="without_team"),
        ],
    )
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_asset_updates_respects_team_name(
        self, mock_get_backend, multi_team, expect_team_tag, session, dag_maker
    ):
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats

        suffix = "with_team" if expect_team_tag else "without_team"

        team_name = f"team_asset_upd_{suffix}"
        team = Team(name=team_name)
        session.add(team)
        session.flush()

        bundle_name = f"bundle_asset_upd_{suffix}"
        bundle = DagBundleModel(name=bundle_name)
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        asset_name = f"metric_asset_{suffix}"
        asset = Asset(uri=f"test://{asset_name}", name=asset_name, group="asset")
        with dag_maker(dag_id=f"asset_dag_{suffix}", bundle_name=bundle_name, session=session):
            EmptyOperator(task_id="task1", outlets=[asset])

        ti = mock.MagicMock()
        ti.dag_id = f"asset_dag_{suffix}"
        ti.task_id = "task1"
        ti.run_id = "run1"
        ti.map_index = -1

        with conf_vars({("core", "multi_team"): multi_team}):
            AssetManager().register_asset_change(task_instance=ti, asset=asset, session=session)

        if expect_team_tag:
            mock_stats.incr.assert_any_call("asset.updates", tags={"team_name": team_name})
        else:
            mock_stats.incr.assert_any_call("asset.updates")


class TestFilterDagsByTeam:
    @conf_vars({("core", "multi_team"): "false"})
    def test_multi_team_disabled_returns_all_dags(self):
        """When multi_team is disabled, all DAGs are returned unchanged."""
        dags = {_make_dag("dag1"), _make_dag("dag2")}
        asset_model = _make_asset_model()

        result = AssetManager._filter_dags_by_team(
            dags_to_queue=dags,
            source_teams={"team_a"},
            asset_model=asset_model,
            source_is_api=False,
            session=mock.Mock(),
        )

        assert result == dags

    @conf_vars({("core", "multi_team"): "true"})
    def test_empty_dags_returns_empty(self):
        """Empty input returns empty output."""
        result = AssetManager._filter_dags_by_team(
            dags_to_queue=set(),
            source_teams={"team_a"},
            asset_model=_make_asset_model(),
            source_is_api=False,
            session=mock.Mock(),
        )

        assert result == set()

    @conf_vars({("core", "multi_team"): "true"})
    def test_same_team_allowed(self):
        """Producer Team A -> Consumer Team A: allowed."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_a"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams={"team_a"},
                asset_model=_make_asset_model(),
                source_is_api=False,
                session=mock.Mock(),
            )

        assert dag in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_cross_team_blocked_without_allow(self):
        """Producer Team A -> Consumer Team B with empty allow_producer_teams: blocked."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_b"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams={"team_a"},
                asset_model=_make_asset_model(scheduled_dags={"dag1": []}),
                source_is_api=False,
                session=mock.Mock(),
            )

        assert dag not in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_cross_team_allowed_via_allow_producer_teams(self):
        """Producer Team A -> Consumer Team B with allow_producer_teams=["team_a"]: allowed."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_b"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams={"team_a"},
                asset_model=_make_asset_model(scheduled_dags={"dag1": ["team_a"]}),
                source_is_api=False,
                session=mock.Mock(),
            )

        assert dag in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_teamless_dag_producer_triggers_all(self):
        """Teamless DAG producer (not API) triggers all consumers including team-bound."""
        dag_team_b = _make_dag("dag1")
        dag_teamless = _make_dag("dag2")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_b"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag_team_b, dag_teamless},
                source_teams=set(),
                asset_model=_make_asset_model(),
                source_is_api=False,
                session=mock.Mock(),
            )

        assert dag_team_b in result
        assert dag_teamless in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_teamless_consumer_accepts_any_source(self):
        """Teamless consumer accepts events from any source."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams={"team_a"},
                asset_model=_make_asset_model(),
                source_is_api=False,
                session=mock.Mock(),
            )

        assert dag in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_teamless_api_user_triggers_only_teamless_consumers(self):
        """Teamless API user can only trigger teamless consumers."""
        dag_with_team = _make_dag("dag1")
        dag_teamless = _make_dag("dag2")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_b"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag_with_team, dag_teamless},
                source_teams=set(),
                asset_model=_make_asset_model(),
                source_is_api=True,
                session=mock.Mock(),
            )

        assert dag_with_team not in result
        assert dag_teamless in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_api_user_same_team_allowed(self):
        """API user Team A -> Consumer Team A: allowed."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_a"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams={"team_a"},
                asset_model=_make_asset_model(),
                source_is_api=True,
                session=mock.Mock(),
            )

        assert dag in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_api_user_cross_team_via_allow_producer_teams(self):
        """API user Team A -> Consumer Team B with allow_producer_teams=["team_a"]: allowed."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_b"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams={"team_a"},
                asset_model=_make_asset_model(scheduled_dags={"dag1": ["team_a"]}),
                source_is_api=True,
                session=mock.Mock(),
            )

        assert dag in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_batch_team_resolution_called_once(self):
        """Batch team resolution is called once for N consumers, not N times."""
        dags = {_make_dag(f"dag{i}") for i in range(5)}

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={}) as mock_mapping:
            AssetManager._filter_dags_by_team(
                dags_to_queue=dags,
                source_teams={"team_a"},
                asset_model=_make_asset_model(),
                source_is_api=False,
                session=mock.Mock(),
            )

        mock_mapping.assert_called_once()

    @conf_vars({("core", "multi_team"): "true"})
    def test_both_teamless_allowed(self):
        """Both producer and consumer teamless: allowed."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams=set(),
                asset_model=_make_asset_model(),
                source_is_api=False,
                session=mock.Mock(),
            )

        assert dag in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_teamless_dag_producer_blocked_when_allow_global_false(self):
        """Teamless DAG producer is blocked when consumer's allow_global_producers=False."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_b"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams=set(),
                asset_model=_make_asset_model(scheduled_dags={"dag1": []}, allow_global={"dag1": False}),
                source_is_api=False,
                session=mock.Mock(),
            )

        assert dag not in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_teamless_dag_producer_allowed_when_allow_global_true(self):
        """Teamless DAG producer allowed when consumer's allow_global_producers=True (default)."""
        dag = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_b"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag},
                source_teams=set(),
                asset_model=_make_asset_model(scheduled_dags={"dag1": []}, allow_global={"dag1": True}),
                source_is_api=False,
                session=mock.Mock(),
            )

        assert dag in result

    @conf_vars({("core", "multi_team"): "true"})
    def test_teamless_api_user_not_affected_by_allow_global(self):
        """Teamless API user behavior unchanged by allow_global — still blocked from team-bound consumers."""
        dag_with_team = _make_dag("dag1")

        with mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping", return_value={"dag1": "team_b"}):
            result = AssetManager._filter_dags_by_team(
                dags_to_queue={dag_with_team},
                source_teams=set(),
                asset_model=_make_asset_model(scheduled_dags={"dag1": []}, allow_global={"dag1": True}),
                source_is_api=True,
                session=mock.Mock(),
            )

        assert dag_with_team not in result

    @conf_vars({("core", "multi_team"): "true"})
    @pytest.mark.parametrize(
        (
            "team_mapping",
            "source_teams",
            "scheduled_dags",
            "allow_consumer_teams",
            "allow_global_consumers",
            "expected_in",
        ),
        [
            pytest.param(
                {"dag1": "team_b"},
                {"team_a"},
                {"dag1": ["team_a"]},
                ["team_a"],
                True,
                False,
                id="consumer_blocked_when_team_not_in_allow_consumer_teams",
            ),
            pytest.param(
                {"dag1": "team_b"},
                {"team_a"},
                {"dag1": ["team_a"]},
                ["team_a", "team_b"],
                True,
                True,
                id="consumer_allowed_when_team_in_allow_consumer_teams",
            ),
            pytest.param(
                {},
                {"team_a"},
                {},
                ["team_b"],
                True,
                True,
                id="teamless_consumer_passes_when_allow_global_consumers_true",
            ),
            pytest.param(
                {},
                {"team_a"},
                {},
                ["team_b"],
                False,
                False,
                id="teamless_consumer_blocked_when_allow_global_consumers_false",
            ),
            pytest.param(
                {"dag1": "team_b"},
                {"team_a"},
                {"dag1": []},
                ["team_b"],
                True,
                False,
                id="both_filters_must_pass_and_logic",
            ),
            pytest.param(
                {"dag1": "team_b"},
                {"team_a"},
                {"dag1": ["team_a"]},
                [],
                True,
                False,
                id="empty_allow_consumer_teams_blocks_all_teams",
            ),
            pytest.param(
                {"dag1": "team_b"},
                {"team_a"},
                {"dag1": ["team_a"]},
                None,
                True,
                True,
                id="none_allow_consumer_teams_means_no_consumer_filtering",
            ),
            pytest.param(
                {},
                {"team_a"},
                {},
                None,
                False,
                False,
                id="teamless_consumer_blocked_when_only_allow_global_false",
            ),
            pytest.param(
                {"dag1": "team_a"},
                {"team_a"},
                {"dag1": ["team_a"]},
                [],
                False,
                True,
                id="same_team_as_producer_always_allowed",
            ),
        ],
    )
    @mock.patch.object(DagModel, "get_dag_id_to_team_name_mapping")
    def test_consumer_team_filtering(
        self,
        mock_mapping,
        team_mapping,
        source_teams,
        scheduled_dags,
        allow_consumer_teams,
        allow_global_consumers,
        expected_in,
    ):
        dag = _make_dag("dag1")
        mock_mapping.return_value = team_mapping

        result = AssetManager._filter_dags_by_team(
            dags_to_queue={dag},
            source_teams=source_teams,
            asset_model=_make_asset_model(scheduled_dags=scheduled_dags)
            if scheduled_dags
            else _make_asset_model(),
            source_is_api=False,
            session=mock.Mock(),
            allow_consumer_teams=allow_consumer_teams,
            allow_global_consumers=allow_global_consumers,
        )

        assert (dag in result) == expected_in
