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
import threading
from collections import Counter
from typing import TYPE_CHECKING, Any
from unittest import mock
from uuid import uuid4

import pytest
from sqlalchemy import delete, event, exc, func, insert, select, update
from sqlalchemy.dialects import mysql, postgresql, sqlite
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
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
    PartitionedAssetKeyLog,
)
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbundle import DagBundleModel
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper
from airflow.partition_mappers.window import WeekWindow
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import PartitionedAtRuntime
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
from airflow.timetables.simple import PartitionedAssetTimetable as CorePartitionedAssetTimetable
from airflow.utils.session import create_session

from tests_common.test_utils.asserts import capture_orm_selects
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_apdr,
    clear_db_logs,
    clear_db_pakl,
)
from unit.listeners import asset_listener

if TYPE_CHECKING:
    from datetime import datetime

pytestmark = pytest.mark.db_test


pytest.importorskip("pydantic", minversion="2.0.0")


@pytest.fixture(autouse=True)
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
        session.flush()

        asset_manager.register_asset_change(task_instance=mock_task_instance, asset=asset, session=session)
        session.flush()

        # Ensure we've created an asset
        assert (
            session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asm.id))
            == 1
        )
        assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 2

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

    @pytest.mark.parametrize("reference_kind", ["asset", "alias", "name", "uri"])
    def test_register_asset_change_does_not_queue_draining_dag(
        self, reference_kind, session, mock_task_instance, testing_dag_bundle
    ):
        asset = Asset(uri="test://draining", name="draining_asset")
        asset_model = AssetModel(uri=asset.uri, name=asset.name, group="asset")
        dag_model = DagModel(
            dag_id=f"draining_{reference_kind}",
            bundle_name="testing",
            is_draining=True,
            is_stale=False,
        )
        session.add_all([asset_model, dag_model])

        source_alias_names = ()
        if reference_kind == "asset":
            asset_model.scheduled_dags = [DagScheduleAssetReference(dag_id=dag_model.dag_id)]
        elif reference_kind == "alias":
            alias = AssetAliasModel(name="draining_alias", group="asset")
            alias.scheduled_dags = [
                DagScheduleAssetAliasReference(alias_id=alias.id, dag_id=dag_model.dag_id)
            ]
            session.add(alias)
            source_alias_names = [alias.name]
        elif reference_kind == "name":
            session.add(DagScheduleAssetNameReference(name=asset.name, dag_id=dag_model.dag_id))
        else:
            session.add(DagScheduleAssetUriReference(uri=asset.uri, dag_id=dag_model.dag_id))
        session.flush()

        AssetManager.register_asset_change(
            task_instance=mock_task_instance,
            asset=asset,
            source_alias_names=source_alias_names,
            session=session,
        )
        session.flush()

        assert session.scalar(select(func.count()).select_from(AssetEvent)) == 1
        assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 0

    def test_register_asset_change_no_downstreams(self, session, mock_task_instance):
        asset_manager = AssetManager()

        asset = Asset(uri="test://asset1", name="never_consumed")
        asm = AssetModel(uri="test://asset1/", name="never_consumed", group="asset")
        session.add(asm)
        session.flush()

        asset_manager.register_asset_change(task_instance=mock_task_instance, asset=asset, session=session)
        session.flush()

        # Ensure we've created an asset
        assert (
            session.scalar(select(func.count()).select_from(AssetEvent).where(AssetEvent.asset_id == asm.id))
            == 1
        )
        assert session.scalar(select(func.count()).select_from(AssetDagRunQueue)) == 0

    def test_register_asset_change_is_atomic_on_caller_session(self, session, mock_task_instance):
        """The AssetEvent is written on the caller's session: visible before commit, gone after rollback.

        Under the old side-session behaviour the event was committed independently and would be
        orphaned if the caller's transaction rolled back. Registration is now atomic with the caller.
        """
        asset_manager = AssetManager()

        asset = Asset(uri="test://atomic1", name="atomic_asset")
        asm = AssetModel(uri="test://atomic1/", name="atomic_asset", group="asset")
        session.add(asm)
        session.flush()

        event = asset_manager.register_asset_change(
            task_instance=mock_task_instance, asset=asset, session=session
        )
        session.flush()

        # The event is written on the caller's session and carries a real id before any commit.
        assert event is not None
        assert event.id is not None
        event_id = event.id
        assert session.get(AssetEvent, event_id) is not None

        # Rolling back the caller's transaction discards the event -> no orphan row.
        session.rollback()
        assert session.get(AssetEvent, event_id) is None

    def test_queue_dagruns_calls_nonpartitioned_helper(self):
        """`_queue_dagruns` delegates non-partitioned dags to the single non-partitioned helper."""
        dag = DagModel(dag_id="dag1")
        session = mock.MagicMock(spec=Session)
        event = mock.MagicMock()
        with (
            mock.patch.object(AssetManager, "_queue_partitioned_dags"),
            mock.patch.object(AssetManager, "_queue_dagruns_nonpartitioned") as mock_helper,
        ):
            AssetManager._queue_dagruns(
                asset_id=1,
                dags_to_queue={dag},
                partition_key=None,
                partition_date=None,
                event=event,
                task_instance=None,
                session=session,
            )
        mock_helper.assert_called_once_with(1, {dag}, event, session)

    @pytest.mark.parametrize("dialect_name", ["postgresql", "sqlite"])
    def test_queue_dagruns_nonpartitioned_insert_or_ignore(self, dialect_name):
        """On postgres/sqlite the helper emits INSERT ... ON CONFLICT DO NOTHING referencing the event."""
        dag = DagModel(dag_id="dag1")
        session = mock.MagicMock(spec=Session)
        event = AssetEvent(asset_id=1)
        event.id = 99
        with mock.patch("airflow.assets.manager.get_dialect_name", return_value=dialect_name):
            AssetManager._queue_dagruns_nonpartitioned(
                asset_id=1, dags_to_queue={dag}, event=event, session=session
            )

        stmt, values = session.execute.call_args.args
        dialect = {"postgresql": postgresql.dialect(), "sqlite": sqlite.dialect()}[dialect_name]
        compiled = str(stmt.compile(dialect=dialect)).upper()
        assert "ON CONFLICT" in compiled
        assert "DO NOTHING" in compiled
        # One ADRQ row is inserted per (dag, event), carrying the denormalized asset_id
        # plus the referenced asset_event_id.
        assert values == [{"asset_id": 1, "target_dag_id": "dag1", "asset_event_id": 99}]

    def test_queue_dagruns_nonpartitioned_upsert_noop_on_mysql(self):
        """On MySQL the helper upserts with a no-op ON DUPLICATE KEY UPDATE (scoped to duplicate
        keys, unlike INSERT IGNORE) referencing the triggering event."""
        dag = DagModel(dag_id="dag1")
        session = mock.MagicMock(spec=Session)
        event = AssetEvent(asset_id=1)
        event.id = 99
        with mock.patch("airflow.assets.manager.get_dialect_name", return_value="mysql"):
            AssetManager._queue_dagruns_nonpartitioned(
                asset_id=1, dags_to_queue={dag}, event=event, session=session
            )

        stmt, values = session.execute.call_args.args
        compiled = str(stmt.compile(dialect=mysql.dialect())).upper()
        assert "ON DUPLICATE KEY UPDATE ASSET_ID = ASSET_DAG_RUN_QUEUE.ASSET_ID" in compiled
        assert values == [{"asset_id": 1, "target_dag_id": "dag1", "asset_event_id": 99}]

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

    def test_register_asset_change_defers_notifications_to_callback_sink(
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

        # When a callback_sink is supplied, listener notifications are collected into it
        # instead of firing inline, so the caller can run them after releasing the lock.
        callback_sink: list = []
        asset_manager.register_asset_change(
            task_instance=mock_task_instance,
            asset=asset,
            session=session,
            callback_sink=callback_sink,
        )
        session.flush()

        assert asset_listener.changed == []
        assert callback_sink

        # Running the collected callbacks fires the listeners.
        for callback in callback_sink:
            callback()
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
        testing_dag = DagModel(dag_id="testing_dag", is_stale=False, bundle_name="testing")
        session.add(testing_dag)
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
                    allow_reuse=True,
                    session=_session,
                ).id
            finally:
                _session.commit()
                _session.close()

        thread_count = 100
        with caplog.at_level(logging.DEBUG):
            with concurrent.futures.ThreadPoolExecutor(max_workers=thread_count) as pool:
                # Consume worker results before log assertions so worker exceptions propagate.
                ids = list(pool.map(lambda _: _get_or_create_apdr(), [None] * thread_count))

        counts = Counter(r.msg for r in caplog.records)
        assert (
            counts["No existing APDR found. Create APDR for key test_partition_key dag_id testing_dag"] == 1
        )
        # Every other thread reused it.
        assert counts["Existing APDR found for key test_partition_key dag_id testing_dag"] == thread_count - 1
        # Creation is guarded by a unique constraint rather than a mutex, so on a backend with
        # real write concurrency some threads lose the INSERT race, re-read, and join the
        # winner. That is the recovery branch doing its job; the counts above already pin the
        # outcome (exactly one creation, everyone else reusing). SQLite serialises writers, so
        # it normally records none of these.
        assert set(counts) <= {
            "No existing APDR found. Create APDR for key test_partition_key dag_id testing_dag",
            "Existing APDR found for key test_partition_key dag_id testing_dag",
            *(k for k in counts if k.startswith("Lost APDR race for key")),
        }

        assert len(set(ids)) == 1
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 1

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_get_or_create_apdr_reuses_winner_after_losing_insert_and_keeps_unflushed_rows(self, session):
        """An APDR INSERT that loses to the unique constraint reuses the row that won.

        Rolling back the savepoint must not expunge rows the caller added before and has not
        flushed, such as the ``Log`` rows ``_queue_partitioned_dags`` adds between APDRs.
        """
        testing_dag = DagModel(dag_id="testing_dag_lost_race", is_stale=False, bundle_name="testing")
        session.add(testing_dag)
        session.commit()
        log_row = Log(event="added before the savepoint")
        session.add(log_row)
        real_lookup = AssetManager._get_latest_pending_apdr

        def lookup_misses_concurrent_insert(*, target_key, target_dag_id, session):
            # Another writer inserts the row right after this lookup, so the INSERT that
            # follows collides with it on the unique constraint.
            lookup.side_effect = real_lookup
            session.execute(
                insert(AssetPartitionDagRun).values(
                    target_dag_id=target_dag_id, partition_key=target_key, pending_partition_key=target_key
                )
            )
            return None

        with mock.patch.object(
            AssetManager,
            "_get_latest_pending_apdr",
            autospec=True,
            side_effect=lookup_misses_concurrent_insert,
        ) as lookup:
            apdr = AssetManager._get_or_create_apdr(
                target_key="2026-05-20",
                target_partition_date=None,
                target_dag=testing_dag,
                rollup_fingerprint={},
                allow_reuse=True,
                session=session,
            )

        assert lookup.call_count == 2
        assert apdr.pending_partition_key == "2026-05-20"
        assert session.scalars(
            select(AssetPartitionDagRun.id).where(AssetPartitionDagRun.target_dag_id == testing_dag.dag_id)
        ).all() == [apdr.id]
        session.flush()
        assert session.scalar(select(Log.event).where(Log.id == log_row.id)) == "added before the savepoint"

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_get_or_create_apdr_suppresses_conflicting_partition_date(self, session):
        """Two events resolving the same target key to different dates → suppress to None.

        Rather than an order-dependent first-event-wins, conflicting carried dates produce a
        deterministic ``None`` so the consumer DagRun is not stamped with a wrong, unstable date.
        """
        testing_dag = DagModel(dag_id="testing_dag_pd_conflict", is_stale=False, bundle_name="testing")
        session.add(testing_dag)
        session.commit()
        fp = {"asset-1|test://asset1/": {"__type": "IdentityMapper", "__var": {}}}

        first = AssetManager._get_or_create_apdr(
            target_key="2026-05-20",
            target_partition_date=timezone.parse("2026-05-20T00:00:00"),
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            allow_reuse=True,
            session=session,
        )
        assert first.partition_date == timezone.parse("2026-05-20T00:00:00")

        # A second contributing event resolves the same key to a DIFFERENT date.
        second = AssetManager._get_or_create_apdr(
            target_key="2026-05-20",
            target_partition_date=timezone.parse("2026-05-21T00:00:00"),
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            allow_reuse=True,
            session=session,
        )
        assert second.id == first.id  # same pending APDR
        assert second.partition_date is None  # conflict suppressed, deterministic

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_get_or_create_apdr_keeps_agreeing_partition_date(self, session):
        """A later event carrying the same (or no) date does not trip the conflict suppression."""
        testing_dag = DagModel(dag_id="testing_dag_pd_agree", is_stale=False, bundle_name="testing")
        session.add(testing_dag)
        session.commit()
        fp = {"asset-1|test://asset1/": {"__type": "IdentityMapper", "__var": {}}}
        source_date = timezone.parse("2026-05-20T00:00:00")

        kwargs = dict(
            target_key="2026-05-20",
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            allow_reuse=True,
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

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_get_or_create_apdr_adopts_date_when_existing_is_none(self, session):
        """An APDR created with no date adopts a later event's carried date (not dropped)."""
        testing_dag = DagModel(dag_id="testing_dag_pd_adopt", is_stale=False, bundle_name="testing")
        session.add(testing_dag)
        session.commit()
        fp = {"asset-1|test://asset1/": {"__type": "IdentityMapper", "__var": {}}}
        source_date = timezone.parse("2026-05-20T00:00:00")

        kwargs = dict(
            target_key="2026-05-20",
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            allow_reuse=True,
            session=session,
        )
        # First event carries no date (e.g. producer had no partition_date).
        first = AssetManager._get_or_create_apdr(target_partition_date=None, **kwargs)
        assert first.partition_date is None
        # A later identity event carries a real date → adopted, not silently dropped.
        adopted = AssetManager._get_or_create_apdr(target_partition_date=source_date, **kwargs)
        assert adopted.id == first.id
        assert adopted.partition_date == source_date

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_get_or_create_apdr_recovers_after_conflict(self, session):
        """Once a conflict has suppressed the date to None, a later event re-adopts a date."""
        testing_dag = DagModel(dag_id="testing_dag_pd_recover", is_stale=False, bundle_name="testing")
        session.add(testing_dag)
        session.commit()
        fp = {"asset-1|test://asset1/": {"__type": "IdentityMapper", "__var": {}}}
        date_1 = timezone.parse("2026-05-20T00:00:00")
        date_2 = timezone.parse("2026-05-21T00:00:00")

        kwargs = dict(
            target_key="2026-05-20",
            target_dag=testing_dag,
            rollup_fingerprint=fp,
            allow_reuse=True,
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

    @pytest.mark.usefixtures("testing_dag_bundle")
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

    @pytest.mark.usefixtures("testing_dag_bundle")
    @pytest.mark.parametrize("allow_reuse", [True, False])
    def test_get_or_create_apdr_allow_reuse(self, session, allow_reuse):
        clear_db_apdr()
        clear_db_pakl()
        dag = DagModel(dag_id="reuse_test_dag", is_stale=False, bundle_name="testing")
        session.add(dag)
        session.flush()

        with capture_orm_selects("asset_partition_dag_run") as lookups:
            apdrs = [
                AssetManager._get_or_create_apdr(
                    target_key="key-1",
                    target_partition_date=None,
                    target_dag=dag,
                    rollup_fingerprint={},
                    allow_reuse=allow_reuse,
                    session=session,
                )
                for _ in range(2)
            ]

        # Two calls. With reuse the first finds nothing and creates (1 unlocked lookup); the
        # second finds the row and then re-reads it by primary key under a share lock before
        # reusing it (1 unlocked lookup + 1 locking read) -- hence 3, not 2. Without reuse
        # nothing is looked up at all. The locking read is the lifetime guarantee: see
        # ``AssetManager._lock_pending_apdr``.
        assert len(lookups) == (3 if allow_reuse else 0), [str(q) for q in lookups]
        locking_reads = [q for q in lookups if "SHARE" in str(q).upper() or "FOR UPDATE" in str(q).upper()]
        assert len(locking_reads) == (1 if allow_reuse else 0), (
            f"expected exactly one locking re-read on the reuse path, got {locking_reads}"
        )
        expected_count = 1 if allow_reuse else 2
        assert len({apdr.id for apdr in apdrs}) == expected_count
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == expected_count
        assert all(apdr.created_dag_run_id is None for apdr in apdrs)
        # Only a reusable row takes part in the one-pending-row-per-key constraint.
        assert {apdr.pending_partition_key for apdr in apdrs} == {"key-1" if allow_reuse else None}

    @pytest.mark.backend("postgres", "mysql")
    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_concurrent_events_from_different_assets_share_one_pending_apdr(self, dag_maker, session):
        """Events from two producer assets racing on one partition key accumulate on one APDR.

        If the second writer looks the key up before the first writer commits its APDR, it finds
        nothing and inserts its own. Two pending APDRs holding one event each can
        never satisfy ``asset_a & asset_b``, and the consumer Dag never runs.
        """
        _clear_partition_db()
        try:
            asset_a = Asset(name="race-asset-a")
            asset_b = Asset(name="race-asset-b")
            with dag_maker(
                dag_id="race-consumer",
                schedule=PartitionedAssetTimetable(
                    assets=asset_a & asset_b, default_partition_mapper=IdentityMapper()
                ),
                serialized=True,
            ):
                EmptyOperator(task_id="hi")
            dag_maker.sync_dagbag_to_db()
            session.commit()

            test_thread = threading.get_ident()
            second_writer_inserting_apdr = threading.Event()

            def on_execute(conn, cursor, statement, parameters, context, executemany):
                if threading.get_ident() != test_thread and statement.startswith(
                    "INSERT INTO asset_partition_dag_run "
                ):
                    second_writer_inserting_apdr.set()

            def run_second_writer():
                with create_session(scoped=False) as second:
                    AssetManager.register_asset_change(asset=asset_b, partition_key="day-1", session=second)

            event.listen(settings.engine, "before_cursor_execute", on_execute)
            try:
                with create_session(scoped=False) as first:
                    AssetManager.register_asset_change(asset=asset_a, partition_key="day-1", session=first)
                    first.flush()
                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                        second_done = pool.submit(run_second_writer)
                        assert second_writer_inserting_apdr.wait(timeout=30)
                        first.commit()
                        second_done.result(timeout=30)
            finally:
                event.remove(settings.engine, "before_cursor_execute", on_execute)

            pending = session.scalars(
                select(AssetPartitionDagRun).where(AssetPartitionDagRun.target_dag_id == "race-consumer")
            ).all()
            assert len(pending) == 1
            logged_events = session.scalar(
                select(func.count())
                .select_from(PartitionedAssetKeyLog)
                .where(PartitionedAssetKeyLog.asset_partition_dag_run_id == pending[0].id)
            )
            assert logged_events == 2
        finally:
            _clear_partition_db()

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_get_or_create_apdr_inserts_again_when_winner_was_consumed(self, session, dag_maker):
        """A winner turned into a Dag run before the loser looks it up has freed the key, so the loser inserts anew."""
        with dag_maker(dag_id="consumed_winner_producer", serialized=True):
            EmptyOperator(task_id="hi")
        dag_run = dag_maker.create_dagrun()
        testing_dag = DagModel(dag_id="testing_dag_consumed_winner", is_stale=False, bundle_name="testing")
        session.add(testing_dag)
        winner = AssetPartitionDagRun(
            target_dag_id=testing_dag.dag_id, partition_key="k", pending_partition_key="k"
        )
        session.add(winner)
        session.commit()
        lookups = []

        def lookup_misses_then_consumes_winner(*, target_key, target_dag_id, session):
            if lookups:
                winner.created_dag_run_id = dag_run.id
                session.flush()
            lookups.append(target_key)
            return None

        with mock.patch.object(
            AssetManager,
            "_get_latest_pending_apdr",
            autospec=True,
            side_effect=lookup_misses_then_consumes_winner,
        ):
            apdr = AssetManager._get_or_create_apdr(
                target_key="k",
                target_partition_date=None,
                target_dag=testing_dag,
                rollup_fingerprint={},
                allow_reuse=True,
                session=session,
            )
        session.flush()

        assert apdr.id != winner.id
        assert apdr.pending_partition_key == "k"
        assert dict(
            session.execute(
                select(AssetPartitionDagRun.id, AssetPartitionDagRun.pending_partition_key).where(
                    AssetPartitionDagRun.target_dag_id == testing_dag.dag_id
                )
            ).all()
        ) == {winner.id: None, apdr.id: "k"}

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_get_or_create_apdr_reraises_when_insert_keeps_conflicting(self, session):
        testing_dag = DagModel(dag_id="testing_dag_no_winner", is_stale=False, bundle_name="testing")
        session.add(testing_dag)
        session.commit()
        session.add(
            AssetPartitionDagRun(
                target_dag_id=testing_dag.dag_id, partition_key="k", pending_partition_key="k"
            )
        )
        session.flush()

        with mock.patch.object(AssetManager, "_get_latest_pending_apdr", autospec=True, return_value=None):
            with pytest.raises(exc.IntegrityError):
                AssetManager._get_or_create_apdr(
                    target_key="k",
                    target_partition_date=None,
                    target_dag=testing_dag,
                    rollup_fingerprint={},
                    allow_reuse=True,
                    session=session,
                )

        assert (
            session.scalar(
                select(func.count())
                .select_from(AssetPartitionDagRun)
                .where(AssetPartitionDagRun.target_dag_id == testing_dag.dag_id)
            )
            == 1
        )

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_queue_dagruns_creates_apdrs_in_dag_id_order(self, session, dag_maker):
        """Partitioned consumers get their APDRs in dag_id order whatever order the set iterates in."""
        _clear_partition_db()
        try:
            asset = Asset(name="ordered-asset")
            for dag_id in ("z-consumer", "a-consumer"):
                with dag_maker(
                    dag_id=dag_id,
                    schedule=PartitionedAssetTimetable(
                        assets=asset, default_partition_mapper=IdentityMapper()
                    ),
                    serialized=True,
                ):
                    EmptyOperator(task_id="hi")
            dag_maker.sync_dagbag_to_db()
            session.commit()

            class ReverseOrderSet(set):
                def __iter__(self):
                    return iter(sorted(super().__iter__(), key=lambda d: d.dag_id, reverse=True))

            real_queue_dagruns = AssetManager._queue_dagruns

            def queue_dagruns_in_reverse_order(*, dags_to_queue, **kwargs):
                real_queue_dagruns(dags_to_queue=ReverseOrderSet(dags_to_queue), **kwargs)

            with mock.patch.object(
                AssetManager, "_queue_dagruns", autospec=True, side_effect=queue_dagruns_in_reverse_order
            ):
                AssetManager.register_asset_change(asset=asset, partition_key="day-1", session=session)
            session.flush()

            assert session.scalars(
                select(AssetPartitionDagRun.target_dag_id).order_by(AssetPartitionDagRun.id)
            ).all() == ["a-consumer", "z-consumer"]
        finally:
            _clear_partition_db()

    @pytest.mark.need_serialized_dag
    @pytest.mark.parametrize(
        ("consumer_dag_ids", "expected"),
        [
            pytest.param(
                ["fanout-consumer"],
                [("fanout-consumer", "k-1"), ("fanout-consumer", "k-2"), ("fanout-consumer", "k-3")],
                id="single-consumer",
            ),
            pytest.param(
                ["z-fanout-consumer", "a-fanout-consumer"],
                [
                    ("a-fanout-consumer", "k-1"),
                    ("a-fanout-consumer", "k-2"),
                    ("a-fanout-consumer", "k-3"),
                    ("z-fanout-consumer", "k-1"),
                    ("z-fanout-consumer", "k-2"),
                    ("z-fanout-consumer", "k-3"),
                ],
                id="multiple-consumers",
            ),
        ],
    )
    @mock.patch.object(IdentityMapper, "to_downstream", autospec=True, return_value=["k-3", "k-2", "k-1"])
    def test_partition_fanout_creates_apdrs_in_key_order(
        self, mock_to_downstream, session, dag_maker, consumer_dag_ids, expected
    ):
        _clear_partition_db()
        try:
            asset = Asset(name="fanout-order-asset")
            for dag_id in consumer_dag_ids:
                with dag_maker(
                    dag_id=dag_id,
                    schedule=PartitionedAssetTimetable(assets=asset, batch_asset_events=True),
                    session=session,
                ):
                    EmptyOperator(task_id="consume")
            with dag_maker(
                dag_id="fanout-producer", schedule=PartitionedAtRuntime(), session=session
            ) as producer:
                EmptyOperator(task_id="emit", outlets=[asset])
            dag_run = dag_maker.create_dagrun(partition_key="source-key", session=session)
            [ti] = dag_run.get_task_instances(session=session)

            TaskInstance.register_asset_changes_in_db(
                ti=ti,
                task_outlets=[outlet.asprofile() for outlet in producer.get_task("emit").outlets],
                outlet_events=[],
                session=session,
            )
            session.flush()

            assert (
                session.execute(
                    select(
                        AssetPartitionDagRun.target_dag_id, AssetPartitionDagRun.pending_partition_key
                    ).order_by(AssetPartitionDagRun.id)
                ).all()
                == expected
            )
            assert (
                session.execute(
                    select(
                        PartitionedAssetKeyLog.target_dag_id, PartitionedAssetKeyLog.target_partition_key
                    ).order_by(PartitionedAssetKeyLog.id)
                ).all()
                == expected
            )
            assert mock_to_downstream.call_count == len(consumer_dag_ids)
        finally:
            session.rollback()
            _clear_partition_db()

    def test_assigning_created_dag_run_id_clears_pending_partition_key(self):
        apdr = AssetPartitionDagRun(target_dag_id="d", partition_key="k", pending_partition_key="k")

        apdr.created_dag_run_id = 123

        assert apdr.pending_partition_key is None

    @pytest.mark.need_serialized_dag
    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_queue_partitioned_dags_stamps_rollup_fingerprint(self, session, dag_maker):
        """APDR created by _queue_partitioned_dags is stamped with the correct rollup fingerprint."""
        from airflow.sdk import Asset, HourWindow, RollupMapper, StartOfHourMapper
        from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
        from airflow.timetables.base import compute_rollup_fingerprint

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

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_partitioned_asset_event_does_not_trigger_non_partitioned_dag(self, session, mock_task_instance):
        """partitioned asset events (events with partition key) must not queue non-partition-aware Dags."""
        asm = AssetModel(uri="test://asset/", name="test_asset", group="asset")
        session.add(asm)
        dag = DagModel(
            dag_id="consumer_dag", is_paused=False, bundle_name="testing", timetable_partitioned=False
        )
        session.add(dag)
        asm.scheduled_dags = [DagScheduleAssetReference(dag_id=dag.dag_id)]
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
    @pytest.mark.usefixtures("testing_dag_bundle")
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
    @pytest.mark.usefixtures("testing_dag_bundle")
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
    @pytest.mark.usefixtures("testing_dag_bundle")
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
    @pytest.mark.usefixtures("testing_dag_bundle")
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
    @pytest.mark.usefixtures("testing_dag_bundle")
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

        ti = dag_maker.create_dagrun().task_instances[0]

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


@pytest.mark.usefixtures("testing_dag_bundle")
class TestApdrAcquisitionConcurrency:
    """
    Two producers reusing one pending APDR, driven through the real acquisition helper.

    ``TestAssetPartitionDateConcurrency`` calls ``_reconcile_partition_date`` directly and so
    never acquires the producer row lock. That is why it could not see the regression this
    class pins: a shared lock taken before reconciliation has to be *upgraded* when a date
    turns out to need writing, and on MySQL two holders of the shared record lock that both
    need the exclusive one deadlock (1213) -- on a single row, with agreeing dates, where no
    key ordering can help. ``_lock_pending_apdr`` therefore picks its strength up front from
    the incoming date, avoiding conversion within this acquisition.

    "Committed" here means a committed ``PartitionedAssetKeyLog`` row for that producer's own
    event, not a returned ORM object.
    """

    D1 = timezone.datetime(2026, 1, 1)
    D2 = timezone.datetime(2026, 2, 2)
    D3 = timezone.datetime(2026, 3, 3)

    @pytest.fixture(autouse=True)
    def _skip_without_write_concurrency(self, session):
        if session.bind.dialect.name == "sqlite":
            pytest.skip("SQLite serialises writers, so the two-transaction schedule cannot run")

    @pytest.fixture
    def fixtures(self, session):
        clear_db_apdr()
        clear_db_pakl()
        asset = AssetModel(uri="test://acq/", name="acq_asset", group="asset")
        dag = DagModel(dag_id="acq_dag", is_stale=False, bundle_name="testing")
        session.add_all([asset, dag])
        session.commit()
        # Distinct events so each producer's link is individually identifiable.
        events = [AssetEvent(asset_id=asset.id, extra={}) for _ in range(2)]
        session.add_all(events)
        session.commit()
        yield asset.id, dag.dag_id, [e.id for e in events]
        clear_db_apdr()
        clear_db_pakl()
        session.rollback()
        session.execute(delete(AssetEvent).where(AssetEvent.asset_id == asset.id))
        session.execute(delete(DagModel).where(DagModel.dag_id == "acq_dag"))
        session.execute(delete(AssetModel).where(AssetModel.name == "acq_asset"))
        session.commit()

    @staticmethod
    def _run_pair(session, asset_id, dag_id, event_ids, incoming):
        """Run two producers concurrently; rendezvous inside the acquisition path."""
        barrier = threading.Barrier(2)
        results: dict[str, dict] = {"A": {}, "B": {}}
        guard = threading.Lock()
        real_reconcile = AssetManager._reconcile_partition_date.__func__

        def wrapped(cls, **kwargs):
            name = threading.current_thread().name
            try:
                # Advisory. If the other producer is blocked on an exclusive row lock it can
                # never arrive, and that serialisation is the intended behaviour -- so the
                # rendezvous is recorded, not required.
                barrier.wait(timeout=2.0)
                overlapped = True
            except threading.BrokenBarrierError:
                overlapped = False
            with guard:
                results[name]["overlapped"] = overlapped
            return real_reconcile(cls, **kwargs)

        def worker(name, event_id, date):
            res = results[name]
            s = settings.Session.session_factory()
            s.begin()
            try:
                res["conn"] = id(s.connection().connection.dbapi_connection)
                apdr = AssetManager._get_or_create_apdr(
                    target_key="acq-key",
                    target_partition_date=date,
                    target_dag=s.get(DagModel, dag_id),
                    rollup_fingerprint={},
                    allow_reuse=True,
                    session=s,
                )
                s.add(
                    PartitionedAssetKeyLog(
                        asset_id=asset_id,
                        asset_event_id=event_id,
                        asset_partition_dag_run_id=apdr.id,
                        target_dag_id=dag_id,
                        target_partition_key="acq-key",
                        source_partition_key="acq-key",
                    )
                )
                s.commit()
                res["committed"] = True
            except Exception as exc:
                orig = getattr(exc, "orig", None)
                res["error"] = f"{type(exc).__name__}:{getattr(orig, 'args', (None,))[0]}"
                res["committed"] = False
                s.rollback()
            finally:
                s.close()

        threads = [
            threading.Thread(target=worker, args=(n, e, d), name=n)
            for n, e, d in zip(("A", "B"), event_ids, incoming)
        ]
        with mock.patch.object(AssetManager, "_reconcile_partition_date", classmethod(wrapped)):
            for t in threads:
                t.start()
            for t in threads:
                t.join(90)
        assert not [t.name for t in threads if t.is_alive()], f"a producer hung: {results}"
        session.rollback()
        return results

    @pytest.mark.parametrize(
        ("seed", "incoming", "marked", "allowed_final"),
        [
            # The deadlocking cases: a date has to be written onto the reused row.
            pytest.param(None, ("D1", "D1"), True, ("D1",), id="undated-same-date-marked"),
            pytest.param(None, ("D1", "D1"), False, ("D1",), id="undated-same-date-unmarked"),
            pytest.param(None, ("D2", "D3"), True, (None,), id="undated-conflicting"),
            # Either order is valid: A-first gives NULL then D3, B-first NULL then D2.
            # Which one wins is decided by lock order, not by which thread started first.
            pytest.param("D1", ("D2", "D3"), True, ("D2", "D3"), id="dated-conflicting"),
            pytest.param("D1", ("D1", "D1"), True, ("D1",), id="dated-same-date"),
            # Controls that passed even with the shared lock; kept so a fix cannot regress them.
            pytest.param("D1", (None, None), True, ("D1",), id="dated-no-incoming"),
            pytest.param(None, (None, None), True, (None,), id="undated-no-incoming"),
        ],
    )
    def test_two_producers_reusing_one_apdr_both_commit(
        self, session, fixtures, seed, incoming, marked, allowed_final
    ):
        asset_id, dag_id, event_ids = fixtures
        names = {"D1": self.D1, "D2": self.D2, "D3": self.D3, None: None}
        row = AssetPartitionDagRun(
            target_dag_id=dag_id,
            created_dag_run_id=None,
            partition_key="acq-key",
            pending_partition_key="acq-key" if marked else None,
            partition_date=names[seed],
            rollup_fingerprint={},
        )
        session.add(row)
        session.commit()
        apdr_id = row.id

        results = self._run_pair(session, asset_id, dag_id, event_ids, [names[i] for i in incoming])

        conns = [results[n].get("conn") for n in ("A", "B")]
        assert None not in conns
        assert len(set(conns)) == 2, f"producers shared a connection: {results}"
        assert all(results[n].get("committed") for n in ("A", "B")), f"a producer failed to commit: {results}"

        links = session.scalars(
            select(PartitionedAssetKeyLog.asset_event_id).order_by(PartitionedAssetKeyLog.asset_event_id)
        ).all()
        assert sorted(links) == sorted(event_ids), (
            f"expected one committed link per producer event, got {list(links)}"
        )
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 1
        # Read the date with a Core select rather than ``session.get``. This session added the
        # row itself, so ``get`` can answer from the identity map with the pre-workers value --
        # an earlier version of this assertion failed against a database that was in fact
        # correct.
        final = session.scalar(
            select(AssetPartitionDagRun.partition_date).where(AssetPartitionDagRun.id == apdr_id)
        )
        allowed = {names[x] for x in allowed_final}
        assert final in allowed, (
            f"seed={seed} incoming={incoming} final={final} not in {allowed} results={results}"
        )

        # The lock strength must match what the producers carried: exclusive when a date may
        # be written (so they cannot overlap), shared otherwise.
        carries_date = any(i is not None for i in incoming)
        overlapped = any(results[n].get("overlapped") for n in ("A", "B"))
        assert overlapped is not carries_date, (
            f"carries_date={carries_date} but overlapped={overlapped}; the lock strength "
            f"decision did not take effect: {results}"
        )

    def test_locking_read_repopulates_the_cached_row(self, session, fixtures):
        """The value read after the lock must be the locked value, not the cached one.

        The unlocked candidate lookup puts the row in the identity map. Without
        ``populate_existing`` the locking SELECT hands back that same instance with its
        pre-lock columns, so the helper would decide from a value the lock does not protect.
        """
        asset_id, dag_id, _events = fixtures
        row = AssetPartitionDagRun(
            target_dag_id=dag_id,
            created_dag_run_id=None,
            partition_key="acq-key",
            pending_partition_key="acq-key",
            partition_date=self.D1,
            rollup_fingerprint={},
        )
        session.add(row)
        session.commit()
        apdr_id = row.id

        producer = settings.Session.session_factory()
        producer.begin()
        try:
            candidate = AssetManager._get_latest_pending_apdr(
                target_key="acq-key", target_dag_id=dag_id, session=producer
            )
            assert candidate is not None
            assert candidate.partition_date == self.D1

            # A competitor commits a different date on its own connection, and is allowed to
            # finish -- this is a staged interleaving, not a timing race.
            other = settings.Session.session_factory()
            other.begin()
            try:
                other.execute(
                    update(AssetPartitionDagRun)
                    .where(AssetPartitionDagRun.id == apdr_id)
                    .values(partition_date=self.D2)
                )
                other.commit()
            finally:
                other.close()

            locked = AssetManager._lock_pending_apdr(
                apdr_id=apdr_id,
                target_key="acq-key",
                target_dag_id=dag_id,
                for_write=True,
                session=producer,
            )
            assert locked is not None
            independent = producer.scalar(
                select(AssetPartitionDagRun.partition_date).where(AssetPartitionDagRun.id == apdr_id)
            )
            assert independent == self.D2, "the competitor's write is not visible at all"
            assert locked.partition_date == self.D2, (
                "the locking read returned the identity-map instance with its pre-lock date; "
                "populate_existing is missing"
            )
        finally:
            producer.rollback()
            producer.close()


@pytest.mark.usefixtures("testing_dag_bundle")
class TestAssetPartitionDateConcurrency:
    """
    Committed counterparts of the standalone date/lifecycle controls.

    Two producers can reuse the same pending APDR and carry different ``partition_date``
    values. ``_reconcile_partition_date`` resolves that inside one conditional UPDATE so the
    database serialises the decision; a Python read-modify-write would let whichever
    transaction committed last win, which is order-dependent and therefore not reproducible.

    Each schedule runs on genuinely independent sessions obtained from
    ``settings.Session.session_factory()`` -- ``settings.Session`` is a ``scoped_session``, so
    calling it twice in one thread returns the *same* session and would prove nothing.
    """

    D1 = timezone.datetime(2026, 1, 1)
    D2 = timezone.datetime(2026, 2, 2)
    D3 = timezone.datetime(2026, 3, 3)

    @pytest.fixture(autouse=True)
    def _skip_without_write_concurrency(self, session):
        if session.bind.dialect.name == "sqlite":
            pytest.skip("SQLite serialises writers, so the two-transaction schedule cannot run")

    @pytest.fixture
    def seeded(self, session):
        """Create the shared target and return ``(apdr_id, dag, asset)``."""
        clear_db_apdr()
        clear_db_pakl()
        asset = AssetModel(uri="test://date_conc/", name="date_conc_asset", group="asset")
        dag = DagModel(dag_id="date_conc_dag", is_stale=False, bundle_name="testing")
        session.add_all([asset, dag])
        session.commit()
        yield asset, dag
        clear_db_apdr()
        clear_db_pakl()
        session.rollback()
        session.execute(delete(DagModel).where(DagModel.dag_id == "date_conc_dag"))
        session.execute(delete(AssetModel).where(AssetModel.name == "date_conc_asset"))
        session.commit()

    def _seed_apdr(self, session, dag, partition_date):
        row = AssetPartitionDagRun(
            target_dag_id=dag.dag_id,
            created_dag_run_id=None,
            partition_key="date-conc-key",
            pending_partition_key="date-conc-key",
            partition_date=partition_date,
            rollup_fingerprint={},
        )
        session.add(row)
        session.commit()
        return row.id

    def _carry_concurrently(self, apdr_id, dag, dates):
        """Both workers read the row, then each carries its own date. Returns observations."""
        barrier = threading.Barrier(len(dates))
        observed: dict[datetime | None, Any] = {}
        lock = threading.Lock()

        def carry(incoming):
            s = settings.Session.session_factory()
            s.begin()
            try:
                row = s.get(AssetPartitionDagRun, apdr_id)
                assert row is not None, "the seeded row disappeared before the schedule ran"
                with lock:
                    observed[incoming] = {"seen": row.partition_date, "ran": False}
                # Both transactions must have read the pre-existing value before either
                # writes; otherwise this degenerates into two sequential carries.
                barrier.wait(timeout=30)
                valid = AssetManager._reconcile_partition_date(
                    apdr=row,
                    target_partition_date=incoming,
                    target_dag_id=dag.dag_id,
                    target_key="date-conc-key",
                    session=s,
                )
                with lock:
                    observed[incoming]["ran"] = True
                    observed[incoming]["valid"] = valid
                s.commit()
            finally:
                s.close()

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(dates)) as pool:
            # list() re-raises anything a worker hit, including BrokenBarrierError. An earlier
            # standalone version of this schedule swallowed those and reported success while
            # every worker had in fact died.
            list(pool.map(carry, dates))
        assert len(observed) == len(dates)
        assert all(o["ran"] for o in observed.values()), f"a worker never reconciled: {observed}"
        return observed

    def test_conflicting_dates_onto_an_undated_row_suppress_to_null(self, session, seeded):
        """C1: both carries disagree, so neither may win.

        Seeded NULL, so a stale read-modify-write would have *both* workers adopt their own
        date and leave an order-dependent winner. The end state must be NULL.
        """
        _asset, dag = seeded
        apdr_id = self._seed_apdr(session, dag, None)

        observed = self._carry_concurrently(apdr_id, dag, [self.D2, self.D3])
        assert all(o["seen"] is None for o in observed.values()), (
            f"workers did not both observe the seeded NULL: {observed}"
        )

        session.rollback()
        final = session.get(AssetPartitionDagRun, apdr_id).partition_date
        assert final is None, f"conflicting dates left {final!r} instead of suppressing to NULL"

    def test_conflicting_dates_onto_a_dated_row_do_not_suppress_to_null(self, session, seeded):
        """C2: the mirror case, which C1 alone cannot distinguish.

        Seeded with ``D1`` and carrying ``D2``/``D3``. A stale read-modify-write suppresses
        both to NULL here, so asserting "not NULL" rejects exactly what C1 cannot.
        """
        _asset, dag = seeded
        apdr_id = self._seed_apdr(session, dag, self.D1)

        self._carry_concurrently(apdr_id, dag, [self.D2, self.D3])

        session.rollback()
        final = session.get(AssetPartitionDagRun, apdr_id).partition_date
        assert final != self.D1, "the seeded date survived two disagreeing carries"
        assert final in (self.D2, self.D3), (
            f"a disagreement against an already-dated row left {final!r}; the row had a date, "
            "so the first carry suppresses it and the second adopts its own"
        )

    def test_matching_date_is_adopted_onto_an_undated_row(self, session, seeded):
        """C3: positive control -- without it, code that never writes passes C1."""
        _asset, dag = seeded
        apdr_id = self._seed_apdr(session, dag, None)

        s = settings.Session.session_factory()
        s.begin()
        try:
            row = s.get(AssetPartitionDagRun, apdr_id)
            assert (
                AssetManager._reconcile_partition_date(
                    apdr=row,
                    target_partition_date=self.D2,
                    target_dag_id=dag.dag_id,
                    target_key="date-conc-key",
                    session=s,
                )
                is True
            )
            s.commit()
        finally:
            s.close()

        session.rollback()
        assert session.get(AssetPartitionDagRun, apdr_id).partition_date == self.D2


class _DescendingDagIdSet(set):
    """A ``set`` that always iterates in descending ``dag_id`` order.

    ``_queue_dagruns`` takes a real set and calls ``difference`` on it, so the substitute has
    to stay a set. Only iteration order is pinned, which is what the production sort consumes.
    """

    def __iter__(self):
        return iter(sorted(set.__iter__(self), key=lambda dag: dag.dag_id, reverse=True))


@pytest.mark.usefixtures("testing_dag_bundle")
class TestAssetPartitionDagRunPendingMarker:
    """
    Cover the ``pending_partition_key`` dedup marker.

    The marker guards *batched* pending rows only. Each test below pins one edge of that
    invariant; several of them fail on the pre-marker implementation, which is noted per test.
    """

    FINGERPRINT = {"asset-1|test://marker/": {"__type": "IdentityMapper", "__var": {}}}

    @pytest.fixture
    def fixture(self, session, request):
        clear_db_apdr()
        clear_db_pakl()
        # Unique per test so rows from a sibling test cannot collide on the primary key or
        # be picked up by the lookups under test.
        suffix = request.node.name.replace("[", "_").replace("]", "")[:40]
        asset = AssetModel(uri=f"test://marker/{suffix}", name=f"marker_asset_{suffix}", group="asset")
        dag = DagModel(dag_id=f"marker_dag_{suffix}", is_stale=False, bundle_name="testing")
        session.add_all([asset, dag])
        session.commit()
        yield asset, dag
        clear_db_pakl()
        clear_db_apdr()

    def _call(self, session, dag, asset, *, key="k", allow_reuse, date=None):
        return AssetManager._get_or_create_apdr(
            target_key=key,
            target_partition_date=date,
            target_dag=dag,
            rollup_fingerprint=self.FINGERPRINT,
            allow_reuse=allow_reuse,
            session=session,
        )

    @staticmethod
    def _make_dag_run_id(session, dag_maker) -> int:
        """Create a real DagRun and return its id (APDR.created_dag_run_id is a FK to dag_run.id)."""
        with dag_maker(dag_id=f"marker_run_src_{uuid4().hex[:8]}", session=session):
            EmptyOperator(task_id="t")
        dr = dag_maker.create_dagrun()
        session.flush()
        return dr.id

    def test_non_batched_rows_get_null_markers_and_stay_distinct(self, session, fixture):
        """allow_reuse=False keeps one row per event; markers stay NULL so the guard is bypassed.

        Fails with an unconditional marker: the second insert violates the constraint.
        """
        asset, dag = fixture
        first = self._call(session, dag, asset, allow_reuse=False)
        second = self._call(session, dag, asset, allow_reuse=False)

        assert first.id != second.id
        assert first.pending_partition_key is None
        assert second.pending_partition_key is None
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 2

    def test_batched_row_is_marked(self, session, fixture):
        """allow_reuse=True marks the row it creates, enrolling it in the guard."""
        asset, dag = fixture
        apdr = self._call(session, dag, asset, allow_reuse=True)
        assert apdr.pending_partition_key == "k"

    def test_duplicate_marked_pending_row_is_rejected_by_the_database(self, session, fixture):
        """The constraint — not a lock — is what prevents a second marked pending row.

        This is the #71070 defect: two producer assets previously took two different
        AssetModel locks and both inserted.
        """
        asset, dag = fixture
        self._call(session, dag, asset, allow_reuse=True)
        session.flush()

        duplicate = AssetPartitionDagRun(
            target_dag_id=dag.dag_id,
            created_dag_run_id=None,
            partition_key="k",
            pending_partition_key="k",
            rollup_fingerprint=self.FINGERPRINT,
        )
        session.add(duplicate)
        with pytest.raises(exc.IntegrityError):
            session.flush()
        session.rollback()

    def test_completion_clears_the_marker_and_frees_the_key(self, session, fixture, dag_maker):
        """Completing a row releases its marker so the next batched event can be marked."""
        asset, dag = fixture
        first = self._call(session, dag, asset, allow_reuse=True)
        assert first.pending_partition_key == "k"

        first.created_dag_run_id = self._make_dag_run_id(session, dag_maker)  # scheduler's ORM assignment
        session.flush()
        assert first.pending_partition_key is None

        second = self._call(session, dag, asset, allow_reuse=True)
        assert second.id != first.id
        assert second.pending_partition_key == "k"

    def test_legacy_null_marker_row_is_reused_without_promotion(self, session, fixture):
        """A pre-migration row has a NULL marker; batched events join it and leave it NULL.

        Promotion is what must *not* happen — see the toggle test for why.
        """
        asset, dag = fixture
        legacy = AssetPartitionDagRun(
            target_dag_id=dag.dag_id,
            created_dag_run_id=None,
            partition_key="k",
            pending_partition_key=None,
            rollup_fingerprint=self.FINGERPRINT,
        )
        session.add(legacy)
        session.flush()

        reused = self._call(session, dag, asset, allow_reuse=True)
        assert reused.id == legacy.id
        assert reused.pending_partition_key is None
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 1

    def test_batching_toggle_does_not_promote_a_marker_onto_a_colliding_row(self, session, fixture):
        """enabled -> disabled -> enabled leaves an older marked row and a newer unmarked one.

        Variant B selects the newer unmarked row. Promoting its marker would collide with the
        older row, so the implementation must reuse it as-is.
        """
        asset, dag = fixture
        marked = self._call(session, dag, asset, allow_reuse=True)  # batching on
        assert marked.pending_partition_key == "k"
        unmarked = self._call(session, dag, asset, allow_reuse=False)  # batching off
        assert unmarked.pending_partition_key is None

        reused = self._call(session, dag, asset, allow_reuse=True)  # batching on again
        assert reused.id == unmarked.id
        assert reused.pending_partition_key is None
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 2

    def test_older_pending_behind_newer_completed_is_reused(self, session, fixture, dag_maker):
        """Documented divergence from the pre-marker lookup.

        The old code ordered by id and reused only if the newest row was pending, so it would
        create a third row here. Selecting the newest *pending* row reuses the older one, which
        is required: creating another marked row could collide with the older row's marker.
        """
        asset, dag = fixture
        older_pending = self._call(session, dag, asset, allow_reuse=False)
        newer = self._call(session, dag, asset, allow_reuse=False)
        newer.created_dag_run_id = self._make_dag_run_id(session, dag_maker)
        session.flush()

        reused = self._call(session, dag, asset, allow_reuse=True)
        assert reused.id == older_pending.id
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 2

    def test_unrelated_integrity_error_is_not_swallowed(self, session, fixture):
        """Only the marker constraint means "lost the race"; anything else must propagate.

        Guards against the recovery branch turning a foreign-key failure into silent reuse.
        """
        asset, dag = fixture
        unrelated = exc.IntegrityError(
            "INSERT ...", {}, Exception("FOREIGN KEY constraint failed: some_other_fkey")
        )
        with mock.patch.object(Session, "flush", side_effect=unrelated):
            with pytest.raises(exc.IntegrityError, match="some_other_fkey"):
                self._call(session, dag, asset, allow_reuse=True)
        session.rollback()

    def test_recovers_when_the_winner_completes_before_the_re_read(self, session, fixture, dag_maker):
        """The losing insert must tolerate the winner vanishing from the pending set.

        A single re-select raised RuntimeError here; the bounded loop re-reads, misses, and
        inserts a fresh row instead.
        """
        asset, dag = fixture
        winner = self._call(session, dag, asset, allow_reuse=True)
        session.flush()
        assert winner.pending_partition_key == "k"

        run_id = self._make_dag_run_id(session, dag_maker)
        real_lookup = AssetManager._get_latest_pending_apdr
        calls = {"n": 0}

        def staged_lookup(*, target_key, target_dag_id, session):
            calls["n"] += 1
            if calls["n"] == 1:
                # Stale read: the producer misses the winner and proceeds to INSERT, which
                # the real unique constraint rejects.
                return None
            if calls["n"] == 2:
                # The scheduler completes the winner in the window between that rejection and
                # this recovery read, so there is no pending row left to join.
                winner.created_dag_run_id = run_id
                session.flush()
                return None
            return real_lookup(target_key=target_key, target_dag_id=target_dag_id, session=session)

        with mock.patch.object(AssetManager, "_get_latest_pending_apdr", staticmethod(staged_lookup)):
            recovered = self._call(session, dag, asset, allow_reuse=True)

        # The conflict branch ran (two staged reads consumed) and recovery produced a new row
        # rather than raising, which is what a single re-select could not do.
        assert calls["n"] >= 2
        assert recovered.id != winner.id
        assert recovered.pending_partition_key == "k"
        assert winner.pending_partition_key is None

    def test_retry_exhaustion_preserves_integrity_error(self, session, fixture):
        """Sustained real conflict with nothing to join surfaces an actionable error, not a spin.

        The conflict comes from the actual unique constraint, not a synthetic exception: a
        marked pending row exists but the lookup never returns it, so every attempt genuinely
        violates the database and the loop runs out of attempts. This mirrors the stale-marker
        state a mixed-version writer can leave behind.
        """
        asset, dag = fixture
        session.add(
            AssetPartitionDagRun(
                target_dag_id=dag.dag_id,
                created_dag_run_id=None,
                partition_key="k",
                pending_partition_key="k",
                rollup_fingerprint=self.FINGERPRINT,
            )
        )
        session.flush()

        with mock.patch.object(AssetManager, "_get_latest_pending_apdr", staticmethod(lambda **kw: None)):
            with pytest.raises(exc.IntegrityError):
                self._call(session, dag, asset, allow_reuse=True)
        session.rollback()

    def test_lifecycle_invalidation_is_bounded(self, session, fixture):
        """Repeatedly ineligible candidates cannot turn acquisition into an unbounded loop."""
        asset, dag = fixture
        self._call(session, dag, asset, allow_reuse=True)

        with mock.patch.object(AssetManager, "_lock_pending_apdr", autospec=True, return_value=None) as lock:
            with pytest.raises(RuntimeError, match="after 3 attempts"):
                self._call(session, dag, asset, allow_reuse=True)
        assert lock.call_count == 3
        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 1

    @staticmethod
    def _drive_fanout(session, dag_maker, consumer_dag_ids, mapper_keys):
        """Run real queueing for ``consumer_dag_ids`` and return the acquisition order.

        Returns the ``(dag_id, target_key)`` pairs that ``_get_or_create_apdr`` was actually
        handed, in call order.
        """
        from airflow.models.taskinstance import TaskInstance
        from airflow.sdk import PartitionedAtRuntime

        asset = Asset(name="fanout-order-asset")
        for consumer_dag_id in consumer_dag_ids:
            with dag_maker(
                dag_id=consumer_dag_id,
                schedule=PartitionedAssetTimetable(assets=asset),
                session=session,
            ):
                EmptyOperator(task_id="t1")
        with dag_maker(
            dag_id="fanout-order-producer", schedule=PartitionedAtRuntime(), session=session
        ) as producer_dag:
            EmptyOperator(task_id="emit", outlets=[asset])
        dr = dag_maker.create_dagrun(partition_key="2026-01-01T00:00:00", session=session)
        [ti] = dr.get_task_instances(session=session)
        session.commit()

        seen: list[tuple[str, str]] = []
        real_helper = AssetManager._get_or_create_apdr.__func__
        real_queue = AssetManager._queue_dagruns.__func__

        def recording(cls, *, target_key, target_dag, **kwargs):
            seen.append((target_dag.dag_id, target_key))
            return real_helper(cls, target_key=target_key, target_dag=target_dag, **kwargs)

        def pin_order(cls, *, dags_to_queue, **kwargs):
            return real_queue(cls, dags_to_queue=_DescendingDagIdSet(dags_to_queue), **kwargs)

        # Patch the real default mapper rather than substituting a locally defined class: the
        # timetable serialises its mapper by import path, so a test-local class cannot
        # round-trip. Descending keys expose an unsorted acquisition order.
        with (
            mock.patch.object(IdentityMapper, "to_downstream", return_value=list(mapper_keys)),
            mock.patch.object(AssetManager, "_queue_dagruns", classmethod(pin_order)),
            mock.patch.object(AssetManager, "_get_or_create_apdr", classmethod(recording)),
        ):
            TaskInstance.register_asset_changes_in_db(
                ti=ti,
                task_outlets=[o.asprofile() for o in producer_dag.get_task("emit").outlets],
                outlet_events=[],
                session=session,
            )
            session.commit()
        return seen

    @pytest.mark.need_serialized_dag
    def test_fanout_keys_are_acquired_in_sorted_order(self, session, dag_maker):
        """Production queueing must acquire fan-out keys in one globally agreed order.

        The marker guard makes each pending INSERT hold a unique-index entry until commit, so
        two transactions touching the same key set in opposite orders deadlock (PostgreSQL
        40P01; MySQL 1213, which aborts the victim entirely). Mappers are not required to
        return a stable order, so ``_queue_partitioned_dags`` sorts before acquiring.

        This drives the real queueing path with a mapper that deliberately returns keys in
        descending order, and asserts the complete expected call sequence. An earlier version
        of this test sorted its own inputs and called the helper directly, so it still passed
        with both production sorting layers reversed; a later version asserted only
        ``seen == sorted(seen)``, which a single reordered pair can still satisfy.
        """
        clear_db_apdr()
        clear_db_pakl()
        try:
            seen = self._drive_fanout(session, dag_maker, ["fanout-order-consumer"], ["k-3", "k-1", "k-2"])
            assert seen == [
                ("fanout-order-consumer", "k-1"),
                ("fanout-order-consumer", "k-2"),
                ("fanout-order-consumer", "k-3"),
            ], f"keys were not acquired in the one agreed order: {seen}"
        finally:
            clear_db_apdr()
            clear_db_pakl()

    @pytest.mark.need_serialized_dag
    def test_fanout_order_is_stable_across_multiple_consumers(self, session, dag_maker):
        """Both sort layers must hold: consumer DAGs first, then keys within each DAG.

        ``dags_to_queue`` is a ``set``, so its natural iteration order depends on object
        addresses and varies per process. The schedule pins it to descending ``dag_id`` so
        that removing the DAG-level sort in ``_queue_dagruns`` fails this test on every run
        rather than roughly half of them.
        """
        clear_db_apdr()
        clear_db_pakl()
        try:
            # Created zz-first so insertion order also disagrees with the required order.
            seen = self._drive_fanout(
                session,
                dag_maker,
                ["fanout-order-zz-consumer", "fanout-order-aa-consumer"],
                ["k-3", "k-1", "k-2"],
            )
            assert seen == [
                ("fanout-order-aa-consumer", "k-1"),
                ("fanout-order-aa-consumer", "k-2"),
                ("fanout-order-aa-consumer", "k-3"),
                ("fanout-order-zz-consumer", "k-1"),
                ("fanout-order-zz-consumer", "k-2"),
                ("fanout-order-zz-consumer", "k-3"),
            ], f"fan-out was not acquired dag-major in sorted order: {seen}"
        finally:
            clear_db_apdr()
            clear_db_pakl()
