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
from sqlalchemy.orm import Session
from uuid6 import uuid7

from airflow import settings
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
from airflow.models.log import Log
from airflow.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper
from airflow.partition_mappers.window import WeekWindow
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_apdr, clear_db_logs, clear_db_pakl
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

        dag_version_id = uuid7()

        def _get_or_create_apdr():
            if TYPE_CHECKING:
                assert settings.Session
                assert settings.Session.session_factory

            _session = settings.Session.session_factory()
            _session.begin()
            try:
                return AssetManager._get_or_create_apdr(
                    target_key="test_partition_key",
                    target_dag=testing_dag,
                    dag_version_id=dag_version_id,
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

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "2"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fan_out_exceeded_skips_queue_and_logs(self, session, dag_maker, mock_task_instance):
        """When the mapper produces more downstream keys than the cap, no APDR is queued, the scheduler logger fires, and a Log row is written."""
        _clear_partition_db()

        asset_def = Asset(uri="s3://bucket/weekly", name="weekly")
        # WeekWindow fans a weekly upstream out into 7 daily downstream keys —
        # well above the cap of 2 set via @conf_vars, so the guard must trip.
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        with dag_maker(
            dag_id="fan_out_dag",
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
        assert "fan_out_dag" in log_extras[0]
        assert "partition_mapper_max_downstream_keys=2" in log_extras[0]
        # The scheduler-log `log.error` line is a separate observable from the
        # DB Log row; pin its keyword fields so a rename / level flip is caught.
        mock_log.error.assert_called_once()
        error_call = mock_log.error.call_args
        assert error_call.kwargs["target_dag"] == "fan_out_dag"
        assert error_call.kwargs["source_partition_key"] == "2024-06-03T00:00:00"
        assert error_call.kwargs["produced_keys"] == 7
        assert error_call.kwargs["max_downstream_keys"] == 2

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "7"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fan_out_at_cap_is_allowed(self, session, dag_maker, mock_task_instance):
        """``len == cap`` is allowed: a WeekWindow fan-out of exactly 7 with cap=7 queues all 7 rows.

        Pairs with ``test_partition_fan_out_one_over_cap_trips`` to pin the
        boundary at ``>`` (not ``>=``) — without this pair, a regression that
        flipped the comparison would still satisfy the existing
        ``cap=2``/``fan_out=7`` test.
        """
        _clear_partition_db()

        asset_def = Asset(uri="s3://bucket/weekly_at_cap", name="weekly_at_cap")
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        with dag_maker(
            dag_id="fan_out_at_cap_dag",
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

        assert session.scalar(select(func.count()).select_from(AssetPartitionDagRun)) == 7
        assert (
            session.scalar(
                select(func.count()).select_from(Log).where(Log.event == "partition fan-out exceeded")
            )
            == 0
        )
        mock_log.error.assert_not_called()

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "6"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fan_out_one_over_cap_trips(self, session, dag_maker, mock_task_instance):
        """``len == cap + 1`` trips: a WeekWindow fan-out of 7 with cap=6 queues nothing and logs."""
        _clear_partition_db()

        asset_def = Asset(uri="s3://bucket/weekly_over_cap", name="weekly_over_cap")
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow())
        with dag_maker(
            dag_id="fan_out_over_cap_dag",
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
        assert "fan_out_over_cap_dag" in log_extras[0]
        assert "partition_mapper_max_downstream_keys=6" in log_extras[0]
        mock_log.error.assert_called_once()
        error_call = mock_log.error.call_args
        assert error_call.kwargs["target_dag"] == "fan_out_over_cap_dag"
        assert error_call.kwargs["source_partition_key"] == "2024-06-03T00:00:00"
        assert error_call.kwargs["produced_keys"] == 7
        assert error_call.kwargs["max_downstream_keys"] == 6

    # --- per-mapper max_downstream_keys override tests ---
    # "override unset → falls back to global" is already covered by the three
    # tests above (test_partition_fanout_exceeded_skips_queue_and_logs,
    # test_partition_fanout_at_cap_is_allowed, test_partition_fanout_one_over_cap_trips).
    # No fifth test is needed for that case.

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
        from airflow.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper
        from airflow.partition_mappers.window import WeekWindow
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

        from tests_common.test_utils.db import clear_db_apdr, clear_db_logs, clear_db_pakl

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

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "3"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fanout_per_mapper_override_looser_than_global_permits(
        self, session, dag_maker, mock_task_instance
    ):
        """Per-mapper max_downstream_keys=10 permits 7 keys even when the global cap is 3.

        Proves the per-mapper override can relax, not just tighten, the cap.
        """
        from airflow.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper
        from airflow.partition_mappers.window import WeekWindow
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

        from tests_common.test_utils.db import clear_db_apdr, clear_db_logs, clear_db_pakl

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
        from airflow.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper
        from airflow.partition_mappers.window import WeekWindow
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

        from tests_common.test_utils.db import clear_db_apdr, clear_db_logs, clear_db_pakl

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
        from airflow.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper
        from airflow.partition_mappers.window import WeekWindow
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable

        from tests_common.test_utils.db import clear_db_apdr, clear_db_logs, clear_db_pakl

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

    @conf_vars({("scheduler", "partition_mapper_max_downstream_keys"): "7"})
    @pytest.mark.usefixtures("clear_assets", "testing_dag_bundle")
    def test_partition_fanout_third_party_mapper_without_max_downstream_keys_attr_falls_back_to_global(
        self, session, dag_maker, mock_task_instance, monkeypatch
    ):
        """A third-party mapper missing ``max_downstream_keys`` falls back to the global cap.

        ``getattr(mapper, "max_downstream_keys", None)`` in ``manager.py`` must return ``None``
        (not raise ``AttributeError``) for mappers that predate this commit or otherwise
        do not expose the attribute. Models the attribute as absent by stripping it from
        the deserialized instance right at the boundary.
        """
        from airflow.partition_mappers.temporal import FanOutMapper, StartOfWeekMapper
        from airflow.partition_mappers.window import WeekWindow
        from airflow.providers.standard.operators.empty import EmptyOperator
        from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
        from airflow.timetables.simple import PartitionedAssetTimetable as CorePartitionedAssetTimetable

        from tests_common.test_utils.db import clear_db_apdr, clear_db_logs, clear_db_pakl

        clear_db_apdr()
        clear_db_pakl()
        clear_db_logs()

        asset_def = Asset(uri="s3://bucket/legacy_mapper", name="legacy_mapper")
        # max_downstream_keys=3 would trip on a 7-key WeekWindow fanout *if* the attribute
        # were visible; after stripping it the global cap (7) must apply instead.
        mapper = FanOutMapper(upstream_mapper=StartOfWeekMapper(), window=WeekWindow(), max_downstream_keys=3)
        with dag_maker(
            dag_id="legacy_mapper_dag",
            schedule=PartitionedAssetTimetable(assets=asset_def, partition_mapper_config={asset_def: mapper}),
            serialized=True,
        ):
            EmptyOperator(task_id="t")
        dag_maker.create_dagrun()
        dag_maker.sync_dagbag_to_db()

        # Wrap get_partition_mapper to delete max_downstream_keys from the returned instance,
        # simulating a custom mapper whose class did not go through the new base init.
        original_get = CorePartitionedAssetTimetable.get_partition_mapper

        def stripped(self, *args, **kwargs):
            resolved = original_get(self, *args, **kwargs)
            if hasattr(resolved, "max_downstream_keys"):
                del resolved.max_downstream_keys
            return resolved

        monkeypatch.setattr(CorePartitionedAssetTimetable, "get_partition_mapper", stripped)

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
