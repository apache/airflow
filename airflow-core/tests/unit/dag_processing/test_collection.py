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

import importlib
import logging
import os
import sys
import textwrap
import warnings
from collections.abc import Generator
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import patch

import pytest
from sqlalchemy import delete, event, func, inspect as sa_inspect, select
from sqlalchemy.exc import OperationalError, SAWarning

import airflow.dag_processing.collection
from airflow import plugins_manager
from airflow._shared.module_loading import qualname
from airflow._shared.timezones import timezone as tz
from airflow.configuration import conf
from airflow.dag_processing.collection import (
    AssetModelOperation,
    DagModelOperation,
    _get_latest_runs_stmt,
    _get_latest_runs_stmt_partitioned,
    _update_dag_tags,
    _update_import_errors,
    update_dag_parsing_results_in_db,
)
from airflow.example_dags.plugins.business_day_window import BusinessDayWindow
from airflow.example_dags.plugins.custom_partition_mapper import PrefixStripMapper
from airflow.example_dags.plugins.workday import AfterWorkdayTimetable
from airflow.exceptions import SerializationError
from airflow.models import DagModel, DagRun
from airflow.models.asset import (
    AssetActive,
    AssetModel,
    DagScheduleAssetNameReference,
    DagScheduleAssetUriReference,
)
from airflow.models.dag import DagTag
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagcode import DagCode
from airflow.models.dagwarning import DagWarning, DagWarningType
from airflow.models.errors import ParseImportError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.trigger import Trigger
from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.chain import ChainMapper
from airflow.partition_mappers.identity import IdentityMapper
from airflow.partition_mappers.temporal import StartOfDayMapper, StartOfMonthMapper
from airflow.partition_mappers.window import DayWindow
from airflow.plugins_manager import AirflowPlugin
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk import (
    DAG,
    Asset,
    AssetAlias,
    AssetAll,
    AssetAndTimeSchedule,
    AssetWatcher,
)
from airflow.sdk.definitions.deadline import AsyncCallback, BaseDeadlineReference, DeadlineAlert
from airflow.sdk.definitions.timetables.assets import AssetOrTimeSchedule, PartitionedAssetTimetable
from airflow.sdk.importers import DagSourceCode
from airflow.serialization.definitions.assets import SerializedAsset
from airflow.serialization.encoders import encode_trigger, ensure_serialized_asset
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.timetables.simple import PartitionedAtRuntime
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.triggers.base import BaseEventTrigger
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_import_errors,
    clear_db_serialized_dags,
    clear_db_teams,
    clear_db_triggers,
)
from tests_common.test_utils.mock_plugins import mock_plugin_manager
from unit.plugins.priority_weight_strategy import StaticTestPriorityWeightStrategy

if TYPE_CHECKING:
    from kgb import SpyAgency

mark_fab_auth_manager_test = pytest.mark.skipif(
    condition="FabAuthManager" not in conf.get("core", "auth_manager"),
    reason="This is only for FabAuthManager. Please set the environment variable `AIRFLOW__CORE__AUTH_MANAGER` to `airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` in `files/airflow-breeze-config/environment_variables.env` before running breeze shell. To run the test, add the flag `--keep-env-variables` to the pytest command.",
)


def test_statement_latest_runs_one_dag():
    with warnings.catch_warnings():
        warnings.simplefilter("error", category=SAWarning)

        stmt = _get_latest_runs_stmt(["fake-dag"])
        compiled_stmt = str(stmt.compile())
        actual = [x.strip() for x in compiled_stmt.splitlines()]
        expected = [
            "SELECT dag_run.id, dag_run.dag_id, dag_run.logical_date, dag_run.data_interval_start, "
            "dag_run.data_interval_end, dag_run.run_after, dag_run.partition_key, "
            "dag_run.partition_date",
            "FROM dag_run",
            "WHERE dag_run.dag_id = :dag_id_1 AND dag_run.logical_date = ("
            "SELECT max(dag_run.logical_date) AS max_logical_date",
            "FROM dag_run",
            "WHERE dag_run.dag_id = :dag_id_2 AND dag_run.run_type IN (__[POSTCOMPILE_run_type_1]))",
        ]
        assert actual == expected, compiled_stmt


@pytest.mark.db_test
def test_statement_latest_runs_loads_timetable_fields(dag_maker, session):
    with dag_maker("fake-dag", schedule=None):
        pass
    dag_maker.sync_dagbag_to_db()

    logical_date = tz.datetime(2025, 1, 1)
    run_after = tz.datetime(2025, 1, 2)

    dag_maker.create_dagrun(
        run_id="latest-run",
        logical_date=logical_date,
        data_interval=(logical_date, run_after),
        run_type=DagRunType.SCHEDULED,
        run_after=run_after,
        session=session,
    )
    session.flush()
    session.expunge_all()  # Ensure we load from DB, not from session cache

    latest = session.scalar(_get_latest_runs_stmt("fake-dag"))
    assert latest is not None
    assert {"run_after", "partition_date", "partition_key"}.isdisjoint(sa_inspect(latest).unloaded)
    assert latest.run_after == run_after
    assert latest.partition_key is None
    assert latest.partition_date is None


@pytest.mark.db_test
def test_statement_latest_runs_partitioned_sorted_by_partition_date(dag_maker, session):
    with dag_maker("fake-dag", schedule=PartitionedAtRuntime()):
        pass
    dag_maker.sync_dagbag_to_db()
    for i, (run_id, partition_key, partition_date) in enumerate(
        (
            ("newest-partition-date", "2025-01-02", tz.datetime(2025, 1, 2)),
            ("older-partition-date", "2025-01-01", tz.datetime(2025, 1, 1)),
            ("null-partition-date", "not-a-time-based-partition", None),
        )
    ):
        dag_maker.create_dagrun(
            run_id=run_id,
            logical_date=None,
            data_interval=None,
            run_type=DagRunType.SCHEDULED,
            run_after=tz.datetime(2025, 1, 1 + i),
            partition_key=partition_key,
            partition_date=partition_date,
            session=session,
        )

    session.flush()
    session.expunge_all()  # Ensure we load from DB, not from session cache

    latest = session.scalar(_get_latest_runs_stmt_partitioned("fake-dag"))
    assert latest is not None
    assert {"run_after", "partition_date", "partition_key"}.isdisjoint(sa_inspect(latest).unloaded)
    assert latest.run_after == tz.datetime(2025, 1, 1)
    assert latest.partition_key == "2025-01-02"
    assert latest.partition_date == tz.datetime(2025, 1, 2)


@pytest.mark.db_test
class TestAssetModelOperation:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_assets()
        clear_db_triggers()

    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        self.clean_db()
        yield
        self.clean_db()

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_sync_assets_preserves_access_control_from_other_bundle(self, dag_maker, session):
        """When a producer bundle (without access_control) is synced after a consumer bundle
        (with access_control), the stored access control fields must not be wiped out."""
        from airflow.models.asset import DagScheduleAssetReference, TaskOutletAssetReference
        from airflow.sdk import AssetAccessControl

        # First sync: consumer bundle sets access_control on the asset (producer_teams on schedule side).
        consumer_asset = Asset(
            "shared_asset",
            access_control=AssetAccessControl(producer_teams=["team1", "team2"], allow_global=False),
        )
        with dag_maker(dag_id="consumer_dag", schedule=[consumer_asset]) as consumer_dag:
            EmptyOperator(task_id="mytask")

        consumer_dags = {consumer_dag.dag_id: LazyDeserializedDAG.from_dag(consumer_dag)}
        orm_dags = DagModelOperation(consumer_dags, "testing", None).add_dags(session=session)
        asset_op = AssetModelOperation.collect(consumer_dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        session.flush()

        ref = session.scalar(
            select(DagScheduleAssetReference).where(DagScheduleAssetReference.dag_id == "consumer_dag")
        )
        assert ref.allow_producer_teams == ["team1", "team2"]
        assert ref.allow_global_producers is False

        # Second sync: producer bundle references the same asset with consumer_teams on the outlet.
        producer_asset = Asset(
            "shared_asset",
            access_control=AssetAccessControl(consumer_teams=["team_ml"], allow_global=False),
        )
        with dag_maker(dag_id="producer_dag", schedule="@once") as producer_dag:
            EmptyOperator(task_id="produce", outlets=[producer_asset])

        producer_dags = {producer_dag.dag_id: LazyDeserializedDAG.from_dag(producer_dag)}
        producer_orm_dags = DagModelOperation(producer_dags, "testing", None).add_dags(session=session)
        asset_op = AssetModelOperation.collect(producer_dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        asset_op.add_task_asset_references(producer_orm_dags, orm_assets, session=session)
        session.flush()

        # Consumer's schedule-side access control must still be preserved.
        session.expire(ref)
        assert ref.allow_producer_teams == ["team1", "team2"]
        assert ref.allow_global_producers is False

        # Producer's outlet-side access control must be stored.
        outlet_ref = session.scalar(
            select(TaskOutletAssetReference).where(TaskOutletAssetReference.dag_id == "producer_dag")
        )
        assert outlet_ref.allow_consumer_teams == ["team_ml"]
        assert outlet_ref.allow_global_consumers is False

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_add_task_outlet_asset_references_updates_consumer_teams_on_change(self, dag_maker, session):
        """When access_control changes, existing outlet references are updated in place."""
        from airflow.models.asset import TaskOutletAssetReference
        from airflow.sdk import AssetAccessControl

        asset = Asset(
            "evolving_asset",
            access_control=AssetAccessControl(consumer_teams=["team_old"], allow_global=True),
        )

        with dag_maker(dag_id="evolving_producer", schedule="@once") as dag:
            EmptyOperator(task_id="produce", outlets=[asset])

        dags = {dag.dag_id: LazyDeserializedDAG.from_dag(dag)}
        orm_dags = DagModelOperation(dags, "testing", None).add_dags(session=session)
        asset_op = AssetModelOperation.collect(dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        asset_op.add_task_asset_references(orm_dags, orm_assets, session=session)
        session.flush()

        ref = session.scalar(
            select(TaskOutletAssetReference).where(TaskOutletAssetReference.dag_id == "evolving_producer")
        )
        assert ref.allow_consumer_teams == ["team_old"]
        assert ref.allow_global_consumers is True

        # Change access_control and re-sync.
        asset.access_control = AssetAccessControl(consumer_teams=["team_new"], allow_global=False)
        dags = {dag.dag_id: LazyDeserializedDAG.from_dag(dag)}
        orm_dags = DagModelOperation(dags, "testing", None).find_orm_dags(session=session)
        asset_op = AssetModelOperation.collect(dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        asset_op.add_task_asset_references(orm_dags, orm_assets, session=session)
        session.flush()

        session.expire(ref)
        assert ref.allow_consumer_teams == ["team_new"]
        assert ref.allow_global_consumers is False

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_add_task_outlet_asset_references_defaults_when_no_access_control(self, dag_maker, session):
        """Outlet references default to None consumer_teams and allow_global_consumers=True."""
        from airflow.models.asset import TaskOutletAssetReference

        asset = Asset("plain_asset")

        with dag_maker(dag_id="plain_producer_dag", schedule="@once") as dag:
            EmptyOperator(task_id="produce", outlets=[asset])

        dags = {dag.dag_id: LazyDeserializedDAG.from_dag(dag)}
        orm_dags = DagModelOperation(dags, "testing", None).add_dags(session=session)
        asset_op = AssetModelOperation.collect(dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        asset_op.add_task_asset_references(orm_dags, orm_assets, session=session)
        session.flush()

        ref = session.scalar(
            select(TaskOutletAssetReference).where(TaskOutletAssetReference.dag_id == "plain_producer_dag")
        )
        assert ref is not None
        assert ref.allow_consumer_teams is None
        assert ref.allow_global_consumers is True

    @pytest.mark.parametrize(
        ("is_active", "is_paused", "expected_num_triggers"),
        [
            (True, True, 0),
            (True, False, 1),
            (False, True, 0),
            (False, False, 0),
        ],
    )
    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_add_asset_trigger_references(
        self, dag_maker, session, is_active, is_paused, expected_num_triggers
    ):
        asset = Asset(
            "test_add_asset_trigger_references_asset",
            watchers=[AssetWatcher(name="test", trigger=FileDeleteTrigger(mock.Mock()))],
        )

        with dag_maker(dag_id="test_add_asset_trigger_references_dag", schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        dags = {dag.dag_id: LazyDeserializedDAG.from_dag(dag)}
        orm_dags = DagModelOperation(dags, "testing", None).add_dags(session=session)

        # Simulate dag unpause and deletion.
        dag_model = orm_dags[dag.dag_id]
        dag_model.is_stale = not is_active
        dag_model.is_paused = is_paused

        asset_op = AssetModelOperation.collect(dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()

        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        asset_op.add_asset_trigger_references(orm_assets, session=session)
        session.flush()

        asset_model = session.scalars(select(AssetModel)).one()
        assert len(asset_model.triggers) == expected_num_triggers

    @pytest.mark.usefixtures("testing_dag_bundle")
    @pytest.mark.parametrize(
        ("use_team", "expected"),
        [
            pytest.param(True, "testing", id="with-team"),
            pytest.param(False, None, id="no-team"),
        ],
    )
    def test_add_asset_trigger_references_populates_team_name(
        self, dag_maker, session, testing_team, use_team, expected
    ):
        asset = Asset(
            "test_trigger_team_asset",
            watchers=[AssetWatcher(name="watcher", trigger=FileDeleteTrigger(mock.Mock()))],
        )

        with dag_maker(dag_id="test_trigger_team_dag", schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        # Use raw DagModelOperation (not dag_maker's bulk_write_to_db) to control team_name
        dags = {dag.dag_id: LazyDeserializedDAG.from_dag(dag)}
        orm_dags = DagModelOperation(dags, "testing", None).add_dags(session=session)
        orm_dags[dag.dag_id].is_stale = False
        orm_dags[dag.dag_id].is_paused = False
        session.flush()

        asset_op = AssetModelOperation.collect(dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        session.flush()

        # Clear any triggers created by dag_maker's bulk_write_to_db
        session.execute(delete(Trigger))
        for asset_model in orm_assets.values():
            asset_model.watchers = []
        session.flush()

        team_name = testing_team.name if use_team else None
        asset_op.add_asset_trigger_references(orm_assets, team_name=team_name, session=session)
        session.flush()

        triggers = session.scalars(select(Trigger)).all()
        assert len(triggers) == 1
        assert triggers[0].team_name == expected

    @pytest.mark.usefixtures("testing_dag_bundle")
    @pytest.mark.parametrize(
        ("queue", "expected"),
        [pytest.param("my_q", "my_q", id="has-queue"), pytest.param(None, None, id="no-queue")],
    )
    def test_add_asset_trigger_references_populates_queue(self, dag_maker, session, queue, expected):
        """Ensure the dag processor tracks the queue value of a `BaseEventTrigger`-type trigger."""
        trigger = FileDeleteTrigger(filepath="/tmp/test.txt", poke_interval=5.0)
        trigger.queue = queue
        asset = Asset("trigger_q_asset", watchers=[AssetWatcher(name="watcher", trigger=trigger)])
        with dag_maker(dag_id="test_trigger_q_dag", schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        dags = {dag.dag_id: LazyDeserializedDAG.from_dag(dag)}
        orm_dags = DagModelOperation(dags, "testing", None).add_dags(session=session)
        orm_dags[dag.dag_id].is_paused = False

        asset_op = AssetModelOperation.collect(dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()

        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        asset_op.add_asset_trigger_references(orm_assets, session=session)
        session.flush()

        triggers = session.scalars(select(Trigger)).all()
        assert len(triggers) == 1
        assert triggers[0].queue == expected

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_add_asset_trigger_references_hash_consistency(self, dag_maker, session):
        """Trigger hash from the DAG-parsed path must equal the hash computed
        from the DB-stored Trigger row.  A mismatch causes the scheduler to
        recreate trigger rows on every heartbeat.
        """
        trigger = FileDeleteTrigger(filepath="/tmp/test.txt", poke_interval=5.0)
        asset = Asset(
            "test_hash_consistency_asset",
            watchers=[AssetWatcher(name="file_watcher", trigger=trigger)],
        )

        with dag_maker(dag_id="test_hash_consistency_dag", schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        dags = {dag.dag_id: LazyDeserializedDAG.from_dag(dag)}
        orm_dags = DagModelOperation(dags, "testing", None).add_dags(session=session)
        orm_dags[dag.dag_id].is_paused = False

        asset_op = AssetModelOperation.collect(dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()

        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        asset_op.add_asset_trigger_references(orm_assets, session=session)
        session.flush()

        # DAG-side hash (same computation as add_asset_trigger_references line 1025)
        encoded = encode_trigger(trigger)
        dag_hash = BaseEventTrigger.hash(encoded["classpath"], encoded["kwargs"])

        # DB-side: expire and re-load the Trigger row to force a real DB read
        asset_model = session.scalars(select(AssetModel)).one()
        assert len(asset_model.triggers) == 1
        orm_trigger = asset_model.triggers[0]
        trigger_id = orm_trigger.id
        session.expire(orm_trigger)
        reloaded = session.get(Trigger, trigger_id)

        # DB-side hash (same computation as add_asset_trigger_references line 1033)
        db_hash = BaseEventTrigger.hash(reloaded.classpath, reloaded.kwargs)

        assert dag_hash == db_hash

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_add_asset_trigger_references_idempotent(self, dag_maker, session):
        """Calling add_asset_trigger_references twice with the same trigger
        must not create duplicate rows.
        """
        trigger = FileDeleteTrigger(filepath="/tmp/test.txt", poke_interval=5.0)
        asset = Asset(
            "test_idempotent_asset",
            watchers=[AssetWatcher(name="file_watcher", trigger=trigger)],
        )

        with dag_maker(dag_id="test_idempotent_dag", schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        dags = {dag.dag_id: LazyDeserializedDAG.from_dag(dag)}
        orm_dags = DagModelOperation(dags, "testing", None).add_dags(session=session)
        orm_dags[dag.dag_id].is_paused = False

        asset_op = AssetModelOperation.collect(dags)
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()

        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)

        # First call — creates the trigger
        asset_op.add_asset_trigger_references(orm_assets, session=session)
        session.flush()
        count_after_first = session.scalar(select(func.count(Trigger.id)))

        # Second call — should be a no-op (hashes match, no diff)
        asset_op.add_asset_trigger_references(orm_assets, session=session)
        session.flush()
        count_after_second = session.scalar(select(func.count(Trigger.id)))

        assert count_after_first == count_after_second

    @pytest.mark.parametrize(
        ("schedule", "model", "columns", "expected"),
        [
            pytest.param(
                Asset.ref(name="name1"),
                DagScheduleAssetNameReference,
                (DagScheduleAssetNameReference.name, DagScheduleAssetNameReference.dag_id),
                [("name1", "test")],
                id="name-ref",
            ),
            pytest.param(
                Asset.ref(uri="foo://1"),
                DagScheduleAssetUriReference,
                (DagScheduleAssetUriReference.uri, DagScheduleAssetUriReference.dag_id),
                [("foo://1", "test")],
                id="uri-ref",
            ),
        ],
    )
    def test_add_dag_asset_name_uri_references(self, dag_maker, session, schedule, model, columns, expected):
        with dag_maker(dag_id="test", schedule=schedule, session=session) as dag:
            pass

        op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        op.add_dag_asset_name_uri_references(session=session)
        assert session.execute(select(*columns)).all() == expected

    def test_change_asset_property_sync_group(self, dag_maker, session):
        asset = Asset("myasset", group="old_group")
        with dag_maker(schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_assets = asset_op.sync_assets(session=session)
        assert len(orm_assets) == 1
        assert next(iter(orm_assets.values())).group == "old_group"

        # Parser should pick up group change.
        asset.group = "new_group"
        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_assets = asset_op.sync_assets(session=session)
        assert len(orm_assets) == 1
        assert next(iter(orm_assets.values())).group == "new_group"

    def test_change_asset_property_sync_extra(self, dag_maker, session):
        asset = Asset("myasset", extra={"foo": "old"})
        with dag_maker(schedule=asset) as dag:
            EmptyOperator(task_id="mytask")

        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_assets = asset_op.sync_assets(session=session)
        assert len(orm_assets) == 1
        assert next(iter(orm_assets.values())).extra == {"foo": "old"}

        # Parser should pick up extra change.
        asset.extra = {"foo": "new"}
        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_assets = asset_op.sync_assets(session=session)
        assert len(orm_assets) == 1
        assert next(iter(orm_assets.values())).extra == {"foo": "new"}

    def test_change_asset_alias_property_sync_group(self, dag_maker, session):
        alias = AssetAlias("myalias", group="old_group")
        with dag_maker(schedule=alias) as dag:
            EmptyOperator(task_id="mytask")

        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_aliases = asset_op.sync_asset_aliases(session=session)
        assert len(orm_aliases) == 1
        assert next(iter(orm_aliases.values())).group == "old_group"

        # Parser should pick up group change.
        alias.group = "new_group"
        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_aliases = asset_op.sync_asset_aliases(session=session)
        assert len(orm_aliases) == 1
        assert next(iter(orm_aliases.values())).group == "new_group"


@pytest.mark.db_test
@pytest.mark.want_activate_assets(False)
class TestAssetModelOperationSyncAssetActive:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_assets()
        clear_db_triggers()

    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        self.clean_db()
        yield
        self.clean_db()

    def test_add_asset_activate(self, dag_maker, session):
        asset = Asset("myasset", "file://myasset/", group="old_group")
        with dag_maker(schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        assert len(orm_assets) == 1

        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        session.flush()
        assert orm_assets["myasset", "file://myasset/"].active is not None

    def test_add_asset_activate_already_exists(self, dag_maker, session):
        asset = Asset(name="myasset", uri="file://myasset/", group="old_group")

        # Set up existing parsing result.
        serialized_asset = ensure_serialized_asset(asset)
        orm_asset = AssetModel.from_serialized(serialized_asset)
        session.add(orm_asset)
        session.add(AssetActive.for_asset(serialized_asset))
        session.flush()

        with dag_maker(schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        assert orm_assets == {("myasset", "file://myasset/"): orm_asset}

        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        session.flush()
        assert orm_assets["myasset", "file://myasset/"].active is not None, "should pick up existing active"

    @pytest.mark.parametrize(
        "existing_assets",
        [
            pytest.param([SerializedAsset("myasset", "file://different/asset", "", {}, [])], id="name"),
            pytest.param([SerializedAsset("another", "file://myasset/", "", {}, [])], id="uri"),
        ],
    )
    def test_add_asset_activate_conflict(self, dag_maker, session, existing_assets):
        session.add_all(AssetModel.from_serialized(a) for a in existing_assets)
        session.flush()
        session.add_all(AssetActive.for_asset(a) for a in existing_assets)
        session.flush()

        asset = Asset(name="myasset", uri="file://myasset/", group="old_group")
        with dag_maker(schedule=[asset]) as dag:
            EmptyOperator(task_id="mytask")

        asset_op = AssetModelOperation.collect({dag.dag_id: LazyDeserializedDAG.from_dag(dag)})
        orm_assets = asset_op.sync_assets(session=session)
        session.flush()
        assert len(orm_assets) == 1

        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
        session.flush()
        assert orm_assets["myasset", "file://myasset/"].active is None, "should not activate due to conflict"


@pytest.mark.need_serialized_dag
@pytest.mark.db_test
class TestUpdateDagParsingResults:
    """Tests centred around the ``update_dag_parsing_results_in_db`` function."""

    @pytest.fixture
    def clean_db(self, session):
        yield
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_import_errors()

    @pytest.fixture(name="dag_import_error_listener")
    def _dag_import_error_listener(self, listener_manager):
        from unit.listeners import dag_import_error_listener

        listener_manager(dag_import_error_listener)
        yield dag_import_error_listener
        dag_import_error_listener.clear()

    @mark_fab_auth_manager_test
    @conf_vars({("core", "min_serialized_dag_update_interval"): "5"})
    @pytest.mark.usefixtures("clean_db")  # sync_perms in fab has bad session commit hygiene
    def test_sync_perms_syncs_dag_specific_perms_on_update(
        self, monkeypatch, spy_agency: SpyAgency, session, time_machine, testing_dag_bundle
    ):
        """Test DAG-specific permissions are synced when a DAG is new or updated"""
        serialized_dags_count = session.scalar(select(func.count(SerializedDagModel.dag_id)))
        assert serialized_dags_count == 0

        time_machine.move_to(tz.datetime(2020, 1, 5, 0, 0, 0), tick=False)

        dag = DAG(dag_id="test")

        sync_perms_spy = spy_agency.spy_on(
            airflow.dag_processing.collection._sync_dag_perms,
            call_original=False,
        )

        def _sync_to_db():
            sync_perms_spy.reset_calls()
            time_machine.shift(20)

            update_dag_parsing_results_in_db("testing", None, [dag], dict(), None, set(), session)

        _sync_to_db()
        spy_agency.assert_spy_called_with(sync_perms_spy, dag, session=session)

        # DAG isn't updated
        _sync_to_db()
        # `_sync_dag_perms` should be called even the DAG isn't updated. Otherwise, any import error will not show up until DAG is updated.
        spy_agency.assert_spy_called_with(sync_perms_spy, dag, session=session)

        # DAG is updated
        dag.tags = {"new_tag"}
        _sync_to_db()
        spy_agency.assert_spy_called_with(sync_perms_spy, dag, session=session)

        serialized_dags_count = session.scalar(select(func.count(SerializedDagModel.dag_id)))

    @patch.object(SerializedDagModel, "write_dag")
    @patch("airflow.serialization.definitions.dag.SerializedDAG.bulk_write_to_db")
    def test_sync_to_db_is_retried(
        self, mock_bulk_write_to_db, mock_s10n_write_dag, testing_dag_bundle, session
    ):
        """Test that important DB operations in db sync are retried on OperationalError"""
        serialized_dags_count = session.scalar(select(func.count(SerializedDagModel.dag_id)))
        assert serialized_dags_count == 0
        mock_dag = mock.MagicMock()
        dags = [mock_dag]

        op_error = OperationalError(statement=mock.ANY, params=mock.ANY, orig=mock.ANY)

        # Mock error for the first 2 tries and a successful third try
        side_effect = [op_error, op_error, mock.ANY]

        mock_bulk_write_to_db.side_effect = side_effect

        mock_session = mock.MagicMock()
        update_dag_parsing_results_in_db(
            "testing",
            None,
            dags=dags,
            import_errors={},
            parse_duration=None,
            warnings=set(),
            session=mock_session,
        )

        # Test that 3 attempts were made to run 'DAG.bulk_write_to_db' successfully
        mock_bulk_write_to_db.assert_has_calls(
            [
                mock.call("testing", None, mock.ANY, None, session=mock.ANY),
                mock.call("testing", None, mock.ANY, None, session=mock.ANY),
                mock.call("testing", None, mock.ANY, None, session=mock.ANY),
            ]
        )
        # Assert that rollback is called twice (i.e. whenever OperationalError occurs)
        mock_session.rollback.assert_has_calls([mock.call(), mock.call()])
        # Check that 'SerializedDagModel.write_dag' is also called
        # Only called once since the other two times the 'DAG.bulk_write_to_db' error'd
        # and the session was roll-backed before even reaching 'SerializedDagModel.write_dag'
        mock_s10n_write_dag.assert_has_calls(
            [
                mock.call(
                    mock_dag,
                    bundle_name="testing",
                    bundle_version=None,
                    version_data=None,
                    min_update_interval=mock.ANY,
                    dag_source_code=None,
                    session=mock_session,
                    _prefetched=mock.ANY,
                ),
            ]
        )

        serialized_dags_count = session.scalar(select(func.count(SerializedDagModel.dag_id)))
        assert serialized_dags_count == 0

    def test_serialized_dags_are_written_to_db_on_sync(self, testing_dag_bundle, session):
        """Test DAGs are Serialized and written to DB when parsing result is updated"""
        serialized_dags_count = session.scalar(select(func.count(SerializedDagModel.dag_id)))
        assert serialized_dags_count == 0

        dag = DAG(dag_id="test")

        update_dag_parsing_results_in_db(
            bundle_name="testing",
            bundle_version=None,
            dags=[LazyDeserializedDAG.from_dag(dag)],
            import_errors={},
            parse_duration=None,
            warnings=set(),
            session=session,
        )

        new_serialized_dags_count = session.scalar(select(func.count(SerializedDagModel.dag_id)))
        assert new_serialized_dags_count == 1

    @pytest.mark.usefixtures("clean_db")
    def test_duplicate_dag_id_creates_dag_warning(self, testing_dag_bundle, session):
        session.add(
            DagModel(
                dag_id="duplicated_dag",
                bundle_name="testing",
                fileloc="/opt/airflow/dags/existing.py",
                relative_fileloc="existing.py",
                is_stale=False,
            )
        )
        session.flush()

        dag = DAG(dag_id="duplicated_dag")
        dag.fileloc = "/opt/airflow/dags/current.py"
        dag.relative_fileloc = "current.py"

        update_dag_parsing_results_in_db(
            bundle_name="testing",
            bundle_version=None,
            dags=[LazyDeserializedDAG.from_dag(dag)],
            import_errors={},
            parse_duration=None,
            warnings=set(),
            session=session,
        )

        warning = session.scalar(
            select(DagWarning).where(
                DagWarning.dag_id == "duplicated_dag",
                DagWarning.warning_type == DagWarningType.DUPLICATE_DAG_ID,
            )
        )

        assert warning is not None
        assert "existing.py" in warning.message
        assert "overwritten" in warning.message

    @pytest.mark.usefixtures("clean_db")
    def test_duplicate_dag_id_warning_is_removed_when_dag_file_matches(self, testing_dag_bundle, session):
        session.add(
            DagModel(
                dag_id="same_file_dag",
                bundle_name="testing",
                fileloc="/opt/airflow/dags/current.py",
                relative_fileloc="current.py",
                is_stale=False,
            )
        )
        session.add(
            DagWarning(
                dag_id="same_file_dag",
                warning_type=DagWarningType.DUPLICATE_DAG_ID,
                message="Previous duplicate dag_id warning",
            )
        )
        session.flush()

        dag = DAG(dag_id="same_file_dag")
        dag.fileloc = "/opt/airflow/dags/current.py"
        dag.relative_fileloc = "current.py"

        update_dag_parsing_results_in_db(
            bundle_name="testing",
            bundle_version=None,
            dags=[LazyDeserializedDAG.from_dag(dag)],
            import_errors={},
            parse_duration=None,
            warnings=set(),
            session=session,
        )

        warning = session.scalar(
            select(DagWarning).where(
                DagWarning.dag_id == "same_file_dag",
                DagWarning.warning_type == DagWarningType.DUPLICATE_DAG_ID,
            )
        )

        assert warning is None

    @pytest.mark.usefixtures("clean_db")
    def test_stale_importer_warnings_are_replaced(self, testing_dag_bundle, session):
        session.add(DagModel(dag_id="imported_dag", bundle_name="testing", fileloc="/dags/imported.py"))
        session.flush()
        session.add_all(
            [
                DagWarning(dag_id="imported_dag", warning_type="test:stale", message="Stale"),
                DagWarning(
                    dag_id="imported_dag", warning_type=DagWarningType.ASSET_CONFLICT, message="Conflict"
                ),
            ]
        )
        session.flush()

        update_dag_parsing_results_in_db(
            bundle_name="testing",
            bundle_version=None,
            dags=[LazyDeserializedDAG.from_dag(DAG(dag_id="imported_dag"))],
            import_errors={},
            parse_duration=None,
            warnings={DagWarning("imported_dag", "test:current", "Current")},
            session=session,
        )

        warning_types = session.scalars(
            select(DagWarning.warning_type).where(DagWarning.dag_id == "imported_dag")
        ).all()
        assert sorted(warning_types) == [DagWarningType.ASSET_CONFLICT.value, "test:current"]

    @pytest.mark.usefixtures("clean_db")
    def test_dag_source_codes_are_written_to_dag_code(self, testing_dag_bundle, session):
        dag = DAG(dag_id="yaml_dag")
        dag.fileloc = "/dags/yaml_dag.yaml"
        dag.relative_fileloc = "yaml_dag.yaml"

        update_dag_parsing_results_in_db(
            bundle_name="testing",
            bundle_version=None,
            dags=[LazyDeserializedDAG.from_dag(dag)],
            import_errors={},
            parse_duration=None,
            warnings=set(),
            session=session,
            dag_source_codes={dag.fileloc: DagSourceCode(source_code="dag_id: yaml_dag\n", language="yaml")},
        )

        dag_code = DagCode.get_latest_dagcode("yaml_dag", session=session)
        assert (dag_code.source_code, dag_code.language) == ("dag_id: yaml_dag\n", "yaml")

    def test_parse_time_written_to_db_on_sync(self, testing_dag_bundle, session):
        """Test that the parse time is correctly written to the DB after parsing"""

        parse_duration = 1.25
        dag = DAG(dag_id="test")
        update_dag_parsing_results_in_db("testing", None, [dag], dict(), parse_duration, set(), session)

        dag_model: DagModel = session.get(DagModel, (dag.dag_id,))
        assert dag_model.last_parse_duration == parse_duration

    def test_timetable_asset_gated_written_to_db_on_sync(self, testing_dag_bundle, session):
        asset = Asset("test")
        gated_dag = DAG(
            dag_id="asset_gated",
            schedule=AssetAndTimeSchedule(
                timetable=CronTriggerTimetable("@daily", timezone="UTC"),
                assets=asset,
            ),
            catchup=False,
        )
        regular_dag = DAG(dag_id="regular", schedule=None)

        update_dag_parsing_results_in_db(
            "testing",
            None,
            [LazyDeserializedDAG.from_dag(gated_dag), LazyDeserializedDAG.from_dag(regular_dag)],
            {},
            None,
            set(),
            session,
        )

        assert session.get(DagModel, gated_dag.dag_id).timetable_asset_gated is True
        assert session.get(DagModel, regular_dag.dag_id).timetable_asset_gated is False

    @patch.object(ParseImportError, "full_file_path")
    @patch.object(SerializedDagModel, "write_dag")
    @pytest.mark.usefixtures("clean_db")
    def test_serialized_dag_errors_are_import_errors(
        self, mock_serialize, mock_full_path, caplog, session, dag_import_error_listener, testing_dag_bundle
    ):
        """
        Test that errors serializing a DAG are recorded as import_errors in the DB
        """
        mock_serialize.side_effect = SerializationError
        caplog.set_level(logging.ERROR)

        dag = DAG(dag_id="test")
        dag.fileloc = "abc.py"
        dag.relative_fileloc = "abc.py"
        mock_full_path.return_value = "abc.py"

        import_errors = {}
        update_dag_parsing_results_in_db(
            "testing", None, [dag], import_errors, None, set(), session, files_parsed={("testing", "abc.py")}
        )
        assert "SerializationError" in caplog.text

        # Should have been edited in place
        err = import_errors.get(("testing", dag.relative_fileloc))
        assert "SerializationError" in err
        dag_model: DagModel = session.get(DagModel, (dag.dag_id,))
        assert dag_model.has_import_errors is True

        import_errors = session.scalars(select(ParseImportError)).all()

        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == dag.relative_fileloc
        assert "SerializationError" in import_error.stacktrace

        # Ensure the listener was notified
        assert len(dag_import_error_listener.new) == 1
        assert len(dag_import_error_listener.existing) == 0
        assert dag_import_error_listener.new["abc.py"] == import_error.stacktrace

    @patch.object(ParseImportError, "full_file_path")
    @mark_fab_auth_manager_test
    @conf_vars({("core", "min_serialized_dag_update_interval"): "5"})
    @pytest.mark.usefixtures("clean_db")
    def test_import_error_persist_for_invalid_access_control_role(
        self,
        mock_full_path,
        monkeypatch,
        dag_maker,
        session,
        time_machine,
        dag_import_error_listener,
        testing_dag_bundle,
    ):
        """
        Test that import errors related to invalid access control role are tracked in the DB until being fixed.
        """
        serialized_dags_count = session.scalar(select(func.count(SerializedDagModel.dag_id)))
        assert serialized_dags_count == 0
        time_machine.move_to(tz.datetime(2020, 1, 5, 0, 0, 0), tick=False)

        # create a DAG and assign it a non-exist role.
        with dag_maker(
            dag_id="test_nonexist_access_control",
            access_control={
                "non_existing_role": {"can_edit", "can_read", "can_delete"},
            },
        ) as dag:
            pass
        dag.fileloc = "test_nonexist_access_control.py"
        dag.relative_fileloc = "test_nonexist_access_control.py"
        mock_full_path.return_value = "test_nonexist_access_control.py"

        # the DAG processor should raise an import error when processing the DAG above.
        import_errors = {}
        # run the DAG parsing.
        update_dag_parsing_results_in_db("testing", None, [dag], import_errors, None, set(), session)
        # expect to get an error with "role does not exist" message.
        err = import_errors.get(("testing", dag.relative_fileloc))
        assert "AirflowException" in err
        assert "role does not exist" in err
        dag_model: DagModel = session.get(DagModel, (dag.dag_id,))
        # the DAG should contain an import error.
        assert dag_model.has_import_errors is True

        prev_import_errors = session.scalars(select(ParseImportError)).all()
        # the import error message should match.
        assert len(prev_import_errors) == 1
        prev_import_error = prev_import_errors[0]
        assert prev_import_error.filename == dag.relative_fileloc
        assert "AirflowException" in prev_import_error.stacktrace
        assert "role does not exist" in prev_import_error.stacktrace

        # this is a new import error.
        assert len(dag_import_error_listener.new) == 1
        assert len(dag_import_error_listener.existing) == 0
        assert (
            dag_import_error_listener.new["test_nonexist_access_control.py"] == prev_import_error.stacktrace
        )

        # the DAG is serialized into the DB.
        serialized_dags_count = session.scalar(select(func.count(SerializedDagModel.dag_id)))
        assert serialized_dags_count == 1

        # run the update again. Even though the DAG is not updated, the processor should raise import error since the access control is not fixed.
        time_machine.move_to(tz.datetime(2020, 1, 5, 0, 0, 5), tick=False)
        update_dag_parsing_results_in_db("testing", None, [dag], dict(), None, set(), session)

        dag_model: DagModel = session.get(DagModel, (dag.dag_id,))
        # the DAG should contain an import error.
        assert dag_model.has_import_errors is True

        import_errors = session.scalars(select(ParseImportError)).all()
        # the import error should still in the DB.
        assert len(import_errors) == 1
        import_error = import_errors[0]
        assert import_error.filename == dag.relative_fileloc
        assert "AirflowException" in import_error.stacktrace
        assert "role does not exist" in import_error.stacktrace

        # the new import error should be the same as the previous one
        assert len(import_errors) == len(prev_import_errors)
        assert import_error.filename == prev_import_error.filename
        assert import_error.filename == dag.relative_fileloc
        assert import_error.stacktrace == prev_import_error.stacktrace

        # there is a new error and an existing error.
        assert len(dag_import_error_listener.new) == 1
        assert len(dag_import_error_listener.existing) == 1
        assert (
            dag_import_error_listener.new["test_nonexist_access_control.py"] == prev_import_error.stacktrace
        )

        # run the update again, but the incorrect access control configuration is removed.
        time_machine.move_to(tz.datetime(2020, 1, 5, 0, 0, 10), tick=False)
        dag.access_control = None
        update_dag_parsing_results_in_db("testing", None, [dag], dict(), None, set(), session)

        dag_model: DagModel = session.get(DagModel, (dag.dag_id,))
        # the import error should be cleared.
        assert dag_model.has_import_errors is False

        import_errors = session.scalars(select(ParseImportError)).all()
        # the import error should be cleared.
        assert len(import_errors) == 0

        # no import error should be introduced.
        assert len(dag_import_error_listener.new) == 1
        assert len(dag_import_error_listener.existing) == 1

    @patch.object(ParseImportError, "full_file_path")
    @pytest.mark.usefixtures("clean_db")
    def test_new_import_error_replaces_old(
        self, mock_full_file_path, session, dag_import_error_listener, testing_dag_bundle
    ):
        """
        Test that existing import error is updated and new record not created
        for a dag with the same filename
        """
        bundle_name = "testing"
        filename = "abc.py"
        mock_full_file_path.return_value = filename
        prev_error = ParseImportError(
            filename=filename,
            bundle_name=bundle_name,
            timestamp=tz.utcnow(),
            stacktrace="Some error",
        )
        session.add(prev_error)
        session.flush()
        prev_error_id = prev_error.id

        update_dag_parsing_results_in_db(
            bundle_name=bundle_name,
            bundle_version=None,
            dags=[],
            import_errors={("testing", "abc.py"): "New error"},
            parse_duration=None,
            warnings=set(),
            session=session,
            files_parsed={("testing", "abc.py")},
        )

        import_error = session.scalar(
            select(ParseImportError).where(
                ParseImportError.filename == filename, ParseImportError.bundle_name == bundle_name
            )
        )

        # assert that the ID of the import error did not change
        assert import_error.id == prev_error_id
        assert import_error.stacktrace == "New error"

        # Ensure the listener was notified
        assert len(dag_import_error_listener.new) == 0
        assert len(dag_import_error_listener.existing) == 1
        assert dag_import_error_listener.existing["abc.py"] == prev_error.stacktrace

    @pytest.mark.usefixtures("clean_db")
    def test_remove_error_clears_import_error(self, testing_dag_bundle, session):
        # Pre-condition: there is an import error for the dag file
        bundle_name = "testing"
        filename = "abc.py"
        prev_error = ParseImportError(
            filename=filename,
            bundle_name=bundle_name,
            timestamp=tz.utcnow(),
            stacktrace="Some error",
        )
        session.add(prev_error)

        # And one for another file we haven't been given results for -- this shouldn't be deleted
        session.add(
            ParseImportError(
                filename="def.py",
                bundle_name=bundle_name,
                timestamp=tz.utcnow(),
                stacktrace="Some error",
            )
        )
        session.flush()

        # Sanity check of pre-condition
        import_errors = set(session.execute(select(ParseImportError.filename, ParseImportError.bundle_name)))
        assert import_errors == {("abc.py", bundle_name), ("def.py", bundle_name)}

        dag = DAG(dag_id="test")
        dag.fileloc = filename
        dag.relative_fileloc = filename

        import_errors = {}
        update_dag_parsing_results_in_db(
            bundle_name,
            bundle_version=None,
            dags=[LazyDeserializedDAG.from_dag(dag)],
            import_errors=dict.fromkeys(import_errors),
            parse_duration=None,
            warnings=set(),
            session=session,
            files_parsed={(bundle_name, "abc.py")},
        )
        dag_model: DagModel = session.get(DagModel, (dag.dag_id,))
        assert dag_model.has_import_errors is False

        import_errors = set(session.execute(select(ParseImportError.filename, ParseImportError.bundle_name)))

        assert import_errors == {("def.py", bundle_name)}

    @pytest.mark.usefixtures("clean_db")
    def test_remove_error_updates_loaded_dag_model(self, testing_dag_bundle, session):
        bundle_name = "testing"
        filename = "abc.py"
        session.add(
            ParseImportError(
                filename=filename,
                bundle_name=bundle_name,
                timestamp=tz.utcnow(),
                stacktrace="Some error",
            )
        )
        session.add(
            ParseImportError(
                filename="def.py",
                bundle_name=bundle_name,
                timestamp=tz.utcnow(),
                stacktrace="Some error",
            )
        )
        session.flush()

        dag = DAG(dag_id="test")
        dag.fileloc = filename
        dag.relative_fileloc = filename
        lazy_deserialized_dags = [LazyDeserializedDAG.from_dag(dag)]

        import_errors = {(bundle_name, filename): "Some error"}
        update_dag_parsing_results_in_db(
            bundle_name,
            bundle_version=None,
            dags=lazy_deserialized_dags,
            import_errors=import_errors,
            parse_duration=None,
            warnings=set(),
            session=session,
            files_parsed={(bundle_name, "abc.py")},
        )
        dag_model = session.get(DagModel, (dag.dag_id,))
        assert dag_model.has_import_errors is True

        import_errors = {}
        update_dag_parsing_results_in_db(
            bundle_name,
            bundle_version=None,
            dags=lazy_deserialized_dags,
            import_errors=import_errors,
            parse_duration=None,
            warnings=set(),
            session=session,
        )
        assert dag_model.has_import_errors is False

    @pytest.mark.usefixtures("clean_db")
    def test_clear_import_error_for_file_without_dags(self, testing_dag_bundle, session):
        """
        Test that import errors are cleared for files that were parsed but no longer contain DAGs.
        """
        bundle_name = "testing"
        filename = "no_dags.py"

        prev_error = ParseImportError(
            filename=filename,
            bundle_name=bundle_name,
            timestamp=tz.utcnow(),
            stacktrace="Previous import error",
        )
        session.add(prev_error)

        # And import error for another file we haven't parsed (this shouldn't be deleted)
        other_file_error = ParseImportError(
            filename="other.py",
            bundle_name=bundle_name,
            timestamp=tz.utcnow(),
            stacktrace="Some error",
        )
        session.add(other_file_error)
        session.flush()

        import_errors = set(session.execute(select(ParseImportError.filename, ParseImportError.bundle_name)))
        assert import_errors == {("no_dags.py", bundle_name), ("other.py", bundle_name)}

        # Simulate parsing the file: it was parsed successfully (no import errors),
        # but it no longer contains any DAGs. By passing files_parsed, we ensure
        # the import error is cleared even though there are no DAGs.
        files_parsed = {(bundle_name, filename)}
        update_dag_parsing_results_in_db(
            bundle_name=bundle_name,
            bundle_version=None,
            dags=[],  # No DAGs in this file
            import_errors={},  # No import errors
            parse_duration=None,
            warnings=set(),
            session=session,
            files_parsed=files_parsed,
        )

        import_errors = set(session.execute(select(ParseImportError.filename, ParseImportError.bundle_name)))
        assert import_errors == {("other.py", bundle_name)}, "Import error for parsed file should be cleared"

    @pytest.mark.usefixtures("clean_db")
    def test_import_error_update_does_not_touch_other_bundle_with_same_relative_fileloc(self, session):
        relative_fileloc = "example_dag.py"
        session.add_all([DagBundleModel(name="bundle_a"), DagBundleModel(name="bundle_b")])
        session.flush()
        session.add_all(
            [
                DagModel(dag_id="dag_in_bundle_a", relative_fileloc=relative_fileloc, bundle_name="bundle_a"),
                DagModel(dag_id="dag_in_bundle_b", relative_fileloc=relative_fileloc, bundle_name="bundle_b"),
            ]
        )
        session.flush()

        update_dag_parsing_results_in_db(
            bundle_name="bundle_a",
            bundle_version=None,
            dags=[],
            import_errors={("bundle_a", relative_fileloc): "Import failed in bundle_a"},
            parse_duration=None,
            warnings=set(),
            session=session,
            files_parsed={("bundle_a", relative_fileloc)},
        )
        session.flush()

        dag_in_bundle_a = session.get(DagModel, "dag_in_bundle_a")
        dag_in_bundle_b = session.get(DagModel, "dag_in_bundle_b")

        assert dag_in_bundle_a is not None
        assert dag_in_bundle_b is not None
        assert dag_in_bundle_a.bundle_name == "bundle_a"
        assert dag_in_bundle_b.bundle_name == "bundle_b"
        assert dag_in_bundle_a.has_import_errors is True
        assert dag_in_bundle_b.has_import_errors is False

    @pytest.mark.need_serialized_dag(False)
    @pytest.mark.parametrize(
        ("attrs", "expected"),
        [
            pytest.param(
                {
                    "_tasks_": [
                        EmptyOperator(task_id="task", owner="owner1"),
                        EmptyOperator(task_id="task2", owner="owner2"),
                        EmptyOperator(task_id="task3"),
                        EmptyOperator(task_id="task4", owner="owner2"),
                    ]
                },
                {"owners": ["owner1", "owner2"]},
                id="tasks-multiple-owners",
            ),
            pytest.param(
                {"is_paused_upon_creation": True},
                {"is_paused": True},
                id="is_paused_upon_creation",
            ),
            pytest.param(
                {},
                {"owners": ["airflow"]},
                id="default-owner",
            ),
            pytest.param(
                {},
                {"fail_fast": False},
                id="default-fail-fast",
            ),
            pytest.param(
                {"fail_fast": True},
                {"fail_fast": True},
                id="fail-fast-true",
            ),
            pytest.param(
                {
                    "_tasks_": [
                        EmptyOperator(task_id="task", owner="owner1"),
                        EmptyOperator(task_id="task2", owner="owner2"),
                        EmptyOperator(task_id="task3"),
                        EmptyOperator(task_id="task4", owner="owner2"),
                    ],
                    "schedule": "0 0 * * *",
                    "catchup": False,
                },
                {
                    "owners": ["owner1", "owner2"],
                    "next_dagrun": tz.datetime(2020, 1, 5, 0, 0, 0),
                    "next_dagrun_data_interval_start": tz.datetime(2020, 1, 5, 0, 0, 0),
                    "next_dagrun_data_interval_end": tz.datetime(2020, 1, 6, 0, 0, 0),
                    "next_dagrun_create_after": tz.datetime(2020, 1, 6, 0, 0, 0),
                },
                id="with-scheduled-dagruns",
            ),
        ],
    )
    @pytest.mark.usefixtures("clean_db")
    def test_dagmodel_properties(self, attrs, expected, session, time_machine, testing_dag_bundle, dag_maker):
        """Test that properties on the dag model are correctly set when dealing with a LazySerializedDag"""
        dt = tz.datetime(2020, 1, 6, 0, 0, 0)
        time_machine.move_to(dt, tick=False)

        tasks = attrs.pop("_tasks_", None)
        with dag_maker("dag", **attrs) as dag:
            ...
        if tasks:
            dag.add_tasks(tasks)

        if attrs.pop("schedule", None):
            dr_kwargs = {
                "dag_id": "dag",
                "run_type": "scheduled",
                "data_interval": (dt, dt + timedelta(minutes=5)),
            }
            dr1 = DagRun(logical_date=dt, run_id="test_run_id_1", **dr_kwargs, start_date=dt)
            session.add(dr1)
        update_dag_parsing_results_in_db(
            bundle_name="testing",
            bundle_version=None,
            dags=[LazyDeserializedDAG.from_dag(dag)],
            import_errors={},
            parse_duration=None,
            warnings=set(),
            session=session,
        )

        orm_dag = session.get(DagModel, ("dag",))

        for attrname, expected_value in expected.items():
            if attrname == "owners":
                assert sorted(orm_dag.owners.split(", ")) == expected_value
            else:
                assert getattr(orm_dag, attrname) == expected_value

        assert orm_dag.last_parsed_time == dt

    def test_existing_dag_is_paused_upon_creation(self, testing_dag_bundle, session, dag_maker):
        with dag_maker("dag_paused", schedule=None) as dag:
            ...
        update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
        orm_dag = session.get(DagModel, ("dag_paused",))
        assert orm_dag.is_paused is False

        with dag_maker("dag_paused", schedule=None, is_paused_upon_creation=True) as dag:
            ...
        update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
        # Since the dag existed before, it should not follow the pause flag upon creation
        orm_dag = session.get(DagModel, ("dag_paused",))
        assert orm_dag.is_paused is False

    def test_bundle_name_and_version_are_stored(self, testing_dag_bundle, session, dag_maker):
        with dag_maker("mydag", schedule=None) as dag:
            ...
        update_dag_parsing_results_in_db("testing", "1.0", [dag], {}, 0.1, set(), session)
        orm_dag = session.get(DagModel, "mydag")
        assert orm_dag.bundle_name == "testing"
        assert orm_dag.bundle_version == "1.0"

    def test_max_active_tasks_explicit_value_is_used(self, testing_dag_bundle, session, dag_maker):
        with dag_maker("dag_max_tasks", schedule=None, max_active_tasks=5) as dag:
            ...
        update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
        orm_dag = session.get(DagModel, "dag_max_tasks")
        assert orm_dag.max_active_tasks == 5

    def test_max_active_tasks_defaults_from_conf_when_none(self, testing_dag_bundle, session, dag_maker):
        # Override config so that when DAG.max_active_tasks is None, DagModel gets the configured default
        with conf_vars({("core", "max_active_tasks_per_dag"): "7"}):
            with dag_maker("dag_max_tasks_default", schedule=None) as dag:
                ...
            update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
            orm_dag = session.get(DagModel, "dag_max_tasks_default")
            assert orm_dag.max_active_tasks == 7

    def test_max_active_runs_explicit_value_is_used(self, testing_dag_bundle, session, dag_maker):
        with dag_maker("dag_max_runs", schedule=None, max_active_runs=3) as dag:
            ...
        update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
        orm_dag = session.get(DagModel, "dag_max_runs")
        assert orm_dag.max_active_runs == 3

    @pytest.mark.parametrize(
        ("field", "cfg_key", "schema_default"),
        [
            ("max_active_runs", "max_active_runs_per_dag", 16),
            ("max_active_tasks", "max_active_tasks_per_dag", 16),
            ("max_consecutive_failed_dag_runs", "max_consecutive_failed_dag_runs_per_dag", 0),
        ],
    )
    def test_config_driven_field_equal_to_schema_default_not_overridden_by_conf(
        self, testing_dag_bundle, session, dag_maker, field, cfg_key, schema_default
    ):
        with conf_vars({("core", cfg_key): "1"}):
            with dag_maker(f"dag_{field}_schema_default", schedule=None, **{field: schema_default}) as dag:
                ...
            update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
            orm_dag = session.get(DagModel, f"dag_{field}_schema_default")
            assert getattr(orm_dag, field) == schema_default

    def test_max_active_runs_defaults_from_conf_when_none(self, testing_dag_bundle, session, dag_maker):
        with conf_vars({("core", "max_active_runs_per_dag"): "4"}):
            with dag_maker("dag_max_runs_default", schedule=None) as dag:
                ...
            update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
            orm_dag = session.get(DagModel, "dag_max_runs_default")
            assert orm_dag.max_active_runs == 4

    def test_max_consecutive_failed_dag_runs_explicit_value_is_used(
        self, testing_dag_bundle, session, dag_maker
    ):
        with dag_maker("dag_max_failed_runs", schedule=None, max_consecutive_failed_dag_runs=2) as dag:
            ...
        update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
        orm_dag = session.get(DagModel, "dag_max_failed_runs")
        assert orm_dag.max_consecutive_failed_dag_runs == 2

    def test_max_consecutive_failed_dag_runs_defaults_from_conf_when_none(
        self, testing_dag_bundle, session, dag_maker
    ):
        with conf_vars({("core", "max_consecutive_failed_dag_runs_per_dag"): "6"}):
            with dag_maker("dag_max_failed_runs_default", schedule=None) as dag:
                ...
            update_dag_parsing_results_in_db("testing", None, [dag], {}, 0.1, set(), session)
            orm_dag = session.get(DagModel, "dag_max_failed_runs_default")
            assert orm_dag.max_consecutive_failed_dag_runs == 6


@pytest.mark.db_test
class TestUpdateImportErrors:
    """Tests for the ``_update_import_errors`` helper."""

    @pytest.fixture(autouse=True)
    def clean_import_errors(self):
        clear_db_import_errors()
        yield
        clear_db_import_errors()

    @pytest.fixture
    def import_error_statements(self, session):
        """
        Collect every SQL statement issued against the ``import_error`` table.

        Matching on the bare table name would also catch statements naming ``dag.has_import_errors``,
        so match the positions where the table itself can appear.
        """
        statements: list[str] = []

        def _capture(conn, cursor, statement, parameters, context, executemany):
            lowered = statement.lower()
            if any(f"{keyword} import_error" in lowered for keyword in ("from", "into", "update")):
                statements.append(statement)

        bind = session.get_bind()
        event.listen(bind, "before_cursor_execute", _capture)
        yield statements
        event.remove(bind, "before_cursor_execute", _capture)

    @staticmethod
    def _selects(statements: list[str]) -> list[str]:
        return [stmt for stmt in statements if stmt.lower().lstrip().startswith("select")]

    def test_no_lookup_when_there_are_no_import_errors(self, session, import_error_statements):
        session.add(ParseImportError(filename="broken.py", bundle_name="testing", stacktrace="boom"))
        session.flush()
        import_error_statements.clear()

        # files_parsed is empty so no DELETE runs either: on backends without DELETE...RETURNING
        # its synchronize_session fallback would emit a SELECT of its own and muddy the assertion.
        _update_import_errors(
            files_parsed=set(),
            import_errors={},
            session=session,
        )

        assert self._selects(import_error_statements) == []

    @patch.object(ParseImportError, "full_file_path", return_value="broken.py")
    def test_existing_error_lookup_is_bounded(self, _mock_full_path, session, import_error_statements):
        session.add_all(
            [
                ParseImportError(filename="broken.py", bundle_name="testing", stacktrace="old"),
                ParseImportError(filename="untouched.py", bundle_name="other", stacktrace="unrelated"),
            ]
        )
        session.flush()
        import_error_statements.clear()

        _update_import_errors(
            files_parsed={("testing", "broken.py")},
            import_errors={("testing", "broken.py"): "new"},
            session=session,
        )

        selects = self._selects(import_error_statements)
        assert selects, "expected the existing-error lookup to run"
        assert all("where" in stmt.lower() for stmt in selects), (
            f"import_error must never be scanned unfiltered, got: {selects}"
        )

        rows = sorted(
            (err.bundle_name, err.filename, err.stacktrace)
            for err in session.scalars(select(ParseImportError))
        )
        assert rows == [
            ("other", "untouched.py", "unrelated"),
            ("testing", "broken.py", "new"),
        ]

    @patch.object(ParseImportError, "full_file_path", return_value="broken.py")
    def test_new_errors_keep_their_own_bundle_name(self, _mock_full_path, session):
        _update_import_errors(
            files_parsed=set(),
            import_errors={
                ("bundle_a", "a.py"): "error a",
                ("bundle_b", "b.py"): "error b",
            },
            session=session,
        )
        session.flush()

        rows = {(err.bundle_name, err.filename) for err in session.scalars(select(ParseImportError))}
        assert rows == {("bundle_a", "a.py"), ("bundle_b", "b.py")}


@pytest.mark.db_test
class TestUpdateDagTags:
    @pytest.fixture(autouse=True)
    def setup_teardown(self, session):
        yield
        session.execute(delete(DagModel).where(DagModel.dag_id == "test_dag"))
        session.commit()

    @pytest.mark.parametrize(
        ("initial_tags", "new_tags", "expected_tags"),
        [
            (["dangerous"], {"DANGEROUS"}, {"DANGEROUS"}),
            (["existing"], {"existing", "new"}, {"existing", "new"}),
            (["tag1", "tag2"], {"tag1"}, {"tag1"}),
            (["keep", "remove", "lowercase"], {"keep", "LOWERCASE", "new"}, {"keep", "LOWERCASE", "new"}),
            (["tag1", "tag2"], set(), set()),
        ],
    )
    def test_update_dag_tags(self, testing_dag_bundle, session, initial_tags, new_tags, expected_tags):
        dag_model = DagModel(dag_id="test_dag", bundle_name="testing")
        dag_model.tags = [DagTag(name=tag, dag_id="test_dag") for tag in initial_tags]
        session.add(dag_model)
        session.commit()

        _update_dag_tags(new_tags, dag_model, session=session)
        session.commit()

        assert {t.name for t in dag_model.tags} == expected_tags


@pytest.mark.db_test
class TestPartitionMapperInfoSync:
    """Verify partition_mapper_info is populated on DagModel during Dag sync."""

    @pytest.fixture(autouse=True)
    def clean_db_around_test(self) -> Generator:
        def reset() -> None:
            clear_db_dags()
            clear_db_assets()
            clear_db_serialized_dags()

        reset()
        yield
        reset()

    def test_partitioned_dag_with_rollup_mapper(self, dag_maker, session):
        """Cover regular Asset, name ref, and uri ref entries in partition_mapper_info."""
        rollup_asset = Asset(uri="s3://bucket/rollup", name="rollup")
        name_ref = Asset.ref(name="ref_by_name")
        uri_ref = Asset.ref(uri="s3://ref")
        rollup_mapper = RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow())

        with dag_maker(
            dag_id="partitioned_with_rollup",
            schedule=PartitionedAssetTimetable(
                assets=AssetAll(rollup_asset, name_ref, uri_ref),
                partition_mapper_config={
                    rollup_asset: rollup_mapper,
                    name_ref: rollup_mapper,
                    uri_ref: rollup_mapper,
                },
            ),
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dag_model = session.get(DagModel, "partitioned_with_rollup")
        assert dag_model.partition_mapper_info == [
            {"name": "rollup", "uri": "s3://bucket/rollup", "is_rollup": True},
            {"name": "ref_by_name", "is_rollup": True},
            {"uri": "s3://ref", "is_rollup": True},
        ]
        assert dag_model.has_rollup_mappers is True
        assert dag_model.is_rollup_asset(name="rollup", uri="s3://bucket/rollup") is True
        assert dag_model.is_rollup_asset(name="ref_by_name", uri="") is True
        assert dag_model.is_rollup_asset(name="", uri="s3://ref") is True

    def test_partitioned_dag_with_default_rollup_mapper(self, dag_maker, session):
        """
        Using only ``default_partition_mapper=RollupMapper(...)`` (the primary
        documented pattern, see ``example_asset_partition.py``) must still
        produce a non-empty ``partition_mapper_info`` with ``is_rollup=True``,
        so the UI's ``has_rollup_mappers`` / ``is_rollup_asset`` checks return
        the right values without inspecting ``partition_mapper_config``.
        """
        rollup_asset = Asset(uri="s3://bucket/rollup", name="rollup")

        with dag_maker(
            dag_id="partitioned_with_default_rollup",
            schedule=PartitionedAssetTimetable(
                assets=rollup_asset,
                default_partition_mapper=RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow()),
            ),
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dag_model = session.get(DagModel, "partitioned_with_default_rollup")
        assert dag_model.partition_mapper_info == [
            {"name": "rollup", "uri": "s3://bucket/rollup", "is_rollup": True},
        ]
        assert dag_model.has_rollup_mappers is True
        assert dag_model.is_rollup_asset(name="rollup", uri="s3://bucket/rollup") is True

    def test_non_partitioned_dag_leaves_info_empty(self, dag_maker, session):
        with dag_maker(
            dag_id="non_partitioned_dag",
            schedule=[Asset(uri="s3://bucket/A", name="A")],
            serialized=True,
        ):
            EmptyOperator(task_id="t")

        dag_model = session.get(DagModel, "non_partitioned_dag")
        assert dag_model.partition_mapper_info == []
        assert dag_model.has_rollup_mappers is False


class TeamDeadlineReference(BaseDeadlineReference):
    """A deadline reference a team-scoped plugin ships; Airflow has no example one to reuse."""

    def _evaluate_with(self, *, session, **kwargs):
        raise NotImplementedError


async def _deadline_callback():
    raise NotImplementedError


def _nested_chain_mapper(depth):
    mapper = PrefixStripMapper("eu")
    for _ in range(depth):
        mapper = ChainMapper(mapper, IdentityMapper())
    return mapper


def _dag_kwargs_using(case):
    """Return the Dag keyword arguments that make it use the plugin class, as a Dag author would."""
    if case == "timetable":
        return {"schedule": AfterWorkdayTimetable()}
    if case == "timetable-in-asset-or-time":
        return {"schedule": AssetOrTimeSchedule(timetable=AfterWorkdayTimetable(), assets=[Asset("a")])}
    if case == "default-partition-mapper":
        return {
            "schedule": PartitionedAssetTimetable(
                assets=Asset("a"), default_partition_mapper=PrefixStripMapper("eu")
            )
        }
    if case == "partition-mapper-deep-in-chain":
        return {
            "schedule": PartitionedAssetTimetable(
                assets=Asset("a"), partition_mapper_config={Asset("a"): _nested_chain_mapper(6)}
            )
        }
    if case == "window-in-rollup-mapper":
        return {
            "schedule": PartitionedAssetTimetable(
                assets=Asset("a"),
                partition_mapper_config={
                    Asset("a"): RollupMapper(window=BusinessDayWindow(), upstream_mapper=StartOfMonthMapper())
                },
            )
        }
    if case == "deadline-reference":
        return {
            "deadline": DeadlineAlert(
                reference=TeamDeadlineReference(),
                interval=timedelta(hours=1),
                callback=AsyncCallback(_deadline_callback),
            )
        }
    raise ValueError(case)


# Each case: the registry the plugin fills, the class it registers, and how the Dag uses it.
SCHEDULING_CLASS_USES = [
    pytest.param("timetables", AfterWorkdayTimetable, "timetable", id="timetable"),
    pytest.param(
        "timetables", AfterWorkdayTimetable, "timetable-in-asset-or-time", id="timetable-in-asset-or-time"
    ),
    pytest.param(
        "partition_mappers", PrefixStripMapper, "default-partition-mapper", id="default-partition-mapper"
    ),
    pytest.param(
        "partition_mappers",
        PrefixStripMapper,
        "partition-mapper-deep-in-chain",
        id="partition-mapper-deep-in-chain",
    ),
    pytest.param("windows", BusinessDayWindow, "window-in-rollup-mapper", id="window-in-rollup-mapper"),
    pytest.param("deadline_references", TeamDeadlineReference, "deadline-reference", id="deadline-reference"),
]


@pytest.mark.db_test
class TestRejectOtherTeamsPluginClasses:
    """A team-scoped plugin's scheduling classes may only be stored for that team's Dags."""

    @pytest.fixture(autouse=True)
    def _clean(self):
        yield
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_import_errors()
        clear_db_dag_bundles()
        clear_db_teams()

    @pytest.fixture
    def bundle(self, testing_team, session):
        """Return a factory creating the "team_bundle" bundle, owned by the given team or none."""

        def create(owned_by_team: bool) -> str:
            bundle = DagBundleModel(name="team_bundle")
            if owned_by_team:
                bundle.teams.append(testing_team)
            session.add(bundle)
            session.flush()
            return bundle.name

        return create

    @staticmethod
    def _plugin(team_name, registry, scheduling_class, name="scheduling_plugin"):
        plugin = AirflowPlugin()
        plugin.name = name
        plugin.team_name = team_name
        setattr(plugin, registry, [scheduling_class])
        return plugin

    @staticmethod
    def _serialized(dag_id="team_dag", **dag_kwargs):
        with DAG(dag_id, **{"schedule": None, **dag_kwargs}) as dag:
            EmptyOperator(task_id="t")
        dag.relative_fileloc = f"{dag_id}.py"
        return LazyDeserializedDAG.from_dag(dag)

    @staticmethod
    def _store(bundle_name, dags, session, warnings=frozenset()):
        import_errors: dict[tuple[str, str], str] = {}
        update_dag_parsing_results_in_db(
            bundle_name=bundle_name,
            bundle_version=None,
            dags=dags,
            import_errors=import_errors,
            parse_duration=None,
            warnings=set(warnings),
            session=session,
        )
        stored = set(session.scalars(select(SerializedDagModel.dag_id)))
        errors = {
            (e.bundle_name, e.filename): e.stacktrace for e in session.scalars(select(ParseImportError))
        }
        return stored, errors

    @conf_vars({("core", "multi_team"): "True"})
    @pytest.mark.parametrize(("registry", "scheduling_class", "usage"), SCHEDULING_CLASS_USES)
    @pytest.mark.parametrize(
        ("plugin_team", "dag_owned_by_team", "allowed"),
        [
            pytest.param("testing", True, True, id="owning-team"),
            pytest.param("other_team", True, False, id="other-team"),
            pytest.param("testing", False, False, id="teamless"),
        ],
    )
    def test_team_class_is_only_stored_for_its_team(
        self, bundle, session, plugin_team, dag_owned_by_team, allowed, registry, scheduling_class, usage
    ):
        bundle_name = bundle(dag_owned_by_team)
        with mock_plugin_manager(plugins=[self._plugin(plugin_team, registry, scheduling_class)]):
            dag = self._serialized(**_dag_kwargs_using(usage))
            stored, errors = self._store(bundle_name, [dag], session)

        if allowed:
            assert stored == {"team_dag"}
            assert errors == {}
        else:
            assert stored == set()
            assert list(errors) == [(bundle_name, "team_dag.py")]
            assert f"belonging to {plugin_team}" in errors[(bundle_name, "team_dag.py")]

    @conf_vars({("core", "multi_team"): "True"})
    @pytest.mark.parametrize(
        "weight_rule",
        [StaticTestPriorityWeightStrategy(), qualname(StaticTestPriorityWeightStrategy)],
        ids=["instance", "dotted-path"],
    )
    def test_weight_rule_is_checked_in_both_spellings(self, bundle, session, weight_rule):
        bundle_name = bundle(True)
        plugin = self._plugin("other_team", "priority_weight_strategies", StaticTestPriorityWeightStrategy)
        with mock_plugin_manager(plugins=[plugin]):
            with DAG("team_dag", schedule=None) as dag:
                EmptyOperator(task_id="t", weight_rule=weight_rule)
            dag.relative_fileloc = "team_dag.py"
            stored, errors = self._store(bundle_name, [LazyDeserializedDAG.from_dag(dag)], session)

        assert stored == set()
        assert "belonging to other_team" in errors[(bundle_name, "team_dag.py")]

    @conf_vars({("core", "multi_team"): "True"})
    def test_only_the_offending_dag_is_dropped(self, bundle, session):
        bundle_name = bundle(True)
        with mock_plugin_manager(plugins=[self._plugin("other_team", "timetables", AfterWorkdayTimetable)]):
            dags = [
                self._serialized("rejected", schedule=AfterWorkdayTimetable()),
                self._serialized("accepted"),
            ]
            stored, errors = self._store(bundle_name, dags, session)

        assert stored == {"accepted"}
        assert list(errors) == [(bundle_name, "rejected.py")]

    @conf_vars({("core", "multi_team"): "True"})
    def test_warning_for_a_rejected_new_dag_is_dropped(self, bundle, session):
        """
        The stability check warns about every Dag in a file, including one being rejected.

        A new Dag has no ``dag`` row to hang that warning on, so storing it would break the
        foreign key and fail the whole write.
        """
        bundle_name = bundle(True)
        warnings = {
            DagWarning("rejected", DagWarningType.RUNTIME_VARYING_VALUE.value, "datetime.now() in args"),
            DagWarning("accepted", DagWarningType.RUNTIME_VARYING_VALUE.value, "datetime.now() in args"),
        }
        with mock_plugin_manager(plugins=[self._plugin("other_team", "timetables", AfterWorkdayTimetable)]):
            dags = [
                self._serialized("rejected", schedule=AfterWorkdayTimetable()),
                self._serialized("accepted"),
            ]
            stored, errors = self._store(bundle_name, dags, session, warnings=warnings)

        assert stored == {"accepted"}
        assert list(errors) == [(bundle_name, "rejected.py")]
        assert set(session.scalars(select(DagWarning.dag_id))) == {"accepted"}

    @conf_vars({("core", "multi_team"): "True"})
    def test_error_is_a_plain_message(self, bundle, session):
        """The UI shows this as-is, so it must read as an explanation, not a traceback."""
        bundle_name = bundle(True)
        with mock_plugin_manager(plugins=[self._plugin("other_team", "timetables", AfterWorkdayTimetable)]):
            stored, errors = self._store(
                bundle_name, [self._serialized(schedule=AfterWorkdayTimetable())], session
            )

        assert errors[(bundle_name, "team_dag.py")] == (
            f"Dag 'team_dag' uses {qualname(AfterWorkdayTimetable)}, which is provided by a plugin "
            "belonging to other_team. This Dag belongs to team 'testing', so it cannot use it. "
            "Move the Dag into a bundle owned by other_team, or have the plugin provide the class "
            "globally instead of for a single team."
        )

    @conf_vars({("core", "multi_team"): "True"})
    def test_class_also_registered_globally_is_available_to_every_dag(self, bundle, session):
        bundle_name = bundle(True)
        plugins = [
            self._plugin(team, "timetables", AfterWorkdayTimetable, name=f"plugin_{i}")
            for i, team in enumerate(["other_team", None])
        ]
        with mock_plugin_manager(plugins=plugins):
            stored, errors = self._store(
                bundle_name, [self._serialized(schedule=AfterWorkdayTimetable())], session
            )

        assert stored == {"team_dag"}
        assert errors == {}

    @conf_vars({("core", "multi_team"): "True"})
    def test_airflow_class_listed_by_a_team_plugin_stays_available(self, bundle, session):
        """
        Every team's cron Dags keep working even if one team's plugin lists the cron timetable.

        The timetable is explicit: a cron string can serialize to a different class, depending on
        ``create_cron_data_intervals``, which would leave the plugin's class out of the Dag.
        """
        bundle_name = bundle(True)
        with mock_plugin_manager(plugins=[self._plugin("other_team", "timetables", CronTriggerTimetable)]):
            stored, errors = self._store(
                bundle_name,
                [self._serialized(schedule=CronTriggerTimetable("0 0 * * *", timezone="UTC"))],
                session,
            )

        assert stored == {"team_dag"}
        assert errors == {}

    def test_nothing_is_rejected_when_multi_team_is_off(self, bundle, session):
        bundle_name = bundle(True)
        with mock_plugin_manager(plugins=[self._plugin("other_team", "timetables", AfterWorkdayTimetable)]):
            stored, errors = self._store(
                bundle_name, [self._serialized(schedule=AfterWorkdayTimetable())], session
            )

        assert stored == {"team_dag"}
        assert errors == {}

    @conf_vars({("core", "multi_team"): "True"})
    def test_class_reloaded_by_the_plugin_loader_is_still_recognised(
        self, bundle, session, tmp_path, monkeypatch, request
    ):
        """
        A Dag importing from a plugin file holds a different class from the one registered.

        The plugin loader executes the file again under its own module entry, so the two
        classes share a qualname but not an identity.
        """
        (tmp_path / "workday.py").write_text(
            textwrap.dedent(
                """\
                from airflow.plugins_manager import AirflowPlugin
                from airflow.timetables.simple import NullTimetable


                class WorkdayTimetable(NullTimetable):
                    pass


                class WorkdayPlugin(AirflowPlugin):
                    name = "workday"
                    team_name = "other_team"
                    timetables = [WorkdayTimetable]
                """
            )
        )
        monkeypatch.syspath_prepend(os.fspath(tmp_path))
        # Both the import below and the plugin loader put a "workday" module in sys.modules, and
        # monkeypatch would restore the loader's at teardown, so remove it outright instead.
        sys.modules.pop("workday", None)
        request.addfinalizer(lambda: sys.modules.pop("workday", None))
        dag_side_class = importlib.import_module("workday").WorkdayTimetable
        plugins, import_errors = plugins_manager._load_plugins_from_plugin_directory(
            plugins_folder=os.fspath(tmp_path)
        )
        assert not import_errors
        assert plugins[0].timetables[0] is not dag_side_class

        bundle_name = bundle(True)
        with mock_plugin_manager(plugins=plugins):
            stored, errors = self._store(bundle_name, [self._serialized(schedule=dag_side_class())], session)

        assert stored == set()
        assert "belonging to other_team" in errors[(bundle_name, "team_dag.py")]
