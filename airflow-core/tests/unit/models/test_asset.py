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

import sqlite3

import pytest
from sqlalchemy import delete, event, select, update
from sqlalchemy.exc import IntegrityError

from airflow import settings
from airflow.assets.manager import AssetManager
from airflow.models.asset import (
    AssetAliasModel,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRunWake,
    DagScheduleAssetAliasReference,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
    PartitionedAssetKeyLog,
    TaskOutletAssetReference,
    expand_alias_to_assets,
    remove_references_to_deleted_dags,
)
from airflow.models.dag import DagModel
from airflow.models.dynamic_region import DynamicRegion
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.variable import Variable
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, AssetAlias
from airflow.serialization.definitions.assets import SerializedAssetAlias
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.dag import sync_dags_to_db
from tests_common.test_utils.db import (
    clear_db_apdr,
    clear_db_assets,
    clear_db_dags,
    clear_db_pakl,
    clear_db_variables,
)

pytestmark = pytest.mark.db_test


@pytest.mark.parametrize("kind", ["sentinel", "root_map", "loop", "nested_map"])
def test_asset_event_keeps_exact_producer_identity_after_clear(dag_maker, session, kind):
    asset = Asset(name="producer_identity", uri="test://producer_identity")
    with dag_maker():
        EmptyOperator(task_id="producer", outlets=[asset])
    run = dag_maker.create_dagrun()
    ti = run.task_instances[0]
    if kind != "sentinel":
        parent = None
        if kind == "nested_map":
            parent = DynamicRegion(dag_id=run.dag_id, run_id=run.run_id, node_id="loop")
            session.add(parent)
            session.flush()
        region = DynamicRegion(
            dag_id=run.dag_id,
            run_id=run.run_id,
            node_id="producer" if kind in {"root_map", "nested_map"} else "loop",
            parent_region_id=parent.id if parent else None,
            parent_region_index=2 if parent else None,
        )
        session.add(region)
        session.flush()
        ti.region_id, ti.region_index = region.id, 3
    ti.state = TaskInstanceState.SUCCESS
    session.flush()

    event = AssetManager.register_asset_change(task_instance=ti, asset=asset, session=session)
    assert event.source_task_instance_id == ti.id
    assert event.source_map_index == (3 if kind in {"root_map", "nested_map"} else -1)
    session.expire(event, ["source_task_instance"])
    assert event.source_task_instance.id == ti.id
    original_id = ti.id

    clear_task_instances([ti], session=session)
    session.flush()
    successor = session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == ti.dag_id,
            TaskInstance.run_id == ti.run_id,
            TaskInstance.task_id == ti.task_id,
            TaskInstance.region_id == ti.region_id,
            TaskInstance.region_index == ti.region_index,
            TaskInstance.working_set.is_(True),
        )
    ).one()
    assert successor.id != original_id
    session.expire(event, ["source_task_instance"])
    assert event.source_task_instance_id == original_id
    assert event.source_task_instance.id == original_id
    assert event.source_task_instance.working_set is None

    legacy = AssetEvent(
        asset_id=event.asset_id,
        source_dag_id=ti.dag_id,
        source_run_id=ti.run_id,
        source_task_id=ti.task_id,
        source_map_index=event.source_map_index,
    )
    session.add(legacy)
    session.flush()
    assert legacy.source_task_instance is None


@pytest.fixture(autouse=True)
def clear_assets():
    from tests_common.test_utils.db import clear_db_assets

    clear_db_assets()
    yield
    clear_db_assets()


def test_asset_alias_from_serialized():
    asset_alias = SerializedAssetAlias(name="test_alias", group="test_group")
    asset_alias_model = AssetAliasModel.from_serialized(asset_alias)
    assert asset_alias_model.name == "test_alias"
    assert asset_alias_model.group == "test_group"


def test_source_task_instance_follows_attributed_task_instance_not_coordinates(dag_maker, session):
    with dag_maker("asset_attempt_history", session=session):
        EmptyOperator(task_id="task")
    run = dag_maker.create_dagrun()
    ti = run.get_task_instance("task")
    ti.state = TaskInstanceState.SUCCESS
    ti = session.merge(ti)
    asset = AssetModel(name="attempt_source", uri="test://attempt_source")
    session.add(asset)
    session.flush()
    event = AssetEvent(
        asset_id=asset.id,
        source_dag_id=ti.dag_id,
        source_run_id=ti.run_id,
        source_task_id=ti.task_id,
        source_map_index=ti.map_index,
        source_task_instance_id=ti.id,
    )
    coordinate_only = AssetEvent(
        asset_id=asset.id,
        source_dag_id=ti.dag_id,
        source_run_id=ti.run_id,
        source_task_id=ti.task_id,
        source_map_index=ti.map_index,
    )
    session.add_all([event, coordinate_only])
    session.flush()
    event_id, coordinate_only_id, old_id = event.id, coordinate_only.id, ti.id
    successor = ti.prepare_db_for_next_try(session)
    session.commit()
    session.expire_all()

    assert successor.id != old_id
    assert session.get(AssetEvent, event_id).source_task_instance.id == old_id
    assert session.get(AssetEvent, coordinate_only_id).source_task_instance is None


class TestAssetAliasModel:
    @pytest.fixture
    def asset_model(self, session):
        """Example asset links to asset alias resolved_asset_alias_2."""
        asset_model = AssetModel(
            id=1,
            uri="test://asset1/",
            name="test_name",
            group="asset",
        )
        session.add(asset_model)
        session.flush()
        return asset_model

    @pytest.fixture
    def asset_alias_1(self, session):
        """Example asset alias links to no assets."""
        asset_alias_model = AssetAliasModel(name="test_name", group="test")
        session.add(asset_alias_model)
        session.flush()
        return asset_alias_model

    @pytest.fixture
    def resolved_asset_alias_2(self, session, asset_model):
        """Example asset alias links to asset asset_alias_1."""
        asset_alias_2 = AssetAliasModel(name="test_name_2")
        asset_alias_2.assets.append(asset_model)
        session.add(asset_alias_2)
        session.flush()
        return asset_alias_2

    def test_expand_alias_to_assets_empty(self, session, asset_alias_1):
        assert list(expand_alias_to_assets(asset_alias_1.name, session=session)) == []

    def test_expand_alias_to_assets_resolved(self, session, resolved_asset_alias_2, asset_model):
        assert list(expand_alias_to_assets(resolved_asset_alias_2.name, session=session)) == [asset_model]


@pytest.mark.parametrize(
    ("select_stmt", "expected_before_clear_1", "expected_before_clear_2"),
    [
        pytest.param(
            select(AssetModel.name, AssetModel.uri, DagScheduleAssetReference.dag_id),
            {("a", "b://b/", "test1"), ("a", "b://b/", "test2")},
            {("a", "b://b/", "test2")},
            id="asset",
        ),
        pytest.param(
            select(DagScheduleAssetNameReference.name, DagScheduleAssetNameReference.dag_id),
            {("a", "test1"), ("a", "test2")},
            {("a", "test2")},
            id="name",
        ),
        pytest.param(
            select(DagScheduleAssetUriReference.uri, DagScheduleAssetUriReference.dag_id),
            {("b://b/", "test1"), ("b://b/", "test2")},
            {("b://b/", "test2")},
            id="uri",
        ),
        pytest.param(
            select(AssetAliasModel.name, DagScheduleAssetAliasReference.dag_id),
            {("x", "test1"), ("x", "test2")},
            {("x", "test2")},
            id="alias",
        ),
        pytest.param(
            select(AssetModel.name, TaskOutletAssetReference.dag_id, TaskOutletAssetReference.task_id),
            {("a", "test1", "t1"), ("a", "test1", "t2"), ("a", "test2", "t1")},
            {("a", "test2", "t1")},
            id="outlet",
        ),
    ],
)
@pytest.mark.usefixtures("testing_dag_bundle")
def test_remove_reference_for_inactive_dag(
    dag_maker,
    session,
    select_stmt,
    expected_before_clear_1,
    expected_before_clear_2,
):
    schedule = [
        Asset(name="a", uri="b://b/"),
        Asset.ref(name="a"),
        Asset.ref(uri="b://b/"),
        AssetAlias("x"),
    ]
    with dag_maker(dag_id="test1", schedule=schedule, session=session) as dag1:
        EmptyOperator(task_id="t1", outlets=Asset(name="a", uri="b://b/"))
        EmptyOperator(task_id="t2", outlets=Asset(name="a", uri="b://b/"))
    with dag_maker(dag_id="test2", schedule=schedule, session=session) as dag2:
        EmptyOperator(task_id="t1", outlets=Asset(name="a", uri="b://b/"))
    assert set(session.execute(select_stmt)) == expected_before_clear_1

    sync_dags_to_db([dag1, dag2])

    def _simulate_soft_dag_deletion(dag_id):
        session.execute(update(DagModel).where(DagModel.dag_id == dag_id).values(is_stale=True))

    _simulate_soft_dag_deletion("test1")
    remove_references_to_deleted_dags(session=session)
    assert set(session.execute(select_stmt)) == expected_before_clear_2

    _simulate_soft_dag_deletion("test2")
    remove_references_to_deleted_dags(session=session)
    assert set(session.execute(select_stmt)) == set()


class TestPartitionDagRunWakes:
    """The wake rows that ask the scheduler to evaluate pending partition Dag runs."""

    @pytest.fixture(autouse=True)
    def _clear_db(self):
        clear_db_dags()
        clear_db_assets()
        clear_db_apdr()
        clear_db_pakl()
        clear_db_variables()
        yield
        clear_db_dags()
        clear_db_assets()
        clear_db_apdr()
        clear_db_pakl()
        clear_db_variables()

    @staticmethod
    def _build_key_log(apdr_id: int) -> PartitionedAssetKeyLog:
        return PartitionedAssetKeyLog(
            asset_id=1,
            asset_event_id=1,
            asset_partition_dag_run_id=apdr_id,
            source_partition_key="k",
            target_dag_id="consumer",
            target_partition_key="k",
        )

    @staticmethod
    def _read_wakes(session) -> list[tuple[int | None, str, str]]:
        return [
            tuple(row)
            for row in session.execute(
                select(
                    AssetPartitionDagRunWake.apdr_id,
                    AssetPartitionDagRunWake.target_dag_id,
                    AssetPartitionDagRunWake.reason,
                ).order_by(AssetPartitionDagRunWake.id)
            )
        ]

    def test_key_log_writes_its_wake_row_in_its_transaction(self, session):
        session.add(self._build_key_log(7))
        session.flush()
        assert self._read_wakes(session) == [(7, "consumer", "key_log")]

        session.rollback()
        assert self._read_wakes(session) == []

    def test_partial_flush_writes_wake_rows_only_for_the_key_logs_it_inserts(self, session):
        first, second = self._build_key_log(1), self._build_key_log(2)
        session.add_all([first, second])

        session.flush([first])
        assert self._read_wakes(session) == [(1, "consumer", "key_log")]

        session.flush()
        assert self._read_wakes(session) == [(1, "consumer", "key_log"), (2, "consumer", "key_log")]

    def test_flush_without_key_logs_writes_no_wake_row(self, session):
        statements: list[str] = []

        def _record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement)

        event.listen(settings.engine, "before_cursor_execute", _record)
        try:
            session.add(AssetModel(name="no-key-log", uri="s3://no-key-log"))
            session.flush()
        finally:
            event.remove(settings.engine, "before_cursor_execute", _record)

        assert statements
        assert not any("asset_partition_dag_run_wake" in statement for statement in statements)

    def test_key_logs_of_a_failed_flush_get_no_wake_row(self, session):
        session.add(Variable(key="taken", val="v"))
        session.commit()
        # Unrelated mappers flush in name order, so the key log is inserted before the variable that fails.
        session.add(self._build_key_log(1))
        session.add(Variable(key="taken", val="v"))
        with pytest.raises(IntegrityError):
            session.flush()
        session.rollback()

        session.add(AssetModel(name="after-the-failure", uri="s3://after-the-failure"))
        session.flush()

        assert self._read_wakes(session) == []

    @pytest.mark.parametrize(
        "schedule",
        [
            pytest.param(lambda asset: [asset], id="asset"),
            pytest.param(lambda asset: [Asset.ref(name=asset.name)], id="name-ref"),
            pytest.param(lambda asset: [Asset.ref(uri=asset.uri)], id="uri-ref"),
            pytest.param(lambda asset: [AssetAlias("activated-alias")], id="alias"),
        ],
    )
    def test_activation_wakes_the_dags_scheduled_on_the_asset(self, dag_maker, session, schedule):
        asset = Asset(name="activated", uri="s3://activated")
        with dag_maker(dag_id="scheduled-on-it", schedule=schedule(asset), session=session):
            EmptyOperator(task_id="t")
        with dag_maker(dag_id="scheduled-elsewhere", schedule=[Asset(name="other")], session=session):
            EmptyOperator(task_id="t")
        with dag_maker(dag_id="producer", schedule=None, session=session):
            EmptyOperator(task_id="t", outlets=[asset])
        asset_model = session.scalar(select(AssetModel).where(AssetModel.name == asset.name))
        alias_model = session.scalar(select(AssetAliasModel).where(AssetAliasModel.name == "activated-alias"))
        if alias_model is not None:
            alias_model.assets.append(asset_model)
        session.execute(delete(AssetPartitionDagRunWake))
        session.flush()

        AssetPartitionDagRunWake.add_for_activated_assets([asset_model], session=session)

        assert self._read_wakes(session) == [(None, "scheduled-on-it", "asset_activated")]

    @pytest.mark.backend("sqlite")
    def test_activation_of_many_assets_stays_within_the_bind_parameter_limit_of_old_sqlite(
        self, dag_maker, session
    ):
        assets = [Asset(name=f"many-{i}", uri=f"s3://many/{i}") for i in range(300)]
        with dag_maker(dag_id="scheduled-on-the-first", schedule=[assets[0]], session=session):
            EmptyOperator(task_id="t")
        with dag_maker(dag_id="producer-of-many", schedule=None, session=session):
            EmptyOperator(task_id="t", outlets=assets)
        session.execute(delete(AssetPartitionDagRunWake))
        session.flush()
        asset_models = session.scalars(select(AssetModel).where(AssetModel.name.like("many-%"))).all()
        assert len(asset_models) == 300
        # The default limit of SQLite before 3.32.
        dbapi_connection = session.connection().connection.dbapi_connection
        limit = dbapi_connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
        try:
            AssetPartitionDagRunWake.add_for_activated_assets(asset_models, session=session)
        finally:
            dbapi_connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, limit)

        assert self._read_wakes(session) == [(None, "scheduled-on-the-first", "asset_activated")]
