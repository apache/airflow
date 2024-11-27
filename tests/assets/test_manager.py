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

import itertools
from datetime import datetime
from unittest import mock

import pytest
from sqlalchemy import delete

from airflow.assets.manager import AssetManager
from airflow.listeners.listener import get_listener_manager
from airflow.models.asset import (
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    DagScheduleAssetAliasReference,
    DagScheduleAssetReference,
)
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.sdk.definitions.asset import Asset, AssetAlias
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic

from tests.listeners import asset_listener

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
    return TaskInstancePydantic(
        id="1",
        task_id="5",
        dag_id="7",
        run_id="11",
        map_index="13",
        start_date=datetime.now(),
        end_date=datetime.now(),
        logical_date=datetime.now(),
        duration=0.1,
        state="success",
        try_number=1,
        max_tries=4,
        hostname="host",
        unixname="unix",
        job_id=13,
        pool="default",
        pool_slots=1,
        queue="default",
        priority_weight=77,
        operator="DummyOperator",
        custom_operator_name="DummyOperator",
        queued_dttm=datetime.now(),
        queued_by_job_id=3,
        pid=12345,
        executor="default",
        executor_config=None,
        updated_at=datetime.now(),
        rendered_map_index="1",
        external_executor_id="x",
        trigger_id=1,
        trigger_timeout=datetime.now(),
        next_method="bla",
        next_kwargs=None,
        dag_version_id=None,
        run_as_user=None,
        task=None,
        test_mode=False,
        dag_run=None,
        dag_model=None,
        raw=False,
        is_trigger_log_context=False,
    )


def create_mock_dag():
    for dag_id in itertools.count(1):
        mock_dag = mock.Mock()
        mock_dag.dag_id = dag_id
        yield mock_dag


class TestAssetManager:
    def test_register_asset_change_asset_doesnt_exist(self, mock_task_instance):
        asset = Asset(uri="asset_doesnt_exist")

        mock_session = mock.Mock()
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

    def test_register_asset_change(self, session, dag_maker, mock_task_instance):
        asset_manager = AssetManager()

        asset = Asset(uri="test_asset_uri")
        dag1 = DagModel(dag_id="dag1", is_active=True)
        dag2 = DagModel(dag_id="dag2", is_active=True)
        session.add_all([dag1, dag2])

        asm = AssetModel(uri="test_asset_uri")
        session.add(asm)
        asm.consuming_dags = [DagScheduleAssetReference(dag_id=dag.dag_id) for dag in (dag1, dag2)]
        session.execute(delete(AssetDagRunQueue))
        session.flush()

        asset_manager.register_asset_change(task_instance=mock_task_instance, asset=asset, session=session)
        session.flush()

        # Ensure we've created an asset
        assert session.query(AssetEvent).filter_by(asset_id=asm.id).count() == 1
        assert session.query(AssetDagRunQueue).count() == 2

    @pytest.mark.usefixtures("clear_assets")
    def test_register_asset_change_with_alias(self, session, dag_maker, mock_task_instance):
        consumer_dag_1 = DagModel(dag_id="conumser_1", is_active=True, fileloc="dag1.py")
        consumer_dag_2 = DagModel(dag_id="conumser_2", is_active=True, fileloc="dag2.py")
        session.add_all([consumer_dag_1, consumer_dag_2])

        asm = AssetModel(uri="test_asset_uri")
        session.add(asm)

        asam = AssetAliasModel(name="test_alias_name")
        session.add(asam)
        asam.consuming_dags = [
            DagScheduleAssetAliasReference(alias_id=asam.id, dag_id=dag.dag_id)
            for dag in (consumer_dag_1, consumer_dag_2)
        ]
        session.execute(delete(AssetDagRunQueue))
        session.flush()

        asset = Asset(uri="test_asset_uri")
        asset_alias = AssetAlias(name="test_alias_name")
        asset_manager = AssetManager()
        asset_manager.register_asset_change(
            task_instance=mock_task_instance,
            asset=asset,
            aliases=[asset_alias],
            source_alias_names=["test_alias_name"],
            session=session,
        )
        session.flush()

        # Ensure we've created an asset
        assert session.query(AssetEvent).filter_by(asset_id=asm.id).count() == 1
        assert session.query(AssetDagRunQueue).count() == 2
        assert session.query(DagPriorityParsingRequest).count() == 2

    def test_register_asset_change_no_downstreams(self, session, mock_task_instance):
        asset_manager = AssetManager()

        asset = Asset(uri="never_consumed")
        asm = AssetModel(uri="never_consumed")
        session.add(asm)
        session.execute(delete(AssetDagRunQueue))
        session.flush()

        asset_manager.register_asset_change(task_instance=mock_task_instance, asset=asset, session=session)
        session.flush()

        # Ensure we've created an asset
        assert session.query(AssetEvent).filter_by(asset_id=asm.id).count() == 1
        assert session.query(AssetDagRunQueue).count() == 0

    @pytest.mark.skip_if_database_isolation_mode
    def test_register_asset_change_notifies_asset_listener(self, session, mock_task_instance):
        asset_manager = AssetManager()
        asset_listener.clear()
        get_listener_manager().add_listener(asset_listener)

        asset = Asset(uri="test_asset_uri_2")
        dag1 = DagModel(dag_id="dag3")
        session.add(dag1)

        asm = AssetModel(uri="test_asset_uri_2")
        session.add(asm)
        asm.consuming_dags = [DagScheduleAssetReference(dag_id=dag1.dag_id)]
        session.flush()

        asset_manager.register_asset_change(task_instance=mock_task_instance, asset=asset, session=session)
        session.flush()

        # Ensure the listener was notified
        assert len(asset_listener.changed) == 1
        assert asset_listener.changed[0].uri == asset.uri

    @pytest.mark.skip_if_database_isolation_mode
    def test_create_assets_notifies_asset_listener(self, session):
        asset_manager = AssetManager()
        asset_listener.clear()
        get_listener_manager().add_listener(asset_listener)

        asset = Asset(uri="test_asset_uri_3")

        asms = asset_manager.create_assets([asset], session=session)

        # Ensure the listener was notified
        assert len(asset_listener.created) == 1
        assert len(asms) == 1
        assert asset_listener.created[0].uri == asset.uri == asms[0].uri
