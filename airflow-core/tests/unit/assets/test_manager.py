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

from airflow import settings
from airflow.assets.manager import AssetManager
from airflow.listeners.listener import get_listener_manager
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
from airflow.sdk.definitions.asset import Asset

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
        mock_task_instance.log.warning.assert_called()

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
        self, session, mock_task_instance, testing_dag_bundle
    ):
        asset_manager = AssetManager()
        asset_listener.clear()
        get_listener_manager().add_listener(asset_listener)

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

    def test_create_assets_notifies_asset_listener(self, session):
        asset_manager = AssetManager()
        asset_listener.clear()
        get_listener_manager().add_listener(asset_listener)

        asset = Asset(uri="test://asset1", name="test_asset_1")

        asms = asset_manager.create_assets([asset], session=session)

        # Ensure the listener was notified
        assert len(asset_listener.created) == 1
        assert len(asms) == 1
        assert asset_listener.created[0].uri == asset.uri == asms[0].uri

    @pytest.mark.usefixtures("dag_maker", "testing_dag_bundle")
    def test_get_or_create_apdr_race_condition(self, session, caplog):
        asm = AssetModel(uri="test://asset1/", name="parition_asset", group="asset")
        testing_dag = DagModel(dag_id="testing_dag", is_stale=False, bundle_name="testing")
        session.add_all([asm, testing_dag])
        session.commit()
        session.flush()
        assert session.query(AssetPartitionDagRun).count() == 0

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
        assert session.query(AssetPartitionDagRun).count() == 1
