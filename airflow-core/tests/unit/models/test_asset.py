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

import pytest
from sqlalchemy import select, update

from airflow.models.asset import (
    AssetAliasModel,
    AssetModel,
    DagScheduleAssetAliasReference,
    DagScheduleAssetNameReference,
    DagScheduleAssetReference,
    DagScheduleAssetUriReference,
    TaskOutletAssetReference,
    expand_alias_to_assets,
    remove_references_to_deleted_dags,
)
from airflow.models.dag import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset, AssetAlias
from airflow.serialization.definitions.assets import SerializedAssetAlias

from tests_common.test_utils.dag import sync_dags_to_db

pytestmark = pytest.mark.db_test


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
