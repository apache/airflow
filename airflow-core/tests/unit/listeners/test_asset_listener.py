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

from airflow.listeners.types import AssetEvent
from airflow.models.asset import AssetModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import Asset
from airflow.serialization.encoders import ensure_serialized_asset

from tests_common.test_utils.db import clear_db_assets
from unit.listeners import asset_listener


@pytest.fixture(autouse=True)
def clean_listener_state():
    """Clear listener state after each test."""
    yield
    asset_listener.clear()


@pytest.fixture
def asset(session):
    asset_uri = "test://asset/"
    asset_name = "test_asset_uri"
    asset_group = "test-group"
    asset = Asset(uri=asset_uri, name=asset_name, group=asset_group)
    asset_model = AssetModel(uri=asset_uri, name=asset_name, group=asset_group)
    session.add(asset_model)
    session.flush()
    yield asset
    clear_db_assets()


@pytest.fixture
def ti(create_task_instance_of_operator, asset, session):
    return create_task_instance_of_operator(
        operator_class=EmptyOperator,
        dag_id="producing_dag",
        task_id="test_task",
        session=session,
        outlets=[asset],
    )


@pytest.mark.db_test
def test_asset_listener_on_asset_changed(asset, ti, listener_manager):
    listener_manager(asset_listener)
    ti.run()
    assert asset_listener.changed == [ensure_serialized_asset(asset)]


@pytest.mark.db_test
def test_asset_listener_on_asset_event_emitted(asset, ti, listener_manager):
    listener_manager(asset_listener)
    ti.run()
    assert asset_listener.emitted == [
        AssetEvent(
            asset=ensure_serialized_asset(asset),
            extra={},
            source_dag_id=ti.dag_id,
            source_task_id=ti.task_id,
            source_run_id=ti.run_id,
            source_map_index=ti.map_index,
            source_aliases=[],
            partition_key=None,
        )
    ]
