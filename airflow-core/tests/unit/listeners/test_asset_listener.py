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

from airflow.listeners.listener import get_listener_manager
from airflow.models.asset import AssetModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.utils.session import provide_session

from unit.listeners import asset_listener


@pytest.fixture(autouse=True)
def clean_listener_manager():
    lm = get_listener_manager()
    lm.clear()
    lm.add_listener(asset_listener)
    yield
    lm = get_listener_manager()
    lm.clear()
    asset_listener.clear()


@pytest.mark.db_test
@provide_session
def test_asset_listener_on_asset_changed_gets_calls(create_task_instance_of_operator, session):
    asset_uri = "test://asset/"
    asset_name = "test_asset_uri"
    asset_group = "test-group"
    asset = Asset(uri=asset_uri, name=asset_name, group=asset_group)
    asset_model = AssetModel(uri=asset_uri, name=asset_name, group=asset_group)
    session.add(asset_model)

    session.flush()

    ti = create_task_instance_of_operator(
        operator_class=EmptyOperator,
        dag_id="producing_dag",
        task_id="test_task",
        session=session,
        outlets=[asset],
    )
    ti.run()

    assert len(asset_listener.changed) == 1
    assert asset_listener.changed[0].uri == asset_uri
    assert asset_listener.changed[0].name == asset_name
    assert asset_listener.changed[0].group == asset_group


@pytest.mark.db_test
@provide_session
def test_asset_listener_on_asset_event_created_gets_calls(create_task_instance_of_operator, session):
    asset_uri = "test://asset/"
    asset_name = "test_asset_uri"
    asset_group = "test-group"
    asset_extra = {
        "static": "some-value",
        "dynamic": "{{ task_instance.task_id }}",
    }
    asset = Asset(uri=asset_uri, name=asset_name, group=asset_group, extra=asset_extra)
    asset_model = AssetModel(uri=asset_uri, name=asset_name, group=asset_group)
    session.add(asset_model)
    session.flush()

    ti = create_task_instance_of_operator(
        operator_class=EmptyOperator,
        dag_id="producing_dag",
        task_id="test_task",
        session=session,
        outlets=[asset],
    )
    ti.run()

    assert len(asset_listener.created_events) == 1
    created_event = asset_listener.created_events[0]
    assert created_event.asset_key.uri == asset_uri
    assert created_event.asset_key.name == asset_name
    assert created_event.extra == {
        "static": "some-value",
        "dynamic": "test_task",
    }
    assert created_event.timestamp is not None
    assert created_event.source_dag_id == "producing_dag"
    assert created_event.source_task_id == "test_task"
    assert created_event.source_run_id == ti.run_id
    assert created_event.source_map_index == ti.map_index
