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
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Metadata
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
    asset_uri = "test://asset1/"
    asset_name = "test_asset_uri1"
    asset_group = "test-group"
    asset = Asset(uri=asset_uri, name=asset_name, group=asset_group, extra={"key": "value"})
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
    assert asset_listener.changed[0].extra == {"key": "value"}


@pytest.mark.db_test
@provide_session
def test_asset_listener_on_asset_changed_with_dynamic_extra(create_task_instance_of_operator, session):
    asset_uri = "test://asset2/"
    asset_name = "test_asset_uri2"
    asset_group = "test-group"
    asset = Asset(uri=asset_uri, name=asset_name, group=asset_group)
    asset_model = AssetModel(uri=asset_uri, name=asset_name, group=asset_group)
    session.add(asset_model)

    session.flush()

    def test_task_callable():
        yield Metadata(Asset(uri=asset_uri, name=asset_name, group=asset_group), extra={"key": "value"})

    ti = create_task_instance_of_operator(
        operator_class=PythonOperator,
        dag_id="producing_dag",
        task_id="test_task",
        session=session,
        outlets=[asset],
        python_callable=test_task_callable,
    )
    ti.run()

    assert len(asset_listener.changed) == 1
    assert asset_listener.changed[0].uri == asset_uri
    assert asset_listener.changed[0].name == asset_name
    assert asset_listener.changed[0].group == asset_group
    assert asset_listener.changed[0].extra == {"key": "value"}


@pytest.mark.db_test
@provide_session
def test_asset_listener_on_asset_changed_with_template_extra(create_task_instance_of_operator, session):
    asset_uri = "test://asset3/"
    asset_name = "test_asset_uri3"
    asset_group = "test-group"
    asset = Asset(uri=asset_uri, name=asset_name, group=asset_group, extra={"task_id": "{{ ti.task_id }}"})
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
    assert asset_listener.changed[0].extra == {"task_id": "test_task"}
