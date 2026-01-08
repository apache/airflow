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

from airflow.models.asset import AssetModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import Asset
from airflow.utils.session import provide_session

from unit.listeners import asset_listener


@pytest.fixture(autouse=True)
def clean_listener_state():
    """Clear listener state after each test."""
    yield
    asset_listener.clear()


@pytest.mark.db_test
@provide_session
def test_asset_listener_on_asset_changed_gets_calls(
    create_task_instance_of_operator, session, listener_manager
):
    listener_manager(asset_listener)
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
