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

from airflow.assets import Asset
from airflow.listeners.listener import get_listener_manager
from airflow.models.asset import AssetModel
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session

from tests.listeners import asset_listener


@pytest.fixture(autouse=True)
def clean_listener_manager():
    lm = get_listener_manager()
    lm.clear()
    lm.add_listener(asset_listener)
    yield
    lm = get_listener_manager()
    lm.clear()
    asset_listener.clear()


@pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
@pytest.mark.db_test
@provide_session
def test_asset_listener_on_asset_changed_gets_calls(create_task_instance_of_operator, session):
    asset_uri = "test_asset_uri"
    asset = Asset(uri=asset_uri)
    asset_model = AssetModel(uri=asset_uri)
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
