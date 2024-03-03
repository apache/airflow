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

from airflow.datasets import Dataset
from airflow.listeners.listener import get_listener_manager
from airflow.models.dataset import DatasetModel
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from tests.listeners import dataset_listener


@pytest.fixture(autouse=True)
def clean_listener_manager():
    lm = get_listener_manager()
    lm.clear()
    lm.add_listener(dataset_listener)
    yield
    lm = get_listener_manager()
    lm.clear()
    dataset_listener.clear()


@pytest.mark.db_test
@provide_session
def test_dataset_listener_on_dataset_changed_gets_calls(create_task_instance_of_operator, session):
    dataset_uri = "test_dataset_uri"
    ds = Dataset(uri=dataset_uri)
    ds_model = DatasetModel(uri=dataset_uri)
    session.add(ds_model)

    session.flush()

    ti = create_task_instance_of_operator(
        operator_class=EmptyOperator,
        dag_id="producing_dag",
        task_id="test_task",
        session=session,
        outlets=[ds],
    )
    ti.run()

    assert len(dataset_listener.changed) == 1
    assert dataset_listener.changed[0].uri == dataset_uri
