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

import uuid

import pytest
from sqlalchemy import select

from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance as TIDataModel
from airflow.callbacks.callback_requests import DagCallbackRequest, TaskCallbackRequest
from airflow.callbacks.database_callback_sink import DatabaseCallbackSink
from airflow.models.callback import Callback, CallbackFetchMethod, DagProcessorCallback, ExecutorCallback
from airflow.utils.state import CallbackState, TaskInstanceState

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_callbacks

pytestmark = pytest.mark.db_test

REQUEST = DagCallbackRequest(
    filepath="dag.py", dag_id="test_dag", run_id="test_run", bundle_name="testing", bundle_version=None
)


@pytest.fixture(autouse=True)
def _clean_callbacks():
    clear_db_callbacks()
    yield
    clear_db_callbacks()


@pytest.mark.parametrize(
    ("route", "expected_class", "expected_state"),
    [
        pytest.param("True", ExecutorCallback, CallbackState.SCHEDULED, id="routing-on"),
        pytest.param("False", DagProcessorCallback, None, id="routing-off"),
    ],
)
def test_send_stores_callbacks_for_routing_only_when_it_is_on(session, route, expected_class, expected_state):
    """With routing on, no Dag processor claims a callback before the scheduler routes it."""
    with conf_vars({("callbacks", "route_dag_processor_callbacks"): route}):
        sink = DatabaseCallbackSink()

    sink.send(REQUEST, session=session)
    session.commit()

    (callback,) = session.scalars(select(Callback))
    assert type(callback) is expected_class
    assert callback.state == expected_state
    assert callback.fetch_method == CallbackFetchMethod.DAG_ATTRIBUTE
    assert callback.bundle_name == "testing"
    assert callback.get_callback_request() == REQUEST


@pytest.mark.parametrize(
    ("route", "expected_classes"),
    [
        pytest.param("True", [], id="routing-on"),
        pytest.param("False", [DagProcessorCallback], id="routing-off"),
    ],
)
def test_send_stores_task_successes_only_for_dag_processors(session, route, expected_classes):
    """Dag processors drop them, so with routing on no worker parses a Dag file for one."""
    request = TaskCallbackRequest(
        filepath="dag.py",
        bundle_name="testing",
        bundle_version=None,
        ti=TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            dag_version_id=uuid.uuid4(),
        ),
        task_callback_type=TaskInstanceState.SUCCESS,
    )
    with conf_vars({("callbacks", "route_dag_processor_callbacks"): route}):
        sink = DatabaseCallbackSink()

    sink.send(request, session=session)
    session.commit()

    assert [type(callback) for callback in session.scalars(select(Callback))] == expected_classes
