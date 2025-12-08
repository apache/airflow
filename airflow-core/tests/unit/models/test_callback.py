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

from airflow.models import Trigger
from airflow.models.callback import (
    Callback,
    CallbackFetchMethod,
    CallbackState,
    ExecutorCallback,
    TriggererCallback,
)
from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.triggers.base import TriggerEvent
from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_callbacks

pytestmark = [pytest.mark.db_test]


async def async_callback():
    """Empty awaitable callable used for unit tests."""
    pass


def sync_callback():
    """Empty (sync) callable used for unit tests"""
    pass


TEST_CALLBACK_KWARGS = {"arg1": "value1"}
TEST_ASYNC_CALLBACK = AsyncCallback(async_callback, kwargs=TEST_CALLBACK_KWARGS)
TEST_SYNC_CALLBACK = SyncCallback(sync_callback, kwargs=TEST_CALLBACK_KWARGS)
TEST_DAG_ID = "test_dag"


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


@pytest.fixture(scope="module", autouse=True)
def clean_db(request):
    yield
    clear_db_callbacks()


class TestCallback:
    @pytest.mark.parametrize(
        ("callback_def", "expected_cb_instance"),
        [
            pytest.param(
                TEST_ASYNC_CALLBACK, TriggererCallback(callback_def=TEST_ASYNC_CALLBACK), id="triggerer"
            ),
            pytest.param(
                TEST_SYNC_CALLBACK,
                ExecutorCallback(
                    callback_def=TEST_SYNC_CALLBACK, fetch_method=CallbackFetchMethod.IMPORT_PATH
                ),
                id="executor",
            ),
        ],
    )
    def test_create_from_sdk_def(self, callback_def, expected_cb_instance):
        returned_cb = Callback.create_from_sdk_def(callback_def)
        assert isinstance(returned_cb, type(expected_cb_instance))
        assert returned_cb.data == expected_cb_instance.data

    def test_create_from_sdk_def_unknown_type(self):
        """Test that unknown callback type raises ValueError"""

        class UnknownCallback:
            pass

        unknown_callback = UnknownCallback()

        with pytest.raises(ValueError, match="Cannot handle Callback of type"):
            Callback.create_from_sdk_def(unknown_callback)

    def test_get_metric_info(self):
        callback = TriggererCallback(TEST_ASYNC_CALLBACK, prefix="deadline_alerts", dag_id=TEST_DAG_ID)
        callback.data["kwargs"] = {"context": {"dag_id": TEST_DAG_ID}, "email": "test@example.com"}
        metric_info = callback.get_metric_info(CallbackState.SUCCESS, "0")

        assert metric_info["stat"] == "deadline_alerts.callback_success"
        assert metric_info["tags"] == {
            "result": "0",
            "path": TEST_ASYNC_CALLBACK.path,
            "kwargs": {"email": "test@example.com"},
            "dag_id": TEST_DAG_ID,
        }


class TestTriggererCallback:
    def test_polymorphic_serde(self, session):
        """Test that TriggererCallback can be serialized and deserialized"""
        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        session.add(callback)
        session.commit()

        retrieved = session.query(Callback).filter_by(id=callback.id).one()
        assert isinstance(retrieved, TriggererCallback)
        assert retrieved.fetch_method == CallbackFetchMethod.IMPORT_PATH
        assert retrieved.data == TEST_ASYNC_CALLBACK.serialize()
        assert retrieved.state == CallbackState.PENDING.value
        assert retrieved.output is None
        assert retrieved.priority_weight == 1
        assert retrieved.created_at is not None
        assert retrieved.trigger_id is None

    def test_queue(self, session):
        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        assert callback.state == CallbackState.PENDING
        assert callback.trigger is None

        callback.queue()
        assert isinstance(callback.trigger, Trigger)
        assert callback.trigger.kwargs["callback_path"] == TEST_ASYNC_CALLBACK.path
        assert callback.trigger.kwargs["callback_kwargs"] == TEST_ASYNC_CALLBACK.kwargs
        assert callback.state == CallbackState.QUEUED

    @pytest.mark.parametrize(
        ("event", "terminal_state"),
        [
            pytest.param(
                TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: "test_result"}),
                True,
                id="success_event",
            ),
            pytest.param(
                TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.FAILED, PAYLOAD_BODY_KEY: "RuntimeError"}),
                True,
                id="failed_event",
            ),
            pytest.param(
                TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.RUNNING}),
                False,
                id="running_event",
            ),
            pytest.param(
                TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.QUEUED, PAYLOAD_BODY_KEY: ""}),
                False,
                id="invalid_event",
            ),
            pytest.param(TriggerEvent({PAYLOAD_STATUS_KEY: "unknown_state"}), False, id="unknown_event"),
        ],
    )
    def test_handle_event(self, session, event, terminal_state):
        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        callback.queue()
        callback.handle_event(event, session)

        status = event.payload[PAYLOAD_STATUS_KEY]
        if status in set(CallbackState):
            assert callback.state == status
        else:
            assert callback.state == CallbackState.QUEUED

        if terminal_state:
            assert callback.trigger is None
            assert callback.output == event.payload[PAYLOAD_BODY_KEY]


class TestExecutorCallback:
    def test_polymorphic_serde(self, session):
        """Test that ExecutorCallback can be serialized and deserialized"""
        callback = ExecutorCallback(TEST_SYNC_CALLBACK, fetch_method=CallbackFetchMethod.IMPORT_PATH)
        session.add(callback)
        session.commit()

        retrieved = session.query(Callback).filter_by(id=callback.id).one()
        assert isinstance(retrieved, ExecutorCallback)
        assert retrieved.fetch_method == CallbackFetchMethod.IMPORT_PATH
        assert retrieved.data == TEST_SYNC_CALLBACK.serialize()
        assert retrieved.state == CallbackState.PENDING.value
        assert retrieved.output is None
        assert retrieved.priority_weight == 1
        assert retrieved.created_at is not None
        assert retrieved.trigger_id is None

    def test_queue(self):
        callback = ExecutorCallback(TEST_SYNC_CALLBACK, fetch_method=CallbackFetchMethod.DAG_ATTRIBUTE)
        assert callback.state == CallbackState.PENDING

        callback.queue()
        assert callback.state == CallbackState.QUEUED


# Note: class DagProcessorCallback is tested in airflow-core/tests/unit/dag_processing/test_manager.py
