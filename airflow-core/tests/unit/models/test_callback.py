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

from airflow.models.callback import (
    Callback,
    CallbackFetchMethod,
    CallbackState,
    ExecutorCallback,
    TriggererCallback,
)
from airflow.sdk.definitions.deadline import AsyncCallback, SyncCallback
from airflow.utils.session import create_session


async def async_callback():
    """Empty awaitable callable used for unit tests."""
    pass


def sync_callback():
    """Empty (sync) callable used for unit tests"""
    pass


TEST_CALLBACK_KWARGS = {"arg1": "value1"}
TEST_ASYNC_CALLBACK = AsyncCallback(async_callback, kwargs=TEST_CALLBACK_KWARGS)
TEST_SYNC_CALLBACK = SyncCallback(sync_callback, kwargs=TEST_CALLBACK_KWARGS)


@pytest.fixture
def session():
    """Fixture that provides a SQLAlchemy session"""
    with create_session() as session:
        yield session


class TestCallback:
    @pytest.mark.parametrize(
        "callback_def, expected_cb_instance",
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

    def test_test_create_from_sdk_def_unknown_type(self):
        """Test that unknown callback type raises ValueError"""

        class UnknownCallback:
            pass

        unknown_callback = UnknownCallback()

        with pytest.raises(ValueError, match="Cannot handle Callback of type"):
            Callback.create_from_sdk_def(unknown_callback)


class TestTriggererCallback:
    @pytest.mark.db_test
    def test_polymorphic_serde(self, session):
        """Test that TriggererCallback can be serialized and deserialized"""
        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        session.add(callback)
        session.commit()

        retrieved = session.query(Callback).filter_by(id=callback.id).one()
        assert isinstance(retrieved, TriggererCallback)
        assert retrieved.fetch_method == CallbackFetchMethod.IMPORT_PATH
        assert retrieved.data == TEST_ASYNC_CALLBACK.serialize()
        assert retrieved.state == CallbackState.PENDING
        assert retrieved.output is None
        assert retrieved.priority_weight == 1
        assert retrieved.created_at is not None
        assert retrieved.trigger_id is None

    def test_queue(self):
        callback = TriggererCallback(TEST_ASYNC_CALLBACK)
        assert callback.state == CallbackState.PENDING

        callback.queue()
        assert callback.state == CallbackState.QUEUED


class TestExecutorCallback:
    @pytest.mark.db_test
    def test_polymorphic_serde(self, session):
        """Test that ExecutorCallback can be serialized and deserialized"""
        callback = ExecutorCallback(TEST_SYNC_CALLBACK, fetch_method=CallbackFetchMethod.IMPORT_PATH)
        session.add(callback)
        session.commit()

        retrieved = session.query(Callback).filter_by(id=callback.id).one()
        assert isinstance(retrieved, ExecutorCallback)
        assert retrieved.fetch_method == CallbackFetchMethod.IMPORT_PATH
        assert retrieved.data == TEST_SYNC_CALLBACK.serialize()
        assert retrieved.state == CallbackState.PENDING
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
