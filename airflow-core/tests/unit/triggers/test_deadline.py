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

from unittest import mock

import pytest

from airflow.models.deadline import DeadlineCallbackState
from airflow.triggers.deadline import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY, DeadlineCallbackTrigger

TEST_CALLBACK_PATH = "classpath.test_callback_for_deadline"
TEST_CALLBACK_KWARGS = {"arg1": "value1"}
TEST_TRIGGER = DeadlineCallbackTrigger(callback_path=TEST_CALLBACK_PATH, callback_kwargs=TEST_CALLBACK_KWARGS)


class TestDeadlineCallbackTrigger:
    @pytest.fixture
    def mock_import_string(self):
        with mock.patch("airflow.triggers.deadline.import_string") as m:
            yield m

    @pytest.mark.parametrize(
        "callback_init_kwargs,expected_serialized_kwargs",
        [
            pytest.param(None, {}, id="no kwargs"),
            pytest.param(TEST_CALLBACK_KWARGS, TEST_CALLBACK_KWARGS, id="non-empty kwargs"),
        ],
    )
    def test_serialization(self, callback_init_kwargs, expected_serialized_kwargs):
        trigger = DeadlineCallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs=callback_init_kwargs,
        )
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.triggers.deadline.DeadlineCallbackTrigger"
        assert kwargs == {
            "callback_path": TEST_CALLBACK_PATH,
            "callback_kwargs": expected_serialized_kwargs,
        }

    @pytest.mark.asyncio
    async def test_run_success(self, mock_import_string):
        callback_return_value = "some value"
        mock_callback = mock.AsyncMock(return_value=callback_return_value)
        mock_import_string.return_value = mock_callback

        trigger_gen = TEST_TRIGGER.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == DeadlineCallbackState.RUNNING

        success_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        mock_callback.assert_called_once_with(**TEST_CALLBACK_KWARGS)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == DeadlineCallbackState.SUCCESS
        assert success_event.payload[PAYLOAD_BODY_KEY] == callback_return_value

    @pytest.mark.asyncio
    async def test_run_failure(self, mock_import_string):
        exc_msg = "Something went wrong"
        mock_callback = mock.AsyncMock(side_effect=RuntimeError(exc_msg))
        mock_import_string.return_value = mock_callback

        trigger_gen = TEST_TRIGGER.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == DeadlineCallbackState.RUNNING

        failure_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        mock_callback.assert_called_once_with(**TEST_CALLBACK_KWARGS)
        assert failure_event.payload[PAYLOAD_STATUS_KEY] == DeadlineCallbackState.FAILED
        assert all(s in failure_event.payload[PAYLOAD_BODY_KEY] for s in ["raise", "RuntimeError", exc_msg])
