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

import os
from unittest import mock

import pytest

from airflow.models.callback import CallbackState
from airflow.sdk import BaseNotifier
from airflow.triggers.callback import (
    PAYLOAD_BODY_KEY,
    PAYLOAD_STATUS_KEY,
    CallbackTrigger,
    InvalidCallbackPathError,
    _validate_callback,
)

TEST_MESSAGE = "test_message"
TEST_CALLBACK_PATH = "classpath.test_callback"
TEST_CALLBACK_KWARGS = {"message": TEST_MESSAGE, "context": {"dag_run": "test"}}


class ExampleAsyncNotifier(BaseNotifier):
    """Example of a properly implemented async notifier."""

    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.message = message

    async def async_notify(self, context):
        return f"Async notification: {self.message}, context: {context}"

    def notify(self, context):
        return f"Sync notification: {self.message}, context: {context}"


async def example_async_callback(message, context=None):
    return f"Async callback: {message}"


def example_sync_callback(message, context=None):
    return f"Sync callback: {message}"


class ExampleAsyncCallableObject:
    async def __call__(self, message, context=None):
        return f"Async callable object: {message}"


class ExamplePlainClass:
    def __init__(self, message, context=None):
        self.message = message


class TestValidateCallback:
    @pytest.mark.parametrize(
        "callback",
        [
            pytest.param(example_async_callback, id="coroutine-function"),
            pytest.param(ExampleAsyncNotifier, id="async-notifier-class"),
            pytest.param(mock.AsyncMock(), id="async-mock"),
        ],
    )
    def test_valid_callbacks_pass(self, callback):
        _validate_callback(callback, "some.callback.path")

    @pytest.mark.parametrize(
        ("callback", "match"),
        [
            pytest.param(os, "does not resolve to a callable", id="module"),
            pytest.param("not-a-callable", "does not resolve to a callable", id="string"),
            pytest.param(42, "does not resolve to a callable", id="int"),
            pytest.param(example_sync_callback, "coroutine function or an awaitable", id="sync-function"),
            pytest.param(os.getcwd, "coroutine function or an awaitable", id="sync-builtin"),
            pytest.param(ExamplePlainClass, "coroutine function or an awaitable", id="plain-class"),
            pytest.param(
                ExampleAsyncCallableObject(),
                "coroutine function or an awaitable",
                id="async-callable-object-rejected-like-the-sdk",
            ),
        ],
    )
    def test_invalid_callbacks_rejected(self, callback, match):
        with pytest.raises(InvalidCallbackPathError, match=match):
            _validate_callback(callback, "some.callback.path")


class TestCallbackTrigger:
    @pytest.fixture
    def trigger(self):
        """Create a fresh trigger per test to avoid shared mutable state."""
        return CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs=dict(TEST_CALLBACK_KWARGS),
        )

    @pytest.fixture
    def mock_import_string(self):
        with mock.patch("airflow.triggers.callback.import_string") as m:
            yield m

    @pytest.mark.parametrize(
        ("callback_init_kwargs", "expected_serialized_kwargs"),
        [
            pytest.param(None, {}, id="no kwargs"),
            pytest.param(TEST_CALLBACK_KWARGS, TEST_CALLBACK_KWARGS, id="non-empty kwargs"),
        ],
    )
    def test_serialization(self, callback_init_kwargs, expected_serialized_kwargs):
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs=callback_init_kwargs,
        )
        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.triggers.callback.CallbackTrigger"
        assert kwargs == {
            "callback_path": TEST_CALLBACK_PATH,
            "callback_kwargs": expected_serialized_kwargs,
        }

    def test_queue_not_inherited_from_task(self):
        """False so `_defer_task` doesn't overwrite the trigger's own `queue` with the task's."""
        assert CallbackTrigger.trigger_queue_inherited_from_task is False

    def test_queue_attribute_is_not_part_of_serialized_kwargs(self):
        """``queue`` is read directly off the trigger by ``Trigger.from_object``, not via serialize()."""
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs=None,
            queue="custom-queue",
        )

        assert trigger.queue == "custom-queue"
        _, kwargs = trigger.serialize()
        assert "queue" not in kwargs

    def test_queue_defaults_to_none(self, trigger):
        assert trigger.queue is None

    @pytest.mark.asyncio
    async def test_run_success_with_async_function(self, trigger, mock_import_string):
        """Test trigger handles async functions correctly."""
        callback_return_value = "some value"
        mock_callback = mock.AsyncMock(return_value=callback_return_value)
        mock_import_string.return_value = mock_callback

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        success_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        # AsyncMock accepts **kwargs, so _accepts_context returns True and context is passed through
        mock_callback.assert_called_once_with(**TEST_CALLBACK_KWARGS)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert success_event.payload[PAYLOAD_BODY_KEY] == callback_return_value

    @pytest.mark.asyncio
    async def test_run_success_with_notifier(self, trigger, mock_import_string):
        """Test trigger handles async notifier classes correctly."""
        mock_import_string.return_value = ExampleAsyncNotifier

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        success_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert (
            success_event.payload[PAYLOAD_BODY_KEY]
            == f"Async notification: {TEST_MESSAGE}, context: {{'dag_run': 'test'}}"
        )

    @pytest.mark.asyncio
    async def test_run_failure(self, trigger, mock_import_string):
        exc_msg = "Something went wrong"
        mock_callback = mock.AsyncMock(side_effect=RuntimeError(exc_msg))
        mock_import_string.return_value = mock_callback

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        failure_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        # AsyncMock accepts **kwargs, so _accepts_context returns True and context is passed through
        mock_callback.assert_called_once_with(**TEST_CALLBACK_KWARGS)
        assert failure_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.FAILED
        assert all(s in failure_event.payload[PAYLOAD_BODY_KEY] for s in ["raise", "RuntimeError", exc_msg])

    @pytest.mark.asyncio
    async def test_run_with_real_coroutine_function_path(self):
        trigger = CallbackTrigger(
            callback_path=f"{__name__}.example_async_callback",
            callback_kwargs={"message": TEST_MESSAGE},
        )
        trigger_gen = trigger.run()

        assert (await anext(trigger_gen)).payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING
        success_event = await anext(trigger_gen)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert success_event.payload[PAYLOAD_BODY_KEY] == f"Async callback: {TEST_MESSAGE}"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "callback_path",
        [
            pytest.param("os.path", id="module"),
            pytest.param("os.sep", id="non-callable-attribute"),
            pytest.param(f"{__name__}.ExamplePlainClass", id="plain-class"),
        ],
    )
    async def test_run_fails_for_invalid_callback_path(self, callback_path):
        trigger = CallbackTrigger(callback_path=callback_path, callback_kwargs={})
        trigger_gen = trigger.run()

        assert (await anext(trigger_gen)).payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING
        failure_event = await anext(trigger_gen)

        assert failure_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.FAILED
        assert (
            "The callback path does not resolve to a valid callback"
            in failure_event.payload[PAYLOAD_BODY_KEY]
        )
        assert "InvalidCallbackPathError" in failure_event.payload[PAYLOAD_BODY_KEY]

    @pytest.mark.asyncio
    async def test_run_refuses_a_sync_callable_without_calling_it(self):
        calls = []

        def sync_spy(*args, **kwargs):
            calls.append((args, kwargs))

        trigger = CallbackTrigger(callback_path=f"{__name__}.example_sync_callback", callback_kwargs={})
        trigger_gen = trigger.run()

        with mock.patch(f"{__name__}.example_sync_callback", sync_spy):
            assert (await anext(trigger_gen)).payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING
            failure_event = await anext(trigger_gen)

        assert calls == []
        assert failure_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.FAILED
        assert "InvalidCallbackPathError" in failure_event.payload[PAYLOAD_BODY_KEY]
