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

from airflow.models.callback import CallbackState
from airflow.sdk import BaseNotifier
from airflow.triggers.callback import (
    PAYLOAD_BODY_KEY,
    PAYLOAD_STATUS_KEY,
    CallbackTrigger,
    _is_notifier_class,
)

TEST_MESSAGE = "test_message"
TEST_CALLBACK_PATH = "classpath.test_callback"
TEST_CONTEXT = {
    "dag_run": {"dag_id": "test_dag"},
    "dag_id": "test_dag",
    "run_id": "test_run",
    "ds": "2024-01-01",
    "ts": "2024-01-01T00:00:00+00:00",
    "deadline": {"id": "abc-123", "deadline_time": "2024-01-01T01:00:00+00:00"},
}
TEST_CALLBACK_KWARGS = {"message": TEST_MESSAGE, "context": TEST_CONTEXT}


class ExampleAsyncNotifier(BaseNotifier):
    """Example of a properly implemented async notifier."""

    template_fields = ("message",)

    def __init__(self, message, **kwargs):
        super().__init__(**kwargs)
        self.message = message

    async def async_notify(self, context):
        return f"Async notification: {self.message}, context: {context}"

    def notify(self, context):
        return f"Sync notification: {self.message}, context: {context}"


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

    @pytest.mark.asyncio
    async def test_run_success_with_async_function(self, trigger, mock_import_string):
        """Test trigger handles async functions correctly and renders templates."""
        callback_return_value = "some value"
        mock_callback = mock.AsyncMock(return_value=callback_return_value)
        mock_import_string.return_value = mock_callback

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        success_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        # Context is popped and passed separately; kwargs are rendered (no-op here since no templates)
        mock_callback.assert_called_once_with(message=TEST_MESSAGE, context=TEST_CONTEXT)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert success_event.payload[PAYLOAD_BODY_KEY] == callback_return_value

    @pytest.mark.asyncio
    async def test_run_success_with_notifier(self, trigger, mock_import_string):
        """Test trigger handles async notifier classes correctly without pre-rendering."""
        mock_import_string.return_value = ExampleAsyncNotifier

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        success_event = await anext(trigger_gen)
        mock_import_string.assert_called_once_with(TEST_CALLBACK_PATH)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert (
            success_event.payload[PAYLOAD_BODY_KEY]
            == f"Async notification: {TEST_MESSAGE}, context: {TEST_CONTEXT}"
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
        # Context is popped and passed separately; kwargs are rendered (no-op here since no templates)
        mock_callback.assert_called_once_with(message=TEST_MESSAGE, context=TEST_CONTEXT)
        assert failure_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.FAILED
        assert all(s in failure_event.payload[PAYLOAD_BODY_KEY] for s in ["raise", "RuntimeError", exc_msg])


class TestTemplateRendering:
    """Tests for Jinja2 template rendering in callback kwargs."""

    @pytest.mark.asyncio
    async def test_run_renders_jinja_templates_in_function_kwargs(self):
        """Plain async function callbacks get their kwargs rendered."""
        context = {"dag_id": "my_dag", "ds": "2024-06-15"}
        trigger = CallbackTrigger(
            callback_path="classpath.test",
            callback_kwargs={
                "message": "DAG {{ dag_id }} missed deadline at {{ ds }}",
                "context": context,
            },
        )
        mock_callback = mock.AsyncMock(return_value="ok")
        with mock.patch("airflow.triggers.callback.import_string", return_value=mock_callback):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        mock_callback.assert_called_once_with(
            message="DAG my_dag missed deadline at 2024-06-15",
            context=context,
        )

    @pytest.mark.asyncio
    async def test_run_does_not_double_render_notifier_kwargs(self):
        """Notifier classes should NOT have kwargs pre-rendered -- they handle it themselves."""
        context = {"dag_id": "my_dag", "ds": "2024-06-15"}
        trigger = CallbackTrigger(
            callback_path="classpath.test",
            callback_kwargs={
                "message": "DAG {{ dag_id }}",
                "context": context,
            },
        )
        with mock.patch("airflow.triggers.callback.import_string", return_value=ExampleAsyncNotifier):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        # The notifier's __await__ renders template_fields, so the final output
        # should show the rendered message (rendered by the notifier, not pre-rendered).
        assert "DAG my_dag" in events[-1].payload[PAYLOAD_BODY_KEY]

    @pytest.mark.asyncio
    async def test_run_renders_nested_kwargs(self):
        """Template rendering works recursively on nested dicts and lists."""
        context = {"dag_id": "etl_pipeline"}
        trigger = CallbackTrigger(
            callback_path="classpath.test",
            callback_kwargs={
                "recipients": ["{{ dag_id }}-team@example.com"],
                "metadata": {"dag": "{{ dag_id }}"},
                "context": context,
            },
        )
        mock_callback = mock.AsyncMock(return_value="ok")
        with mock.patch("airflow.triggers.callback.import_string", return_value=mock_callback):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        mock_callback.assert_called_once_with(
            recipients=["etl_pipeline-team@example.com"],
            metadata={"dag": "etl_pipeline"},
            context=context,
        )

    @pytest.mark.asyncio
    async def test_run_skips_rendering_when_no_context(self):
        """Without context, kwargs pass through unrendered."""
        trigger = CallbackTrigger(
            callback_path="classpath.test",
            callback_kwargs={"message": "{{ dag_id }}"},
        )
        mock_callback = mock.AsyncMock(return_value="ok")
        with mock.patch("airflow.triggers.callback.import_string", return_value=mock_callback):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        mock_callback.assert_called_once_with(message="{{ dag_id }}")

    @pytest.mark.asyncio
    async def test_notifier_template_fields_rendered_with_context(self):
        """Notifier template_fields are rendered using the provided context."""
        context = {"dag_id": "my_dag", "ds": "2024-06-15"}
        trigger = CallbackTrigger(
            callback_path="classpath.test",
            callback_kwargs={
                "message": "Alert for {{ dag_id }} on {{ ds }}",
                "context": context,
            },
        )
        with mock.patch("airflow.triggers.callback.import_string", return_value=ExampleAsyncNotifier):
            events = [event async for event in trigger.run()]

        assert events[-1].payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        # The notifier's __await__ renders template_fields (self.message), so the
        # notification body contains the rendered message.
        assert "Alert for my_dag on 2024-06-15" in events[-1].payload[PAYLOAD_BODY_KEY]


class TestHelpers:
    """Tests for module-level helper functions."""

    def test_is_notifier_class_with_notifier(self):
        assert _is_notifier_class(ExampleAsyncNotifier) is True

    def test_is_notifier_class_with_function(self):
        async def my_func():
            pass

        assert _is_notifier_class(my_func) is False

    def test_is_notifier_class_with_non_notifier_class(self):
        class MyClass:
            pass

        assert _is_notifier_class(MyClass) is False

    def test_is_notifier_class_with_notifier_instance(self):
        """Instances are not classes -- should return False."""
        instance = ExampleAsyncNotifier(message="hi")
        assert _is_notifier_class(instance) is False

    def test_render_template_renders_strings(self):
        """CallbackTrigger.render_template renders string values using context."""
        trigger = CallbackTrigger(callback_path="", callback_kwargs={})
        result = trigger.render_template(
            {"message": "Hello {{ name }}", "count": 5},
            {"name": "World"},
        )
        assert result == {"message": "Hello World", "count": 5}

    def test_render_template_handles_nested_structures(self):
        """CallbackTrigger.render_template works recursively on nested structures."""
        trigger = CallbackTrigger(callback_path="", callback_kwargs={})
        result = trigger.render_template(
            {"items": ["{{ x }}", "{{ y }}"], "meta": {"key": "{{ x }}"}},
            {"x": "a", "y": "b"},
        )
        assert result == {"items": ["a", "b"], "meta": {"key": "a"}}

    def test_render_template_missing_key_renders_empty(self):
        """Missing context keys render as empty strings (Jinja2 default undefined)."""
        trigger = CallbackTrigger(callback_path="", callback_kwargs={})
        result = trigger.render_template(
            {"message": "Hello {{ nonexistent }}"},
            {"name": "World"},
        )
        assert result == {"message": "Hello "}

    def test_render_template_no_templates_is_noop(self):
        """Non-template strings and non-string values pass through unchanged."""
        trigger = CallbackTrigger(callback_path="", callback_kwargs={})
        kwargs = {"message": "plain text", "count": 42}
        result = trigger.render_template(kwargs, {"dag_id": "test"})
        assert result == kwargs
