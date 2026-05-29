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

import sys
from unittest import mock

import pytest

from airflow.models.callback import CallbackState
from airflow.sdk import BaseNotifier
from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY, CallbackTrigger

TEST_MESSAGE = "test_message"
TEST_CALLBACK_PATH = "classpath.test_callback"
TEST_CALLBACK_KWARGS = {"message": TEST_MESSAGE}
TEST_CALLBACK_CONTEXT = {"dag_run": "test"}


class ExampleAsyncNotifier(BaseNotifier):
    """Example of a properly implemented async notifier."""

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
        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs=dict(TEST_CALLBACK_KWARGS),
        )
        # Simulate the TriggerRunner setting context (built from dag_run_data)
        trigger.context = TEST_CALLBACK_CONTEXT
        return trigger

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
        mock_callback.assert_called_once_with(**TEST_CALLBACK_KWARGS, context=TEST_CALLBACK_CONTEXT)
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
        mock_callback.assert_called_once_with(**TEST_CALLBACK_KWARGS, context=TEST_CALLBACK_CONTEXT)
        assert failure_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.FAILED
        assert all(s in failure_event.payload[PAYLOAD_BODY_KEY] for s in ["raise", "RuntimeError", exc_msg])

    @pytest.mark.asyncio
    async def test_run_without_context(self, mock_import_string):
        """Test trigger calls callback without context when self.context is None."""
        callback_return_value = "no context value"
        mock_callback = mock.AsyncMock(return_value=callback_return_value)
        mock_import_string.return_value = mock_callback

        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs={"message": TEST_MESSAGE},
        )
        # self.context is None by default (not set by TriggerRunner)

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        success_event = await anext(trigger_gen)
        # Context is None, so callback is called without context parameter
        mock_callback.assert_called_once_with(message=TEST_MESSAGE)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert success_event.payload[PAYLOAD_BODY_KEY] == callback_return_value


class TestEnsureBundleModuleRegistered:
    """Tests for _ensure_bundle_module_registered."""

    def test_registers_module_from_matching_bundle(self, tmp_path):
        from airflow.triggers.callback import _ensure_bundle_module_registered

        stem = "my_dag_file"
        mod_name = f"unusual_prefix_{'e' * 40}_{stem}"
        (tmp_path / f"{stem}.py").write_text("LOADED = True\n")

        fake_bundle = mock.Mock()
        fake_bundle.name = "test-bundle"
        fake_bundle.path = tmp_path

        with mock.patch(
            "airflow.dag_processing.bundles.manager.DagBundlesManager"
        ) as mock_mgr:
            mock_mgr.return_value.get_all_dag_bundles.return_value = [fake_bundle]
            _ensure_bundle_module_registered(f"{mod_name}.my_func")

        assert mod_name in sys.modules
        assert sys.modules[mod_name].LOADED is True
        sys.modules.pop(mod_name)

    def test_noop_when_module_already_in_sys_modules(self, tmp_path):
        from airflow.triggers.callback import _ensure_bundle_module_registered

        stem = "cached_mod"
        mod_name = f"unusual_prefix_{'f' * 40}_{stem}"

        sentinel = mock.Mock()
        sys.modules[mod_name] = sentinel
        try:
            with mock.patch(
                "airflow.dag_processing.bundles.manager.DagBundlesManager"
            ) as mock_mgr:
                _ensure_bundle_module_registered(f"{mod_name}.fn")
                mock_mgr.assert_not_called()
        finally:
            sys.modules.pop(mod_name, None)

    def test_graceful_when_no_bundle_contains_module(self, tmp_path):
        from airflow.triggers.callback import _ensure_bundle_module_registered

        mod_name = "unusual_prefix_" + "a" * 40 + "_absent_module"
        fake_bundle = mock.Mock()
        fake_bundle.name = "empty-bundle"
        fake_bundle.path = tmp_path  # stem file doesn't exist here

        with mock.patch(
            "airflow.dag_processing.bundles.manager.DagBundlesManager"
        ) as mock_mgr:
            mock_mgr.return_value.get_all_dag_bundles.return_value = [fake_bundle]
            # Should not raise
            _ensure_bundle_module_registered(f"{mod_name}.fn")

        assert mod_name not in sys.modules

    def test_noop_for_non_mangled_path(self):
        from airflow.triggers.callback import _ensure_bundle_module_registered

        # A normal dotted path has no unusual_prefix_ prefix — the function returns
        # early when split("_", 3) doesn't yield at least 4 parts, so no bundle
        # manager is ever instantiated.
        before = set(sys.modules)
        _ensure_bundle_module_registered("airflow.providers.slack.notifiers.MyNotifier")
        after = set(sys.modules)
        assert after == before  # no new modules added
