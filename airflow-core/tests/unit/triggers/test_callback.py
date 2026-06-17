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
from collections import deque
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
        trigger._callback_context = TEST_CALLBACK_CONTEXT
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
    async def test_run_renders_jinja_kwargs(self, mock_import_string):
        """String kwargs containing Jinja are rendered against the context before the callback is
        called — matching the synchronous executor path (regression test for the async path
        previously passing kwargs through verbatim)."""
        mock_callback = mock.AsyncMock(return_value="ok")
        mock_import_string.return_value = mock_callback

        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs={"rendered": "run={{ dag_run }}", "plain": "no-jinja", "n": 42},
        )
        trigger._callback_context = TEST_CALLBACK_CONTEXT  # {"dag_run": "test"}

        trigger_gen = trigger.run()
        await anext(trigger_gen)  # RUNNING
        await anext(trigger_gen)  # SUCCESS

        # "{{ dag_run }}" -> "test"; non-jinja string and non-string kwargs pass through untouched.
        mock_callback.assert_called_once_with(
            rendered="run=test", plain="no-jinja", n=42, context=TEST_CALLBACK_CONTEXT
        )

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
        # _callback_context is None by default (not set by TriggerRunner)

        trigger_gen = trigger.run()

        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        success_event = await anext(trigger_gen)
        # Context is None, so callback is called without context parameter
        mock_callback.assert_called_once_with(message=TEST_MESSAGE)
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert success_event.payload[PAYLOAD_BODY_KEY] == callback_return_value

    @pytest.mark.asyncio
    async def test_run_with_both_runtime_and_stale_kwargs_context(self, mock_import_string):
        """A 3.2.x-serialized callback trigger has a stale ``context`` in ``callback_kwargs``; a 3.3
        TriggerRunner ALSO sets ``_callback_context`` (it does so unconditionally for callback
        triggers with dag_run_data — triggerer_job_runner.py:1360). The runtime context must WIN,
        and the stale kwargs ``context`` MUST be stripped so it is not double-passed (which would
        raise ``TypeError: got multiple values for keyword argument 'context'``).
        """
        mock_callback = mock.AsyncMock(return_value="ok")
        mock_import_string.return_value = mock_callback

        trigger = CallbackTrigger(
            callback_path=TEST_CALLBACK_PATH,
            callback_kwargs={"message": TEST_MESSAGE, "context": {"dag_run": "stale_legacy"}},
        )
        # New path: the TriggerRunner injected a fresh runtime context.
        trigger._callback_context = {"dag_run": "fresh_runtime"}

        trigger_gen = trigger.run()
        running_event = await anext(trigger_gen)
        assert running_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING

        # Must NOT raise "multiple values for keyword argument 'context'".
        success_event = await anext(trigger_gen)

        # Runtime context wins; the stale kwargs context is stripped (passed exactly once).
        mock_callback.assert_called_once_with(message=TEST_MESSAGE, context={"dag_run": "fresh_runtime"})
        assert success_event.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS


class TestEnsureBundleModuleRegistered:
    """Tests for _ensure_bundle_module_registered."""

    def test_registers_module_from_matching_bundle(self, tmp_path):
        from airflow.triggers.callback import _ensure_bundle_module_registered
        from airflow.utils.file import get_unique_dag_module_name

        stem = "my_dag_file"
        (tmp_path / f"{stem}.py").write_text("LOADED = True\n")
        # Use the real mangled name so the hash verification passes
        mod_name = get_unique_dag_module_name(str(tmp_path / f"{stem}.py"))

        fake_bundle = mock.Mock()
        fake_bundle.name = "test-bundle"
        fake_bundle.path = tmp_path

        with mock.patch("airflow.triggers.callback.DagBundlesManager") as mock_mgr:
            mock_mgr.return_value.get_all_dag_bundles.return_value = [fake_bundle]
            _ensure_bundle_module_registered(f"{mod_name}.my_func")

        assert mod_name in sys.modules
        assert sys.modules[mod_name].LOADED is True
        sys.modules.pop(mod_name)

    def test_adds_bundle_path_to_sys_path_for_sibling_imports(self, tmp_path):
        """The bundle dir must be put on sys.path so callbacks can import sibling modules.

        Parity with the executor/sync path (callback_supervisor) which appends the bundle
        path to sys.path; without it, ``import sibling_helper`` inside an async callback
        fails with ModuleNotFoundError on the triggerer.
        """
        from airflow.triggers.callback import _ensure_bundle_module_registered
        from airflow.utils.file import get_unique_dag_module_name

        stem = "dag_with_sibling"
        (tmp_path / f"{stem}.py").write_text("LOADED = True\n")
        (tmp_path / "sibling_helper.py").write_text("HELPER = 'ok'\n")
        mod_name = get_unique_dag_module_name(str(tmp_path / f"{stem}.py"))

        fake_bundle = mock.Mock()
        fake_bundle.name = "test-bundle"
        fake_bundle.path = tmp_path

        bundle_path = str(tmp_path)
        original_sys_path = list(sys.path)
        try:
            with mock.patch("airflow.triggers.callback.DagBundlesManager") as mock_mgr:
                mock_mgr.return_value.get_all_dag_bundles.return_value = [fake_bundle]
                _ensure_bundle_module_registered(f"{mod_name}.my_func")

            assert bundle_path in sys.path
            # The sibling module is now importable because its directory is on sys.path.
            import importlib

            sibling = importlib.import_module("sibling_helper")
            assert sibling.HELPER == "ok"
        finally:
            sys.modules.pop(mod_name, None)
            sys.modules.pop("sibling_helper", None)
            sys.path[:] = original_sys_path


async def _callback_raising_system_exit(**kwargs):
    """An async callback that raises a BaseException (not Exception) subclass.

    ``CallbackTrigger.run()`` only catches ``Exception``, so a ``SystemExit`` raised
    here escapes ``run()`` and propagates into ``TriggerRunner.run_trigger``'s
    ``async for`` loop — exactly the path the BaseException handler guards.
    """
    raise SystemExit(2)


class TestCallbackTriggerBaseExceptionHandling:
    """Regression tests for the triggerer BaseException handler (fix #3).

    A callback that raises a ``BaseException`` subclass (``SystemExit``,
    ``KeyboardInterrupt``, ``GeneratorExit``, or a custom ``BaseException``) must NOT
    crash the triggerer event loop or leave the Callback row stuck in ``RUNNING``
    forever. ``run_trigger`` must instead emit a terminal ``FAILED`` event for the
    callback and return without re-raising.
    """

    def test_callback_trigger_system_exit_emits_failed_event_and_does_not_propagate(self, monkeypatch):
        """A SystemExit from a CallbackTrigger callback yields a FAILED event, not a crash."""
        import asyncio

        from airflow.jobs.triggerer_job_runner import TriggerRunner

        # Disable the greenback portal so run_trigger doesn't require the supervisor's
        # event-loop setup (parity with how the inline run_trigger tests drive it).
        monkeypatch.setenv("AIRFLOW_DISABLE_GREENBACK_PORTAL", "true")

        callback_path = f"{__name__}._callback_raising_system_exit"
        trigger = CallbackTrigger(callback_path=callback_path, callback_kwargs={})

        trigger_runner = TriggerRunner()
        trigger_runner.triggers = {
            1: {"task": mock.MagicMock(spec=asyncio.Task), "is_watcher": False, "name": "cb", "events": 0}
        }

        # Must NOT raise SystemExit out of run_trigger — that is what would tear down
        # the triggerer event loop. The handler swallows it and returns cleanly.
        asyncio.run(trigger_runner.run_trigger(1, trigger))

        # The RUNNING event is emitted by CallbackTrigger.run() before the callback is
        # invoked; the terminal FAILED event is emitted by the BaseException handler.
        statuses = [entry.event.payload[PAYLOAD_STATUS_KEY] for entry in trigger_runner.events]
        assert CallbackState.FAILED in statuses, (
            "BaseException handler must emit a terminal FAILED event so the Callback "
            "row is not stuck in RUNNING forever"
        )
        # Callback triggers are terminalised via an event, never via the failure queue.
        assert trigger_runner.failed_triggers == deque()
        # The FAILED body names the BaseException type so operators can diagnose it.
        failed_bodies = [
            entry.event.payload.get(PAYLOAD_BODY_KEY)
            for entry in trigger_runner.events
            if entry.event.payload[PAYLOAD_STATUS_KEY] == CallbackState.FAILED
        ]
        assert any("SystemExit" in (body or "") for body in failed_bodies)
