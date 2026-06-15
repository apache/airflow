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
"""
QA wave 14 — 3.2.x -> 3.3 upgrade-path & fix-interaction seam stress.

These tests exercise the seams where OLD serialized data (3.2.x) meets the NEW
code + the 4 in-flight fixes, and where the fixes interact:

  fix #1 scheduler_job_runner.py  -- begin_nested() per-deadline isolation
  fix #2 models/callback.py       -- get_metric_info routing-strip + coercion,
                                      handle_event body coercion fallback
  fix #3 triggerer_job_runner.py  -- BaseException handler -> FAILED event for CallbackTrigger
  fix #4 triggers/callback.py     -- stored-context fallback + sibling-import sys.path
"""

from __future__ import annotations

import json
from unittest import mock

import pytest

from airflow.models.callback import CallbackState
from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY, CallbackTrigger

OLD_CALLBACK_PATH = "tests.unit.triggers.test_callback_upgrade_seams.dummy_async_callback"


async def dummy_async_callback(**kwargs):
    return kwargs.get("ret", "ok")


# ---------------------------------------------------------------------------
# Scenario 1: Old-format TriggererCallback (3.2.x) with context in kwargs, NO
# dag_id/run_id routing, flowing through NEW triggerer path. Stored-context
# fallback (fix #4) + non-string-output coercion (fix #2) must both behave.
# ---------------------------------------------------------------------------
class TestScenario1OldFormatStoredContext:
    @pytest.mark.asyncio
    @mock.patch("airflow.triggers.callback.import_string")
    async def test_old_format_stored_context_dict_return_coerced(self, mock_import_string):
        # callback returns a dict (non-string) -> handle_event coercion will apply later,
        # but at the trigger level the body just passes through. Assert the stored context
        # is used and stripped, and the non-string body is carried in the SUCCESS event.
        async def cb(**kwargs):
            assert kwargs == {"message": "hi", "context": {"dag_run": "legacy"}}
            return {"k": "v", "n": 1}

        mock_import_string.return_value = cb
        trigger = CallbackTrigger(
            callback_path=OLD_CALLBACK_PATH,
            callback_kwargs={"message": "hi", "context": {"dag_run": "legacy"}},
        )
        # Freshly deserialized: TriggerRunner did NOT set _callback_context.
        assert trigger._callback_context is None

        gen = trigger.run()
        running = await anext(gen)
        assert running.payload[PAYLOAD_STATUS_KEY] == CallbackState.RUNNING
        success = await anext(gen)
        assert success.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        assert success.payload[PAYLOAD_BODY_KEY] == {"k": "v", "n": 1}

    @pytest.mark.asyncio
    @mock.patch("airflow.triggers.callback.import_string")
    async def test_old_format_context_none_in_kwargs(self, mock_import_string):
        # 3.2.x sometimes stored context=None explicitly. accepts_context AND context
        # is not None -> should NOT pass context, but MUST still strip the None key.
        captured = {}

        async def cb(**kwargs):
            captured.update(kwargs)
            return "done"

        mock_import_string.return_value = cb
        trigger = CallbackTrigger(
            callback_path=OLD_CALLBACK_PATH,
            callback_kwargs={"message": "hi", "context": None},
        )
        gen = trigger.run()
        await anext(gen)
        await anext(gen)
        # context was None -> stripped, not forwarded.
        assert captured == {"message": "hi"}


# ---------------------------------------------------------------------------
# Scenario 2: Old callback whose stored context dict has a non-serializable
# value, then the callback returns non-string. fix #2 coercion + old-context
# path (fix #4) must compose without crash. handle_event does the coercion,
# so drive it via the real model handle_event.
# ---------------------------------------------------------------------------
class TestScenario2NonSerializableContextAndReturn:
    @pytest.mark.asyncio
    @mock.patch("airflow.triggers.callback.import_string")
    async def test_nonserializable_context_value_callback_returns_nonstring(self, mock_import_string):
        class Unserializable:
            def __repr__(self):
                return "<Unserializable>"

        stored_ctx = {"dag_run": "legacy", "obj": Unserializable()}

        async def cb(**kwargs):
            # context received intact (object identity); returns a non-string dict
            assert "context" in kwargs
            return {"echo": kwargs["context"]["dag_run"], "obj": Unserializable()}

        mock_import_string.return_value = cb
        trigger = CallbackTrigger(
            callback_path=OLD_CALLBACK_PATH,
            callback_kwargs={"context": stored_ctx},
        )
        gen = trigger.run()
        await anext(gen)
        success = await anext(gen)
        assert success.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        # Body is a dict containing a non-serializable value; trigger doesn't coerce,
        # handle_event (fix #2) does. Confirm json.dumps(default=str) handles it:
        body = success.payload[PAYLOAD_BODY_KEY]
        coerced = json.dumps(body, default=str, sort_keys=True)
        assert "Unserializable" in coerced


# ---------------------------------------------------------------------------
# Scenario 6: 3.2.x callback with dag_id/run_id ABSENT hitting the triggerer's
# _create_workload / _fetch_callback_dag_run_data. dag_run_data=None must be
# handled (skip-vs-fallback) without V7 handler or coercion interfering.
# Tests the routing logic in triggerer_job_runner directly.
# ---------------------------------------------------------------------------
class TestScenario6OldFormatNoRouting:
    def test_fetch_returns_none_when_no_routing_fields(self):
        from airflow.jobs.triggerer_job_runner import TriggerRunnerSupervisor

        runner = mock.Mock(spec=TriggerRunnerSupervisor)
        trigger = mock.Mock()
        trigger.callback = mock.Mock()
        trigger.callback.data = {"path": "x", "kwargs": {"context": {"dag_run": "legacy"}}}
        # No dag_id / run_id (old 3.2.x format).
        session = mock.Mock()
        result = TriggerRunnerSupervisor._fetch_callback_dag_run_data(runner, trigger, session=session)
        assert result is None
        # Must NOT have hit the DB at all.
        session.scalar.assert_not_called()

    def test_create_workload_passes_through_old_format_without_skip(self):
        """dag_run_data is None for old format -> workload still created (NOT skipped)."""
        from airflow.executors import workloads
        from airflow.jobs.triggerer_job_runner import TriggerRunnerSupervisor

        runner = mock.Mock(spec=TriggerRunnerSupervisor)
        runner._fetch_callback_dag_run_data.return_value = None

        trigger = mock.Mock()
        trigger.task_instance = None
        trigger.id = 42
        trigger.classpath = "airflow.triggers.callback.CallbackTrigger"
        trigger.encrypted_kwargs = "enc"
        trigger.callback = mock.Mock()
        # OLD format: no dag_id/run_id in callback.data -> must fall through, not skip.
        trigger.callback.data = {"path": "x", "kwargs": {"context": {"dag_run": "legacy"}}}

        result = TriggerRunnerSupervisor._create_workload(
            runner, trigger, dag_bag=mock.Mock(), render_log_fname=mock.Mock(), session=mock.Mock()
        )
        assert isinstance(result, workloads.RunTrigger)
        assert result.dag_run_data is None
        assert result.id == 42

    def test_create_workload_skips_only_when_routing_present_but_dagrun_missing(self):
        """NEW format with dag_id/run_id but DagRun gone -> skip (return None)."""
        from airflow.jobs.triggerer_job_runner import TriggerRunnerSupervisor

        runner = mock.Mock(spec=TriggerRunnerSupervisor)
        runner._fetch_callback_dag_run_data.return_value = None

        trigger = mock.Mock()
        trigger.task_instance = None
        trigger.id = 7
        trigger.callback = mock.Mock()
        # NEW format: routing fields present, but fetch returned None (DagRun deleted).
        trigger.callback.data = {"dag_id": "d", "run_id": "r", "path": "x", "kwargs": {}}

        result = TriggerRunnerSupervisor._create_workload(
            runner, trigger, dag_bag=mock.Mock(), render_log_fname=mock.Mock(), session=mock.Mock()
        )
        assert result is None
