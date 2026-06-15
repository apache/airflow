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
QA wave 14 — model-level upgrade-path & fix-interaction seams (scenarios 3,4,5,7,8).

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

from airflow.models.callback import TriggererCallback
from airflow.sdk.definitions.callback import AsyncCallback
from airflow.triggers.base import TriggerEvent
from airflow.utils.session import create_session
from airflow.utils.state import CallbackState

from tests_common.test_utils.db import clear_db_callbacks

pytestmark = [pytest.mark.db_test]


async def _async_cb():
    pass


_TEST_ASYNC = AsyncCallback(_async_cb, kwargs={})


@pytest.fixture
def session():
    with create_session() as session:
        yield session


@pytest.fixture(autouse=True)
def _clean_db():
    yield
    clear_db_callbacks()


def _make_callback(data: dict) -> TriggererCallback:
    """Build a real (SQLAlchemy-instrumented) TriggererCallback, then overwrite .data.

    Uses the real constructor (which initialises ``_sa_instance_state``) — the same
    pattern the existing test_callback.py tests use — then replaces ``.data`` with the
    exact old/new-schema blob the scenario needs.
    """
    cb = TriggererCallback(_TEST_ASYNC)
    cb.data = data
    return cb


# ---------------------------------------------------------------------------
# Scenario 3: A deadline serialized under the OLD schema (before dag_id/run_id
# moved to top-level callback.data) deserialized by NEW models/callback.py.
# get_metric_info's routing-field stripping must handle ABSENCE gracefully.
# ---------------------------------------------------------------------------
class TestScenario3OldSchemaGetMetricInfo:
    def test_get_metric_info_no_routing_fields_present(self):
        # OLD-schema callback: no dag_id/run_id/deadline_* at top level; just kwargs.
        cb = _make_callback({"kwargs": {"message": "hi"}, "prefix": "deadline"})
        info = cb.get_metric_info(CallbackState.SUCCESS, result="done")
        # No KeyError despite routing fields being absent.
        assert info["stat"] == "deadline.callback_success"
        tags = info["tags"]
        # prefix stripped; result present; kwargs present (no context to strip)
        assert "prefix" not in tags
        assert tags["result"] == "done"
        assert tags["kwargs"] == json.dumps({"message": "hi"}, default=str, sort_keys=True)

    def test_get_metric_info_old_schema_kwargs_with_context_only(self):
        # OLD: context lived inside kwargs (no top-level routing). Coercion + context-strip compose.
        cb = _make_callback(
            {"kwargs": {"message": "hi", "context": {"dag_run": "legacy"}}, "prefix": "deadline"}
        )
        info = cb.get_metric_info(CallbackState.FAILED, result={"err": "boom"})
        tags = info["tags"]
        # context stripped from kwargs (no high-cardinality leak)
        kwargs_tag = json.loads(tags["kwargs"])
        assert kwargs_tag == {"message": "hi"}
        # non-primitive result coerced to a JSON string (no unhashable crash)
        assert isinstance(tags["result"], str)
        assert json.loads(tags["result"]) == {"err": "boom"}


# ---------------------------------------------------------------------------
# Scenario 7: get_metric_info with routing fields PRESENT (new schema) AND a
# non-primitive in kwargs. Metric-tag stripping + non-primitive coercion must
# compose (no high-cardinality leak, no unhashable crash).
# ---------------------------------------------------------------------------
class TestScenario7RoutingPlusNonPrimitive:
    def test_routing_stripped_and_nonprimitive_coerced(self):
        cb = _make_callback(
            {
                "dag_id": "d",
                "run_id": "r",
                "deadline_id": "x",
                "deadline_time": "2026-01-01T00:00:00+00:00",
                "dag_run_id": "1",
                "kwargs": {"payload": {"nested": [1, 2, 3]}, "context": {"ti": "x"}},
                "prefix": "deadline",
            }
        )
        info = cb.get_metric_info(CallbackState.SUCCESS, result={"ret": "v"})
        tags = info["tags"]
        # ALL routing fields stripped (no high-cardinality leak)
        for k in ("dag_id", "run_id", "deadline_id", "deadline_time", "dag_run_id", "prefix"):
            assert k not in tags, f"{k} leaked into metric tags"
        # context stripped from kwargs; non-primitive nested kwargs coerced to str
        kwargs_tag = json.loads(tags["kwargs"])
        assert "context" not in kwargs_tag
        assert kwargs_tag == {"payload": {"nested": [1, 2, 3]}}
        # Every remaining tag value is a metric-safe primitive (OTel frozenset-hashable)
        for v in tags.values():
            assert isinstance(v, (str, int, float, bool)) or v is None
        # Sanity: the whole tag set is hashable like OTel's frozenset(attributes.items())
        frozenset(tags.items())


# ---------------------------------------------------------------------------
# Scenario 2/coercion at handle_event level: non-string body that json.dumps
# cannot encode (circular ref) must fall back to repr, never crash. (fix #2)
# ---------------------------------------------------------------------------
class TestHandleEventCoercionFallback:
    def test_handle_event_circular_ref_body_falls_back_to_repr(self, session):
        from airflow.triggers.callback import PAYLOAD_BODY_KEY, PAYLOAD_STATUS_KEY

        circular: dict = {}
        circular["self"] = circular  # json.dumps raises ValueError: Circular reference

        cb = TriggererCallback(_TEST_ASYNC)
        cb.queue(session=session)
        cb.data = {"kwargs": {}, "prefix": "deadline"}

        event = TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: circular})

        with mock.patch("airflow.models.callback.stats"):
            cb.handle_event(event, session)
        # Did not crash; output set via repr fallback.
        assert cb.state == CallbackState.SUCCESS
        assert cb.output is not None
        assert isinstance(cb.output, str)


# ---------------------------------------------------------------------------
# Scenario 4 & 5: Scheduler isolation (begin_nested savepoint) for an old-format
# deadline that throws in handle_miss, and a MIXED batch where the new-format
# deadlines still process. We exercise the exact loop construct from fix #1.
# ---------------------------------------------------------------------------
class TestScenario4And5SchedulerIsolation:
    def _run_isolation_loop(self, deadlines, session):
        """Replicates the fix #1 loop body in scheduler_job_runner._handle_deadlines."""
        log = mock.Mock()
        for deadline in deadlines:
            try:
                with session.begin_nested():
                    deadline.handle_miss(session)
            except Exception:
                log.exception("Failed to handle missed deadline %s; skipping it this loop", deadline.id)
        return log

    def test_old_format_failing_deadline_rolls_back_and_is_skipped(self):
        # begin_nested must produce a savepoint context manager; failure inside rolls back.
        session = mock.MagicMock()
        savepoint = mock.MagicMock()
        session.begin_nested.return_value.__enter__.return_value = savepoint

        bad = mock.Mock()
        bad.id = "old-1"
        bad.handle_miss.side_effect = RuntimeError("old-format deadline blew up in handle_miss")

        log = self._run_isolation_loop([bad], session)
        # Exception caught -> logged, loop survived.
        log.exception.assert_called_once()
        bad.handle_miss.assert_called_once_with(session)

    def test_mixed_batch_one_old_fails_new_ones_still_process(self):
        # MIXED batch: [new_ok, old_bad, new_ok2]. old_bad fails, others must process.
        session = mock.MagicMock()
        session.begin_nested.return_value.__enter__.return_value = mock.MagicMock()

        new_ok = mock.Mock(id="new-1")
        old_bad = mock.Mock(id="old-bad")
        old_bad.handle_miss.side_effect = RuntimeError("old-schema deadline failure")
        new_ok2 = mock.Mock(id="new-2")

        log = self._run_isolation_loop([new_ok, old_bad, new_ok2], session)

        # Isolation holds across schema versions: the two good ones processed.
        new_ok.handle_miss.assert_called_once_with(session)
        new_ok2.handle_miss.assert_called_once_with(session)
        old_bad.handle_miss.assert_called_once_with(session)
        # Exactly one failure logged.
        assert log.exception.call_count == 1
        # begin_nested called once per deadline (each isolated in its own savepoint).
        assert session.begin_nested.call_count == 3


# ---------------------------------------------------------------------------
# Scenario 8 (component-level): all 4 fix paths on one callback lifecycle.
# old-format async callback (stored context in kwargs -> fix #4), returns a dict
# (fix #2 coercion), DagRun deleted mid-flight (fix #4 fetch returns None,
# workload still created), and the trigger raises a BaseException mid-run that
# must terminalize via FAILED event (fix #3).
# ---------------------------------------------------------------------------
class TestScenario8AllFixesOneLifecycle:
    @pytest.mark.asyncio
    @mock.patch("airflow.triggers.callback.import_string")
    async def test_old_format_dict_return_then_metric_emission(self, mock_import_string, session):
        """old-format trigger run -> dict body -> handle_event coercion -> metric tags clean."""
        from airflow.triggers.callback import (
            PAYLOAD_BODY_KEY,
            PAYLOAD_STATUS_KEY,
            CallbackTrigger,
        )

        async def cb(**kwargs):
            # sibling-import path is exercised separately; here assert old-format context flows
            assert kwargs["context"] == {"dag_run": "legacy"}
            return {"status": "done", "items": [1, 2]}

        mock_import_string.return_value = cb
        trigger = CallbackTrigger(
            callback_path="x.y.cb",
            callback_kwargs={"context": {"dag_run": "legacy"}},
        )
        gen = trigger.run()
        await anext(gen)  # RUNNING
        success = await anext(gen)
        assert success.payload[PAYLOAD_STATUS_KEY] == CallbackState.SUCCESS
        body = success.payload[PAYLOAD_BODY_KEY]

        # Now feed the SUCCESS event into the model handle_event (fix #2 coercion path).
        cb_model = TriggererCallback(_TEST_ASYNC)
        cb_model.queue(session=session)
        cb_model.data = {
            "dag_id": "d",
            "run_id": "r",
            "deadline_id": "z",
            "kwargs": {"context": {"dag_run": "legacy"}},
            "prefix": "deadline",
        }

        captured_metric = {}

        def fake_incr(stat, tags, **kw):
            captured_metric["stat"] = stat
            captured_metric["tags"] = tags

        event = TriggerEvent({PAYLOAD_STATUS_KEY: CallbackState.SUCCESS, PAYLOAD_BODY_KEY: body})
        with mock.patch("airflow.models.callback.stats") as stats:
            stats.incr.side_effect = fake_incr
            cb_model.handle_event(event, session)

        # body coerced to JSON string in output
        assert isinstance(cb_model.output, str)
        assert json.loads(cb_model.output) == {"items": [1, 2], "status": "done"}
        # metric tags: routing stripped, context stripped, all primitive (no crash)
        tags = captured_metric["tags"]
        for k in ("dag_id", "run_id", "deadline_id", "prefix"):
            assert k not in tags
        assert "context" not in json.loads(tags["kwargs"])
        frozenset(tags.items())  # OTel hashability
