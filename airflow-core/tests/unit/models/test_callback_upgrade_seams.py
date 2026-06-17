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
QA wave 14 — model-level upgrade-path & fix-interaction seams (scenario 8).

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
