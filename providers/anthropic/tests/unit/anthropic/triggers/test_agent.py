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

import time
from unittest import mock

import pytest

from airflow.providers.anthropic.triggers.agent import AnthropicAgentSessionTrigger
from airflow.triggers.base import TriggerEvent

pytest.importorskip("anthropic")

TRIGGER_PATH = "airflow.providers.anthropic.triggers.agent"
POLL = "airflow.providers.anthropic.hooks.anthropic.AnthropicHook.poll_session_completion"


def _trigger(end_time=None, expect_outcome=False):
    return AnthropicAgentSessionTrigger(
        conn_id="anthropic_default",
        session_id="sess_1",
        poll_interval=1.0,
        end_time=end_time if end_time is not None else time.time() + 3600,
        expect_outcome=expect_outcome,
        kickoff_event_id="evt_kick",
    )


def test_serialization():
    end_time = time.time() + 3600
    path, kwargs = _trigger(end_time).serialize()
    assert path == "airflow.providers.anthropic.triggers.agent.AnthropicAgentSessionTrigger"
    assert kwargs == {
        "conn_id": "anthropic_default",
        "session_id": "sess_1",
        "poll_interval": 1.0,
        "end_time": end_time,
        "expect_outcome": False,
        "kickoff_event_id": "evt_kick",
    }


@pytest.mark.asyncio
@mock.patch(f"{TRIGGER_PATH}.AnthropicHook", autospec=True)
async def test_on_kill_archives_session(mock_hook_cls):
    # A user killing the deferred task archives the still-running session (Airflow 3.3+).
    await _trigger().on_kill()
    mock_hook_cls.return_value.archive_session.assert_called_once_with("sess_1")


@pytest.mark.asyncio
@mock.patch(POLL)
async def test_done_success_yields_success(mock_poll):
    mock_poll.return_value = (True, None)
    event = await _trigger().run().__anext__()
    assert event.payload["status"] == "success"
    assert event.payload["session_id"] == "sess_1"


@pytest.mark.asyncio
@mock.patch(POLL)
async def test_done_error_yields_error(mock_poll):
    mock_poll.return_value = (True, "Session sess_1 terminated.")
    event = await _trigger().run().__anext__()
    assert event.payload["status"] == "error"
    assert "terminated" in event.payload["message"]


@pytest.mark.asyncio
@mock.patch(POLL)
async def test_timeout_yields_timeout(mock_poll):
    mock_poll.return_value = (False, None)
    event = await _trigger(end_time=time.time() - 1).run().__anext__()
    assert event.payload["status"] == "timeout"


@pytest.mark.asyncio
@mock.patch(f"{TRIGGER_PATH}.asyncio.sleep")
@mock.patch(POLL)
async def test_polls_until_done(mock_poll, mock_sleep):
    mock_poll.side_effect = [(False, None), (False, None), (True, None)]
    event = await _trigger().run().__anext__()
    assert event.payload["status"] == "success"
    assert mock_poll.call_count == 3
    assert mock_sleep.await_count == 2


@pytest.mark.asyncio
@mock.patch(f"{TRIGGER_PATH}.asyncio.sleep")
@mock.patch(POLL)
async def test_persistent_error_yields_error_after_retries(mock_poll, mock_sleep):
    mock_poll.side_effect = RuntimeError("kaboom")
    event = await _trigger().run().__anext__()
    assert event == TriggerEvent({"status": "error", "session_id": "sess_1", "message": "kaboom"})
    assert mock_poll.call_count == 5


@pytest.mark.asyncio
@mock.patch(f"{TRIGGER_PATH}.asyncio.sleep")
@mock.patch(POLL)
async def test_transient_error_then_success(mock_poll, mock_sleep):
    mock_poll.side_effect = [RuntimeError("blip"), (True, None)]
    event = await _trigger().run().__anext__()
    assert event.payload["status"] == "success"


@pytest.mark.asyncio
@mock.patch(POLL)
async def test_outcome_failure_yields_error(mock_poll):
    mock_poll.return_value = (True, "Outcome not satisfied for session sess_1: max_iterations_reached.")
    event = await _trigger(expect_outcome=True).run().__anext__()
    assert event.payload["status"] == "error"
    assert "max_iterations_reached" in event.payload["message"]
