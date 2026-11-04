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
from unittest.mock import patch

import pytest

from airflow.providers.openai.hooks.openai import OpenAIHook
from airflow.providers.openai.triggers.agent import OpenAIAgentSessionTrigger


@pytest.mark.asyncio
async def test_trigger_completes():
    trigger = OpenAIAgentSessionTrigger("openai_default", "session", 1, time.time() + 60)
    event = {"status": "success", "session_id": "session", "turn_id": "turn", "usage": None}
    with patch.object(OpenAIHook, "poll_agent_session", autospec=True, return_value=event):
        events = [result.payload async for result in trigger.run()]
    assert events == [event]


@pytest.mark.asyncio
async def test_trigger_timeout_survives_serialization():
    trigger = OpenAIAgentSessionTrigger("openai_default", "session", 1, time.time() - 1)
    _, kwargs = trigger.serialize()
    with patch.object(OpenAIHook, "poll_agent_session", autospec=True) as poll:
        events = [result.payload async for result in OpenAIAgentSessionTrigger(**kwargs).run()]
    assert events[0]["status"] == "timeout"
    poll.assert_not_called()


@pytest.mark.asyncio
async def test_trigger_gives_up_after_consecutive_poll_errors():
    trigger = OpenAIAgentSessionTrigger("openai_default", "session", 0, time.time() + 60)
    with patch.object(
        OpenAIHook, "poll_agent_session", autospec=True, side_effect=RuntimeError("API failed")
    ) as poll:
        events = [result.payload async for result in trigger.run()]
    assert events == [{"status": "error", "session_id": "session", "message": "API failed"}]
    assert poll.call_count == OpenAIHook.MAX_CONSECUTIVE_POLL_FAILURES


@pytest.mark.asyncio
async def test_trigger_recovers_from_transient_poll_error():
    trigger = OpenAIAgentSessionTrigger("openai_default", "session", 0, time.time() + 60)
    event = {"status": "success", "session_id": "session", "turn_id": "turn", "usage": None}
    with patch.object(
        OpenAIHook,
        "poll_agent_session",
        autospec=True,
        side_effect=[RuntimeError("API failed"), None, RuntimeError("API failed again"), event],
    ):
        events = [result.payload async for result in trigger.run()]
    assert events == [event]


@pytest.mark.asyncio
async def test_trigger_kill():
    trigger = OpenAIAgentSessionTrigger("openai_default", "session", 1, time.time() + 60)
    with patch.object(OpenAIHook, "cancel_agent_session", autospec=True) as cancel:
        await trigger.on_kill()
    assert cancel.call_args.args[1] == "session"
