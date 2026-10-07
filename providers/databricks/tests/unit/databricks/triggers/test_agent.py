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

from airflow.providers.databricks.triggers.agent import DatabricksAgentInvocationTrigger

APP_URL = "https://agent.databricksapps.com"
INVOCATION_ID = "550e8400-e29b-41d4-a716-446655440000"


def test_serialization():
    trigger = DatabricksAgentInvocationTrigger(
        app_url=APP_URL,
        invocation_id=INVOCATION_ID,
        databricks_conn_id="oauth",
        session_id="conversation",
        polling_period_seconds=2,
    )
    path, kwargs = trigger.serialize()
    assert path == "airflow.providers.databricks.triggers.agent.DatabricksAgentInvocationTrigger"
    assert kwargs == {
        "app_url": APP_URL,
        "invocation_id": INVOCATION_ID,
        "databricks_conn_id": "oauth",
        "session_id": "conversation",
        "polling_period_seconds": 2,
    }
    assert DatabricksAgentInvocationTrigger(**kwargs).serialize() == trigger.serialize()


@pytest.mark.parametrize("period", [0, -1])
def test_invalid_polling_period(period):
    with pytest.raises(ValueError, match="must be positive"):
        DatabricksAgentInvocationTrigger(
            app_url=APP_URL, invocation_id=INVOCATION_ID, polling_period_seconds=period
        )


@mock.patch("airflow.providers.databricks.triggers.agent.asyncio.sleep", autospec=True)
@mock.patch("airflow.providers.databricks.triggers.agent.DatabricksAgentHook", autospec=True)
@pytest.mark.parametrize("status", ["completed", "failed", "interrupted"])
@pytest.mark.asyncio
async def test_run(hook_class, sleep, status):
    hook = hook_class.return_value
    hook.__aenter__.return_value = hook
    hook.a_get_invocation.side_effect = [
        {"status": "running"},
        {"status": status, "output": "private output"},
    ]
    trigger = DatabricksAgentInvocationTrigger(
        app_url=APP_URL, invocation_id=INVOCATION_ID, databricks_conn_id="oauth", session_id="conversation"
    )
    events = [event async for event in trigger.run()]
    assert [event.payload for event in events] == [{"status": "success", "invocation_id": INVOCATION_ID}]
    hook_class.assert_called_once_with(APP_URL, "oauth")
    assert hook.a_get_invocation.await_args_list == [mock.call(INVOCATION_ID, "conversation")] * 2
    sleep.assert_awaited_once_with(10)
    hook.__aexit__.assert_awaited_once()


@mock.patch("airflow.providers.databricks.triggers.agent.DatabricksAgentHook", autospec=True)
@pytest.mark.parametrize("failure", [RuntimeError("unavailable"), {}])
@pytest.mark.asyncio
async def test_polling_failure(hook_class, failure):
    hook = hook_class.return_value
    hook.__aenter__.return_value = hook
    hook.a_get_invocation.side_effect = [failure]
    trigger = DatabricksAgentInvocationTrigger(app_url=APP_URL, invocation_id=INVOCATION_ID)
    events = [event async for event in trigger.run()]
    assert [event.payload for event in events] == [{"status": "error", "invocation_id": INVOCATION_ID}]
