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

from airflow.providers.databricks.exceptions import DatabricksApiError
from airflow.providers.databricks.triggers.genie import DatabricksGenieMessageTrigger

SPACE_ID = "space/1"
CONVERSATION_ID = "conversation-1"
MESSAGE_ID = "message-1"
ENDPOINT = f"2.0/genie/spaces/space%2F1/conversations/{CONVERSATION_ID}/messages/{MESSAGE_ID}"
IDS = {"conversation_id": CONVERSATION_ID, "message_id": MESSAGE_ID}


def make_trigger(**kwargs) -> DatabricksGenieMessageTrigger:
    return DatabricksGenieMessageTrigger(
        space_id=SPACE_ID,
        conversation_id=CONVERSATION_ID,
        message_id=MESSAGE_ID,
        databricks_conn_id="genie",
        polling_period_seconds=2,
        **kwargs,
    )


@pytest.fixture
def hook():
    with mock.patch(
        "airflow.providers.databricks.triggers.genie.DatabricksGenieHook", autospec=True
    ) as hook_class:
        hook = hook_class.return_value
        hook.__aenter__.return_value = hook
        hook._endpoint_url.side_effect = lambda endpoint: f"https://workspace.cloud.databricks.com/{endpoint}"
        yield hook


@pytest.fixture
def sleep():
    with mock.patch("airflow.providers.databricks.triggers.genie.asyncio.sleep", autospec=True) as sleep:
        yield sleep


async def events(trigger) -> list[dict]:
    return [event.payload async for event in trigger.run()]


def test_serialization_has_ids_and_no_credentials():
    trigger = make_trigger(retry_limit=5, retry_delay=3)
    path, kwargs = trigger.serialize()
    assert path == "airflow.providers.databricks.triggers.genie.DatabricksGenieMessageTrigger"
    assert kwargs == {
        "space_id": SPACE_ID,
        "conversation_id": CONVERSATION_ID,
        "message_id": MESSAGE_ID,
        "databricks_conn_id": "genie",
        "polling_period_seconds": 2,
        "retry_limit": 5,
        "retry_delay": 3,
    }
    assert DatabricksGenieMessageTrigger(**kwargs).serialize() == trigger.serialize()


@pytest.mark.parametrize("period", [0, -1])
def test_invalid_polling_period(period):
    with pytest.raises(ValueError, match="must be positive"):
        DatabricksGenieMessageTrigger(
            space_id=SPACE_ID,
            conversation_id=CONVERSATION_ID,
            message_id=MESSAGE_ID,
            polling_period_seconds=period,
        )


@pytest.mark.parametrize("terminal", ["COMPLETED", "FAILED", "CANCELLED", "QUERY_RESULT_EXPIRED"])
@pytest.mark.asyncio
async def test_waits_for_terminal_status(hook, sleep, terminal):
    hook._a_do_api_call.side_effect = [{"status": "SUBMITTED"}, {"status": "ASKING_AI"}, {"status": terminal}]

    assert await events(make_trigger()) == [{**IDS, "status": "success", "message_status": terminal}]

    hook._a_do_api_call.assert_has_calls([mock.call(("GET", ENDPOINT))] * 3)
    assert sleep.await_args_list == [mock.call(2), mock.call(2)]


@pytest.mark.asyncio
async def test_restarted_trigger_polls_same_message(hook, sleep):
    hook._a_do_api_call.side_effect = [{"status": "EXECUTING_QUERY"}, {"status": "COMPLETED"}]
    restarted = DatabricksGenieMessageTrigger(**make_trigger().serialize()[1])

    assert await events(restarted) == [{**IDS, "status": "success", "message_status": "COMPLETED"}]


@pytest.mark.parametrize("response", [{"status": "SOMETHING_NEW"}, {}, ["not", "an", "object"]])
@pytest.mark.asyncio
async def test_unknown_status(hook, sleep, response):
    hook._a_do_api_call.return_value = response
    assert await events(make_trigger()) == [{**IDS, "status": "error", "error_type": "invalid_response"}]
    sleep.assert_not_called()


@pytest.mark.parametrize("status_code", [401, 403, 404, 429, 503, None])
@pytest.mark.asyncio
async def test_api_error(hook, status_code):
    hook._a_do_api_call.side_effect = DatabricksApiError("boom", http_status_code=status_code)
    assert await events(make_trigger()) == [
        {**IDS, "status": "error", "error_type": "api_error", "status_code": status_code}
    ]


@pytest.mark.asyncio
async def test_unexpected_error(hook):
    hook._a_do_api_call.side_effect = RuntimeError("private detail")
    payloads = await events(make_trigger())
    assert payloads == [{**IDS, "status": "error", "error_type": "unexpected_error"}]


@pytest.mark.parametrize(
    "base",
    ["http://workspace.cloud.databricks.com", "ftp://workspace.cloud.databricks.com"],
)
@pytest.mark.asyncio
async def test_refuses_cleartext_connection(hook, base):
    hook._endpoint_url.side_effect = lambda endpoint: f"{base}/{endpoint}"
    assert await events(make_trigger()) == [{**IDS, "status": "error", "error_type": "insecure_connection"}]
    hook._a_do_api_call.assert_not_called()


@pytest.mark.asyncio
async def test_allows_loopback_http(hook):
    hook._endpoint_url.side_effect = lambda endpoint: f"http://127.0.0.1:8080/{endpoint}"
    hook._a_do_api_call.return_value = {"status": "COMPLETED"}
    assert await events(make_trigger()) == [{**IDS, "status": "success", "message_status": "COMPLETED"}]


@pytest.mark.asyncio
async def test_hook_configuration():
    with mock.patch(
        "airflow.providers.databricks.triggers.genie.DatabricksGenieHook", autospec=True
    ) as hook_class:
        hook_class.return_value.__aenter__.side_effect = RuntimeError("stop")
        assert await events(make_trigger(retry_limit=4, retry_delay=2)) == [
            {**IDS, "status": "error", "error_type": "unexpected_error"}
        ]
    hook_class.assert_called_once_with(
        databricks_conn_id="genie", retry_limit=4, retry_delay=2, caller="DatabricksGenieMessageTrigger"
    )
