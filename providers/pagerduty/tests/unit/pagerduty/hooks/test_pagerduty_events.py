#
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
from aioresponses import aioresponses

from airflow.models import Connection
from airflow.providers.pagerduty.hooks.pagerduty_events import (
    PagerdutyEventsAsyncHook,
    PagerdutyEventsHook,
    prepare_event_data,
)

DEFAULT_CONN_ID = "pagerduty_events_default"


@pytest.fixture(autouse=True)
def events_connections(create_connection_without_db):
    create_connection_without_db(
        Connection(conn_id=DEFAULT_CONN_ID, conn_type="pagerduty_events", password="events_token")
    )


@pytest.fixture
def aioresponse():
    """
    Creates mock async API response.
    """
    with aioresponses() as async_response:
        yield async_response


class TestPrepareEventData:
    def test_prepare_event_data(self):
        exp_event_data = {
            "action": "trigger",
            "dedup_key": "random",
            "payload": {
                "severity": "error",
                "source": "airflow_test",
                "summary": "test",
            },
        }
        even_data = prepare_event_data(
            summary="test", source="airflow_test", severity="error", dedup_key="random"
        )
        assert even_data == exp_event_data

    def test_prepare_event_data_invalid_action(self):
        with pytest.raises(ValueError):
            prepare_event_data(summary="test", severity="error", action="should_raise")

    def test_prepare_event_missing_dedup_key(self):
        with pytest.raises(ValueError):
            prepare_event_data(summary="test", severity="error", action="acknowledge")


class TestPagerdutyEventsHook:
    def test_get_integration_key_from_password(self, events_connections):
        hook = PagerdutyEventsHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        assert hook.integration_key == "events_token", "token initialised."

    def test_token_parameter_override(self, events_connections):
        hook = PagerdutyEventsHook(integration_key="override_key", pagerduty_events_conn_id=DEFAULT_CONN_ID)
        assert hook.integration_key == "override_key", "token initialised."

    def test_create_change_event(self, requests_mock, events_connections):
        hook = PagerdutyEventsHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        mock_response_body = {
            "message": "Change event processed",
            "status": "success",
        }
        requests_mock.post("https://events.pagerduty.com/v2/change/enqueue", json=mock_response_body)
        resp = hook.create_change_event(summary="test", source="airflow")
        assert resp is None, "No response expected for change event"

    def test_send_event(self, requests_mock, events_connections):
        hook = PagerdutyEventsHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        dedup_key = "samplekeyhere"
        mock_response_body = {
            "status": "success",
            "message": "Event processed",
            "dedup_key": dedup_key,
        }
        requests_mock.post("https://events.pagerduty.com/v2/enqueue", json=mock_response_body)
        resp = hook.send_event(summary="test", source="airflow_test", severity="error", dedup_key=dedup_key)
        assert resp == dedup_key


class TestPagerdutyEventsAsyncHook:
    @pytest.mark.asyncio
    async def test_get_integration_key_from_password(self, events_connections):
        hook = PagerdutyEventsAsyncHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        integration_key = await hook.get_integration_key()
        assert integration_key == "events_token", "token initialised."

    @pytest.mark.asyncio
    async def test_get_integration_key_parameter_override(self, events_connections):
        hook = PagerdutyEventsAsyncHook(
            integration_key="override_key", pagerduty_events_conn_id=DEFAULT_CONN_ID
        )
        integration_key = await hook.get_integration_key()
        assert integration_key == "override_key", "token initialised."

    @pytest.mark.asyncio
    async def test_send_event_with_payload(self, events_connections, aioresponse):
        hook = PagerdutyEventsAsyncHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)

        with mock.patch("aiohttp.ClientSession.post", new_callable=mock.AsyncMock) as mocked_function:
            await hook.send_event(summary="test", source="airflow_test", severity="error", dedup_key="random")
            assert mocked_function.call_args.kwargs.get("json") == {
                "event_action": "trigger",
                "routing_key": "events_token",
                "action": "trigger",
                "payload": {"summary": "test", "severity": "error", "source": "airflow_test"},
                "dedup_key": "random",
            }
            assert mocked_function.call_args.kwargs.get("headers") == PagerdutyEventsAsyncHook.default_headers
            assert mocked_function.call_args.kwargs.get("auth") is None

    @pytest.mark.asyncio
    async def test_send_event_with_success(self, events_connections, aioresponse):
        hook = PagerdutyEventsAsyncHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        exp_response = {"dedup_key": "random"}
        aioresponse.post("https://events.pagerduty.com/v2/enqueue", status=200, payload=exp_response)
        res = await hook.send_event(
            summary="test", source="airflow_test", severity="error", dedup_key="random"
        )
        assert res == exp_response["dedup_key"]
