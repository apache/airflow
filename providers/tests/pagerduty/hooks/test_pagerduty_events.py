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

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.pagerduty.hooks.pagerduty import PagerdutyEventsHook
from airflow.utils import db

pytestmark = pytest.mark.db_test

DEFAULT_CONN_ID = "pagerduty_events_default"


@pytest.fixture(scope="class")
def events_connections():
    db.merge_conn(
        Connection(
            conn_id=DEFAULT_CONN_ID, conn_type="pagerduty_events", password="events_token"
        )
    )


class TestPagerdutyEventsHook:
    def test_get_integration_key_from_password(self, events_connections):
        hook = PagerdutyEventsHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        assert hook.integration_key == "events_token", "token initialised."

    def test_token_parameter_override(self, events_connections):
        hook = PagerdutyEventsHook(
            integration_key="override_key", pagerduty_events_conn_id=DEFAULT_CONN_ID
        )
        assert hook.integration_key == "override_key", "token initialised."

    def test_create_event(self, requests_mock, events_connections):
        hook = PagerdutyEventsHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        mock_response_body = {
            "status": "success",
            "message": "Event processed",
            "dedup_key": "samplekeyhere",
        }
        requests_mock.post(
            "https://events.pagerduty.com/v2/enqueue", json=mock_response_body
        )
        with pytest.warns(
            AirflowProviderDeprecationWarning,
            match="This method will be deprecated. Please use the `PagerdutyEventsHook.send_event` to interact with the Events API",
        ):
            resp = hook.create_event(
                summary="test",
                source="airflow_test",
                severity="error",
            )
        assert resp == mock_response_body

    def test_create_change_event(self, requests_mock, events_connections):
        hook = PagerdutyEventsHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        change_event_id = "change_event_id"
        mock_response_body = {"id": change_event_id}
        requests_mock.post(
            "https://events.pagerduty.com/v2/change/enqueue", json=mock_response_body
        )
        resp = hook.create_change_event(summary="test", source="airflow")
        assert resp == change_event_id

    def test_send_event(self, requests_mock, events_connections):
        hook = PagerdutyEventsHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        dedup_key = "samplekeyhere"
        mock_response_body = {
            "status": "success",
            "message": "Event processed",
            "dedup_key": dedup_key,
        }
        requests_mock.post(
            "https://events.pagerduty.com/v2/enqueue", json=mock_response_body
        )
        resp = hook.send_event(
            summary="test", source="airflow_test", severity="error", dedup_key=dedup_key
        )
        assert resp == dedup_key

    def test_create_event_deprecation_warning(self, requests_mock, events_connections):
        hook = PagerdutyEventsHook(pagerduty_events_conn_id=DEFAULT_CONN_ID)
        mock_response_body = {
            "status": "success",
            "message": "Event processed",
            "dedup_key": "samplekeyhere",
        }
        requests_mock.post(
            "https://events.pagerduty.com/v2/enqueue", json=mock_response_body
        )
        warning = (
            "This method will be deprecated. Please use the `PagerdutyEventsHook.send_event`"
            " to interact with the Events API"
        )
        with pytest.warns(AirflowProviderDeprecationWarning, match=warning):
            hook.create_event(
                summary="test",
                source="airflow_test",
                severity="error",
            )
