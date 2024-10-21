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

from airflow.models import Connection
from airflow.providers.atlassian.jira.sensors.jira import JiraTicketSensor
from airflow.utils import timezone

from tests_common.test_utils.compat import connection_as_json

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
MINIMAL_TEST_TICKET = {
    "id": "911539",
    "self": "https://sandbox.localhost/jira/rest/api/2/issue/911539",
    "key": "TEST-1226",
    "fields": {
        "labels": ["test-label-1", "test-label-2"],
        "description": "this is a test description",
    },
}


@pytest.fixture
def mocked_jira_client():
    with mock.patch("airflow.providers.atlassian.jira.hooks.jira.Jira", autospec=True) as m:
        m.return_value = mock.Mock(name="jira_client_for_test")
        yield m


class TestJiraSensor:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW_CONN_JIRA_DEFAULT".upper(),
            connection_as_json(
                Connection(
                    conn_id="jira_default",
                    conn_type="jira",
                    host="https://localhost/jira/",
                    port=443,
                    login="user",
                    password="password",
                    extra='{"verify": false, "project": "AIRFLOW"}',
                )
            ),
        )

    def test_issue_label_set(self, mocked_jira_client):
        mocked_jira_client.return_value.issue.return_value = MINIMAL_TEST_TICKET
        sensor = JiraTicketSensor(
            task_id="search-ticket-test",
            ticket_id="TEST-1226",
            field="labels",
            expected_value="test-label-1",
            timeout=518400,
            poke_interval=10,
        )

        assert sensor.poke({})

        assert mocked_jira_client.called
        assert mocked_jira_client.return_value.issue.called
