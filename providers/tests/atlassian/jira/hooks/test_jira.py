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

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.atlassian.jira.hooks.jira import JiraHook

from tests_common.test_utils.compat import connection_as_json


@pytest.fixture
def mocked_jira_client():
    with mock.patch("airflow.providers.atlassian.jira.hooks.jira.Jira", autospec=True) as m:
        m.return_value = mock.Mock(name="jira_client")
        yield m


class TestJiraHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.conn_id = "jira_default"
        self.conn_id_with_str_verify = "jira_default_with_str"
        self.host = "https://localhost/jira/"
        self.port = 443
        self.login = "user"
        self.password = "password"
        self.proxies = None

        monkeypatch.setenv(
            f"AIRFLOW_CONN_{self.conn_id}".upper(),
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
        monkeypatch.setenv(
            f"AIRFLOW_CONN_{self.conn_id_with_str_verify}".upper(),
            connection_as_json(
                Connection(
                    conn_id=self.conn_id_with_str_verify,
                    conn_type="jira",
                    host="https://localhost/jira/",
                    port=443,
                    login="user",
                    password="password",
                    extra='{"verify": "False", "project": "AIRFLOW"}',
                )
            ),
        )

    def test_jira_client_connection(self, mocked_jira_client):
        jira_hook = JiraHook(proxies=self.proxies)

        mocked_jira_client.assert_called_once_with(
            url=self.host,
            username=self.login,
            password=self.password,
            verify_ssl=False,
            proxies=self.proxies,
        )
        assert isinstance(jira_hook.client, mock.Mock)
        assert jira_hook.client.name == mocked_jira_client.return_value.name

    def test_jira_client_connection_with_str(self, mocked_jira_client):
        warning_message = "Extra parameter `verify` using str is deprecated and will be removed"

        with pytest.warns(AirflowProviderDeprecationWarning, match=warning_message):
            jira_hook = JiraHook(jira_conn_id=self.conn_id_with_str_verify, proxies=self.proxies)

        mocked_jira_client.assert_called_once_with(
            url=self.host,
            username=self.login,
            password=self.password,
            verify_ssl=False,
            proxies=self.proxies,
        )
        assert isinstance(jira_hook.client, mock.Mock)
        assert jira_hook.client.name == mocked_jira_client.return_value.name
