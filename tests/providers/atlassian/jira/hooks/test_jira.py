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

from unittest.mock import Mock, patch

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.models import Connection
from airflow.providers.atlassian.jira.hooks.jira import JiraHook
from airflow.utils import db

pytestmark = pytest.mark.db_test

jira_client_mock = Mock(name="jira_client")


class TestJiraHook:
    depcrecation_message = (
        "Extra parameter `verify` using str is deprecated and will be removed "
        "in a future release. Please use `verify` using bool instead."
    )
    conn_id = "jira_default"
    conn_id_with_str_verify = "jira_default_with_str"
    conn_type = "jira"
    host = "https://localhost/jira/"
    port = 443
    login = "user"
    password = "password"
    proxies = None

    def setup_method(self):
        db.merge_conn(
            Connection(
                conn_id=self.conn_id,
                conn_type="jira",
                host="https://localhost/jira/",
                port=443,
                login="user",
                password="password",
                extra='{"verify": false, "project": "AIRFLOW"}',
            )
        )
        db.merge_conn(
            Connection(
                conn_id=self.conn_id_with_str_verify,
                conn_type="jira",
                host="https://localhost/jira/",
                port=443,
                login="user",
                password="password",
                extra='{"verify": "False", "project": "AIRFLOW"}',
            )
        )

    @patch("airflow.providers.atlassian.jira.hooks.jira.Jira", autospec=True, return_value=jira_client_mock)
    def test_jira_client_connection(self, jira_mock):
        jira_hook = JiraHook(proxies=self.proxies)

        jira_mock.assert_called_once_with(
            url=self.host,
            username=self.login,
            password=self.password,
            verify_ssl=False,
            proxies=self.proxies,
        )
        assert isinstance(jira_hook.client, Mock)
        assert jira_hook.client.name == jira_mock.return_value.name

    @patch("airflow.providers.atlassian.jira.hooks.jira.Jira", autospec=True, return_value=jira_client_mock)
    def test_jira_client_connection_with_str(self, jira_mock):
        with pytest.warns(AirflowProviderDeprecationWarning, match=self.depcrecation_message):
            jira_hook = JiraHook(jira_conn_id=self.conn_id_with_str_verify, proxies=self.proxies)

        jira_mock.assert_called_once_with(
            url=self.host,
            username=self.login,
            password=self.password,
            verify_ssl=False,
            proxies=self.proxies,
        )
        assert isinstance(jira_hook.client, Mock)
        assert jira_hook.client.name == jira_mock.return_value.name
