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
from airflow.providers.atlassian.jira.hooks.jira import JiraAsyncHook, JiraHook

from tests_common.test_utils.compat import connection_as_json


@pytest.fixture
def aioresponse():
    """
    Creates mock async API response.
    """
    with aioresponses() as async_response:
        yield async_response


@pytest.fixture
def mocked_jira_client():
    with mock.patch("airflow.providers.atlassian.jira.hooks.jira.Jira", autospec=True) as m:
        m.return_value = mock.Mock(name="jira_client")
        yield m


class TestJiraHook:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.conn_id = "jira_default"
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

    def test_jira_client_connection(self, mocked_jira_client):
        jira_hook = JiraHook(proxies=self.proxies)

        mocked_jira_client.assert_called_once_with(
            url=self.host,
            username=self.login,
            password=self.password,
            verify_ssl=False,
            proxies=self.proxies,
            api_version="2",
            api_root="rest/api",
        )
        assert isinstance(jira_hook.client, mock.Mock)
        assert jira_hook.client.name == mocked_jira_client.return_value.name


class TestJiraAsyncHook:
    @pytest.fixture
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="jira_default",
                conn_type="jira",
                host="http://test.atlassian.net",
                login="login",
                password="password",
                extra='{"verify_ssl": false}',
            )
        )

    @pytest.mark.parametrize(
        ("api_version", "api_result"),
        [
            pytest.param(
                "2",
                "2",
            ),
            pytest.param(
                1,
                1,
            ),
            pytest.param(
                "latest",
                "latest",
            ),
        ],
    )
    def test_api_version(self, api_version, api_result):
        """Test different API versions"""
        hook = JiraAsyncHook(jira_conn_id="jira_default", api_version=api_version)
        assert hook.api_version == api_result

    @pytest.mark.parametrize(
        ("hook_kwargs", "resource", "api_root", "api_version", "resource_url_result"),
        [
            pytest.param({"jira_conn_id": "jira_default"}, "issue", "abc/api", 1, "abc/api/1/issue"),
            pytest.param(
                {"jira_conn_id": "jira_default", "api_version": 2}, "issue", "abc/api", 1, "abc/api/1/issue"
            ),
            pytest.param(
                {"jira_conn_id": "jira_default", "api_version": 2},
                "issue",
                "abc/api",
                None,
                "abc/api/2/issue",
            ),
            pytest.param(
                {"jira_conn_id": "jira_default", "api_root": "notset/api"},
                "issue",
                "abc/api",
                1,
                "abc/api/1/issue",
            ),
            pytest.param(
                {"jira_conn_id": "jira_default", "api_root": "notset/api"},
                "issue",
                "notset/api",
                1,
                "notset/api/1/issue",
            ),
        ],
    )
    def test_get_resource_url(self, hook_kwargs, resource, api_root, api_version, resource_url_result):
        """Test expected resource url"""
        hook = JiraAsyncHook(**hook_kwargs)
        assert (
            hook.get_resource_url(resource=resource, api_root=api_root, api_version=api_version)
            == resource_url_result
        )

    @pytest.mark.asyncio
    async def test_create_issue_with_provided_conn(self, setup_connections):
        """Asserts that async http hook can read a jira connection."""
        hook = JiraAsyncHook(jira_conn_id="jira_default")
        fields = {
            "project": {"key": "TEST"},
            "issuetype": {"name": "Task"},
            "summary": "test rest",
            "description": "rest rest",
        }
        with mock.patch("aiohttp.ClientSession.post", new_callable=mock.AsyncMock) as mocked_function:
            await hook.create_issue(fields)
            assert mocked_function.call_args.kwargs.get("verify_ssl") is False
            assert mocked_function.call_args.kwargs.get("auth").login == "login"
            assert mocked_function.call_args.kwargs.get("auth").password == "password"

    @pytest.mark.asyncio
    async def test_create_issue_with_success(self, aioresponse, setup_connections):
        """Asserts that create issue return with success."""
        hook = JiraAsyncHook(jira_conn_id="jira_default")
        fields = {
            "project": {"key": "TEST"},
            "issuetype": {"name": "Task"},
            "summary": "test rest",
            "description": "rest rest",
        }
        aioresponse.post("http://test.atlassian.net/rest/api/2/issue", status=200)

        res = await hook.create_issue(fields)
        assert res.status == 200
