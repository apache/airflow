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
from airflow.providers.atlassian.jira.operators.jira import JiraOperator
from airflow.utils import timezone

from dev.tests_common.test_utils.compat import connection_as_json

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


class TestJiraOperator:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        monkeypatch.setenv(
            "AIRFLOW_CONN_JIRA_DEFAULT",
            connection_as_json(
                Connection(
                    conn_id="jira_default",
                    conn_type="jira",
                    host="https://localhost/jira/",
                    port=443,
                    extra='{"verify": false, "project": "AIRFLOW"}',
                )
            ),
        )
        with mock.patch("airflow.models.baseoperator.BaseOperator.xcom_push", return_value=None) as m:
            self.mocked_xcom_push = m
            yield

    def test_operator_init_with_optional_args(self):
        jira_operator = JiraOperator(task_id="jira_list_issue_types", jira_method="issue_types")

        assert jira_operator.jira_method_args == {}
        assert jira_operator.result_processor is None
        assert jira_operator.get_jira_resource_method is None

    def test_project_issue_count(self, mocked_jira_client):
        mocked_jira_client.return_value.get_project_issues_count.return_value = 10
        op = JiraOperator(
            task_id="get-issue-count",
            jira_method="get_project_issues_count",
            jira_method_args={"project": "ABC"},
        )

        op.execute({})

        assert mocked_jira_client.called
        assert mocked_jira_client.return_value.get_project_issues_count.called
        self.mocked_xcom_push.assert_called_once_with(mock.ANY, key="id", value=None)

    def test_issue_search(self, mocked_jira_client):
        jql_str = "issuekey=TEST-1226"
        mocked_jira_client.return_value.jql_get_list_of_tickets.return_value = MINIMAL_TEST_TICKET
        op = JiraOperator(
            task_id="search-ticket-test",
            jira_method="jql_get_list_of_tickets",
            jira_method_args={"jql": jql_str, "limit": "1"},
        )

        op.execute({})

        assert mocked_jira_client.called
        assert mocked_jira_client.return_value.jql_get_list_of_tickets.called
        self.mocked_xcom_push.assert_called_once_with(mock.ANY, key="id", value="911539")

    def test_update_issue(self, mocked_jira_client):
        mocked_jira_client.return_value.issue_add_comment.return_value = MINIMAL_TEST_TICKET

        op = JiraOperator(
            task_id="add_comment_test",
            jira_method="issue_add_comment",
            jira_method_args={"issue_key": MINIMAL_TEST_TICKET.get("key"), "comment": "this is test comment"},
        )

        op.execute({})

        assert mocked_jira_client.called
        assert mocked_jira_client.return_value.issue_add_comment.called
        self.mocked_xcom_push.assert_called_once_with(mock.ANY, key="id", value="911539")
