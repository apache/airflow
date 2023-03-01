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

from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.atlassian.jira.operators.jira import JiraOperator
from airflow.utils import db, timezone

DEFAULT_DATE = timezone.datetime(2017, 1, 1)
jira_client_mock = Mock(name="jira_client_for_test")

minimal_test_ticket = {
    "id": "911539",
    "self": "https://sandbox.localhost/jira/rest/api/2/issue/911539",
    "key": "TEST-1226",
    "fields": {
        "labels": ["test-label-1", "test-label-2"],
        "description": "this is a test description",
    },
}


class TestJiraOperator:
    def setup_method(self):
        args = {"owner": "airflow", "start_date": DEFAULT_DATE}
        dag = DAG("test_dag_id", default_args=args)
        self.dag = dag
        db.merge_conn(
            Connection(
                conn_id="jira_default",
                conn_type="jira",
                host="https://localhost/jira/",
                port=443,
                extra='{"verify": "False", "project": "AIRFLOW"}',
            )
        )

    def test_operator_init_with_optional_args(self):
        jira_operator = JiraOperator(task_id="jira_list_issue_types", jira_method="issue_types")

        assert jira_operator.jira_method_args == {}
        assert jira_operator.result_processor is None
        assert jira_operator.get_jira_resource_method is None

    @patch("airflow.providers.atlassian.jira.hooks.jira.Jira", autospec=True, return_value=jira_client_mock)
    def test_issue_search(self, jira_mock):
        jql_str = "issuekey=TEST-1226"
        jira_mock.return_value.jql_get_list_of_tickets.return_value = minimal_test_ticket

        jira_ticket_search_operator = JiraOperator(
            task_id="search-ticket-test",
            jira_method="jql_get_list_of_tickets",
            jira_method_args={"jql": jql_str, "limit": "1"},
            dag=self.dag,
        )

        jira_ticket_search_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        assert jira_mock.called
        assert jira_mock.return_value.jql_get_list_of_tickets.called

    @patch("airflow.providers.atlassian.jira.hooks.jira.Jira", autospec=True, return_value=jira_client_mock)
    def test_update_issue(self, jira_mock):
        jira_mock.return_value.issue_add_comment.return_value = minimal_test_ticket

        add_comment_operator = JiraOperator(
            task_id="add_comment_test",
            jira_method="issue_add_comment",
            jira_method_args={"issue_key": minimal_test_ticket.get("key"), "comment": "this is test comment"},
            dag=self.dag,
        )

        add_comment_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        assert jira_mock.called
        assert jira_mock.return_value.issue_add_comment.called
