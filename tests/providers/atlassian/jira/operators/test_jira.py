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

import unittest
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


class TestJiraOperator(unittest.TestCase):
    def setUp(self):
        args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        dag = DAG('test_dag_id', default_args=args)
        self.dag = dag
        db.merge_conn(
            Connection(
                conn_id='jira_default',
                conn_type='jira',
                host='https://localhost/jira/',
                port=443,
                extra='{"verify": "False", "project": "AIRFLOW"}',
            )
        )

    @patch("airflow.providers.atlassian.jira.hooks.jira.JIRA", autospec=True, return_value=jira_client_mock)
    def test_issue_search(self, jira_mock):
        jql_str = 'issuekey=TEST-1226'
        jira_mock.return_value.search_issues.return_value = minimal_test_ticket

        jira_ticket_search_operator = JiraOperator(
            task_id='search-ticket-test',
            jira_method="search_issues",
            jira_method_args={'jql_str': jql_str, 'maxResults': '1'},
            dag=self.dag,
        )

        jira_ticket_search_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        assert jira_mock.called
        assert jira_mock.return_value.search_issues.called

    @patch("airflow.providers.atlassian.jira.hooks.jira.JIRA", autospec=True, return_value=jira_client_mock)
    def test_update_issue(self, jira_mock):
        jira_mock.return_value.add_comment.return_value = True

        add_comment_operator = JiraOperator(
            task_id='add_comment_test',
            jira_method="add_comment",
            jira_method_args={'issue': minimal_test_ticket.get("key"), 'body': 'this is test comment'},
            dag=self.dag,
        )

        add_comment_operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

        assert jira_mock.called
        assert jira_mock.return_value.add_comment.called
