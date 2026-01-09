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

from airflow.providers.atlassian.jira.hooks.jira import JiraHook
from airflow.providers.atlassian.jira.notifications.jira import (
    JiraNotifier,
    send_jira_notification,
)

jira_create_issue_payload = dict(
    description="Test operator failed",
    summary="Test Jira issue",
    project=dict(id=10000),
    issuetype=dict(id=10003),
    labels=["airflow-dag-failure"],
)


class TestJiraNotifier:
    @mock.patch.object(JiraHook, "get_conn")
    def test_jira_notifier(self, mock_jira_hook, create_dag_without_db):
        notifier = send_jira_notification(
            jira_conn_id="jira_default",
            project_id=10000,
            description="Test operator failed",
            summary="Test Jira issue",
            issue_type_id=10003,
            labels=["airflow-dag-failure"],
        )
        notifier({"dag": create_dag_without_db("test_jira_notifier")})
        mock_jira_hook.return_value.create_issue.assert_called_once_with(jira_create_issue_payload)

    @mock.patch.object(JiraHook, "get_conn")
    def test_jira_notifier_with_notifier_class(self, mock_jira_hook, create_dag_without_db):
        notifier = JiraNotifier(
            jira_conn_id="jira_default",
            project_id=10000,
            description="Test operator failed",
            summary="Test Jira issue",
            issue_type_id=10003,
            labels=["airflow-dag-failure"],
        )
        notifier({"dag": create_dag_without_db("test_jira_notifier")})
        mock_jira_hook.return_value.create_issue.assert_called_once_with(jira_create_issue_payload)

    @mock.patch.object(JiraHook, "get_conn")
    def test_jira_notifier_templated(self, mock_jira_hook, create_dag_without_db):
        notifier = send_jira_notification(
            jira_conn_id="jira_default",
            project_id=10000,
            description="Test operator failed for dag: {{ dag.dag_id }}.",
            summary="Test Jira issue",
            issue_type_id=10003,
            labels=["airflow-dag-failure"],
        )
        notifier({"dag": create_dag_without_db("test_jira_notifier")})
        mock_jira_hook.return_value.create_issue.assert_called_once_with(
            dict(
                description="Test operator failed for dag: test_jira_notifier.",
                summary="Test Jira issue",
                project=dict(id=10000),
                issuetype=dict(id=10003),
                labels=["airflow-dag-failure"],
            )
        )

    def test_jira_notifier_get_fields(self):
        notifier = JiraNotifier(
            jira_conn_id="jira_default",
            project_id=10000,
            description="Test operator failed",
            summary="Test Jira issue",
            issue_type_id=10003,
            labels=["airflow-dag-failure"],
        )
        assert notifier._get_fields() == jira_create_issue_payload

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.atlassian.jira.notifications.jira.JiraAsyncHook.create_issue",
        new_callable=mock.AsyncMock,
    )
    async def test_jira_async_notifier(self, mock_jira_hook, create_dag_without_db):
        notifier = send_jira_notification(
            jira_conn_id="jira_default",
            api_root="test/rest",
            project_id=10000,
            description="Test operator failed",
            summary="Test Jira issue",
            issue_type_id=10003,
            labels=["airflow-dag-failure"],
        )
        await notifier.async_notify({"dag": create_dag_without_db("test_jira_notifier")})
        mock_jira_hook.assert_called_once_with(jira_create_issue_payload)
