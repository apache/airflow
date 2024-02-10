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

from functools import cached_property
from typing import Any

from airflow.notifications.basenotifier import BaseNotifier
from airflow.providers.atlassian.jira.hooks.jira import JiraHook


class JiraNotifier(BaseNotifier):
    """
    Jira notifier for creating Jira issues upon failures.

    :param jira_conn_id: The HTTP connection ID for the Jira instance.
    :param proxies: Proxies to make the Jira REST API call. Optional
    :param description: The content for the body of the issue
    :param summary: The title of the issue
    :param project_id: The ID of the project under which the issue will be created
    :param issue_type_id: The ID of the issue category
    :param labels: The labels to be applied to the issue
    """

    template_fields = ("description", "summary", "project_id", "issue_type_id", "labels")

    def __init__(
        self,
        *,
        jira_conn_id: str = JiraHook.default_conn_name,
        proxies: Any | None = None,
        description: str,
        summary: str,
        project_id: int,
        issue_type_id: int,
        labels: list[str] | None = None,
    ):
        super().__init__()
        self.jira_conn_id = jira_conn_id
        self.proxies = proxies
        self.description = description
        self.summary = summary
        self.project_id = project_id
        self.issue_type_id = issue_type_id
        self.labels = labels or []

    @cached_property
    def hook(self) -> JiraHook:
        return JiraHook(jira_conn_id=self.jira_conn_id, proxies=self.proxies)

    def notify(self, context) -> None:
        fields = dict(
            description=self.description,
            summary=self.summary,
            project=dict(id=self.project_id),
            issuetype=dict(id=self.issue_type_id),
            labels=self.labels,
        )
        self.hook.get_conn().create_issue(fields)


send_jira_notification = JiraNotifier
