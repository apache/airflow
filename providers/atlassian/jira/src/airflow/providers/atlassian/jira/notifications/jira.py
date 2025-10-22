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

from airflow.providers.atlassian.jira.hooks.jira import JiraAsyncHook, JiraHook
from airflow.providers.atlassian.jira.version_compat import AIRFLOW_V_3_1_PLUS
from airflow.providers.common.compat.sdk import BaseNotifier


class JiraNotifier(BaseNotifier):
    """
    Jira notifier for creating Jira issues upon failures.

    :param jira_conn_id: The HTTP connection ID for the Jira instance.
    :param proxies: Proxies to make the Jira REST API call. Optional
    :param api_version: Jira api version to use. Optional
    :param api_root: root for the api requests. Optional
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
        api_version: str | int = "2",
        api_root: str = "rest/api",
        description: str,
        summary: str,
        project_id: int,
        issue_type_id: int,
        labels: list[str] | None = None,
        **kwargs,
    ):
        if AIRFLOW_V_3_1_PLUS:
            #  Support for passing context was added in 3.1.0
            super().__init__(**kwargs)
        else:
            super().__init__()
        self.jira_conn_id = jira_conn_id
        self.proxies = proxies
        self.api_version = api_version
        self.api_root = api_root
        self.description = description
        self.summary = summary
        self.project_id = project_id
        self.issue_type_id = issue_type_id
        self.labels = labels or []

    @cached_property
    def hook(self) -> JiraHook:
        return JiraHook(
            jira_conn_id=self.jira_conn_id,
            proxies=self.proxies,
            api_version=self.api_version,
            api_root=self.api_root,
        )

    @cached_property
    def async_hook(self) -> JiraAsyncHook:
        return JiraAsyncHook(
            jira_conn_id=self.jira_conn_id,
            proxies=self.proxies,
            api_version=self.api_version,
            api_root=self.api_root,
        )

    def _get_fields(self) -> dict[str, Any]:
        """Get the required Jira fields to create an issue."""
        return {
            "description": self.description,
            "summary": self.summary,
            "project": {"id": self.project_id},
            "issuetype": {"id": self.issue_type_id},
            "labels": self.labels,
        }

    def notify(self, context) -> None:
        fields = self._get_fields()
        self.hook.get_conn().create_issue(fields)

    async def async_notify(self, context) -> None:
        """Create a Jira issue (async)."""
        fields = self._get_fields()
        await self.async_hook.create_issue(fields)


send_jira_notification = JiraNotifier
