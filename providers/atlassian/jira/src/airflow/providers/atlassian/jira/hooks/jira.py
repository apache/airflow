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
"""
Hook for JIRA.

.. spelling:word-list::

    ClientResponse
    aiohttp
    reqrep
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, cast

import aiohttp
from atlassian import Jira

from airflow.providers.common.compat.sdk import AirflowException, BaseHook
from airflow.providers.http.hooks.http import HttpAsyncHook

if TYPE_CHECKING:
    from aiohttp.client_reqrep import ClientResponse


class JiraHook(BaseHook):
    """
    Jira interaction hook, a Wrapper around Atlassian Jira Python SDK.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :param proxies: Proxies to make the Jira REST API call. Optional
    :param api_root: root for the api requests. Optional
    :param api_version: Jira api version to use. Optional
    """

    default_conn_name = "jira_default"
    conn_type = "jira"
    conn_name_attr = "jira_conn_id"
    hook_name = "JIRA"

    def __init__(
        self,
        jira_conn_id: str = default_conn_name,
        proxies: Any | None = None,
        api_root: str = "rest/api",
        api_version: str | int = "2",
    ) -> None:
        super().__init__()
        self.jira_conn_id = jira_conn_id
        self.proxies = proxies
        self.api_root = api_root
        self.api_version = api_version
        self.client: Jira | None = None
        self.get_conn()

    def get_conn(self) -> Jira:
        if not self.client:
            self.log.debug("Creating Jira client for conn_id: %s", self.jira_conn_id)

            verify = True
            if not self.jira_conn_id:
                raise AirflowException("Failed to create jira client. no jira_conn_id provided")

            conn = self.get_connection(self.jira_conn_id)
            if conn.extra is not None:
                extra_options = conn.extra_dejson
                verify = extra_options.get("verify", verify)
                # only required attributes are taken for now,
                # more can be added ex: timeout, cloud, session

            self.client = Jira(
                url=cast("str", conn.host),
                username=conn.login,
                password=conn.password,
                verify_ssl=verify,
                proxies=self.proxies,
                api_version=self.api_version,
                api_root=self.api_root,
            )

        return self.client

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to Atlassian Jira Connection form."""
        from flask_babel import lazy_gettext
        from wtforms import BooleanField

        return {
            "verify": BooleanField(lazy_gettext("Verify SSL"), default=True),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Atlassian Jira Connection."""
        return {
            "hidden_fields": ["schema", "extra"],
            "relabeling": {},
        }


class JiraAsyncHook(HttpAsyncHook):
    """
    Async Jira interaction hook, interacts with Jira with HTTP.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    :param api_root: root for the api requests
    :param api_version: api version to use
    :param proxies: Specify proxies to use. Defaults to None.

    """

    default_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    default_conn_name = "jira_default"
    conn_type = "jira"
    conn_name_attr = "jira_conn_id"
    hook_name = "Async HTTP JIRA"

    def __init__(
        self,
        jira_conn_id: str = default_conn_name,
        api_root: str = "rest/api",
        api_version: str | int = "2",
        proxies: Any | None = None,
    ) -> None:
        super().__init__()
        self.method = "POST"
        self.api_root = api_root
        self.api_version = api_version
        self.http_conn_id = jira_conn_id
        self.proxies = proxies

    def get_resource_url(
        self, resource: str, api_root: str | None = None, api_version: str | int | None = None
    ) -> str:
        """
        Create the resource url.

        :param resource: Jira resource
        :param api_root: root for the api requests
        :param api_version: request payload
        :return: resource URL

        """
        if api_root is None:
            api_root = self.api_root
        if api_version is None:
            api_version = self.api_version
        return "/".join(str(s).strip("/") for s in [api_root, api_version, resource] if s is not None)

    async def create_issue(self, fields: str | dict) -> ClientResponse:
        """
        Create an issue or a sub-task from a JSON representation.

        :param fields: JSON data. mandatory keys are issue type, summary and project
        :return: client response
        """
        path = self.get_resource_url("issue")

        session_kwargs: dict[str, Any] = {}
        if self.proxies:
            session_kwargs["proxy"] = self.proxies

        async with aiohttp.ClientSession(**session_kwargs) as session:
            return await super().run(
                session=session,
                endpoint=path,
                data=json.dumps({"fields": fields}),
                headers=self.default_headers,
            )
