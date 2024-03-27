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
"""Hook for JIRA."""

from __future__ import annotations

import warnings
from typing import Any

from atlassian import Jira

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook


class JiraHook(BaseHook):
    """
    Jira interaction hook, a Wrapper around Atlassian Jira Python SDK.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    """

    default_conn_name = "jira_default"
    conn_type = "jira"
    conn_name_attr = "jira_conn_id"
    hook_name = "JIRA"

    def __init__(self, jira_conn_id: str = default_conn_name, proxies: Any | None = None) -> None:
        super().__init__()
        self.jira_conn_id = jira_conn_id
        self.proxies = proxies
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

                # verify
                if isinstance(verify, str):
                    warnings.warn(
                        "Extra parameter `verify` using str is deprecated and will be removed "
                        "in a future release. Please use `verify` using bool instead.",
                        AirflowProviderDeprecationWarning,
                        stacklevel=2,
                    )
                    verify = True
                    if extra_options["verify"].lower() == "false":
                        verify = False

            self.client = Jira(
                url=conn.host,
                username=conn.login,
                password=conn.password,
                verify_ssl=verify,
                proxies=self.proxies,
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
