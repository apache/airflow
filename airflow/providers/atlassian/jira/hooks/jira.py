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
"""Hook for JIRA"""
from __future__ import annotations

from typing import Any

from jira import JIRA
from jira.exceptions import JIRAError

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook


class JiraHook(BaseHook):
    """
    Jira interaction hook, a Wrapper around JIRA Python SDK.

    :param jira_conn_id: reference to a pre-defined Jira Connection
    """

    default_conn_name = 'jira_default'
    conn_type = "jira"
    conn_name_attr = "jira_conn_id"
    hook_name = "JIRA"

    def __init__(self, jira_conn_id: str = default_conn_name, proxies: Any | None = None) -> None:
        super().__init__()
        self.jira_conn_id = jira_conn_id
        self.proxies = proxies
        self.client: JIRA | None = None
        self.get_conn()

    def get_conn(self) -> JIRA:
        if not self.client:
            self.log.debug('Creating Jira client for conn_id: %s', self.jira_conn_id)

            get_server_info = True
            validate = True
            extra_options = {}
            if not self.jira_conn_id:
                raise AirflowException('Failed to create jira client. no jira_conn_id provided')

            conn = self.get_connection(self.jira_conn_id)
            if conn.extra is not None:
                extra_options = conn.extra_dejson
                # only required attributes are taken for now,
                # more can be added ex: async, logging, max_retries

                # verify
                if 'verify' in extra_options and extra_options['verify'].lower() == 'false':
                    extra_options['verify'] = False

                # validate
                if 'validate' in extra_options and extra_options['validate'].lower() == 'false':
                    validate = False

                if 'get_server_info' in extra_options and extra_options['get_server_info'].lower() == 'false':
                    get_server_info = False

            try:
                self.client = JIRA(
                    conn.host,
                    options=extra_options,
                    basic_auth=(conn.login, conn.password),
                    get_server_info=get_server_info,
                    validate=validate,
                    proxies=self.proxies,
                )
            except JIRAError as jira_error:
                raise AirflowException(f'Failed to create jira client, jira error: {str(jira_error)}')
            except Exception as e:
                raise AirflowException(f'Failed to create jira client, error: {str(e)}')

        return self.client
