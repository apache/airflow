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

"""This module allows to connect to a Github."""
from typing import Dict, Optional

from github import Github as GithubClient

from airflow.hooks.base import BaseHook


class GithubHook(BaseHook):
    """
    Interact with Github.

    Performs a connection to GitHub and retrieves client.

    :param github_conn_id: Reference to :ref:`GitHub connection id <howto/connection:github>`.
    :type github_conn_id: str
    """

    conn_name_attr = 'github_conn_id'
    default_conn_name = 'github_default'
    conn_type = 'github'
    hook_name = 'Github'

    def __init__(self, github_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.github_conn_id = github_conn_id
        self.client: Optional[GithubClient] = None
        self.get_conn()

    def get_conn(self) -> GithubClient:
        """
        Function that initiates a new GitHub connection
        with token and hostname ( for GitHub Enterprise )
        """
        if self.client is not None:
            return self.client

        conn = self.get_connection(self.github_conn_id)
        access_token = conn.password
        host = conn.host

        if not host:
            self.client = GithubClient(login_or_token=access_token)
        else:
            self.client = GithubClient(login_or_token=access_token, base_url=host)

        return self.client

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema', 'port', 'login', 'extra'],
            "relabeling": {
                'host': 'Github Enterprise Url (Optional)',
                'password': 'Github Access Token',
            },
            "placeholders": {
                'host': 'https://{hostname}/api/v3 (for Github Enterprise Connection)',
                'password': 'token credentials auth',
            },
        }
