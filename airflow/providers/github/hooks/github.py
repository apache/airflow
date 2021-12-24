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

from typing import Dict

from github import Github
from airflow.hooks.base import BaseHook
from airflow.models import Connection


class GithubHook(BaseHook):
    """
    Interact with Github.

    Performs a connection to Github and retrieves client.

    :param github_conn_id: Reference to :ref:`Github connection id <howto/connection:github>`.
    :type github_conn_id: str
    """

    conn_name_attr = 'github_conn_id'
    default_conn_name = 'github_default'
    conn_type = 'github'
    hook_name = 'Github'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.github_conn_id = conn_id
        self.connection = kwargs.pop("connection", None)
        self.client = None
        self.extras: Dict = {}
        self.uri = None
        self.org_name = None

    def get_client(self, uri, token, base_url=DEFAULT_BASE_URL):
        return Github(login_or_token=token, base_url)


    def get_conn(self) -> Github:
        """
        Function that initiates a new Github connection
        with token and hostname name
        """
        self.connection = self.get_connection(self.github_conn_id)
        self.extras = self.connection.extra_dejson.copy()

        self.uri = self.get_uri(self.connection)
        self.log.info('URI: %s', self.uri)

        if self.client is not None:
            return self.client

        token = self.connection.extra_dejson.get('token')
        self.org_name = self.connection.extra_dejson.get('org_name')

        self.log.info('URI: %s', self.uri)
        self.log.info('Organization: %s', self.org_name)

        self.client = self.get_client(self.uri, token, self.org_name)

        return self.client

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema', 'port', 'host', 'login'],
            "relabeling": {
                'host': 'Github Url(Optional)',
                'password': 'Github Access Token',
            },
            "placeholders": {
                'host': 'https://{hostname}/api/v3, Use for Github Enterprise ',
                'password': 'token credentials auth',
            },
        }
