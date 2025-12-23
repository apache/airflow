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
"""This module allows you to connect to GitHub."""

from __future__ import annotations

from typing import TYPE_CHECKING

from github import Auth, Github as GithubClient

from airflow.providers.common.compat.sdk import BaseHook


class GithubHook(BaseHook):
    """
    Interact with GitHub.

    Performs a connection to GitHub and retrieves client.

    :param github_conn_id: Reference to :ref:`GitHub connection id <howto/connection:github>`.
    """

    conn_name_attr = "github_conn_id"
    default_conn_name = "github_default"
    conn_type = "github"
    hook_name = "GitHub"

    def __init__(self, github_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.github_conn_id = github_conn_id
        self.client: GithubClient | None = None
        self.get_conn()

    def get_conn(self) -> GithubClient:
        """Initiate a new GitHub connection with token and hostname (for GitHub Enterprise)."""
        if self.client is not None:
            return self.client

        conn = self.get_connection(self.github_conn_id)
        access_token = conn.password
        host = conn.host
        extras = conn.extra_dejson or {}

        if access_token:
            auth: Auth.Auth = Auth.Token(access_token)
        elif extras:
            if key_path := extras.get("key_path"):
                if not key_path.endswith(".pem"):
                    raise ValueError("Unrecognised key file: expected a .pem private key")
                with open(key_path) as key_file:
                    private_key = key_file.read()
            else:
                raise ValueError("No key_path provided for GitHub App authentication.")

            app_id = extras.get("app_id")
            installation_id = extras.get("installation_id")
            if not isinstance(installation_id, int):
                raise ValueError("The provided installation_id should be integer.")
            if not isinstance(app_id, (str | int)):
                raise ValueError("The provided app_id should be integer or string.")
            token_permissions = extras.get("token_permissions", None)

            auth = Auth.AppAuth(app_id, private_key).get_installation_auth(installation_id, token_permissions)
        else:
            raise ValueError("No access token or authentication method provided.")

        if not host:
            self.client = GithubClient(auth=auth)
        else:
            self.client = GithubClient(auth=auth, base_url=host)

        return self.client

    @classmethod
    def get_ui_field_behaviour(cls) -> dict:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "login", "extra"],
            "relabeling": {"host": "GitHub Enterprise URL (Optional)", "password": "GitHub Access Token"},
            "placeholders": {"host": "https://{hostname}/api/v3 (for GitHub Enterprise)"},
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test GitHub connection."""
        try:
            if TYPE_CHECKING:
                assert self.client
            self.client.get_user().id
            return True, "Successfully connected to GitHub."
        except Exception as e:
            return False, str(e)
