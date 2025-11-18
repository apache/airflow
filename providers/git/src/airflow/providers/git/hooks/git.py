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

import contextlib
import json
import logging
import os
import tempfile
from typing import Any

from airflow.exceptions import AirflowException
from airflow.providers.common.compat.sdk import BaseHook

log = logging.getLogger(__name__)


class GitHook(BaseHook):
    """
    Hook for git repositories.

    :param git_conn_id: Connection ID for SSH connection to the repository

    """

    conn_name_attr = "git_conn_id"
    default_conn_name = "git_default"
    conn_type = "git"
    hook_name = "GIT"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "login": "Username or Access Token name",
                "host": "Repository URL",
                "password": "Access Token (optional)",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "key_file": "optional/path/to/keyfile",
                        "private_key": "optional inline private key",
                    }
                )
            },
        }

    def __init__(
        self, git_conn_id: str = "git_default", repo_url: str | None = None, *args, **kwargs
    ) -> None:
        super().__init__()
        connection = self.get_connection(git_conn_id)
        self.repo_url = repo_url or connection.host
        self.user_name = connection.login or "user"
        self.auth_token = connection.password
        self.private_key = connection.extra_dejson.get("private_key")
        self.key_file = connection.extra_dejson.get("key_file")
        self.strict_host_key_checking = connection.extra_dejson.get("strict_host_key_checking", "no")
        self.env: dict[str, str] = {}

        if self.key_file and self.private_key:
            raise AirflowException("Both 'key_file' and 'private_key' cannot be provided at the same time")
        self._process_git_auth_url()

    def _build_ssh_command(self, key_path: str) -> str:
        return (
            f"ssh -i {key_path} "
            f"-o IdentitiesOnly=yes "
            f"-o StrictHostKeyChecking={self.strict_host_key_checking}"
        )

    def _process_git_auth_url(self):
        if not isinstance(self.repo_url, str):
            return
        if self.auth_token and self.repo_url.startswith("https://"):
            self.repo_url = self.repo_url.replace("https://", f"https://{self.user_name}:{self.auth_token}@")
        elif self.auth_token and self.repo_url.startswith("http://"):
            self.repo_url = self.repo_url.replace("http://", f"http://{self.user_name}:{self.auth_token}@")
        elif self.repo_url.startswith("http://"):
            # if no auth token, use the repo url as is
            self.repo_url = self.repo_url
        elif not self.repo_url.startswith("git@") or not self.repo_url.startswith("https://"):
            self.repo_url = os.path.expanduser(self.repo_url)

    def set_git_env(self, key: str) -> None:
        self.env["GIT_SSH_COMMAND"] = self._build_ssh_command(key)

    @contextlib.contextmanager
    def configure_hook_env(self):
        if self.private_key:
            with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp_keyfile:
                tmp_keyfile.write(self.private_key)
                tmp_keyfile.flush()
                os.chmod(tmp_keyfile.name, 0o600)
                self.set_git_env(tmp_keyfile.name)
                yield
        else:
            self.set_git_env(self.key_file)
            yield
