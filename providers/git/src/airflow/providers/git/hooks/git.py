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
import shlex
import stat
import tempfile
from typing import Any
from urllib.parse import quote as urlquote

from airflow.providers.common.compat.sdk import AirflowException, BaseHook

log = logging.getLogger(__name__)


class GitHook(BaseHook):
    """
    Hook for git repositories.

    :param git_conn_id: Connection ID for SSH connection to the repository
    :param repo_url: Explicit Git repository URL to override the connection's host.

    Connection extra fields:

    * ``key_file`` — path to an SSH private key file.
    * ``private_key`` — inline SSH private key string (mutually exclusive with ``key_file``).
    * ``private_key_passphrase`` — passphrase for the private key (key_file or inline).
    * ``strict_host_key_checking`` — ``"yes"`` or ``"no"`` (default ``"no"``).
    * ``known_hosts_file`` — path to a custom SSH known-hosts file.
    * ``ssh_config_file`` — path to a custom SSH config file.
    * ``host_proxy_cmd`` — SSH ProxyCommand string (e.g. for bastion/jump hosts).
    * ``ssh_port`` — non-default SSH port.
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
                        "private_key_passphrase": "",
                        "strict_host_key_checking": "no",
                        "known_hosts_file": "",
                        "ssh_config_file": "",
                        "host_proxy_cmd": "",
                        "ssh_port": "",
                    }
                )
            },
        }

    def __init__(
        self, git_conn_id: str = "git_default", repo_url: str | None = None, *args, **kwargs
    ) -> None:
        super().__init__()
        connection = self.get_connection(git_conn_id)
        extra = connection.extra_dejson

        self.repo_url = repo_url or connection.host
        self.user_name = connection.login or "user"
        self.auth_token = connection.password

        # SSH key authentication
        self.private_key = extra.get("private_key")
        self.key_file = extra.get("key_file")
        self.private_key_passphrase = extra.get("private_key_passphrase")

        # SSH connection options
        self.strict_host_key_checking = extra.get("strict_host_key_checking", "no")
        self.known_hosts_file = extra.get("known_hosts_file")
        self.ssh_config_file = extra.get("ssh_config_file")
        self.host_proxy_cmd = extra.get("host_proxy_cmd")
        self.ssh_port: int | None = int(extra["ssh_port"]) if extra.get("ssh_port") else None

        self.env: dict[str, str] = {}

        if self.key_file and self.private_key:
            raise AirflowException("Both 'key_file' and 'private_key' cannot be provided at the same time")
        self._process_git_auth_url()

    _VALID_STRICT_HOST_KEY_CHECKING = frozenset({"yes", "no", "accept-new", "off", "ask"})

    def _build_ssh_command(self, key_path: str | None = None) -> str:
        parts = ["ssh"]

        if key_path:
            parts.append(f"-i {shlex.quote(key_path)}")
            parts.append("-o IdentitiesOnly=yes")

        if self.strict_host_key_checking not in self._VALID_STRICT_HOST_KEY_CHECKING:
            raise ValueError(
                f"Invalid strict_host_key_checking value: {self.strict_host_key_checking!r}. "
                f"Must be one of {sorted(self._VALID_STRICT_HOST_KEY_CHECKING)}"
            )
        parts.append(f"-o StrictHostKeyChecking={self.strict_host_key_checking}")

        if self.known_hosts_file:
            parts.append(f"-o UserKnownHostsFile={shlex.quote(self.known_hosts_file)}")
        elif self.strict_host_key_checking == "no":
            parts.append("-o UserKnownHostsFile=/dev/null")

        if self.ssh_config_file:
            parts.append(f"-F {shlex.quote(self.ssh_config_file)}")

        if self.host_proxy_cmd:
            parts.append(f"-o ProxyCommand={shlex.quote(self.host_proxy_cmd)}")

        if self.ssh_port:
            parts.append(f"-p {self.ssh_port}")

        return " ".join(parts)

    def _process_git_auth_url(self):
        if not isinstance(self.repo_url, str):
            return
        if self.auth_token and self.repo_url.startswith("https://"):
            encoded_user = urlquote(self.user_name, safe="")
            encoded_token = urlquote(self.auth_token, safe="")
            self.repo_url = self.repo_url.replace("https://", f"https://{encoded_user}:{encoded_token}@", 1)
        elif self.auth_token and self.repo_url.startswith("http://"):
            encoded_user = urlquote(self.user_name, safe="")
            encoded_token = urlquote(self.auth_token, safe="")
            self.repo_url = self.repo_url.replace("http://", f"http://{encoded_user}:{encoded_token}@", 1)
        elif self.repo_url.startswith("http://"):
            # if no auth token, use the repo url as is
            pass
        elif not self.repo_url.startswith("git@") and not self.repo_url.startswith("https://"):
            self.repo_url = os.path.expanduser(self.repo_url)

    def set_git_env(self, key: str | None = None) -> None:
        self.env["GIT_SSH_COMMAND"] = self._build_ssh_command(key)

    @contextlib.contextmanager
    def _passphrase_askpass_env(self):
        """Set up SSH_ASKPASS so ssh can unlock passphrase-protected keys non-interactively."""
        if not self.private_key_passphrase:
            yield
            return

        with tempfile.NamedTemporaryFile(mode="w", suffix=".sh", delete=True) as askpass_script:
            askpass_script.write(f"#!/bin/sh\necho {shlex.quote(self.private_key_passphrase)}\n")
            askpass_script.flush()
            os.chmod(askpass_script.name, stat.S_IRWXU)

            old_askpass = os.environ.get("SSH_ASKPASS")
            old_display = os.environ.get("DISPLAY")
            old_askpass_require = os.environ.get("SSH_ASKPASS_REQUIRE")
            try:
                os.environ["SSH_ASKPASS"] = askpass_script.name
                os.environ["SSH_ASKPASS_REQUIRE"] = "force"
                # DISPLAY must be set for SSH_ASKPASS to be used
                os.environ.setdefault("DISPLAY", ":")
                self.env["SSH_ASKPASS"] = askpass_script.name
                self.env["SSH_ASKPASS_REQUIRE"] = "force"
                self.env.setdefault("DISPLAY", os.environ["DISPLAY"])
                yield
            finally:
                for var, old_val in [
                    ("SSH_ASKPASS", old_askpass),
                    ("DISPLAY", old_display),
                    ("SSH_ASKPASS_REQUIRE", old_askpass_require),
                ]:
                    if old_val is None:
                        os.environ.pop(var, None)
                    else:
                        os.environ[var] = old_val

    @contextlib.contextmanager
    def configure_hook_env(self):
        if self.private_key:
            with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp_keyfile:
                tmp_keyfile.write(self.private_key)
                tmp_keyfile.flush()
                os.chmod(tmp_keyfile.name, 0o600)
                self.set_git_env(tmp_keyfile.name)
                with self._passphrase_askpass_env():
                    yield
        elif self.key_file:
            self.set_git_env(self.key_file)
            with self._passphrase_askpass_env():
                yield
        elif self.host_proxy_cmd or self.ssh_port or self.ssh_config_file or self.known_hosts_file:
            self.set_git_env()
            yield
        else:
            self.set_git_env(self.key_file)
            yield
