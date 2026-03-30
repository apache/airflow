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

import os

import pytest
from git import Repo

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.git.hooks.git import GitHook

from tests_common.test_utils.config import conf_vars


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


GIT_DEFAULT_BRANCH = "main"

AIRFLOW_HTTPS_URL = "https://github.com/apache/airflow.git"
AIRFLOW_HTTP_URL = "http://github.com/apache/airflow.git"
AIRFLOW_GIT = "git@github.com:apache/airflow.git"
ACCESS_TOKEN = "my_access_token"
CONN_DEFAULT = "git_default"
CONN_HTTPS = "my_git_conn"
CONN_HTTP = "my_git_conn_http"
CONN_HTTP_NO_AUTH = "my_git_conn_http_no_auth"
CONN_ONLY_PATH = "my_git_conn_only_path"
CONN_ONLY_INLINE_KEY = "my_git_conn_only_inline_key"
CONN_BOTH_PATH_INLINE = "my_git_conn_both_path_inline"
CONN_NO_REPO_URL = "my_git_conn_no_repo_url"


@pytest.fixture
def git_repo(tmp_path_factory):
    directory = tmp_path_factory.mktemp("repo")
    repo = Repo.init(directory)
    repo.git.symbolic_ref("HEAD", f"refs/heads/{GIT_DEFAULT_BRANCH}")
    file_path = directory / "test_dag.py"
    with open(file_path, "w") as f:
        f.write("hello world")
    repo.index.add([file_path])
    repo.index.commit("Initial commit")
    return (directory, repo)


class TestGitHook:
    @classmethod
    def teardown_class(cls) -> None:
        return

    # TODO: Potential performance issue, converted setup_class to a setup_connections function level fixture
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=CONN_DEFAULT,
                host=AIRFLOW_GIT,
                conn_type="git",
                extra='{"key_file": "/files/pkey.pem"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_HTTPS,
                host=AIRFLOW_HTTPS_URL,
                password=ACCESS_TOKEN,
                conn_type="git",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_HTTP,
                host=AIRFLOW_HTTP_URL,
                password=ACCESS_TOKEN,
                conn_type="git",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_HTTP_NO_AUTH,
                host=AIRFLOW_HTTP_URL,
                conn_type="git",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_ONLY_PATH,
                host="path/to/repo",
                conn_type="git",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_ONLY_INLINE_KEY,
                host="path/to/repo",
                conn_type="git",
                extra={
                    "private_key": "inline_key",
                },
            )
        )

    @pytest.mark.parametrize(
        ("conn_id", "hook_kwargs", "expected_repo_url"),
        [
            (CONN_DEFAULT, {}, AIRFLOW_GIT),
            (CONN_HTTPS, {}, f"https://user:{ACCESS_TOKEN}@github.com/apache/airflow.git"),
            (
                CONN_HTTPS,
                {"repo_url": "https://github.com/apache/zzzairflow"},
                f"https://user:{ACCESS_TOKEN}@github.com/apache/zzzairflow",
            ),
            (CONN_HTTP, {}, f"http://user:{ACCESS_TOKEN}@github.com/apache/airflow.git"),
            (
                CONN_HTTP,
                {"repo_url": "http://github.com/apache/zzzairflow"},
                f"http://user:{ACCESS_TOKEN}@github.com/apache/zzzairflow",
            ),
            (CONN_HTTP_NO_AUTH, {}, AIRFLOW_HTTP_URL),
            (
                CONN_HTTP_NO_AUTH,
                {"repo_url": "http://github.com/apache/zzzairflow"},
                "http://github.com/apache/zzzairflow",
            ),
            (CONN_ONLY_PATH, {}, "path/to/repo"),
        ],
    )
    def test_correct_repo_urls(self, conn_id, hook_kwargs, expected_repo_url):
        hook = GitHook(git_conn_id=conn_id, **hook_kwargs)
        assert hook.repo_url == expected_repo_url

    def test_env_var_with_configure_hook_env(self, create_connection_without_db):
        default_hook = GitHook(git_conn_id=CONN_DEFAULT)
        with default_hook.configure_hook_env():
            assert default_hook.env == {
                "GIT_SSH_COMMAND": "ssh -i /files/pkey.pem -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
            }
        create_connection_without_db(
            Connection(
                conn_id="my_git_conn_strict",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra='{"key_file": "/files/pkey.pem", "strict_host_key_checking": "yes"}',
            )
        )

        strict_default_hook = GitHook(git_conn_id="my_git_conn_strict")
        with strict_default_hook.configure_hook_env():
            assert strict_default_hook.env == {
                "GIT_SSH_COMMAND": "ssh -i /files/pkey.pem -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes"
            }

    def test_given_both_private_key_and_key_file(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id=CONN_BOTH_PATH_INLINE,
                host="path/to/repo",
                conn_type="git",
                extra={
                    "key_file": "path/to/key",
                    "private_key": "inline_key",
                },
            )
        )

        with pytest.raises(
            AirflowException, match="Both 'key_file' and 'private_key' cannot be provided at the same time"
        ):
            GitHook(git_conn_id=CONN_BOTH_PATH_INLINE)

    def test_key_file_git_hook_has_env_with_configure_hook_env(self):
        hook = GitHook(git_conn_id=CONN_DEFAULT)

        assert hasattr(hook, "env")
        with hook.configure_hook_env():
            assert hook.env == {
                "GIT_SSH_COMMAND": "ssh -i /files/pkey.pem -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
            }

    def test_private_key_lazy_env_var(self):
        hook = GitHook(git_conn_id=CONN_ONLY_INLINE_KEY)
        assert hook.env == {}

        hook.set_git_env("dummy_inline_key")
        assert hook.env == {
            "GIT_SSH_COMMAND": "ssh -i dummy_inline_key -o IdentitiesOnly=yes -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"
        }

    def test_configure_hook_env(self):
        hook = GitHook(git_conn_id=CONN_ONLY_INLINE_KEY)
        assert hasattr(hook, "private_key")

        hook.set_git_env("dummy_inline_key")

        with hook.configure_hook_env():
            command = hook.env.get("GIT_SSH_COMMAND")
            temp_key_path = command.split()[2]
            assert os.path.exists(temp_key_path)

        assert not os.path.exists(temp_key_path)

    def test_ssh_port(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="git_with_port",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={"key_file": "/files/pkey.pem", "ssh_port": "2222"},
            )
        )
        hook = GitHook(git_conn_id="git_with_port")
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert "-p 2222" in cmd

    def test_proxy_command(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="git_with_proxy",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={
                    "key_file": "/files/pkey.pem",
                    "host_proxy_cmd": "ssh -W %h:%p bastion.example.com",
                },
            )
        )
        hook = GitHook(git_conn_id="git_with_proxy")
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert 'ProxyCommand="ssh -W %h:%p bastion.example.com"' in cmd

    def test_known_hosts_file(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="git_known_hosts",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={
                    "key_file": "/files/pkey.pem",
                    "strict_host_key_checking": "yes",
                    "known_hosts_file": "/etc/ssh/known_hosts",
                },
            )
        )
        hook = GitHook(git_conn_id="git_known_hosts")
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert "-o StrictHostKeyChecking=yes" in cmd
            assert "-o UserKnownHostsFile=/etc/ssh/known_hosts" in cmd
            assert "/dev/null" not in cmd

    def test_ssh_config_file(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="git_ssh_config",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={
                    "key_file": "/files/pkey.pem",
                    "ssh_config_file": "/home/user/.ssh/config",
                },
            )
        )
        hook = GitHook(git_conn_id="git_ssh_config")
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert "-F /home/user/.ssh/config" in cmd

    def test_no_key_with_ssh_options_sets_env(self, create_connection_without_db):
        """SSH options without a key still produce GIT_SSH_COMMAND."""
        create_connection_without_db(
            Connection(
                conn_id="git_proxy_only",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={"host_proxy_cmd": "ssh -W %h:%p bastion"},
            )
        )
        hook = GitHook(git_conn_id="git_proxy_only")
        assert hook.env == {}
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert cmd.startswith("ssh ")
            assert "-i " not in cmd
            assert "ProxyCommand" in cmd

    def test_default_user_known_hosts_devnull_when_no_strict_checking(self):
        """When strict_host_key_checking=no and no known_hosts_file, /dev/null is used."""
        hook = GitHook(git_conn_id=CONN_DEFAULT)
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert "-o UserKnownHostsFile=/dev/null" in cmd

    def test_passphrase_sets_askpass_env(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="git_passphrase",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={
                    "key_file": "/files/pkey.pem",
                    "private_key_passphrase": "my_secret",
                },
            )
        )
        hook = GitHook(git_conn_id="git_passphrase")
        with hook.configure_hook_env():
            assert "SSH_ASKPASS" in hook.env
            assert hook.env["SSH_ASKPASS_REQUIRE"] == "force"
            askpass_path = hook.env["SSH_ASKPASS"]
            assert os.path.exists(askpass_path)

    def test_passphrase_askpass_cleaned_up(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="git_passphrase_cleanup",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={
                    "private_key": "inline_key",
                    "private_key_passphrase": "my_secret",
                },
            )
        )
        hook = GitHook(git_conn_id="git_passphrase_cleanup")
        askpass_path = None
        with hook.configure_hook_env():
            askpass_path = hook.env.get("SSH_ASKPASS")
            assert askpass_path is not None
            assert os.path.exists(askpass_path)
        # Both the askpass script and the temp key file should be cleaned up
        assert not os.path.exists(askpass_path)
