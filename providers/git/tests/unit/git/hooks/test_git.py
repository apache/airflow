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
import os
import warnings

import pytest
from git import Repo

from airflow.exceptions import AirflowProviderDeprecationWarning
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
CONN_APP_INLINE_KEY = "git_app_inline_key"
CONN_APP_ONLY_APP_ID = "git_app_only_app_id"
CONN_APP_ONLY_INSTALLATION_ID = "git_app_only_installation_id"
CONN_APP_NO_KEY = "git_app_no_key"
CONN_APP_INVALID_APP_ID = "git_app_invalid_app_id"
CONN_APP_INVALID_INSTALLATION_ID = "git_app_invalid_installation_id"


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
        create_connection_without_db(
            Connection(
                conn_id="my_git_conn_strict",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra='{"key_file": "/files/pkey.pem", "strict_host_key_checking": "yes"}',
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_APP_INLINE_KEY,
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={
                    "github_app_id": "12345",
                    "github_installation_id": "67890",
                    "private_key": "inline_pem_key",
                },
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_APP_ONLY_APP_ID,
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={"github_app_id": "12345"},
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_APP_ONLY_INSTALLATION_ID,
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={"github_installation_id": "67890"},
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_APP_NO_KEY,
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={"github_app_id": "12345", "github_installation_id": "67890"},
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_APP_INVALID_APP_ID,
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={
                    "github_app_id": "not_an_int",
                    "github_installation_id": "67890",
                    "private_key": "inline_pem_key",
                },
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_APP_INVALID_INSTALLATION_ID,
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={
                    "github_app_id": "12345",
                    "github_installation_id": "not_an_int",
                    "private_key": "inline_pem_key",
                },
            )
        )

    @pytest.mark.parametrize(
        ("conn_id", "hook_kwargs", "expected_repo_url", "warns_on_default"),
        [
            (CONN_DEFAULT, {}, AIRFLOW_GIT, True),
            (CONN_HTTPS, {}, f"https://user:{ACCESS_TOKEN}@github.com/apache/airflow.git", False),
            (
                CONN_HTTPS,
                {"repo_url": "https://github.com/apache/zzzairflow"},
                f"https://user:{ACCESS_TOKEN}@github.com/apache/zzzairflow",
                False,
            ),
            (
                CONN_HTTPS,
                {"repo_url": AIRFLOW_GIT},
                AIRFLOW_GIT,
                True,
            ),
            (CONN_HTTP, {}, f"http://user:{ACCESS_TOKEN}@github.com/apache/airflow.git", False),
            (
                CONN_HTTP,
                {"repo_url": "http://github.com/apache/zzzairflow"},
                f"http://user:{ACCESS_TOKEN}@github.com/apache/zzzairflow",
                False,
            ),
            (CONN_HTTP_NO_AUTH, {}, AIRFLOW_HTTP_URL, False),
            (
                CONN_HTTP_NO_AUTH,
                {"repo_url": "http://github.com/apache/zzzairflow"},
                "http://github.com/apache/zzzairflow",
                False,
            ),
            (CONN_ONLY_PATH, {}, "path/to/repo", False),
        ],
    )
    def test_correct_repo_urls(self, conn_id, hook_kwargs, expected_repo_url, warns_on_default):
        warning_context = (
            pytest.warns(AirflowProviderDeprecationWarning, match="accept-new")
            if warns_on_default
            else contextlib.nullcontext()
        )
        with warning_context:
            hook = GitHook(git_conn_id=conn_id, **hook_kwargs)
        assert hook.repo_url == expected_repo_url

    def test_env_var_with_configure_hook_env(self, create_connection_without_db):
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
            default_hook = GitHook(git_conn_id=CONN_DEFAULT)
        with default_hook.configure_hook_env():
            assert default_hook.env == {
                "GIT_SSH_COMMAND": "ssh -i /files/pkey.pem -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
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
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
            hook = GitHook(git_conn_id=CONN_DEFAULT)

        assert hasattr(hook, "env")
        with hook.configure_hook_env():
            assert hook.env == {
                "GIT_SSH_COMMAND": "ssh -i /files/pkey.pem -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
            }

    def test_private_key_lazy_env_var(self):
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
            hook = GitHook(git_conn_id=CONN_ONLY_INLINE_KEY)
        assert hook.env == {}

        hook.set_git_env("dummy_inline_key")
        assert hook.env == {
            "GIT_SSH_COMMAND": "ssh -i dummy_inline_key -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
        }

    def test_configure_hook_env(self):
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
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
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
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
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
            hook = GitHook(git_conn_id="git_with_proxy")
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert "ProxyCommand='ssh -W %h:%p bastion.example.com'" in cmd

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
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
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
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
            hook = GitHook(git_conn_id="git_proxy_only")
        assert hook.env == {}
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert cmd.startswith("ssh ")
            assert "-i " not in cmd
            assert "ProxyCommand" in cmd

    def test_user_known_hosts_devnull_when_strict_checking_disabled(self, create_connection_without_db):
        """When strict_host_key_checking=no and no known_hosts_file, /dev/null is used."""
        create_connection_without_db(
            Connection(
                conn_id="git_strict_no",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={"key_file": "/files/pkey.pem", "strict_host_key_checking": "no"},
            )
        )
        hook = GitHook(git_conn_id="git_strict_no")
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert "-o StrictHostKeyChecking=no" in cmd
            assert "-o UserKnownHostsFile=/dev/null" in cmd

    def test_default_strict_host_key_checking_is_accept_new(self):
        """Relying on the default verifies host keys (accept-new) and warns about the change."""
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
            hook = GitHook(git_conn_id=CONN_DEFAULT)
        assert hook.strict_host_key_checking == "accept-new"
        with hook.configure_hook_env():
            cmd = hook.env["GIT_SSH_COMMAND"]
            assert "-o StrictHostKeyChecking=accept-new" in cmd
            assert "/dev/null" not in cmd

    def test_explicit_strict_host_key_checking_does_not_warn(self, create_connection_without_db):
        """Setting strict_host_key_checking explicitly suppresses the deprecation warning."""
        create_connection_without_db(
            Connection(
                conn_id="git_strict_explicit",
                host=AIRFLOW_GIT,
                conn_type="git",
                extra={"key_file": "/files/pkey.pem", "strict_host_key_checking": "accept-new"},
            )
        )
        with warnings.catch_warnings():
            warnings.simplefilter("error", AirflowProviderDeprecationWarning)
            hook = GitHook(git_conn_id="git_strict_explicit")
        assert hook.strict_host_key_checking == "accept-new"

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
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
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
        with pytest.warns(AirflowProviderDeprecationWarning, match="accept-new"):
            hook = GitHook(git_conn_id="git_passphrase_cleanup")
        askpass_path = None
        with hook.configure_hook_env():
            askpass_path = hook.env.get("SSH_ASKPASS")
            assert askpass_path is not None
            assert os.path.exists(askpass_path)
        # Both the askpass script and the temp key file should be cleaned up
        assert not os.path.exists(askpass_path)

    # --- GitHub App auth tests ---

    def test_only_app_id_without_installation_id_raises(self):
        with pytest.raises(
            AirflowException, match="Both 'github_app_id' and 'github_installation_id' must be provided"
        ):
            GitHook(git_conn_id=CONN_APP_ONLY_APP_ID)

    def test_only_installation_id_without_app_id_raises(self):
        with pytest.raises(
            AirflowException,
            match="Both 'github_app_id' and 'github_installation_id' must be provided",
        ):
            GitHook(git_conn_id=CONN_APP_ONLY_INSTALLATION_ID)

    def test_app_id_and_installation_id_without_key_raises(self):
        with pytest.raises(
            AirflowException,
            match="Missing inline private_key or key_file for GitHub App Auth",
        ):
            GitHook(git_conn_id=CONN_APP_NO_KEY)

    def test_app_auth_with_key_file_reads_file(self, create_connection_without_db, tmp_path, monkeypatch):
        key_file = tmp_path / "app_key.pem"
        key_file.write_text("file_pem_key_content")
        create_connection_without_db(
            Connection(
                conn_id="git_app_key_file",
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={
                    "github_app_id": "12345",
                    "github_installation_id": "67890",
                    "key_file": str(key_file),
                },
            )
        )
        from datetime import datetime, timedelta, timezone

        mock_expiry = datetime.now(timezone.utc) + timedelta(hours=1)
        monkeypatch.setattr(
            "airflow.providers.git.hooks.git.GitHook._get_github_app_token",
            lambda self: ("x-access-token", "ghs_test_token", mock_expiry),
        )
        hook = GitHook(git_conn_id="git_app_key_file")

        assert hook.private_key == "file_pem_key_content"

    def test_app_auth_with_missing_key_file_raises(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="git_app_missing_key_file",
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={
                    "github_app_id": "12345",
                    "github_installation_id": "67890",
                    "key_file": "/nonexistent/path/key.pem",
                },
            )
        )
        with pytest.raises(AirflowException, match="Failed to read GitHub App private key file"):
            GitHook(git_conn_id="git_app_missing_key_file")

    def test_app_auth_defers_token_fetch(self, monkeypatch):
        """GitHub App token is not fetched in __init__, only on configure_hook_env."""
        from datetime import datetime, timedelta, timezone

        mock_called = []

        def mock_get_token(self):
            mock_called.append(True)
            return ("x-access-token", "ghs_test_token", datetime.now(timezone.utc) + timedelta(hours=1))

        monkeypatch.setattr(
            "airflow.providers.git.hooks.git.GitHook._get_github_app_token",
            mock_get_token,
        )
        # __init__ should NOT call _get_github_app_token
        hook = GitHook(git_conn_id=CONN_APP_INLINE_KEY)
        assert len(mock_called) == 0
        assert hook.auth_token == ""
        assert hook.github_app_id == "12345"
        assert hook.github_installation_id == "67890"

        # First call to configure_hook_env should trigger the token fetch
        with hook.configure_hook_env():
            assert len(mock_called) == 1
            assert hook.auth_token == "ghs_test_token"
            assert hook.user_name == "x-access-token"

    def test_app_auth_success_stores_app_id_and_installation_id(self):
        """App ID and installation ID are stored at __init__ time."""
        hook = GitHook(git_conn_id=CONN_APP_INLINE_KEY)
        assert hook.github_app_id == "12345"
        assert hook.github_installation_id == "67890"

    @pytest.mark.parametrize(
        ("app_id", "installation_id"),
        [
            ("12345", "67890"),
            (12345, 67890),
        ],
    )
    def test_app_id_and_installation_id_are_stored_as_provided(
        self, app_id, installation_id, create_connection_without_db, monkeypatch
    ):
        from datetime import datetime, timedelta, timezone

        create_connection_without_db(
            Connection(
                conn_id="git_app_int_check",
                host=AIRFLOW_HTTPS_URL,
                conn_type="git",
                extra={
                    "github_app_id": app_id,
                    "github_installation_id": installation_id,
                    "private_key": "inline_pem_key",
                },
            )
        )
        monkeypatch.setattr(
            "airflow.providers.git.hooks.git.GitHook._get_github_app_token",
            lambda self: ("x-access-token", "token", datetime.now(timezone.utc) + timedelta(hours=1)),
        )
        hook = GitHook(git_conn_id="git_app_int_check")
        assert hook.github_app_id == app_id
        assert hook.github_installation_id == installation_id

    def test_github_app_token_refresh_near_expiry(self, monkeypatch):
        """Token is refreshed when near expiry during configure_hook_env."""
        from datetime import datetime, timedelta, timezone

        mock_get_token_call_count = [0]

        def mock_get_token(self):
            mock_get_token_call_count[0] += 1
            # First call returns token expiring in 3 minutes
            if mock_get_token_call_count[0] == 1:
                return (
                    "x-access-token",
                    f"token_{mock_get_token_call_count[0]}",
                    datetime.now(timezone.utc) + timedelta(minutes=3),
                )
            # Second call (refresh) returns token expiring in 1 hour
            return (
                "x-access-token",
                f"token_{mock_get_token_call_count[0]}",
                datetime.now(timezone.utc) + timedelta(hours=1),
            )

        monkeypatch.setattr(
            "airflow.providers.git.hooks.git.GitHook._get_github_app_token",
            mock_get_token,
        )
        hook = GitHook(git_conn_id=CONN_APP_INLINE_KEY)
        assert mock_get_token_call_count[0] == 0  # No call in __init__

        # First configure_hook_env triggers first token fetch
        with hook.configure_hook_env():
            assert mock_get_token_call_count[0] == 1
            assert hook.auth_token == "token_1"

        # Second configure_hook_env triggers refresh (token near expiry)
        with hook.configure_hook_env():
            assert mock_get_token_call_count[0] == 2
            assert hook.auth_token == "token_2"

    def test_github_app_integration_call_shape(self, monkeypatch):
        """Verify GithubIntegration is called with correct arguments."""
        from datetime import datetime, timedelta, timezone
        from unittest import mock

        mock_integration = mock.MagicMock()
        mock_access_token = mock.MagicMock()
        mock_access_token.token = "ghs_test_token"
        mock_access_token.expires_at = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_integration.get_access_token.return_value = mock_access_token

        import sys
        from types import SimpleNamespace

        fake_github = SimpleNamespace(
            Auth=SimpleNamespace(AppAuth=lambda app_id, key: "auth"),
            GithubIntegration=lambda auth: mock_integration,
        )
        monkeypatch.setitem(sys.modules, "github", fake_github)

        hook = GitHook(git_conn_id=CONN_APP_INLINE_KEY)
        with hook.configure_hook_env():
            # Verify get_access_token was called with installation_id kwarg
            assert mock_integration.get_access_token.call_count == 1
            _, kwargs = mock_integration.get_access_token.call_args
            assert "installation_id" in kwargs
            assert str(kwargs["installation_id"]) == "67890"
