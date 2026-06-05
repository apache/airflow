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
"""Tests for the framework-agnostic skill source resolver."""

from __future__ import annotations

import os
from unittest.mock import patch

import pytest

from airflow.providers.common.ai.skills import GitSkills, resolve_skills

pytest.importorskip("git")
pytest.importorskip("airflow.providers.git.hooks.git")


class TestGitSkills:
    def test_defaults(self):
        source = GitSkills(repo_url="https://github.com/org/repo")
        assert source.repo_url == "https://github.com/org/repo"
        assert source.conn_id is None
        assert source.path == ""
        assert source.branch is None

    def test_holds_no_secret(self):
        source = GitSkills(repo_url="u", conn_id="github_skills")
        assert "github_skills" in repr(source)
        assert not hasattr(source, "token")
        assert not hasattr(source, "password")


class TestResolveLocal:
    def test_local_dirs_pass_through_untouched(self, tmp_path):
        d1, d2 = str(tmp_path / "a"), str(tmp_path / "b")
        with resolve_skills([d1, d2]) as dirs:
            assert dirs == [d1, d2]

    def test_unsupported_source_raises_typeerror(self):
        with pytest.raises(TypeError, match="Unsupported skill source"):
            with resolve_skills([123]):
                pass


class TestResolveGitWithHook:
    """Credentials come from the git connection via GitHook; never the environment."""

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_conn_id_uses_githook_and_scrubs_config(self, mock_repo, mock_githook):
        mock_githook.return_value.repo_url = "https://user:tok@github.com/org/repo"
        mock_githook.return_value.env = {}
        src = GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default", path="skills")

        with resolve_skills([src]) as dirs:
            assert dirs[0].endswith(os.sep + "skills")

        mock_githook.assert_called_once_with(
            git_conn_id="git_default", repo_url="https://github.com/org/repo"
        )
        # Cloned via the hook's token-bearing URL...
        assert mock_repo.clone_from.call_args.args[0] == "https://user:tok@github.com/org/repo"
        # ...with interactive credential prompts disabled...
        assert mock_repo.clone_from.call_args.kwargs["env"]["GIT_TERMINAL_PROMPT"] == "0"
        # ...then .git/config is scrubbed back to the credential-free URL.
        mock_repo.clone_from.return_value.remote.return_value.set_url.assert_called_once_with(
            "https://github.com/org/repo"
        )

    @pytest.mark.parametrize(
        "repo_url",
        ["https://user:tok@github.com/org/repo", "https://tok@github.com/org/repo"],
    )
    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_repo_url_with_embedded_credentials_rejected(self, mock_repo, mock_githook, repo_url):
        # Credentials in the URL would land in the serialized DAG and be written
        # back to .git/config by the scrub -- reject them outright.
        with pytest.raises(ValueError, match="must not embed"):
            with resolve_skills([GitSkills(repo_url=repo_url, conn_id="git_default")]):
                pass
        mock_githook.assert_not_called()
        mock_repo.clone_from.assert_not_called()

    @pytest.mark.parametrize("bad_path", ["/etc", "../outside", "a/../../b"])
    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_absolute_or_traversing_path_rejected(self, mock_repo, mock_githook, bad_path):
        with pytest.raises(ValueError, match="relative sub-path"):
            with resolve_skills(
                [GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default", path=bad_path)]
            ):
                pass
        mock_repo.clone_from.assert_not_called()

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_http_with_conn_id_is_rejected(self, mock_repo, mock_githook):
        with pytest.raises(ValueError, match="http://"):
            with resolve_skills([GitSkills(repo_url="http://github.com/org/repo", conn_id="git_default")]):
                pass
        mock_githook.assert_not_called()
        mock_repo.clone_from.assert_not_called()

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_no_conn_id_clones_anonymously_without_hook(self, mock_repo, mock_githook):
        with resolve_skills([GitSkills(repo_url="https://github.com/org/repo")]):
            pass
        mock_githook.assert_not_called()
        assert mock_repo.clone_from.call_args.args[0] == "https://github.com/org/repo"

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_failed_clone_removes_temp_dir(self, mock_repo, mock_githook, tmp_path):
        mock_githook.return_value.repo_url = "https://github.com/org/repo"
        mock_githook.return_value.env = {}
        mock_repo.clone_from.side_effect = RuntimeError("boom")
        created = str(tmp_path / "clone")
        os.makedirs(created)
        with patch("airflow.providers.common.ai.skills.tempfile.mkdtemp", return_value=created):
            with pytest.raises(RuntimeError):
                with resolve_skills(
                    [GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default")]
                ):
                    pass
        assert not os.path.exists(created)

    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_clone_error_scrubs_token_from_message(self, mock_repo, mock_githook):
        mock_githook.return_value.repo_url = "https://user:ghp_secret@github.com/org/repo"
        mock_githook.return_value.env = {}
        mock_repo.clone_from.side_effect = Exception(
            "fatal: unable to access https://user:ghp_secret@github.com/org/repo"
        )
        with pytest.raises(RuntimeError) as exc_info:
            with resolve_skills([GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default")]):
                pass
        assert "ghp_secret" not in str(exc_info.value)


class TestResolveGitCleanup:
    @patch("airflow.providers.git.hooks.git.GitHook")
    @patch("git.Repo")
    def test_clone_dir_removed_on_exit(self, mock_repo, mock_githook):
        mock_githook.return_value.repo_url = "https://github.com/org/repo"
        mock_githook.return_value.env = {}
        with resolve_skills([GitSkills(repo_url="https://github.com/org/repo", conn_id="git_default")]):
            dest = mock_repo.clone_from.call_args.args[1]
            assert os.path.isdir(dest)
        assert not os.path.exists(dest)
