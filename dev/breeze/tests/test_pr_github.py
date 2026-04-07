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
"""Tests for the pr_github module — GitHub API interaction layer."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.utils.pr_github import (
    BOT_ACCOUNT_LOGINS,
    COLLABORATOR_ASSOCIATIONS,
    DANGEROUS_ANSWER_VALUES,
    TRUSTED_REPOSITORIES,
    github_rest,
    graphql_mutation,
    graphql_request,
    is_bot_account,
    resolve_github_token,
    validate_llm_safety,
)


# ===========================================================================
# TestIsBotAccount
# ===========================================================================
class TestIsBotAccount:
    def test_known_bots(self):
        for bot in BOT_ACCOUNT_LOGINS:
            assert is_bot_account(bot) is True

    def test_bot_suffix(self):
        assert is_bot_account("my-custom-app[bot]") is True

    def test_regular_user(self):
        assert is_bot_account("testuser") is False

    def test_case_insensitive(self):
        assert is_bot_account("Dependabot") is True
        assert is_bot_account("DEPENDABOT") is True


# ===========================================================================
# TestResolveGithubToken
# ===========================================================================
class TestResolveGithubToken:
    def test_explicit_token_returned(self):
        assert resolve_github_token("my-token") == "my-token"

    @patch("airflow_breeze.utils.pr_github.run_command")
    def test_gh_cli_fallback(self, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout="gh-token\n")
        assert resolve_github_token(None) == "gh-token"

    @patch("airflow_breeze.utils.pr_github.run_command")
    def test_no_token_available(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        assert resolve_github_token(None) is None


# ===========================================================================
# TestGraphqlRequest
# ===========================================================================
class TestGraphqlRequest:
    @patch("requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = MagicMock(
            status_code=200,
            json=lambda: {"data": {"viewer": {"login": "test"}}},
        )
        result = graphql_request("token", "query { viewer { login } }", {})
        assert result == {"viewer": {"login": "test"}}

    @patch("requests.post")
    def test_retry_on_502(self, mock_post):
        fail_response = MagicMock(status_code=502)
        ok_response = MagicMock(
            status_code=200,
            json=lambda: {"data": {"ok": True}},
        )
        mock_post.side_effect = [fail_response, ok_response]
        result = graphql_request("token", "query {}", {})
        assert result == {"ok": True}
        assert mock_post.call_count == 2


# ===========================================================================
# TestGraphqlMutation
# ===========================================================================
class TestGraphqlMutation:
    @patch("airflow_breeze.utils.pr_github.graphql_request")
    def test_success_returns_true(self, mock_gql):
        mock_gql.return_value = {"ok": True}
        assert graphql_mutation("token", "mutation {}", {}) is True

    @patch("airflow_breeze.utils.pr_github.graphql_request", side_effect=SystemExit(1))
    def test_failure_returns_false(self, mock_gql):
        assert graphql_mutation("token", "mutation {}", {}) is False


# ===========================================================================
# TestGithubRest
# ===========================================================================
class TestGithubRest:
    @patch("requests.post")
    def test_success(self, mock_post):
        mock_post.return_value = MagicMock(status_code=200)
        assert github_rest("token", "post", "https://api.github.com/test") is True

    @patch("requests.post")
    def test_failure_code(self, mock_post):
        mock_post.return_value = MagicMock(status_code=404, text="Not found")
        assert github_rest("token", "post", "https://api.github.com/test") is False

    @patch("requests.post", side_effect=Exception("network"))
    def test_retry_on_exception(self, mock_post):
        result = github_rest("token", "post", "https://api.github.com/test")
        assert result is False
        assert mock_post.call_count == 3


# ===========================================================================
# TestValidateLlmSafety
# ===========================================================================
class TestValidateLlmSafety:
    def test_untrusted_repo_exits(self):
        with pytest.raises(SystemExit):
            validate_llm_safety("unknown/repo", None)

    def test_trusted_repo_no_exit(self):
        validate_llm_safety("apache/airflow", None)

    def test_dangerous_answer_exits(self):
        for val in DANGEROUS_ANSWER_VALUES:
            with pytest.raises(SystemExit):
                validate_llm_safety("apache/airflow", val)

    def test_safe_answer_no_exit(self):
        validate_llm_safety("apache/airflow", "s")
        validate_llm_safety("apache/airflow", "q")


# ===========================================================================
# TestConstants
# ===========================================================================
class TestConstants:
    def test_collaborator_associations(self):
        assert "COLLABORATOR" in COLLABORATOR_ASSOCIATIONS
        assert "MEMBER" in COLLABORATOR_ASSOCIATIONS
        assert "OWNER" in COLLABORATOR_ASSOCIATIONS

    def test_trusted_repos(self):
        assert "apache/airflow" in TRUSTED_REPOSITORIES
