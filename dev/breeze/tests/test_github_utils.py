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

import subprocess
from unittest import mock

import pytest

from airflow_breeze.utils.github import (
    env_without_github_tokens,
    retrieve_github_token,
    run_gh_command,
)
from airflow_breeze.utils.shared_options import set_dry_run


def _completed_process(returncode: int, stdout: str = "") -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(args=["gh"], returncode=returncode, stdout=stdout, stderr="")


def test_env_without_github_tokens_removes_ambient_token_vars(monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "gh-token")
    monkeypatch.setenv("GITHUB_TOKEN", "github-token")
    monkeypatch.setenv("OTHER_VAR", "kept")

    cleaned_env = env_without_github_tokens()

    assert "GH_TOKEN" not in cleaned_env
    assert "GITHUB_TOKEN" not in cleaned_env
    assert cleaned_env["OTHER_VAR"] == "kept"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_prefers_clean_gh_auth_token(mock_run, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "env-gh-token")
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.return_value = _completed_process(returncode=0, stdout="stored-gh-token\n")

    assert retrieve_github_token() == "stored-gh-token"

    mock_run.assert_called_once()
    call_env = mock_run.call_args.kwargs["env"]
    assert "GH_TOKEN" not in call_env
    assert "GITHUB_TOKEN" not in call_env


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_falls_back_to_env_token(mock_run, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "env-gh-token")
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.return_value = _completed_process(returncode=1)

    assert retrieve_github_token() == "env-gh-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_falls_back_to_env_token_when_gh_is_missing(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.side_effect = FileNotFoundError

    assert retrieve_github_token() == "env-github-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_falls_back_to_env_token_when_gh_returns_whitespace(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.return_value = _completed_process(returncode=0, stdout="  \n")

    assert retrieve_github_token() == "env-github-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_keeps_explicit_token(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-token")

    assert retrieve_github_token("explicit-token") == "explicit-token"

    mock_run.assert_not_called()


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_retrieve_github_token_does_not_treat_env_token_argument_as_explicit(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-token")
    mock_run.return_value = _completed_process(returncode=0, stdout="stored-gh-token\n")

    assert retrieve_github_token("env-token") == "stored-gh-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_retries_with_original_env_after_clean_env_failure(mock_run, monkeypatch):
    monkeypatch.setenv("GH_TOKEN", "env-gh-token")
    monkeypatch.setenv("GITHUB_TOKEN", "env-github-token")
    mock_run.side_effect = [
        _completed_process(returncode=1),
        _completed_process(returncode=0),
    ]

    result = run_gh_command(["gh", "workflow", "run", "docs.yml"], capture_output=True)

    assert result.returncode == 0
    assert mock_run.call_count == 2
    first_env = mock_run.call_args_list[0].kwargs["env"]
    second_env = mock_run.call_args_list[1].kwargs["env"]
    assert "GH_TOKEN" not in first_env
    assert "GITHUB_TOKEN" not in first_env
    assert second_env["GH_TOKEN"] == "env-gh-token"
    assert second_env["GITHUB_TOKEN"] == "env-github-token"


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_does_not_retry_after_clean_env_success(mock_run, monkeypatch):
    monkeypatch.setenv("GITHUB_TOKEN", "env-token")
    mock_run.return_value = _completed_process(returncode=0)

    result = run_gh_command(["gh", "api", "repos/apache/airflow"], capture_output=True)

    assert result.returncode == 0
    mock_run.assert_called_once()


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_raises_when_check_true_and_no_env_token_to_retry(mock_run, monkeypatch):
    monkeypatch.delenv("GH_TOKEN", raising=False)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    mock_run.return_value = _completed_process(returncode=1)

    with pytest.raises(subprocess.CalledProcessError) as ctx:
        run_gh_command(["gh", "api", "repos/apache/airflow"], capture_output=True, check=True)

    assert ctx.value.returncode == 1


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_raises_when_gh_is_missing(mock_run):
    mock_run.side_effect = FileNotFoundError

    with pytest.raises(FileNotFoundError):
        run_gh_command(["gh", "api", "repos/apache/airflow"], capture_output=True)


@mock.patch("airflow_breeze.utils.github.subprocess.run")
def test_run_gh_command_skips_subprocess_in_dry_run(mock_run):
    set_dry_run(True)
    try:
        result = run_gh_command(["gh", "workflow", "run", "docs.yml"], capture_output=True)
    finally:
        set_dry_run(False)

    assert result.returncode == 0
    mock_run.assert_not_called()
