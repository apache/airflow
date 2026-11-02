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
"""Connection-backed authentication tests for HuggingFaceHook."""
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from airflow.exceptions import AirflowNotFoundException

from airflow.providers.huggingface.hooks.huggingface import HuggingFaceHook
from huggingface_hub import GitCommitInfo


def git_commit_info(commit_id, title, message=""):
    return GitCommitInfo(
        commit_id=commit_id,
        authors=["unit-test"],
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        title=title,
        message=message,
        formatted_title=None,
        formatted_message=None,
    )


@pytest.fixture
def connection_env(monkeypatch):
    # Airflow resolves this as a real Connection, without a metadata DB fixture.
    monkeypatch.setenv(
        "AIRFLOW_CONN_HUGGINGFACE_TEST",
        '{"conn_type":"huggingface","password":"test-secret"}',
    )


def test_connection_resolution_uses_password_only(connection_env):
    hook = HuggingFaceHook("huggingface_test")
    assert hook.get_token() == "test-secret"


def test_get_conn_caches_autospecced_hf_api(connection_env):
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api:
        hook = HuggingFaceHook("huggingface_test")
        first = hook.get_conn()
        assert hook.get_conn() is first
        api.assert_called_once_with(token="test-secret")


def test_connection_test_success_and_failure(connection_env):
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api:
        hook = HuggingFaceHook("huggingface_test")
        assert hook.test_connection() == (True, "Connection successfully tested")
        api.return_value.whoami.assert_called_once_with()
        api.return_value.whoami.side_effect = ValueError("invalid token")
        assert hook.test_connection() == (False, "invalid token")


def test_no_connection_defers_to_huggingface_hub_fallback(monkeypatch):
    monkeypatch.setenv("HF_TOKEN", "ambient-token")
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api:
        hook = HuggingFaceHook(None)
        hook.get_conn()
        api.assert_called_once_with(token=None)


def test_default_missing_connection_allows_sdk_fallback(monkeypatch):
    monkeypatch.delenv("AIRFLOW_CONN_HUGGINGFACE_DEFAULT", raising=False)
    with patch.object(HuggingFaceHook, "get_connection", side_effect=AirflowNotFoundException("missing")):
        assert HuggingFaceHook().get_token() is None


def test_explicit_invalid_connection_fails(monkeypatch):
    with patch.object(HuggingFaceHook, "get_connection", side_effect=AirflowNotFoundException("missing")):
        with pytest.raises(AirflowNotFoundException):
            HuggingFaceHook("explicit_bad_id").get_token()


@pytest.mark.parametrize("error_kind", ["value", "gated"])
def test_load_dataset_passes_token_and_propagates_library_exception(connection_env, error_kind):
    from airflow.providers.huggingface.hooks import huggingface
    import httpx

    if error_kind == "gated":
        from huggingface_hub.utils import GatedRepoError

        error = GatedRepoError(
            "gated", response=httpx.Response(403, request=httpx.Request("GET", "https://huggingface.co"))
        )
    else:
        error = ValueError("invalid or gated dataset")
    with patch.object(huggingface, "load_dataset", autospec=True, side_effect=error) as load:
        hook = HuggingFaceHook("huggingface_test")
        with pytest.raises(type(error)) as raised:
            hook.load_dataset("org/private-data")
    assert raised.value is error
    load.assert_called_once_with("org/private-data", token="test-secret")


def test_hub_repository_exception_propagates(connection_env):
    from huggingface_hub.utils import RepositoryNotFoundError

    hook = HuggingFaceHook("huggingface_test")
    import httpx
    error = RepositoryNotFoundError("missing", response=httpx.Response(404, request=httpx.Request("GET", "https://huggingface.co")))
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api:
        api.return_value.dataset_info.side_effect = error
        with pytest.raises(RepositoryNotFoundError) as raised:
            hook.get_dataset_info("org/missing")
    assert raised.value is error


def test_connection_extra_token_is_ignored(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_HUGGINGFACE_EXTRA_TEST",
                       '{"conn_type":"huggingface","extra":{"token":"ignored"}}')
    assert HuggingFaceHook("huggingface_extra_test").get_token() is None


def test_find_commit_by_marker_matches_title_and_only_returns_final_upload_part():
    marker = "airflow-artifact-sha256:abc:target:def"
    partial = git_commit_info("a" * 40, f"Publish [{marker}] (part 1-of-2)")
    final = git_commit_info("b" * 40, f"Publish [{marker}] (part 2-of-2)")
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api_type:
        api_type.return_value.list_repo_commits.return_value = [final, partial]
        hook = HuggingFaceHook(None)
        commit = hook.find_commit_by_marker("org/data", marker, revision="release")

    assert commit is final
    assert not HuggingFaceHook._commit_is_complete(
        SimpleNamespace(title=f"Publish [{marker}] (part 2)")
    )
    api_type.return_value.list_repo_commits.assert_called_once_with(
        "org/data", repo_type="dataset", revision="release"
    )


def test_find_commit_by_marker_scans_pull_request_refs_when_requested():
    marker = "huggingface-stream-publish:target"
    final = git_commit_info("c" * 40, f"Stream [{marker}]")
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api_type:
        api = api_type.return_value
        api.list_repo_refs.return_value.pull_requests = [SimpleNamespace(ref="refs/pr/7")]
        api.list_repo_commits.side_effect = [[], [final]]
        hook = HuggingFaceHook(None)
        commit = hook.find_commit_by_marker("org/data", marker, include_pull_requests=True)

    assert commit is final
    api.list_repo_refs.assert_called_once_with("org/data", repo_type="dataset", include_pull_requests=True)
    assert api.list_repo_commits.call_args_list[0].kwargs["revision"] == "main"
    assert api.list_repo_commits.call_args_list[1].kwargs["revision"] == "refs/pr/7"
