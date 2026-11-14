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

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

import pytest
from huggingface_hub import GitCommitInfo
from huggingface_hub.utils import RepositoryNotFoundError, RevisionNotFoundError

from airflow.exceptions import AirflowNotFoundException
from airflow.providers.huggingface.hooks.huggingface import HuggingFaceHook


def git_commit_info(commit_id, title, message=""):
    return GitCommitInfo(
        commit_id=commit_id,
        authors=["unit-test"],
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
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


def test_empty_named_connection_disables_ambient_token_fallback(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_HUGGINGFACE_PUBLIC", '{"conn_type":"huggingface"}')
    monkeypatch.setenv("HF_TOKEN", "worker-write-token")
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api:
        hook = HuggingFaceHook("huggingface_public")
        hook.get_conn()
        api.assert_called_once_with(token=False)

        with patch("datasets.load_dataset", autospec=True) as load:
            hook.load_dataset("org/public")
        load.assert_called_once_with("org/public", token=False)

        dataset = Mock(spec=["push_to_hub"])
        hook.push_to_hub(dataset, "org/public")
        dataset.push_to_hub.assert_called_once_with("org/public", token=False)


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


def test_load_dataset_passes_token_and_propagates_library_exception(connection_env):
    error = ValueError("invalid or gated dataset")
    with patch("datasets.load_dataset", autospec=True, side_effect=error) as load:
        hook = HuggingFaceHook("huggingface_test")
        with pytest.raises(type(error)) as raised:
            hook.load_dataset("org/private-data")
    assert raised.value is error
    load.assert_called_once_with("org/private-data", token="test-secret")


def test_connection_extra_token_is_ignored(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_HUGGINGFACE_EXTRA_TEST",
        '{"conn_type":"huggingface","extra":{"token":"ignored"}}',
    )
    assert HuggingFaceHook("huggingface_extra_test").get_token() is False


def test_find_commit_by_marker_matches_unsuffixed_final_commit_and_rejects_parts():
    marker = "airflow-artifact-sha256:abc:target:def"
    partial = git_commit_info("a" * 40, f"Publish [{marker}] (part 00001-of-00002)")
    final = git_commit_info("b" * 40, f"Publish [{marker}]")
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api_type:
        api_type.return_value.list_repo_commits.return_value = [final, partial]
        hook = HuggingFaceHook(None)
        commit = hook.find_commit_by_marker("org/data", marker, revision="release")

    assert commit is final
    assert not HuggingFaceHook._commit_is_complete(
        SimpleNamespace(title=f"Publish [{marker}] (part 00002-of-00003)")
    )
    assert HuggingFaceHook._commit_is_complete(SimpleNamespace(title=f"Publish [{marker}]"))
    api_type.return_value.list_repo_commits.assert_called_once_with(
        "org/data", repo_type="dataset", revision="release"
    )


def test_find_commit_by_marker_does_not_reuse_historical_commit():
    marker = "airflow-artifact-sha256:abc:target:def"
    old_commit = git_commit_info("a" * 40, f"Publish [{marker}]")
    latest_commit = git_commit_info("b" * 40, "Publish newer artifact")
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api_type:
        api_type.return_value.list_repo_commits.return_value = [latest_commit, old_commit]
        hook = HuggingFaceHook(None)
        commit = hook.find_commit_by_marker("org/data", marker, revision="release")

    assert commit is None


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


@pytest.mark.asyncio
async def test_aget_dataset_info_builds_url_and_returns_info(connection_env, mocker):
    calls = []

    async def fake_get(client, url, *, headers, **kwargs):
        calls.append((url, headers))
        return SimpleNamespace(json=lambda: {"id": "org/data", "private": False})

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)
    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_connection",
        new=AsyncMock(return_value=SimpleNamespace(password="test-secret")),
    )

    hook = HuggingFaceHook("huggingface_test")
    info = await hook.aget_dataset_info("org/data")

    assert info.id == "org/data"
    assert calls[0][0].endswith("/api/datasets/org/data")


@pytest.mark.asyncio
async def test_aget_dataset_info_quotes_revision(connection_env, mocker):
    calls = []

    async def fake_get(client, url, *, headers, **kwargs):
        calls.append(url)
        return SimpleNamespace(json=lambda: {"id": "org/data"})

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)

    hook = HuggingFaceHook("huggingface_test")
    await hook.aget_dataset_info("org/data", revision="release 1")

    assert calls[0].endswith("/api/datasets/org/data/revision/release%201")


@pytest.mark.asyncio
async def test_aget_token_falls_back_to_ambient_for_default_connection(monkeypatch, mocker):
    monkeypatch.setenv("AIRFLOW_CONN_HUGGINGFACE_DEFAULT", '{"conn_type":"huggingface"}')
    hook = HuggingFaceHook(None)

    assert await hook._aget_token() is None


@pytest.mark.asyncio
async def test_aget_token_raises_for_explicit_missing_connection():
    with patch.object(HuggingFaceHook, "get_connection", side_effect=AirflowNotFoundException("missing")):
        hook = HuggingFaceHook("explicit_bad_id")
        with pytest.raises(AirflowNotFoundException):
            await hook._aget_token()


def _commit_payload(commit_id, title, message=""):
    return {
        "id": commit_id,
        "authors": [{"user": "unit-test"}],
        "date": "2026-01-01T00:00:00.000Z",
        "title": title,
        "message": message,
    }


@pytest.mark.asyncio
async def test_afind_commit_by_marker_matches_first_ref(connection_env, mocker):
    marker = "airflow-artifact-sha256:abc:target:def"

    async def fake_get(client, url, *, headers, **kwargs):
        assert url.endswith("/api/datasets/org/data/commits/main")
        return SimpleNamespace(
            json=lambda: [
                _commit_payload("b" * 40, f"Publish [{marker}]"),
            ]
        )

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)
    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_connection",
        new=AsyncMock(return_value=SimpleNamespace(password="test-secret")),
    )

    hook = HuggingFaceHook("huggingface_test")
    commit = await hook.afind_commit_by_marker("org/data", marker)

    assert commit is not None
    assert commit.commit_id == "b" * 40
    assert commit.title == f"Publish [{marker}]"


@pytest.mark.asyncio
async def test_afind_commit_by_marker_does_not_reuse_historical_commit(connection_env, mocker):
    marker = "airflow-artifact-sha256:abc:target:def"

    async def fake_get(client, url, *, headers, **kwargs):
        return SimpleNamespace(
            json=lambda: [
                _commit_payload("b" * 40, "Publish newer artifact"),
                _commit_payload("a" * 40, f"Publish [{marker}]"),
            ]
        )

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)

    hook = HuggingFaceHook(None)
    assert await hook.afind_commit_by_marker("org/data", marker) is None


@pytest.mark.asyncio
async def test_afind_commit_by_marker_rejects_part_commits(connection_env, mocker):
    marker = "airflow-artifact-sha256:abc:target:def"

    async def fake_get(client, url, *, headers, **kwargs):
        return SimpleNamespace(
            json=lambda: [_commit_payload("a" * 40, f"Publish [{marker}] (part 00001-of-00002)")]
        )

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)

    hook = HuggingFaceHook(None)
    assert await hook.afind_commit_by_marker("org/data", marker) is None


@pytest.mark.asyncio
async def test_afind_commit_by_marker_scans_pull_request_refs(connection_env, mocker):
    marker = "huggingface-stream-publish:target"
    urls = []

    async def fake_get(client, url, *, headers, **kwargs):
        urls.append(url)

        def payload():
            if url.endswith("/refs"):
                return {"pullRequests": [{"name": "7", "ref": "refs/pr/7", "targetCommit": "deadbeef"}]}
            if url.endswith("/commits/main"):
                return []
            if url.endswith("refs%2Fpr%2F7") or url.endswith("/commits/refs/pr/7"):
                return [_commit_payload("c" * 40, f"Stream [{marker}]")]
            raise AssertionError(f"unexpected url {url}")

        return SimpleNamespace(json=payload)

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)

    hook = HuggingFaceHook(None)
    commit = await hook.afind_commit_by_marker("org/data", marker, include_pull_requests=True)

    assert commit is not None
    assert commit.commit_id == "c" * 40
    assert any(url.endswith("/refs") for url in urls)
    assert urls.index(next(u for u in urls if u.endswith("/commits/main"))) < urls.index(
        next(u for u in urls if "/commits/" in u and "main" not in u)
    )


def _hf_error(cls):
    import httpx2

    return cls(
        "missing",
        response=httpx2.Response(
            404, headers={}, request=httpx2.Request("GET", "https://huggingface.co/api/datasets/org/data")
        ),
    )


@pytest.mark.asyncio
async def test_afind_commit_by_marker_repository_not_found_returns_none(connection_env, mocker):
    async def fake_get(client, url, *, headers, **kwargs):
        raise _hf_error(RepositoryNotFoundError)

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)

    hook = HuggingFaceHook(None)
    assert await hook.afind_commit_by_marker("org/data", "marker") is None


@pytest.mark.asyncio
async def test_afind_commit_by_marker_revision_not_found_skips_ref(connection_env, mocker):
    async def fake_get(client, url, *, headers, **kwargs):
        raise _hf_error(RevisionNotFoundError)

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)

    hook = HuggingFaceHook(None)
    assert await hook.afind_commit_by_marker("org/data", "marker", revision="missing") is None


@pytest.mark.asyncio
async def test_afind_commit_by_marker_empty_commits_returns_none(connection_env, mocker):
    async def fake_get(client, url, *, headers, **kwargs):
        return SimpleNamespace(json=list)

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)

    hook = HuggingFaceHook(None)
    assert await hook.afind_commit_by_marker("org/data", "marker") is None
