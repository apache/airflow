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

import importlib
import inspect
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest
from huggingface_hub import GitCommitInfo
from huggingface_hub.utils import HFValidationError, RevisionNotFoundError

try:
    from huggingface_hub.utils import get_async_session
except ImportError:
    get_async_session = None  # type: ignore[assignment]

from airflow.exceptions import AirflowNotFoundException, AirflowOptionalProviderFeatureException
from airflow.providers.huggingface.hooks.huggingface import HuggingFaceHook

requires_async_session = pytest.mark.skipif(
    get_async_session is None, reason="requires huggingface-hub>=1.0.0"
)


def _async_http():
    annotation = inspect.signature(get_async_session).return_annotation
    module_name = annotation if isinstance(annotation, str) else annotation.__module__
    return importlib.import_module(module_name.split(".")[0])


def _mock_async_session(handler):
    http = _async_http()

    def create_client():
        return http.AsyncClient(transport=http.MockTransport(handler))

    return create_client


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


@pytest.mark.parametrize(
    "invoke",
    [
        pytest.param(lambda hook: hook.aget_dataset_info("org/data"), id="aget_dataset_info"),
        pytest.param(
            lambda hook: hook.afind_commit_by_marker("org/data", "marker"), id="afind_commit_by_marker"
        ),
    ],
)
@pytest.mark.asyncio
async def test_async_metadata_calls_require_hub_with_async_session(invoke, mocker):
    mocker.patch("airflow.providers.huggingface.hooks.huggingface.get_async_session", None)
    hook = HuggingFaceHook(None)

    with pytest.raises(AirflowOptionalProviderFeatureException, match="huggingface-hub 1.0.0"):
        await invoke(hook)


@requires_async_session
@pytest.mark.parametrize(
    "invoke",
    [
        pytest.param(lambda hook: hook.aget_dataset_info("org/data/extra"), id="aget_dataset_info"),
        pytest.param(
            lambda hook: hook.afind_commit_by_marker("org/data/extra", "marker"),
            id="afind_commit_by_marker",
        ),
    ],
)
@pytest.mark.asyncio
async def test_async_metadata_rejects_invalid_repo_id(invoke, mocker):
    session = mocker.patch("airflow.providers.huggingface.hooks.huggingface._create_async_session")

    with pytest.raises(HFValidationError):
        await invoke(HuggingFaceHook(None))
    session.assert_not_called()


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


@requires_async_session
@pytest.mark.asyncio
async def test_aget_dataset_info_empty_named_connection_sends_no_authorization_header(monkeypatch, mocker):
    monkeypatch.setenv("AIRFLOW_CONN_HUGGINGFACE_PUBLIC", '{"conn_type":"huggingface"}')
    monkeypatch.setenv("HF_TOKEN", "worker-write-token")
    http = _async_http()
    requests = []

    def handler(request):
        requests.append(request)
        return http.Response(200, json={"id": "org/public", "private": False})

    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_session",
        side_effect=_mock_async_session(handler),
    )

    hook = HuggingFaceHook("huggingface_public")
    info = await hook.aget_dataset_info("org/public")

    assert info.id == "org/public"
    assert len(requests) == 1
    assert "authorization" not in requests[0].headers


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


@requires_async_session
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


@requires_async_session
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


@requires_async_session
@pytest.mark.asyncio
async def test_afind_commit_by_marker_empty_commits_returns_none(connection_env, mocker):
    async def fake_get(client, url, *, headers, **kwargs):
        return SimpleNamespace(json=list)

    mocker.patch.object(HuggingFaceHook, "_aget", side_effect=fake_get)

    hook = HuggingFaceHook(None)
    assert await hook.afind_commit_by_marker("org/data", "marker") is None


@requires_async_session
@pytest.mark.parametrize(
    ("revision", "expected_path"),
    [
        (None, "/api/datasets/org/data"),
        ("release 1", "/api/datasets/org/data/revision/release%201"),
    ],
)
@pytest.mark.asyncio
async def test_aget_dataset_info_sends_http_request(revision, expected_path, connection_env, mocker):
    http = _async_http()
    requests = []

    def handler(request):
        requests.append(request)
        return http.Response(200, json={"id": "org/data", "private": False})

    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_session",
        side_effect=_mock_async_session(handler),
    )

    hook = HuggingFaceHook("huggingface_test")
    info = await hook.aget_dataset_info("org/data", revision=revision)

    assert info.id == "org/data"
    assert info.private is False
    assert len(requests) == 1
    request = requests[0]
    assert request.method == "GET"
    assert request.url.raw_path.decode().split("?")[0] == expected_path
    assert request.headers["authorization"] == "Bearer test-secret"


@requires_async_session
@pytest.mark.asyncio
async def test_afind_commit_by_marker_sends_http_requests(connection_env, mocker):
    http = _async_http()
    marker = "huggingface-stream-publish:target"
    requests = []

    def handler(request):
        requests.append(request)
        path = request.url.raw_path.decode()
        if path == "/api/datasets/org/data/refs?include_prs=1":
            return http.Response(
                200,
                json={"pullRequests": [{"name": "7", "ref": "refs/pr/7", "targetCommit": "deadbeef"}]},
            )
        if path.startswith("/api/datasets/org/data/commits/main"):
            return http.Response(200, json=[])
        if path.startswith("/api/datasets/org/data/commits/refs%2Fpr%2F7"):
            return http.Response(200, json=[_commit_payload("c" * 40, f"Stream [{marker}]")])
        raise AssertionError(f"unexpected request {path}")

    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_session",
        side_effect=_mock_async_session(handler),
    )

    hook = HuggingFaceHook("huggingface_test")
    commit = await hook.afind_commit_by_marker("org/data", marker, include_pull_requests=True)

    assert commit is not None
    assert commit.commit_id == "c" * 40
    assert commit.title == f"Stream [{marker}]"
    assert commit.created_at == datetime(2026, 1, 1, tzinfo=UTC)
    assert [request.url.raw_path.decode() for request in requests] == [
        "/api/datasets/org/data/refs?include_prs=1",
        "/api/datasets/org/data/commits/main",
        "/api/datasets/org/data/commits/refs%2Fpr%2F7",
    ]
    assert {request.headers["authorization"] for request in requests} == {"Bearer test-secret"}


@requires_async_session
@pytest.mark.asyncio
async def test_afind_commit_by_marker_returns_none_when_pull_request_refs_are_unauthorized(mocker):
    http = _async_http()
    paths = []

    def handler(request):
        paths.append(request.url.raw_path.decode())
        return http.Response(401, json={"error": "missing"})

    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_session",
        side_effect=_mock_async_session(handler),
    )

    hook = HuggingFaceHook(None)
    assert await hook.afind_commit_by_marker("org/data", "marker", include_pull_requests=True) is None
    assert paths == ["/api/datasets/org/data/refs?include_prs=1"]


@requires_async_session
@pytest.mark.asyncio
async def test_afind_commit_by_marker_requests_each_ref_once(mocker):
    http = _async_http()
    commit_paths = []

    def handler(request):
        path = request.url.raw_path.decode()
        if path == "/api/datasets/org/data/refs?include_prs=1":
            return http.Response(
                200,
                json={"pullRequests": [{"name": "7", "ref": "refs/pr/7", "targetCommit": "deadbeef"}]},
            )
        if path.startswith("/api/datasets/org/data/commits/"):
            commit_paths.append(path)
            return http.Response(200, json=[_commit_payload("c" * 40, "Publish newer artifact")])
        raise AssertionError(f"unexpected request {path}")

    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_session",
        side_effect=_mock_async_session(handler),
    )

    hook = HuggingFaceHook(None)
    commit = await hook.afind_commit_by_marker(
        "org/data",
        "airflow-artifact-sha256:abc:target:def",
        revision="refs/pr/7",
        include_pull_requests=True,
    )

    assert commit is None
    assert commit_paths == ["/api/datasets/org/data/commits/refs%2Fpr%2F7"]


@requires_async_session
@pytest.mark.parametrize(
    ("status", "headers", "expected"),
    [
        pytest.param(401, {}, None, id="missing-or-private-repo"),
        pytest.param(404, {"X-Error-Code": "RevisionNotFound"}, None, id="missing-revision"),
    ],
)
@pytest.mark.asyncio
async def test_afind_commit_by_marker_maps_hub_errors_from_http(status, headers, expected, mocker):
    http = _async_http()

    def handler(request):
        return http.Response(status, headers=headers, json={"error": "missing"})

    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_session",
        side_effect=_mock_async_session(handler),
    )

    hook = HuggingFaceHook(None)
    assert await hook.afind_commit_by_marker("org/data", "marker", revision="missing") is expected


@requires_async_session
@pytest.mark.asyncio
async def test_aget_dataset_info_surfaces_hub_http_errors(mocker):
    http = _async_http()

    def handler(request):
        return http.Response(404, headers={"X-Error-Code": "RevisionNotFound"}, json={"error": "missing"})

    mocker.patch(
        "airflow.providers.huggingface.hooks.huggingface.get_async_session",
        side_effect=_mock_async_session(handler),
    )

    hook = HuggingFaceHook(None)
    with pytest.raises(RevisionNotFoundError):
        await hook.aget_dataset_info("org/data", revision="missing")


def test_find_commit_by_marker_honours_subclass_commit_is_complete():
    class SubclassHook(HuggingFaceHook):
        @staticmethod
        def _commit_is_complete(commit):
            return True

    marker = "airflow-artifact-sha256:abc:target:def"
    partial = git_commit_info("a" * 40, f"Publish [{marker}] (part 00001-of-00002)")
    with patch("airflow.providers.huggingface.hooks.huggingface.HfApi", autospec=True) as api_type:
        api_type.return_value.list_repo_commits.return_value = [partial]
        commit = SubclassHook(None).find_commit_by_marker("org/data", marker)

    assert commit is partial
