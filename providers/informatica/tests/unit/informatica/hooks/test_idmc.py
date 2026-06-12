#
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

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import aiohttp
import pytest
from requests import Response

from airflow.providers.informatica.hooks.idmc import (
    IDMCAuthVersion,
    IDMCRunStatus,
    InformaticaIDMCError,
    InformaticaIDMCHook,
    _IDMCSession,
    _normalise_activity_state,
    _normalise_status,
)


def _connection(extras: dict | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        host="dm-us.informaticacloud.com",
        login="user",
        password="pw",
        extra_dejson=extras or {},
    )


def _response(
    payload: object,
    *,
    ok: bool = True,
    status_code: int = 200,
    text: str = "",
    reason: str = "OK",
    content: bytes | None = b"{}",
) -> MagicMock:
    response = MagicMock(spec=Response)
    response.ok = ok
    response.status_code = status_code
    response.text = text
    response.reason = reason
    response.content = content
    response.json.return_value = payload
    return response


@pytest.fixture
def hook():
    return InformaticaIDMCHook(informatica_idmc_conn_id="idmc_test")


def test_default_auth_version_is_v2(hook):
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        assert hook.auth_version is IDMCAuthVersion.V2


def test_explicit_auth_version_overrides_extras():
    h = InformaticaIDMCHook(informatica_idmc_conn_id="x", auth_version="v3")
    with patch.object(
        InformaticaIDMCHook, "get_connection", return_value=_connection({"auth_version": "v2"})
    ):
        assert h.auth_version is IDMCAuthVersion.V3


def test_extras_select_v3_auth_version(hook):
    with patch.object(
        InformaticaIDMCHook, "get_connection", return_value=_connection({"auth_version": "v3"})
    ):
        assert hook.auth_version is IDMCAuthVersion.V3


def test_login_base_url_uses_https_by_default():
    conn = _connection()
    assert InformaticaIDMCHook._login_base_url(conn) == "https://dm-us.informaticacloud.com"


def test_login_base_url_passes_full_url_through():
    conn = _connection()
    conn.host = "https://custom.example.com/"
    assert InformaticaIDMCHook._login_base_url(conn) == "https://custom.example.com"


def test_root_url_from_base_removes_saas_path():
    assert InformaticaIDMCHook._root_url_from_base("https://server.example.com/saas/") == (
        "https://server.example.com"
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("SUCCESS", IDMCRunStatus.SUCCESS.value),
        ("Completed", IDMCRunStatus.SUCCESS.value),
        ("warning", IDMCRunStatus.WARNING.value),
        ("FAILED", IDMCRunStatus.FAILED.value),
        ("FAILURE", IDMCRunStatus.FAILED.value),
        ("STOPPED", IDMCRunStatus.CANCELLED.value),
        ("QUEUED", IDMCRunStatus.QUEUED.value),
        ("RUNNING", IDMCRunStatus.RUNNING.value),
        ("something else", IDMCRunStatus.RUNNING.value),
        (None, IDMCRunStatus.RUNNING.value),
    ],
)
def test_normalise_status(raw, expected):
    assert _normalise_status(raw) == expected


@pytest.mark.parametrize(
    ("raw", "is_stopped", "expected"),
    [
        (1, False, IDMCRunStatus.SUCCESS.value),
        ("2", False, IDMCRunStatus.WARNING.value),
        (3, False, IDMCRunStatus.FAILED.value),
        (4, False, IDMCRunStatus.QUEUED.value),
        (3, True, IDMCRunStatus.CANCELLED.value),
        ("Running", False, IDMCRunStatus.RUNNING.value),
    ],
)
def test_normalise_activity_state(raw, is_stopped, expected):
    assert _normalise_activity_state(raw, is_stopped=is_stopped) == expected


def test_idmc_run_status_is_terminal_and_is_successful():
    assert IDMCRunStatus.is_terminal(IDMCRunStatus.SUCCESS.value) is True
    assert IDMCRunStatus.is_terminal(IDMCRunStatus.RUNNING.value) is False
    assert IDMCRunStatus.is_successful(IDMCRunStatus.SUCCESS.value) is True
    assert IDMCRunStatus.is_successful(IDMCRunStatus.WARNING.value) is True
    assert IDMCRunStatus.is_successful(IDMCRunStatus.FAILED.value) is False


def test_normalise_task_type_accepts_aliases_and_short_codes():
    assert InformaticaIDMCHook._normalise_task_type("MAPPING_TASK") == "MTT"
    assert InformaticaIDMCHook._normalise_task_type("Mapping Task") == "MTT"
    assert InformaticaIDMCHook._normalise_task_type("Replication Task") == "DRS"
    assert InformaticaIDMCHook._normalise_task_type("PCS") == "PCS"
    assert InformaticaIDMCHook._normalise_task_type("taskflow") == "WORKFLOW"
    assert InformaticaIDMCHook._normalise_task_type("linear taskflow") == "WORKFLOW"


def test_normalise_task_type_rejects_unknown():
    with pytest.raises(InformaticaIDMCError, match="Unsupported IDMC task type"):
        InformaticaIDMCHook._normalise_task_type("UNKNOWN_TASK")


def test_normalise_task_type_rejects_unsupported_short_code():
    with pytest.raises(InformaticaIDMCError, match="Unsupported IDMC task type"):
        InformaticaIDMCHook._normalise_task_type("RTM")


def test_v2_login_payload_requires_credentials():
    conn = _connection()
    conn.login = None
    with pytest.raises(InformaticaIDMCError, match="v2 login requires"):
        InformaticaIDMCHook._build_v2_login_payload(conn)


def test_v2_login_payload_includes_security_domain():
    conn = _connection({"security_domain": "Native"})
    payload = InformaticaIDMCHook._build_v2_login_payload(conn)
    assert payload == {
        "@type": "login",
        "username": "user",
        "password": "pw",
        "securitydomain": "Native",
    }


def test_v3_select_product_picks_named_match():
    payload = {
        "products": [
            {"name": "Other", "baseApiUrl": "https://other"},
            {"name": "Integration Cloud", "baseApiUrl": "https://idmc-target/"},
        ]
    }
    assert InformaticaIDMCHook._select_v3_product(payload, "Integration Cloud") == "https://idmc-target"


def test_v3_select_product_errors_when_named_missing():
    payload = {"products": [{"name": "Other", "baseApiUrl": "https://other"}]}
    with pytest.raises(InformaticaIDMCError, match="Integration Cloud"):
        InformaticaIDMCHook._select_v3_product(payload, "Integration Cloud")


def test_v3_select_product_errors_when_no_products():
    with pytest.raises(InformaticaIDMCError, match="did not include a product"):
        InformaticaIDMCHook._select_v3_product({"products": []}, "Integration Cloud")


@patch("airflow.providers.informatica.hooks.idmc.requests", autospec=True)
def test_login_v2_caches_session(mock_requests, hook):
    mock_requests.post.return_value = _response(
        {
            "icSessionId": "sid-123",
            "serverUrl": "https://server.example.com/saas/",
        }
    )
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        session = hook.session()
    assert session.session_id == "sid-123"
    assert session.base_api_url == "https://server.example.com/saas"
    assert session.root_api_url == "https://server.example.com"
    assert session.session_header_name == "icSessionId"
    # cached on a second call
    with patch.object(InformaticaIDMCHook, "get_connection") as mock_get:
        again = hook.session()
        assert again is session
        mock_get.assert_not_called()


@patch("airflow.providers.informatica.hooks.idmc.requests", autospec=True)
def test_login_v3_uses_jwt_header(mock_requests):
    h = InformaticaIDMCHook(informatica_idmc_conn_id="idmc_test", auth_version="v3")
    mock_requests.post.return_value = _response(
        {
            "userInfo": {"sessionId": "jwt-456"},
            "products": [{"name": "Integration Cloud", "baseApiUrl": "https://di.example.com/"}],
        }
    )
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        session = h.session()
    assert session.session_id == "jwt-456"
    assert session.base_api_url == "https://di.example.com"
    assert session.root_api_url == "https://di.example.com"
    assert session.session_header_name == "INFA-SESSION-ID"


@patch("airflow.providers.informatica.hooks.idmc.requests", autospec=True)
def test_login_v2_raises_when_response_missing_session(mock_requests, hook):
    mock_requests.post.return_value = _response({})
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        with pytest.raises(InformaticaIDMCError, match="missing icSessionId"):
            hook.session()


@patch("airflow.providers.informatica.hooks.idmc.requests", autospec=True)
def test_login_v2_raises_on_http_error(mock_requests, hook):
    mock_requests.post.return_value = _response(
        {},
        ok=False,
        status_code=401,
        text="bad creds",
        reason="Unauthorized",
    )
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        with pytest.raises(InformaticaIDMCError, match="returned 401"):
            hook.session()


def test_request_includes_session_header(hook):
    cached = _IDMCSession(
        session_id="sid",
        base_api_url="https://server/saas",
        root_api_url="https://server",
        session_header_name="icSessionId",
    )
    hook._session = cached

    captured: dict = {}

    def fake_request(method, url, headers, json, params, timeout, verify):
        captured.update(
            method=method, url=url, headers=headers, json=json, params=params, timeout=timeout, verify=verify
        )
        return _response({"runId": "999"}, content=b'{"runId": "999"}')

    with (
        patch("airflow.providers.informatica.hooks.idmc.requests", autospec=True) as mock_requests,
        patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()),
    ):
        mock_requests.request.side_effect = fake_request
        result = hook._request(
            "POST", "/api/v2/job", json_body={"taskId": "abc", "@type": "job", "taskType": "MTT"}
        )

    assert result == {"runId": "999"}
    assert captured["url"] == "https://server/saas/api/v2/job"
    assert captured["headers"]["icSessionId"] == "sid"
    assert captured["headers"]["Accept"] == "application/json"
    assert captured["json"] == {"taskId": "abc", "@type": "job", "taskType": "MTT"}


def test_v2_endpoint_uses_v2_header_after_v3_login(hook):
    hook._session = _IDMCSession(
        session_id="sid",
        base_api_url="https://server/saas",
        root_api_url="https://server",
        session_header_name="INFA-SESSION-ID",
    )
    captured: dict = {}

    def fake_request(method, url, headers, json, params, timeout, verify):
        captured.update(url=url, headers=headers)
        return _response([], content=b"[]")

    with (
        patch("airflow.providers.informatica.hooks.idmc.requests", autospec=True) as mock_requests,
        patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()),
    ):
        mock_requests.request.side_effect = fake_request
        hook._request("GET", "/api/v2/activity/activityLog", params={"runId": "1", "taskId": "t"})

    assert captured["headers"]["icSessionId"] == "sid"
    assert "INFA-SESSION-ID" not in captured["headers"]


def test_root_endpoint_uses_root_url_and_service_header(hook):
    hook._session = _IDMCSession(
        session_id="sid",
        base_api_url="https://server/saas",
        root_api_url="https://server",
        session_header_name="INFA-SESSION-ID",
    )
    captured: dict = {}

    def fake_request(method, url, headers, json, params, timeout, verify):
        captured.update(url=url, headers=headers)
        return _response({"RunId": "tf-1"}, content=b'{"RunId": "tf-1"}')

    with (
        patch("airflow.providers.informatica.hooks.idmc.requests", autospec=True) as mock_requests,
        patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()),
    ):
        mock_requests.request.side_effect = fake_request
        hook._request("POST", "/active-bpel/rt/MyFlow", json_body={}, base_url="root")

    assert captured["url"] == "https://server/active-bpel/rt/MyFlow"
    assert captured["headers"]["IDS-SESSION-ID"] == "sid"


def test_request_raises_on_http_error(hook):
    hook._session = _IDMCSession(
        session_id="sid",
        base_api_url="https://server/saas",
        root_api_url="https://server",
        session_header_name="icSessionId",
    )
    response = _response({}, ok=False, status_code=500, text="boom", reason="Server Error")
    with (
        patch("airflow.providers.informatica.hooks.idmc.requests", autospec=True) as mock_requests,
        patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()),
    ):
        mock_requests.request.return_value = response
        with pytest.raises(InformaticaIDMCError, match="returned 500"):
            hook._request("GET", "/x")


def test_start_task_requires_id_or_federated_id(hook):
    with pytest.raises(InformaticaIDMCError, match="task_id or task_federated_id"):
        hook.start_task(task_type="MTT")


@patch.object(InformaticaIDMCHook, "_request")
def test_start_task_returns_normalised_run_id(mock_request, hook):
    mock_request.return_value = {"runId": "123", "taskId": "task-1", "taskName": "demo"}
    result = hook.start_task(task_id="task-1", task_type="Mapping Task", callback_url="https://cb")
    args, kwargs = mock_request.call_args
    assert args == ("POST", "/api/v2/job")
    body = kwargs["json_body"]
    assert body["taskId"] == "task-1"
    assert body["taskType"] == "MTT"
    assert body["callbackURL"] == "https://cb"
    assert result["run_id"] == "123"
    assert result["task_id"] == "task-1"
    assert result["task_type"] == "MTT"


@patch.object(InformaticaIDMCHook, "_request")
def test_start_task_raises_when_no_run_id(mock_request, hook):
    mock_request.return_value = {}
    with pytest.raises(InformaticaIDMCError, match="did not include a runId"):
        hook.start_task(task_id="t", task_type="MTT")


@patch.object(InformaticaIDMCHook, "_request")
def test_start_taskflow_uses_active_bpel_endpoint(mock_request, hook):
    mock_request.return_value = {"RunId": "tf-1"}
    result = hook.start_taskflow("MyFlow", input_parameters={"k": "v"})
    args, kwargs = mock_request.call_args
    assert args == ("POST", "/active-bpel/rt/MyFlow")
    assert kwargs["json_body"] == {"k": "v"}
    assert kwargs["base_url"] == "root"
    assert result["run_id"] == "tf-1"


@patch.object(InformaticaIDMCHook, "_request")
def test_start_taskflow_accepts_scalar_run_id(mock_request, hook):
    mock_request.return_value = "tf-2"
    result = hook.start_taskflow("MyFlow")
    assert result["run_id"] == "tf-2"


def test_start_taskflow_requires_name(hook):
    with pytest.raises(InformaticaIDMCError, match="non-empty"):
        hook.start_taskflow("")


@patch.object(InformaticaIDMCHook, "_request")
def test_get_task_run_status_handles_list_response(mock_request, hook):
    mock_request.return_value = [{"state": 1, "taskId": "task-1"}]
    info = hook.get_task_run_status("42", task_id="task-1")
    assert info["status"] == IDMCRunStatus.SUCCESS.value
    assert info["raw_status"] == 1
    assert mock_request.call_args.kwargs["params"] == {"runId": "42", "taskId": "task-1"}


@patch.object(InformaticaIDMCHook, "_request")
def test_get_task_run_status_returns_running_when_no_entries(mock_request, hook):
    mock_request.return_value = []
    info = hook.get_task_run_status("42", task_id="task-1")
    assert info["status"] == IDMCRunStatus.RUNNING.value


@patch.object(InformaticaIDMCHook, "_request")
def test_get_task_run_status_uses_parent_activity_log_entry_with_empty_child_entries(mock_request, hook):
    mock_request.return_value = {"@type": "activityLogEntry", "runId": 42, "state": 3, "entries": []}
    info = hook.get_task_run_status("42", task_id="task-1")
    assert info["status"] == IDMCRunStatus.FAILED.value
    assert info["raw"] == mock_request.return_value


@patch.object(InformaticaIDMCHook, "_request")
def test_get_task_run_status_prefers_parent_activity_log_entry_over_child_entries(mock_request, hook):
    mock_request.return_value = {
        "@type": "activityLogEntry",
        "runId": 42,
        "state": 1,
        "entries": [{"@type": "activityLogEntry", "runId": 0, "state": 3}],
    }
    info = hook.get_task_run_status("42", task_id="task-1")
    assert info["status"] == IDMCRunStatus.SUCCESS.value
    assert info["raw_status"] == 1


@patch.object(InformaticaIDMCHook, "_request")
def test_get_taskflow_run_status_normalises_status(mock_request, hook):
    mock_request.return_value = {"status": "Failed"}
    info = hook.get_taskflow_run_status("99")
    assert info["status"] == IDMCRunStatus.FAILED.value
    assert mock_request.call_args.kwargs["base_url"] == "root"


@patch.object(InformaticaIDMCHook, "_request")
def test_cancel_task_posts_stop_payload(mock_request, hook):
    mock_request.return_value = {}
    hook.cancel_task(task_id="task-1", task_type="Mapping Task")
    args, kwargs = mock_request.call_args
    assert args == ("POST", "/api/v2/job/stop")
    assert kwargs["json_body"] == {"@type": "job", "taskType": "MTT", "taskId": "task-1"}


@patch.object(InformaticaIDMCHook, "_request")
def test_cancel_taskflow_uses_terminate_endpoint(mock_request, hook):
    mock_request.return_value = {}
    hook.cancel_taskflow("tf-1")
    args, kwargs = mock_request.call_args
    assert args == ("PUT", "/active-bpel/services/tf/terminate")
    assert kwargs["json_body"] == {"runid": ["tf-1"]}
    assert kwargs["base_url"] == "root"


def test_get_ui_field_behaviour_relabels_login_and_password():
    behaviour = InformaticaIDMCHook.get_ui_field_behaviour()
    assert behaviour["relabeling"]["login"] == "Username"
    assert "schema" in behaviour["hidden_fields"]


@pytest.mark.asyncio
async def test_async_request_wraps_client_errors(hook):
    hook._session = _IDMCSession(
        session_id="sid",
        base_api_url="https://srv/saas",
        root_api_url="https://srv",
        session_header_name="icSessionId",
    )

    class FailingSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        def request(self, **kwargs):
            raise aiohttp.ClientError("network down")

    with (
        patch(
            "airflow.providers.informatica.hooks.idmc.aiohttp.ClientSession", return_value=FailingSession()
        ),
        patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()),
    ):
        with pytest.raises(InformaticaIDMCError, match="network down"):
            await hook._async_request("GET", "/api/v2/activity/activityLog")


@pytest.mark.asyncio
async def test_async_request_returns_raw_text_for_non_json_response(hook):
    hook._session = _IDMCSession(
        session_id="sid",
        base_api_url="https://srv/saas",
        root_api_url="https://srv",
        session_header_name="icSessionId",
    )

    class TextResponse:
        status = 200
        content_length = None

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def json(self, content_type=None):
            raise ValueError("not json")

        async def text(self):
            return "plain text"

    class TextSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        def request(self, **kwargs):
            return TextResponse()

    with (
        patch("airflow.providers.informatica.hooks.idmc.aiohttp.ClientSession", return_value=TextSession()),
        patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()),
    ):
        result = await hook._async_request("GET", "/api/v2/activity/activityLog")

    assert result == {"raw": "plain text"}


@pytest.mark.asyncio
async def test_aget_task_run_status_uses_async_request(hook):
    cached = _IDMCSession(
        session_id="sid",
        base_api_url="https://srv/saas",
        root_api_url="https://srv",
        session_header_name="icSessionId",
    )
    hook._session = cached

    async def fake_async_request(method, endpoint, params=None, **kwargs):
        assert method == "GET"
        assert endpoint == "/api/v2/activity/activityLog"
        assert params == {"runId": "1", "taskId": "task-1"}
        return [{"state": 1}]

    hook._async_request = fake_async_request  # type: ignore[assignment]
    info = await hook.aget_task_run_status("1", task_id="task-1")
    assert info["status"] == IDMCRunStatus.SUCCESS.value


@pytest.mark.asyncio
async def test_aget_task_run_status_uses_parent_activity_log_entry(hook):
    async def fake_async_request(method, endpoint, params=None, **kwargs):
        return {"@type": "activityLogEntry", "runId": 1, "state": 2, "entries": []}

    hook._async_request = fake_async_request  # type: ignore[assignment]
    info = await hook.aget_task_run_status("1", task_id="task-1")
    assert info["status"] == IDMCRunStatus.WARNING.value


@pytest.mark.asyncio
async def test_aget_taskflow_run_status_uses_async_request(hook):
    hook._session = _IDMCSession(
        session_id="sid",
        base_api_url="https://srv/saas",
        root_api_url="https://srv",
        session_header_name="INFA-SESSION-ID",
    )

    async def fake_async_request(method, endpoint, params=None, **kwargs):
        assert endpoint == "/active-bpel/services/tf/status/77"
        assert kwargs["base_url"] == "root"
        return {"status": "WARNING"}

    hook._async_request = fake_async_request  # type: ignore[assignment]
    info = await hook.aget_taskflow_run_status("77")
    assert info["status"] == IDMCRunStatus.WARNING.value
