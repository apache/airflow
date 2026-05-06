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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.informatica.hooks.idmc import (
    IDMCAuthVersion,
    IDMCRunStatus,
    InformaticaIDMCError,
    InformaticaIDMCHook,
    _IDMCSession,
    _normalise_status,
)


def _connection(extras: dict | None = None) -> MagicMock:
    conn = MagicMock()
    conn.host = "dm-us.informaticacloud.com"
    conn.login = "user"
    conn.password = "pw"
    conn.extra_dejson = extras or {}
    return conn


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


def test_idmc_run_status_is_terminal_and_is_successful():
    assert IDMCRunStatus.is_terminal(IDMCRunStatus.SUCCESS.value) is True
    assert IDMCRunStatus.is_terminal(IDMCRunStatus.RUNNING.value) is False
    assert IDMCRunStatus.is_successful(IDMCRunStatus.SUCCESS.value) is True
    assert IDMCRunStatus.is_successful(IDMCRunStatus.WARNING.value) is True
    assert IDMCRunStatus.is_successful(IDMCRunStatus.FAILED.value) is False


def test_normalise_task_type_accepts_aliases_and_short_codes():
    assert InformaticaIDMCHook._normalise_task_type("MAPPING_TASK") == "MTT"
    assert InformaticaIDMCHook._normalise_task_type("Mapping Task") == "MTT"
    assert InformaticaIDMCHook._normalise_task_type("PCS") == "PCS"
    assert InformaticaIDMCHook._normalise_task_type("taskflow") == "TASKFLOW"


def test_normalise_task_type_rejects_unknown():
    with pytest.raises(InformaticaIDMCError, match="Unsupported IDMC task type"):
        InformaticaIDMCHook._normalise_task_type("UNKNOWN_TASK")


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


def test_v3_select_product_falls_back_to_first_when_named_missing():
    payload = {"products": [{"name": "Other", "baseApiUrl": "https://other"}]}
    assert InformaticaIDMCHook._select_v3_product(payload, "Integration Cloud") == "https://other"


def test_v3_select_product_errors_when_no_products():
    with pytest.raises(InformaticaIDMCError, match="did not include a product"):
        InformaticaIDMCHook._select_v3_product({"products": []}, "Integration Cloud")


@patch("airflow.providers.informatica.hooks.idmc.requests")
def test_login_v2_caches_session(mock_requests, hook):
    response = MagicMock()
    response.ok = True
    response.json.return_value = {
        "icSessionId": "sid-123",
        "serverUrl": "https://server.example.com/saas/",
    }
    mock_requests.post.return_value = response
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        session = hook.session()
    assert session.session_id == "sid-123"
    assert session.base_api_url == "https://server.example.com/saas"
    assert session.session_header_name == "icSessionId"
    # cached on a second call
    with patch.object(InformaticaIDMCHook, "get_connection") as mock_get:
        again = hook.session()
        assert again is session
        mock_get.assert_not_called()


@patch("airflow.providers.informatica.hooks.idmc.requests")
def test_login_v3_uses_jwt_header(mock_requests):
    h = InformaticaIDMCHook(informatica_idmc_conn_id="idmc_test", auth_version="v3")
    response = MagicMock()
    response.ok = True
    response.json.return_value = {
        "userInfo": {"sessionId": "jwt-456"},
        "products": [{"name": "Integration Cloud", "baseApiUrl": "https://di.example.com/"}],
    }
    mock_requests.post.return_value = response
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        session = h.session()
    assert session.session_id == "jwt-456"
    assert session.base_api_url == "https://di.example.com"
    assert session.session_header_name == "INFA-SESSION-ID"


@patch("airflow.providers.informatica.hooks.idmc.requests")
def test_login_v2_raises_when_response_missing_session(mock_requests, hook):
    response = MagicMock()
    response.ok = True
    response.json.return_value = {}
    mock_requests.post.return_value = response
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        with pytest.raises(InformaticaIDMCError, match="missing icSessionId"):
            hook.session()


@patch("airflow.providers.informatica.hooks.idmc.requests")
def test_login_v2_raises_on_http_error(mock_requests, hook):
    response = MagicMock()
    response.ok = False
    response.status_code = 401
    response.text = "bad creds"
    response.reason = "Unauthorized"
    mock_requests.post.return_value = response
    with patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()):
        with pytest.raises(InformaticaIDMCError, match="returned 401"):
            hook.session()


def test_request_includes_session_header(hook):
    cached = _IDMCSession(session_id="sid", base_api_url="https://server", session_header_name="icSessionId")
    hook._session = cached

    captured: dict = {}

    def fake_request(method, url, headers, json, params, timeout, verify):
        captured.update(
            method=method, url=url, headers=headers, json=json, params=params, timeout=timeout, verify=verify
        )
        response = MagicMock()
        response.ok = True
        response.content = b'{"runId": "999"}'
        response.json.return_value = {"runId": "999"}
        return response

    with (
        patch("airflow.providers.informatica.hooks.idmc.requests") as mock_requests,
        patch.object(InformaticaIDMCHook, "get_connection", return_value=_connection()),
    ):
        mock_requests.request.side_effect = fake_request
        result = hook._request(
            "POST", "/api/v2/job", json_body={"taskId": "abc", "@type": "job", "taskType": "MTT"}
        )

    assert result == {"runId": "999"}
    assert captured["url"] == "https://server/api/v2/job"
    assert captured["headers"]["icSessionId"] == "sid"
    assert captured["headers"]["Accept"] == "application/json"
    assert captured["json"] == {"taskId": "abc", "@type": "job", "taskType": "MTT"}


def test_request_raises_on_http_error(hook):
    hook._session = _IDMCSession(
        session_id="sid", base_api_url="https://server", session_header_name="icSessionId"
    )
    response = MagicMock()
    response.ok = False
    response.status_code = 500
    response.text = "boom"
    response.reason = "Server Error"
    with (
        patch("airflow.providers.informatica.hooks.idmc.requests") as mock_requests,
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
    mock_request.return_value = {"runId": "123", "taskName": "demo"}
    result = hook.start_task(task_id="task-1", task_type="Mapping Task", callback_url="https://cb")
    args, kwargs = mock_request.call_args
    assert args == ("POST", "/api/v2/job")
    body = kwargs["json_body"]
    assert body["taskId"] == "task-1"
    assert body["taskType"] == "MTT"
    assert body["callbackURL"] == "https://cb"
    assert result["run_id"] == "123"
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
    assert kwargs["json_body"] == {"inputs": {"k": "v"}}
    assert result["run_id"] == "tf-1"


def test_start_taskflow_requires_name(hook):
    with pytest.raises(InformaticaIDMCError, match="non-empty"):
        hook.start_taskflow("")


@patch.object(InformaticaIDMCHook, "_request")
def test_get_task_run_status_handles_list_response(mock_request, hook):
    mock_request.return_value = [{"runStatus": "SUCCESS"}]
    info = hook.get_task_run_status("42")
    assert info["status"] == IDMCRunStatus.SUCCESS.value
    assert info["raw_status"] == "SUCCESS"


@patch.object(InformaticaIDMCHook, "_request")
def test_get_task_run_status_returns_running_when_no_entries(mock_request, hook):
    mock_request.return_value = []
    info = hook.get_task_run_status("42")
    assert info["status"] == IDMCRunStatus.RUNNING.value


@patch.object(InformaticaIDMCHook, "_request")
def test_get_taskflow_run_status_normalises_status(mock_request, hook):
    mock_request.return_value = {"status": "Failed"}
    info = hook.get_taskflow_run_status("99")
    assert info["status"] == IDMCRunStatus.FAILED.value


@patch.object(InformaticaIDMCHook, "_request")
def test_cancel_task_posts_stop_payload(mock_request, hook):
    mock_request.return_value = {}
    hook.cancel_task("42")
    args, kwargs = mock_request.call_args
    assert args == ("POST", "/api/v2/job/stop")
    assert kwargs["json_body"]["runId"] == "42"


def test_get_ui_field_behaviour_relabels_login_and_password():
    behaviour = InformaticaIDMCHook.get_ui_field_behaviour()
    assert behaviour["relabeling"]["login"] == "Username"
    assert "schema" in behaviour["hidden_fields"]


@pytest.mark.asyncio
async def test_aget_task_run_status_uses_async_request(hook):
    cached = _IDMCSession(session_id="sid", base_api_url="https://srv", session_header_name="icSessionId")
    hook._session = cached

    async def fake_async_request(method, endpoint, params=None):
        assert method == "GET"
        assert endpoint == "/api/v2/activity/activityLog"
        assert params == {"runId": "1"}
        return [{"runStatus": "SUCCESS"}]

    hook._async_request = fake_async_request  # type: ignore[assignment]
    info = await hook.aget_task_run_status("1")
    assert info["status"] == IDMCRunStatus.SUCCESS.value


@pytest.mark.asyncio
async def test_aget_taskflow_run_status_uses_async_request(hook):
    hook._session = _IDMCSession(
        session_id="sid", base_api_url="https://srv", session_header_name="INFA-SESSION-ID"
    )

    async def fake_async_request(method, endpoint, params=None):
        assert endpoint == "/active-bpel/services/tf/status/77"
        return {"status": "WARNING"}

    hook._async_request = fake_async_request  # type: ignore[assignment]
    info = await hook.aget_taskflow_run_status("77")
    assert info["status"] == IDMCRunStatus.WARNING.value
