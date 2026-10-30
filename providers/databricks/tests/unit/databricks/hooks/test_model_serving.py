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

import pytest

pytest.importorskip("airflow.providers.common.ai.hooks.pydantic_ai")
pytest.importorskip("pydantic_ai.providers.openai")

import importlib
import json
import sys
import threading
from unittest import mock

import httpx2
from pydantic_ai import Agent
from pydantic_ai.models.openai import OpenAIChatModel

from airflow.models import Connection
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.databricks import hooks as databricks_hooks
from airflow.providers.databricks.get_provider_info import get_provider_info
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook
from airflow.providers.databricks.hooks.model_serving import (
    _UNUSED_TOKEN_PLACEHOLDER,
    PydanticAIDatabricksHook,
    _DatabricksServingAuth,
)

MODULE_PATH = "airflow.providers.databricks.hooks.model_serving"
SERVING_URL = "https://adb-1.azuredatabricks.net/serving-endpoints"
CONNECTION_EXTRA = {"model": "databricks-claude-sonnet-4-5", "databricks_conn_id": "databricks_conn"}
DATABRICKS_CONN_URI = Connection(conn_type="pydanticai_databricks", extra=CONNECTION_EXTRA).get_uri()
LEFTOVER_PASSWORD_CONN_URI = Connection(
    conn_type="pydanticai_databricks", password="leftover-password", extra=CONNECTION_EXTRA
).get_uri()
PRIMARY_CONN_URI = Connection(
    conn_type="pydanticai",
    password="sk-test",
    extra={"model": "openai:gpt-5", "fallback_conn_ids": ["fallback_conn"]},
).get_uri()


def _mock_databricks_hook(mock_hook_cls):
    mock_hook = mock_hook_cls.return_value
    mock_hook._endpoint_url.return_value = SERVING_URL
    mock_hook._get_token.return_value = "real-token"
    return mock_hook


class TestDatabricksConnIdResolution:
    def test_hook_argument_wins_over_extra(self):
        hook = PydanticAIDatabricksHook(databricks_conn_id="explicit_conn")
        assert hook._get_databricks_conn_id({"databricks_conn_id": "extra_conn"}) == "explicit_conn"

    def test_falls_back_to_extra(self):
        hook = PydanticAIDatabricksHook()
        assert hook._get_databricks_conn_id({"databricks_conn_id": "extra_conn"}) == "extra_conn"

    def test_raises_when_neither_is_set(self):
        hook = PydanticAIDatabricksHook(llm_conn_id="pydanticai_conn")
        with pytest.raises(ValueError, match="Databricks connection"):
            hook._get_databricks_conn_id({})


class TestGetProviderKwargs:
    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    def test_ignores_connection_password_and_host(self, mock_hook_cls):
        """Leftover Password/Host on the pydanticai_databricks connection must not reach the provider."""
        _mock_databricks_hook(mock_hook_cls)

        hook = PydanticAIDatabricksHook(databricks_conn_id="databricks_conn")
        kwargs = hook._get_provider_kwargs(
            api_key="leftover-password", base_url="https://ignored.example.com", extra={}
        )

        assert kwargs["base_url"] == SERVING_URL
        mock_hook_cls.return_value._endpoint_url.assert_called_once_with("serving-endpoints")
        assert kwargs["api_key"] == _UNUSED_TOKEN_PLACEHOLDER
        mock_hook_cls.return_value._get_token.assert_not_called()
        assert isinstance(kwargs["http_client"], httpx2.AsyncClient)
        # Pin that per-request refresh is actually wired up: deleting `auth=...` from
        # `_get_databricks_hook` would leave every assertion above green.
        client_auth = kwargs["http_client"].auth
        assert isinstance(client_auth, _DatabricksServingAuth)
        assert client_auth._databricks_hook is mock_hook_cls.return_value

    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    def test_databricks_hook_built_once(self, mock_hook_cls):
        """A second call must reuse the same Databricks hook, not rebuild it."""
        _mock_databricks_hook(mock_hook_cls)

        hook = PydanticAIDatabricksHook(databricks_conn_id="databricks_conn")
        first = hook._get_provider_kwargs(None, None, {})
        second = hook._get_provider_kwargs(None, None, {})

        mock_hook_cls.assert_called_once_with(
            databricks_conn_id="databricks_conn", caller="PydanticAIDatabricksHook"
        )
        assert first["http_client"] is second["http_client"]

    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    def test_failed_first_build_is_not_remembered(self, mock_hook_cls):
        """A failed first build must not leave half-built state that the second call silently reuses."""
        _mock_databricks_hook(mock_hook_cls)
        mock_hook_cls.return_value._endpoint_url.side_effect = [RuntimeError("no host"), SERVING_URL]

        hook = PydanticAIDatabricksHook(databricks_conn_id="databricks_conn")
        with pytest.raises(RuntimeError, match="no host"):
            hook._get_provider_kwargs(None, None, {})
        kwargs = hook._get_provider_kwargs(None, None, {})

        assert kwargs["base_url"] == SERVING_URL
        assert isinstance(kwargs["http_client"], httpx2.AsyncClient)


class TestBareModelNameQualification:
    def test_bare_model_name_gets_openai_chat_prefix(self):
        hook = PydanticAIDatabricksHook(model_id="databricks-claude-sonnet-4-5")
        assert (
            hook._qualify_model_name("databricks-claude-sonnet-4-5")
            == "openai-chat:databricks-claude-sonnet-4-5"
        )

    def test_already_prefixed_model_name_is_unchanged(self):
        hook = PydanticAIDatabricksHook(model_id="openai:my-endpoint")
        assert hook._qualify_model_name("openai:my-endpoint") == "openai:my-endpoint"


class TestDatabricksServingAuth:
    def test_auth_flow_refreshes_token_per_request(self):
        """Each request must ask the Databricks hook again, not reuse headers set on the client."""
        databricks_hook = mock.create_autospec(BaseDatabricksHook, instance=True)
        databricks_hook._get_token.side_effect = ["token-1", "token-2"]
        seen_auth_headers = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen_auth_headers.append(request.headers["Authorization"])
            return httpx2.Response(200, json={"ok": True})

        client = httpx2.Client(
            auth=_DatabricksServingAuth(databricks_hook), transport=httpx2.MockTransport(handler)
        )
        client.get("https://example.com/x")
        client.get("https://example.com/x")

        assert seen_auth_headers == ["Bearer token-1", "Bearer token-2"]
        databricks_hook._get_token.assert_called_with(raise_error=True)

    @pytest.mark.asyncio
    async def test_async_auth_flow_refreshes_off_the_event_loop(self):
        """`_get_token` may block (OAuth refresh, a connection lookup); it must run in a worker
        thread, not inline on the event loop -- calling `BaseHook.get_connection` synchronously
        from the loop thread while an async send is in flight raises `DeadlockImminentError`."""
        loop_thread_ident = threading.get_ident()
        seen_threads: list[threading.Thread] = []
        tokens = iter(["token-1", "token-2"])

        def get_token(raise_error: bool = False) -> str:
            seen_threads.append(threading.current_thread())
            return next(tokens)

        databricks_hook = mock.create_autospec(BaseDatabricksHook, instance=True)
        databricks_hook._get_token.side_effect = get_token
        seen_auth_headers = []

        async def handler(request: httpx2.Request) -> httpx2.Response:
            seen_auth_headers.append(request.headers["Authorization"])
            return httpx2.Response(200, json={"ok": True})

        async with httpx2.AsyncClient(
            auth=_DatabricksServingAuth(databricks_hook), transport=httpx2.MockTransport(handler)
        ) as client:
            await client.get("https://example.com/x")
            await client.get("https://example.com/x")

        assert seen_auth_headers == ["Bearer token-1", "Bearer token-2"]
        assert seen_threads, "_get_token was never called"
        assert all(thread.ident != loop_thread_ident for thread in seen_threads)
        # The dedicated executor, not asyncio's default one: blocking lookups there can starve
        # `asend()`'s reply read.
        assert all(thread.name.startswith("databricks-serving-auth") for thread in seen_threads)


class TestGetConn:
    @mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=LEFTOVER_PASSWORD_CONN_URI)
    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    def test_get_conn_returns_chat_model_for_endpoint(self, mock_hook_cls):
        _mock_databricks_hook(mock_hook_cls)
        model = PydanticAIDatabricksHook(llm_conn_id="test_conn").get_conn()

        assert isinstance(model, OpenAIChatModel)
        assert model.model_name == "databricks-claude-sonnet-4-5"
        mock_hook_cls.assert_called_once_with(
            databricks_conn_id="databricks_conn", caller="PydanticAIDatabricksHook"
        )

    @mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=DATABRICKS_CONN_URI)
    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    @pytest.mark.asyncio
    async def test_request_goes_to_serving_endpoints_with_databricks_token(self, mock_hook_cls):
        """Drive a real request through the model: URL, bearer token and endpoint name on the wire."""
        mock_hook = _mock_databricks_hook(mock_hook_cls)
        mock_hook._get_token.return_value = "real-token"
        seen: list[httpx2.Request] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen.append(request)
            return httpx2.Response(
                200,
                json={
                    "id": "1",
                    "object": "chat.completion",
                    "created": 0,
                    "model": "databricks-claude-sonnet-4-5",
                    "choices": [
                        {
                            "index": 0,
                            "finish_reason": "stop",
                            "message": {"role": "assistant", "content": "hi"},
                        }
                    ],
                },
            )

        hook = PydanticAIDatabricksHook(llm_conn_id="test_conn")
        kwargs = hook._get_provider_kwargs(None, None, hook._get_conn_and_extra()[1])
        # Reuse the hook's real auth object but swap in a MockTransport. `_http_client` is private;
        # there is no public seam for injecting a transport.
        client = httpx2.AsyncClient(auth=kwargs["http_client"].auth, transport=httpx2.MockTransport(handler))
        with mock.patch.object(hook, "_http_client", client):
            result = await Agent(hook.get_conn()).run("hello")

        assert result.output == "hi"
        assert len(seen) == 1
        assert str(seen[0].url) == f"{SERVING_URL}/chat/completions"
        assert seen[0].headers["Authorization"] == "Bearer real-token"
        assert _UNUSED_TOKEN_PLACEHOLDER not in seen[0].headers["Authorization"]
        assert json.loads(seen[0].content)["model"] == "databricks-claude-sonnet-4-5"

    @mock.patch.dict(
        "os.environ",
        AIRFLOW_CONN_PRIMARY_CONN=PRIMARY_CONN_URI,
        AIRFLOW_CONN_FALLBACK_CONN=DATABRICKS_CONN_URI,
    )
    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    def test_fallback_chain_dispatches_to_databricks_hook(self, mock_hook_cls):
        """A generic pydanticai primary can fail over to a pydanticai_databricks connection."""
        _mock_databricks_hook(mock_hook_cls)
        model = PydanticAIHook(llm_conn_id="primary_conn").get_conn()

        assert type(model).__name__ == "FallbackModel"
        mock_hook_cls.assert_called_once_with(
            databricks_conn_id="databricks_conn", caller="PydanticAIDatabricksHook"
        )


class TestTestConnection:
    @mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=DATABRICKS_CONN_URI)
    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    def test_success_fetches_token_once(self, mock_hook_cls):
        _mock_databricks_hook(mock_hook_cls)
        ok, message = PydanticAIDatabricksHook(llm_conn_id="test_conn").test_connection()

        assert ok is True
        assert message
        mock_hook_cls.return_value._get_token.assert_called_once_with(raise_error=True)

    @mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=DATABRICKS_CONN_URI)
    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    def test_token_failure_is_reported(self, mock_hook_cls):
        _mock_databricks_hook(mock_hook_cls)
        mock_hook_cls.return_value._get_token.side_effect = RuntimeError("boom")
        ok, message = PydanticAIDatabricksHook(llm_conn_id="test_conn").test_connection()

        assert ok is False
        assert "boom" in message

    @pytest.mark.parametrize(
        "extra",
        [
            pytest.param({"model": "databricks-claude-sonnet-4-5"}, id="no-databricks-conn-id"),
            pytest.param({"databricks_conn_id": "databricks_conn"}, id="no-model"),
        ],
    )
    @mock.patch(f"{MODULE_PATH}.BaseDatabricksHook", autospec=True)
    def test_unresolvable_model_is_reported_without_token_fetch(self, mock_hook_cls, extra):
        _mock_databricks_hook(mock_hook_cls)
        connection = Connection(conn_type="pydanticai_databricks", extra=extra)
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=connection.get_uri()):
            ok, message = PydanticAIDatabricksHook(llm_conn_id="test_conn").test_connection()

        assert ok is False
        assert message
        mock_hook_cls.return_value._get_token.assert_not_called()


class TestUiFieldBehaviour:
    def test_matches_provider_yaml(self):
        entry = next(
            c
            for c in get_provider_info()["connection-types"]
            if c["connection-type"] == "pydanticai_databricks"
        )
        behaviour = PydanticAIDatabricksHook.get_ui_field_behaviour()

        assert entry["ui-field-behaviour"]["hidden-fields"] == behaviour["hidden_fields"]
        assert entry["ui-field-behaviour"]["placeholders"] == behaviour["placeholders"]
        assert entry["ui-field-behaviour"]["relabeling"] == behaviour["relabeling"]


class TestCommonAiVersionGuard:
    def test_import_succeeds_with_supported_pydantic_ai_hook(self):
        """Positive control: re-importing without the stub must not raise."""
        # patch.dict restores sys.modules only; patch.object restores the parent package attribute.
        with mock.patch.dict(sys.modules), mock.patch.object(databricks_hooks, "model_serving"):
            sys.modules.pop(MODULE_PATH, None)
            importlib.import_module(MODULE_PATH)

    def test_import_rejects_pydantic_ai_hook_without_fallback_conn_ids(self):
        class _OldPydanticAIHook:
            # Stand-in for common-ai 0.9.0, whose __init__ has no fallback_conn_ids.
            def __init__(self, llm_conn_id=None, model_id=None, **kwargs):
                pass

        with (
            mock.patch.dict(sys.modules),
            mock.patch.object(databricks_hooks, "model_serving"),
            mock.patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", _OldPydanticAIHook),
        ):
            sys.modules.pop(MODULE_PATH, None)
            with pytest.raises(AirflowOptionalProviderFeatureException, match="0.10.0"):
                importlib.import_module(MODULE_PATH)
