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
pytest.importorskip("pydantic_ai.providers.snowflake")

import importlib
import sys
import threading
from unittest import mock

import httpx2
from pydantic_ai.providers.snowflake import SnowflakeProvider

from airflow.models import Connection
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.snowflake import hooks as snowflake_hooks
from airflow.providers.snowflake.hooks.cortex_model import (
    _UNUSED_TOKEN_PLACEHOLDER,
    CORTEX_CHAT_COMPLETIONS_PATH,
    PydanticAISnowflakeHook,
    _SnowflakeCortexAuth,
)
from airflow.providers.snowflake.utils._rest_auth import SnowflakeRestTokenProvider

MODULE_PATH = "airflow.providers.snowflake.hooks.cortex_model"


class TestSnowflakeConnIdResolution:
    def test_hook_argument_wins_over_extra(self):
        hook = PydanticAISnowflakeHook(snowflake_conn_id="explicit_conn")
        assert hook._get_snowflake_conn_id({"snowflake_conn_id": "extra_conn"}) == "explicit_conn"

    def test_falls_back_to_extra(self):
        hook = PydanticAISnowflakeHook()
        assert hook._get_snowflake_conn_id({"snowflake_conn_id": "extra_conn"}) == "extra_conn"

    def test_raises_when_neither_is_set(self):
        hook = PydanticAISnowflakeHook(llm_conn_id="pydanticai_conn")
        with pytest.raises(ValueError, match="Snowflake connection"):
            hook._get_snowflake_conn_id({})


class TestGetProviderKwargs:
    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_ignores_connection_password_and_omits_api_key(self, mock_hook_cls, mock_provider_cls):
        """Regression: a leftover Password on the pydanticai_snowflake connection must not be
        forwarded as api_key -- the base hook's generic handling raises TypeError against
        SnowflakeProvider (which takes `token`, not `api_key`) and silently falls back to
        env-var auth; this hook's own kwargs must never trigger that path."""
        mock_hook_cls.return_value._get_static_conn_params = {"account": "airflow"}

        hook = PydanticAISnowflakeHook(snowflake_conn_id="snowflake_conn")
        kwargs = hook._get_provider_kwargs(
            api_key="leftover-password", base_url="https://ignored.example.com", extra={}
        )

        assert "api_key" not in kwargs
        assert kwargs["base_url"] == f"https://airflow.snowflakecomputing.com{CORTEX_CHAT_COMPLETIONS_PATH}"
        assert kwargs["token"] == _UNUSED_TOKEN_PLACEHOLDER
        mock_provider_cls.return_value.get_token.assert_not_called()
        assert isinstance(kwargs["http_client"], httpx2.AsyncClient)
        # Pin that per-request refresh is actually wired up: deleting `auth=...` from
        # `_get_provider_kwargs` would leave every assertion above green.
        client_auth = kwargs["http_client"].auth
        assert isinstance(client_auth, _SnowflakeCortexAuth)
        assert client_auth._token_provider is mock_provider_cls.return_value

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_base_url_prefers_host_extra(self, mock_hook_cls, mock_provider_cls):
        mock_hook_cls.return_value._get_static_conn_params = {
            "account": "airflow",
            "host": "custom.example.com",
        }

        hook = PydanticAISnowflakeHook(snowflake_conn_id="snowflake_conn")
        kwargs = hook._get_provider_kwargs(None, None, {})

        assert kwargs["base_url"] == f"https://custom.example.com{CORTEX_CHAT_COMPLETIONS_PATH}"

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_base_url_appends_region(self, mock_hook_cls, mock_provider_cls):
        mock_hook_cls.return_value._get_static_conn_params = {
            "account": "airflow",
            "region": "us-east-2.aws",
        }

        hook = PydanticAISnowflakeHook(snowflake_conn_id="snowflake_conn")
        kwargs = hook._get_provider_kwargs(None, None, {})

        assert (
            kwargs["base_url"]
            == f"https://airflow.us-east-2.aws.snowflakecomputing.com{CORTEX_CHAT_COMPLETIONS_PATH}"
        )

    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_base_url_rejects_account_outside_charset(self, mock_hook_cls):
        mock_hook_cls.return_value._get_static_conn_params = {"account": "acct.example.com/x"}

        hook = PydanticAISnowflakeHook(snowflake_conn_id="snowflake_conn")
        with pytest.raises(ValueError, match="Invalid Snowflake account"):
            hook._get_provider_kwargs(None, None, {})

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_failed_init_is_not_remembered_by_second_call(self, mock_hook_cls, mock_provider_cls):
        """A failed first init must not leave a half-built state that the second call silently reuses."""
        mock_hook_cls.return_value._get_static_conn_params = {"account": "acct.example.com/x"}

        hook = PydanticAISnowflakeHook(snowflake_conn_id="snowflake_conn")
        with pytest.raises(ValueError, match="Invalid Snowflake account"):
            hook._get_provider_kwargs(None, None, {})
        with pytest.raises(ValueError, match="Invalid Snowflake account"):
            hook._get_provider_kwargs(None, None, {})

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    @pytest.mark.asyncio
    async def test_placeholder_token_is_overridden_on_the_wire(self, mock_hook_cls, mock_provider_cls):
        """The placeholder handed to SnowflakeProvider must never reach Snowflake."""
        mock_hook_cls.return_value._get_static_conn_params = {"account": "airflow"}
        real_headers = {
            "Authorization": "Bearer real-token",
            "X-Snowflake-Authorization-Token-Type": "OAUTH",
        }
        mock_provider_cls.return_value.build_auth_headers.return_value = real_headers

        hook = PydanticAISnowflakeHook(snowflake_conn_id="snowflake_conn")
        kwargs = hook._get_provider_kwargs(None, None, {})

        seen_headers: list[httpx2.Headers] = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen_headers.append(request.headers)
            return httpx2.Response(200, json={})

        # Reuse the hook's real auth object but swap in a MockTransport; only public attributes.
        client = httpx2.AsyncClient(auth=kwargs["http_client"].auth, transport=httpx2.MockTransport(handler))
        provider = SnowflakeProvider(**{**kwargs, "http_client": client})
        await provider.client.post("/chat/completions", body={"model": "m", "messages": []}, cast_to=object)

        assert len(seen_headers) == 1
        assert seen_headers[0]["Authorization"] == real_headers["Authorization"]
        assert "X-Snowflake-Authorization-Token-Type" in seen_headers[0]

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_snowflake_hook_and_provider_built_once(self, mock_hook_cls, mock_provider_cls):
        """A second call must reuse the same SnowflakeHook/token provider, not rebuild them."""
        mock_hook_cls.return_value._get_static_conn_params = {"account": "airflow"}

        hook = PydanticAISnowflakeHook(snowflake_conn_id="snowflake_conn")
        hook._get_provider_kwargs(None, None, {})
        hook._get_provider_kwargs(None, None, {})

        mock_hook_cls.assert_called_once_with(snowflake_conn_id="snowflake_conn")
        mock_provider_cls.assert_called_once()


class TestBareModelNameQualification:
    def test_bare_model_name_gets_snowflake_prefix(self):
        hook = PydanticAISnowflakeHook(model_id="claude-4-sonnet")
        assert hook._qualify_model_name("claude-4-sonnet") == "snowflake:claude-4-sonnet"

    def test_already_prefixed_model_name_is_unchanged(self):
        hook = PydanticAISnowflakeHook(model_id="snowflake:claude-4-sonnet")
        assert hook._qualify_model_name("snowflake:claude-4-sonnet") == "snowflake:claude-4-sonnet"


class TestSnowflakeCortexAuth:
    def test_auth_flow_refreshes_token_per_request(self):
        """Each request must ask the token provider again, not reuse headers set on the client."""
        provider = mock.create_autospec(SnowflakeRestTokenProvider, instance=True)
        provider.build_auth_headers.side_effect = [
            {"Authorization": "Bearer token-1", "X-Snowflake-Authorization-Token-Type": "OAUTH"},
            {"Authorization": "Bearer token-2", "X-Snowflake-Authorization-Token-Type": "OAUTH"},
        ]
        seen_auth_headers = []

        def handler(request: httpx2.Request) -> httpx2.Response:
            seen_auth_headers.append(request.headers["Authorization"])
            return httpx2.Response(200, json={"ok": True})

        client = httpx2.Client(auth=_SnowflakeCortexAuth(provider), transport=httpx2.MockTransport(handler))
        client.get("https://example.com/x")
        client.get("https://example.com/x")

        assert seen_auth_headers == ["Bearer token-1", "Bearer token-2"]

    @pytest.mark.asyncio
    async def test_async_auth_flow_refreshes_off_the_event_loop(self):
        """`build_auth_headers` may block (OAuth refresh, an Azure connection lookup); it must
        run in a worker thread, not inline on the event loop -- calling `BaseHook.get_connection`
        synchronously from the loop thread while an async send is in flight raises
        `DeadlockImminentError` (task-sdk `execution_time/comms.py`)."""
        loop_thread_ident = threading.get_ident()
        seen_idents: list[int] = []
        tokens = iter(["token-1", "token-2"])

        def build_auth_headers() -> dict[str, str]:
            seen_idents.append(threading.get_ident())
            return {
                "Authorization": f"Bearer {next(tokens)}",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
            }

        provider = mock.create_autospec(SnowflakeRestTokenProvider, instance=True)
        provider.build_auth_headers.side_effect = build_auth_headers

        seen_auth_headers = []

        async def handler(request: httpx2.Request) -> httpx2.Response:
            seen_auth_headers.append(request.headers["Authorization"])
            return httpx2.Response(200, json={"ok": True})

        async with httpx2.AsyncClient(
            auth=_SnowflakeCortexAuth(provider), transport=httpx2.MockTransport(handler)
        ) as client:
            await client.get("https://example.com/x")
            await client.get("https://example.com/x")

        assert seen_auth_headers == ["Bearer token-1", "Bearer token-2"]
        assert seen_idents, "build_auth_headers was never called"
        assert all(ident != loop_thread_ident for ident in seen_idents)


class TestGetConn:
    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_get_conn_uses_explicit_credentials_despite_connection_password(
        self, mock_hook_cls, mock_provider_cls
    ):
        """End-to-end: get_conn() must not raise when the connection also carries a password.

        The base hook raises ``ValueError`` (it never falls back to env-var auth) when the
        provider class rejects the kwargs it derives from the connection, so a model that
        resolves means the explicit-credentials path was taken.
        """
        mock_hook_cls.return_value._get_static_conn_params = {"account": "airflow"}

        connection_kwargs = {
            "conn_type": "pydanticai_snowflake",
            "password": "leftover-password",
            "extra": {"model": "snowflake:claude-4-sonnet", "snowflake_conn_id": "snowflake_conn"},
        }
        with mock.patch.dict(
            "os.environ",
            AIRFLOW_CONN_TEST_CONN=Connection(**connection_kwargs).get_uri(),
        ):
            hook = PydanticAISnowflakeHook(llm_conn_id="test_conn")
            model = hook.get_conn()

        assert model is not None
        mock_hook_cls.assert_called_once_with(snowflake_conn_id="snowflake_conn")

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_fallback_chain_dispatches_to_snowflake_hook(self, mock_hook_cls, mock_provider_cls):
        """A generic pydanticai primary can fail over to a pydanticai_snowflake connection."""
        mock_hook_cls.return_value._get_static_conn_params = {"account": "airflow"}

        primary_kwargs = {
            "conn_type": "pydanticai",
            "password": "sk-test",
            "extra": {"model": "openai:gpt-5", "fallback_conn_ids": ["fallback_conn"]},
        }
        fallback_kwargs = {
            "conn_type": "pydanticai_snowflake",
            "extra": {"model": "snowflake:claude-4-sonnet", "snowflake_conn_id": "snowflake_conn"},
        }
        with mock.patch.dict(
            "os.environ",
            AIRFLOW_CONN_PRIMARY_CONN=Connection(**primary_kwargs).get_uri(),
            AIRFLOW_CONN_FALLBACK_CONN=Connection(**fallback_kwargs).get_uri(),
        ):
            hook = PydanticAIHook(llm_conn_id="primary_conn")
            model = hook.get_conn()

        assert type(model).__name__ == "FallbackModel"
        mock_hook_cls.assert_called_once_with(snowflake_conn_id="snowflake_conn")

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_fallback_token_endpoint_failure_does_not_break_construction(
        self, mock_hook_cls, mock_provider_cls
    ):
        """A fallback's broken token endpoint must not fail the primary's get_conn()."""
        mock_hook_cls.return_value._get_static_conn_params = {"account": "airflow"}
        mock_provider_cls.return_value.get_token.side_effect = RuntimeError("token endpoint down")

        primary_kwargs = {
            "conn_type": "pydanticai",
            "password": "sk-test",
            "extra": {"model": "openai:gpt-5", "fallback_conn_ids": ["fallback_conn"]},
        }
        fallback_kwargs = {
            "conn_type": "pydanticai_snowflake",
            "extra": {"model": "snowflake:claude-4-sonnet", "snowflake_conn_id": "snowflake_conn"},
        }
        with mock.patch.dict(
            "os.environ",
            AIRFLOW_CONN_PRIMARY_CONN=Connection(**primary_kwargs).get_uri(),
            AIRFLOW_CONN_FALLBACK_CONN=Connection(**fallback_kwargs).get_uri(),
        ):
            model = PydanticAIHook(llm_conn_id="primary_conn").get_conn()

        assert type(model).__name__ == "FallbackModel"


class TestTestConnection:
    CONNECTION_EXTRA = {"model": "snowflake:claude-4-sonnet", "snowflake_conn_id": "snowflake_conn"}

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_success_fetches_token_once(self, mock_hook_cls, mock_provider_cls):
        mock_hook_cls.return_value._get_static_conn_params = {"account": "airflow"}
        connection = Connection(conn_type="pydanticai_snowflake", extra=self.CONNECTION_EXTRA)
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=connection.get_uri()):
            ok, message = PydanticAISnowflakeHook(llm_conn_id="test_conn").test_connection()

        assert ok is True
        assert message
        mock_provider_cls.return_value.get_token.assert_called_once_with()

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_token_failure_is_reported(self, mock_hook_cls, mock_provider_cls):
        mock_hook_cls.return_value._get_static_conn_params = {"account": "airflow"}
        mock_provider_cls.return_value.get_token.side_effect = RuntimeError("boom")
        connection = Connection(conn_type="pydanticai_snowflake", extra=self.CONNECTION_EXTRA)
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=connection.get_uri()):
            ok, message = PydanticAISnowflakeHook(llm_conn_id="test_conn").test_connection()

        assert ok is False
        assert "boom" in message

    @mock.patch(f"{MODULE_PATH}.SnowflakeRestTokenProvider", autospec=True)
    @mock.patch(f"{MODULE_PATH}.SnowflakeHook", autospec=True)
    def test_resolve_failure_skips_token_fetch(self, mock_hook_cls, mock_provider_cls):
        connection = Connection(
            conn_type="pydanticai_snowflake", extra={"model": "snowflake:claude-4-sonnet"}
        )
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TEST_CONN=connection.get_uri()):
            ok, message = PydanticAISnowflakeHook(llm_conn_id="test_conn").test_connection()

        assert ok is False
        assert "Snowflake connection" in message
        mock_provider_cls.return_value.get_token.assert_not_called()


class TestCommonAiVersionGuard:
    def test_import_succeeds_with_supported_pydantic_ai_hook(self):
        """Positive control: re-importing without the stub must not raise."""
        # patch.dict restores sys.modules only; patch.object restores the parent package attribute.
        with mock.patch.dict(sys.modules), mock.patch.object(snowflake_hooks, "cortex_model"):
            sys.modules.pop(MODULE_PATH, None)
            importlib.import_module(MODULE_PATH)

    def test_import_rejects_pydantic_ai_hook_without_fallback_conn_ids(self):
        class _OldPydanticAIHook:
            # Stand-in for common-ai 0.9.0, whose __init__ has no fallback_conn_ids.
            def __init__(self, llm_conn_id=None, model_id=None, **kwargs):
                pass

        with (
            mock.patch.dict(sys.modules),
            mock.patch.object(snowflake_hooks, "cortex_model"),
            mock.patch("airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook", _OldPydanticAIHook),
        ):
            sys.modules.pop(MODULE_PATH, None)
            with pytest.raises(AirflowOptionalProviderFeatureException, match="0.10.0"):
                importlib.import_module(MODULE_PATH)
