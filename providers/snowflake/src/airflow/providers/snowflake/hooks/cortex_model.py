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
"""Expose Snowflake Cortex's OpenAI-compatible chat endpoint as a pydantic-ai model hook."""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils._rest_auth import SnowflakeRestTokenProvider, get_cortex_base_url

_COMMON_AI_REQUIREMENT_MESSAGE = (
    "This feature requires apache-airflow-providers-common-ai>=0.10.0, which adds "
    "'fallback_conn_ids' to PydanticAIHook. "
    "Install with apache-airflow-providers-snowflake[common.ai]."
)

try:
    import httpx2

    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
except ImportError:
    raise AirflowOptionalProviderFeatureException(_COMMON_AI_REQUIREMENT_MESSAGE)

if "fallback_conn_ids" not in inspect.signature(PydanticAIHook.__init__).parameters:
    raise AirflowOptionalProviderFeatureException(_COMMON_AI_REQUIREMENT_MESSAGE)

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from httpx2 import Request, Response

CORTEX_CHAT_COMPLETIONS_PATH = "/api/v2/cortex/v1"

# SnowflakeProvider rejects an empty token, but _SnowflakeCortexAuth overwrites Authorization on
# every request, so this value is never sent.
_UNUSED_TOKEN_PLACEHOLDER = "unused-overridden-per-request"


class _SnowflakeCortexAuth(httpx2.Auth):
    """
    Refresh the ``Authorization`` header on every request from a shared token provider.

    ``build_auth_headers()`` may block: it can call ``requests.post`` with retries for an
    expiring OAuth token, or -- for ``azure_conn_id`` -- resolve an Airflow connection and fetch
    an Azure token on every call. Resolving a connection synchronously from the event-loop
    thread while an async send is in flight raises ``DeadlockImminentError`` (see
    ``task-sdk`` ``execution_time/comms.py``), so ``async_auth_flow`` is overridden to run the
    refresh in a worker thread instead of the httpx2 default of driving the sync ``auth_flow``
    inline on the loop. ``auth_flow`` itself is kept for sync ``httpx2.Client`` callers, which
    have no event loop to block.
    """

    def __init__(self, token_provider: SnowflakeRestTokenProvider) -> None:
        self._token_provider = token_provider

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        request.headers.update(self._token_provider.build_auth_headers())
        yield request

    async def async_auth_flow(self, request: Request) -> AsyncGenerator[Request, Response]:
        request.headers.update(await asyncio.to_thread(self._token_provider.build_auth_headers))
        yield request


class PydanticAISnowflakeHook(PydanticAIHook):
    """
    Hook for Snowflake Cortex's OpenAI-compatible chat endpoint via pydantic-ai.

    Unlike the other ``PydanticAI*`` hooks, credentials do not live on this connection: they are
    read from an existing ``snowflake`` connection (OAuth, PAT, or key-pair JWT -- whichever that
    connection is configured for), refreshed on every request the same way as
    ``SnowflakeCortexAgentHook`` and ``SnowflakeSqlApiHook``. See
    ``SnowflakeRestTokenProvider``. The underlying ``httpx2.AsyncClient`` is built once and lives as
    long as this hook instance; nothing currently closes it (``SnowflakeProvider`` only owns and
    closes a client it built itself, not one passed in).

    Connection fields:
        - **extra** JSON: ``{"model": "snowflake:claude-4-sonnet",
          "snowflake_conn_id": "snowflake_default"}``

    Model family support (pydantic-ai-slim's ``SnowflakeProvider.model_profile``): Claude
    (``claude*``) and OpenAI (``openai-*``) models support tools and structured output;
    other families (``llama*``, ``snowflake-llama*``, ``mistral*``, ``mixtral*``,
    ``deepseek*``, and any unlisted family) do not support tools, and structured output
    falls back to prompted mode. Use a Claude or OpenAI family model for a tool-using agent.

    :param llm_conn_id: Airflow connection ID for this ``pydanticai_snowflake`` connection.
    :param model_id: Model identifier, e.g. ``"snowflake:claude-4-sonnet"``. A bare name (no
        recognized platform prefix) is qualified with ``snowflake:``.
    :param fallback_conn_ids: See :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`.
    :param snowflake_conn_id: Connection ID of an existing Snowflake connection to source
        credentials, account, and host from. Takes precedence over the connection extra's
        ``snowflake_conn_id``; one of the two is required.
    """

    conn_type = "pydanticai_snowflake"
    default_conn_name = "pydanticai_snowflake_default"
    hook_name = "Pydantic AI (Snowflake Cortex)"
    model_provider = "snowflake"

    def __init__(
        self,
        llm_conn_id: str | None = None,
        model_id: str | None = None,
        fallback_conn_ids: list[str] | None = None,
        *,
        snowflake_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(llm_conn_id, model_id, fallback_conn_ids, **kwargs)
        self.snowflake_conn_id = snowflake_conn_id
        self._token_provider: SnowflakeRestTokenProvider | None = None
        self._cortex_base_url: str | None = None
        self._http_client: httpx2.AsyncClient | None = None

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login", "host", "password"],
            "relabeling": {},
            "placeholders": {
                "extra": '{"model": "snowflake:claude-4-sonnet", "snowflake_conn_id": "snowflake_default"}',
            },
        }

    def _get_snowflake_conn_id(self, extra: dict[str, Any]) -> str:
        snowflake_conn_id = self.snowflake_conn_id or extra.get("snowflake_conn_id")
        if not snowflake_conn_id:
            raise ValueError(
                f"Connection '{self.llm_conn_id}' has no Snowflake connection to source credentials "
                "from. Set snowflake_conn_id on the hook or the connection's extra field, pointing "
                "at an existing Snowflake connection."
            )
        return snowflake_conn_id

    def _get_token_provider(self, extra: dict[str, Any]) -> SnowflakeRestTokenProvider:
        """
        Build the Snowflake hook, token provider, base URL, and HTTP client once.

        Reused for this hook's lifetime -- including the ``httpx2.AsyncClient``, which nothing
        else owns or closes (``SnowflakeProvider`` only owns and closes a client it built itself),
        so building a fresh one on every call would leak one per call.
        """
        if self._token_provider is None:
            snowflake_hook = SnowflakeHook(snowflake_conn_id=self._get_snowflake_conn_id(extra))
            token_provider = SnowflakeRestTokenProvider(snowflake_hook)
            cortex_base_url = (
                get_cortex_base_url(snowflake_hook._get_static_conn_params) + CORTEX_CHAT_COMPLETIONS_PATH
            )
            http_client = httpx2.AsyncClient(auth=_SnowflakeCortexAuth(token_provider))
            self._token_provider = token_provider
            self._cortex_base_url = cortex_base_url
            self._http_client = http_client
        return self._token_provider

    def _get_provider_kwargs(
        self,
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Return kwargs for ``SnowflakeProvider``.

        .. note::
            ``api_key`` and ``base_url`` (sourced from ``conn.password`` and ``conn.host``) are
            intentionally ignored: this connection hides those fields in the UI, and credentials
            and host come from the Snowflake connection named by ``snowflake_conn_id`` instead.
        """
        self._get_token_provider(extra)
        return {
            "base_url": self._cortex_base_url,
            "token": _UNUSED_TOKEN_PLACEHOLDER,
            "http_client": self._http_client,
        }

    def test_connection(self) -> tuple[bool, str]:
        """
        Test connection by resolving the model and fetching a Snowflake token.

        Calls Snowflake's token endpoint, so a bad credential is reported here. Does NOT call
        the LLM API.
        """
        ok, message = super().test_connection()
        if not ok:
            return ok, message
        try:
            self._get_token_provider(self._get_conn_and_extra()[1]).get_token()
        except Exception as e:
            return False, str(e)
        return True, "Model resolved and Snowflake token fetched successfully."
