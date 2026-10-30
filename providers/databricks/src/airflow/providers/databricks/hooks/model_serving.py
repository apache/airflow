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
"""Expose Databricks Model Serving's OpenAI-compatible chat endpoint as a pydantic-ai model hook."""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextvars
import functools
import inspect
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

_COMMON_AI_REQUIREMENT_MESSAGE = (
    "This feature requires apache-airflow-providers-common-ai>=0.10.0, which adds "
    "'fallback_conn_ids' to PydanticAIHook. "
    "Install with apache-airflow-providers-databricks[common.ai]."
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

# OpenAIProvider rejects an empty api_key, but _DatabricksServingAuth overwrites Authorization on
# every request, so this value is never sent.
_UNUSED_TOKEN_PLACEHOLDER = "unused-overridden-per-request"

# Token refreshes run here, never on asyncio's default executor: ``asend()`` reads its reply on that
# executor while holding the comms lock, so blocking lookups that fill it leave nothing to run the
# read and the task hangs (see ``_executor`` in common.ai's ``hooks/pydantic_ai.py``). Some
# ``_get_token`` paths do such lookups at request time (AWS federation resolves a second Airflow
# connection through ``StsHook``). One worker also serializes these token refreshes among themselves
# on Airflow 3.0 and 3.1, where the channel to the supervisor has no lock of its own; it is independent
# of common.ai's single-worker executor, so the two do not serialize against each other.
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1, thread_name_prefix="databricks-serving-auth")


class _DatabricksServingAuth(httpx2.Auth):
    """
    Refresh the ``Authorization`` header on every request from a Databricks hook.

    ``_get_token()`` may block: the OAuth service principal, Azure AD and federation paths make
    HTTP or SDK calls, with retries, to fetch an expiring token. ``async_auth_flow`` is overridden
    to run the refresh on a worker thread instead of the httpx2 default of driving the sync
    ``auth_flow`` inline on the event loop. ``auth_flow`` itself is kept for sync ``httpx2.Client``
    callers, which have no event loop to block.
    """

    def __init__(self, databricks_hook: BaseDatabricksHook) -> None:
        self._databricks_hook = databricks_hook

    def _get_auth_header(self) -> str:
        return f"Bearer {self._databricks_hook._get_token(raise_error=True)}"

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        request.headers["Authorization"] = self._get_auth_header()
        yield request

    async def async_auth_flow(self, request: Request) -> AsyncGenerator[Request, Response]:
        call = functools.partial(contextvars.copy_context().run, self._get_auth_header)
        request.headers["Authorization"] = await asyncio.get_running_loop().run_in_executor(_executor, call)
        yield request


class PydanticAIDatabricksHook(PydanticAIHook):
    """
    Hook for Databricks Model Serving's OpenAI-compatible chat endpoint via pydantic-ai.

    Unlike the other ``PydanticAI*`` hooks, credentials do not live on this connection: they are
    read from an existing ``databricks`` connection (personal access token, OAuth, Azure or
    federated credentials -- whichever that connection is configured for) and refreshed on every
    request through ``BaseDatabricksHook._get_token``. Username/password basic auth is not
    supported. The underlying ``httpx2.AsyncClient`` is built once and lives as long as this hook
    instance; nothing currently closes it.

    Connection fields:
        - **extra** JSON: ``{"model": "databricks-claude-sonnet-4-5",
          "databricks_conn_id": "databricks_default"}``

    A bare model name is a serving endpoint name and is qualified as ``openai-chat:<endpoint>``,
    so requests use the Chat Completions API that every serving endpoint speaks. Databricks
    limits apply: no parallel tool calls, at most 32 tools, and a JSON-schema subset without
    ``$ref``, ``anyOf``, ``oneOf``, ``allOf``, ``prefixItems`` or ``pattern``.

    :param llm_conn_id: Airflow connection ID for this ``pydanticai_databricks`` connection.
    :param model_id: Serving endpoint name, e.g. ``"databricks-claude-sonnet-4-5"``.
    :param fallback_conn_ids: See :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`.
    :param databricks_conn_id: Connection ID of an existing Databricks connection to source the
        workspace host and credentials from. Takes precedence over the connection extra's
        ``databricks_conn_id``; one of the two is required.
    """

    conn_type = "pydanticai_databricks"
    default_conn_name = "pydanticai_databricks_default"
    hook_name = "Pydantic AI (Databricks Model Serving)"
    model_provider = "openai-chat"

    def __init__(
        self,
        llm_conn_id: str | None = None,
        model_id: str | None = None,
        fallback_conn_ids: list[str] | None = None,
        *,
        databricks_conn_id: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(llm_conn_id, model_id, fallback_conn_ids, **kwargs)
        self.databricks_conn_id = databricks_conn_id
        self._databricks_hook: BaseDatabricksHook | None = None
        self._base_url: str | None = None
        self._http_client: httpx2.AsyncClient | None = None

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour for the Airflow connection form."""
        return {
            "hidden_fields": ["schema", "port", "login", "host", "password"],
            "relabeling": {},
            "placeholders": {
                "extra": '{"model": "databricks-claude-sonnet-4-5", "databricks_conn_id": "databricks_default"}',
            },
        }

    def _get_databricks_conn_id(self, extra: dict[str, Any]) -> str:
        databricks_conn_id = self.databricks_conn_id or extra.get("databricks_conn_id")
        if not databricks_conn_id:
            raise ValueError(
                f"Connection '{self.llm_conn_id}' has no Databricks connection to source credentials "
                "from. Set databricks_conn_id on the hook or the connection's extra field, pointing "
                "at an existing Databricks connection."
            )
        return databricks_conn_id

    def _get_databricks_hook(self, extra: dict[str, Any]) -> BaseDatabricksHook:
        """
        Build the Databricks hook, base URL, and HTTP client once.

        Reused for this hook's lifetime -- including the ``httpx2.AsyncClient``, which nothing
        else owns or closes, so building a fresh one on every call would leak one per call.
        """
        if self._databricks_hook is None:
            databricks_hook = BaseDatabricksHook(
                databricks_conn_id=self._get_databricks_conn_id(extra),
                caller="PydanticAIDatabricksHook",
            )
            base_url = databricks_hook._endpoint_url("serving-endpoints")
            http_client = httpx2.AsyncClient(auth=_DatabricksServingAuth(databricks_hook))
            self._databricks_hook = databricks_hook
            self._base_url = base_url
            self._http_client = http_client
        return self._databricks_hook

    def _get_provider_kwargs(
        self,
        api_key: str | None,
        base_url: str | None,
        extra: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Return kwargs for ``OpenAIProvider``.

        .. note::
            ``api_key`` and ``base_url`` (sourced from ``conn.password`` and ``conn.host``) are
            intentionally ignored: this connection hides those fields in the UI, and credentials
            and host come from the Databricks connection named by ``databricks_conn_id`` instead.
        """
        self._get_databricks_hook(extra)
        return {
            "base_url": self._base_url,
            "api_key": _UNUSED_TOKEN_PLACEHOLDER,
            "http_client": self._http_client,
        }

    def test_connection(self) -> tuple[bool, str]:
        """
        Test connection by resolving the model and fetching a Databricks token.

        A bad credential is reported here. Does NOT call the serving endpoint.
        """
        ok, message = super().test_connection()
        if not ok:
            return ok, message
        try:
            self._get_databricks_hook(self._get_conn_and_extra()[1])._get_token(raise_error=True)
        except Exception as e:
            return False, str(e)
        return True, "Model resolved and Databricks token fetched successfully."
