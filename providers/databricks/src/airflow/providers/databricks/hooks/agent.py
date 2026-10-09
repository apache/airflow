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
"""Invoke agents served by Databricks DurableAgentServer."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from uuid import UUID, uuid4

import aiohttp
import requests
from tenacity import RetryError, stop_before_delay

from airflow.providers.common.compat.sdk import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.databricks.exceptions import DatabricksAgentInvocationTimeout, DatabricksApiError
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

if TYPE_CHECKING:
    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents.base import (
        BaseManagedAgentHook,
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
        ManagedAgentResponse,
    )
else:
    try:
        from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
        from airflow.providers.common.ai.managed_agents.base import (
            BaseManagedAgentHook,
            ManagedAgentCapabilities,
            ManagedAgentRef,
            ManagedAgentResponse,
        )
    except ImportError as e:
        _common_ai_import_error = e

        def _needs_common_ai(*args: Any, **kwargs: Any) -> Any:
            raise AirflowOptionalProviderFeatureException(
                "Consulting a Databricks agent needs the 'common.ai' extra: "
                "pip install 'apache-airflow-providers-databricks[common.ai]'."
            ) from _common_ai_import_error

        class BaseManagedAgentHook:
            """Keep background invocation available without the Common AI extra."""

            agent = _needs_common_ai

        ManagedAgentCapabilities = ManagedAgentRef = ManagedAgentResponse = _needs_common_ai
        ManagedAgentInvocationError = _needs_common_ai


_AGENT_OPTIONS = frozenset({"input_key", "invocation_id"})


class DatabricksAgentHook(BaseDatabricksHook, BaseManagedAgentHook):
    """
    Invoke a DurableAgentServer deployed on Databricks Apps.

    With the ``common.ai`` extra installed, ``hook.agent(app_url)`` exposes the synchronous
    invocation API through the managed-agent contract. Background invocations remain available
    through ``create_invocation`` and ``get_invocation`` without that extra.

    :param app_url: HTTPS base URL for background invocation methods. Defaults to ``None``;
        managed-agent calls receive the app URL as their ``agent`` argument.
    :param databricks_conn_id: Databricks workspace connection configured with
        ``service_principal_oauth=true``, a client ID in login and a client secret in password.
        Defaults to ``databricks_default``.
    :param timeout_seconds: HTTP request timeout in seconds. Defaults to ``180``.
    :param retry_limit: Maximum number of attempts for transient HTTP failures. Defaults to ``3``.
    :param retry_delay: Minimum delay between HTTP retries in seconds. Defaults to ``1``.
    """

    def __init__(
        self,
        app_url: str | None = None,
        databricks_conn_id: str = "databricks_default",
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1,
    ) -> None:
        super().__init__(databricks_conn_id, timeout_seconds, retry_limit, retry_delay)
        self.app_url = self._validate_app_url(app_url) if app_url is not None else None

    agent_platform = "databricks.agent_runtime"

    @staticmethod
    def _validate_app_url(app_url: str | None) -> str:
        if app_url is None:
            raise ValueError("app_url is required for background invocation methods")
        parsed = urlsplit(app_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.path not in ("", "/")
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("app_url must be an HTTPS base URL without credentials, path, query or fragment")
        return app_url.rstrip("/")

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        """
        Resolve an app's HTTPS base URL without contacting Databricks.

        :param agent: HTTPS base URL of the deployed app.
        :return: Platform-qualified app URL with its trailing slash removed.
        """
        return ManagedAgentRef(platform=self.agent_platform, name=self._validate_app_url(agent))

    def get_agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        """
        Report session, structured-output and invocation-ID support.

        :param agent: HTTPS base URL of the deployed app.
        :return: Capabilities of the synchronous invocation API.
        """
        self.resolve_agent(agent)
        return ManagedAgentCapabilities(sessions=True, structured_output=True, trace=True)

    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        """
        Invoke an agent synchronously through the Common AI contract.

        :param agent: HTTPS base URL of the deployed app.
        :param request: Prompt or messages, optional session and request timeout. Vendor options
            accept ``input_key`` to send a prompt under an agent-specific key and ``invocation_id``
            to reuse a caller-provided UUID. By default, prompts are converted to messages and
            a new UUID is generated for each call.
        :return: Answer text, full invocation response, structured output, session and invocation ID.
        """
        app_url = self.resolve_agent(agent).name
        unknown = set(request.vendor_options) - _AGENT_OPTIONS
        if unknown:
            raise ValueError(
                f"vendor_options {sorted(unknown)} are not accepted; expected {sorted(_AGENT_OPTIONS)}"
            )
        input_key = request.vendor_options.get("input_key")
        if input_key is not None and (not isinstance(input_key, str) or not input_key):
            raise ValueError("vendor_options['input_key'] must be a nonempty string")
        if input_key is not None and request.messages is not None:
            raise ValueError("vendor_options['input_key'] only applies to a prompt")
        invocation_id = (
            str(UUID(request.vendor_options["invocation_id"]))
            if "invocation_id" in request.vendor_options
            else str(uuid4())
        )
        payload: dict[str, Any] = {
            "id": invocation_id,
            "input": {input_key: request.prompt}
            if input_key is not None
            else {"messages": request.as_messages()},
        }
        if request.session_id is not None:
            payload["session_id"] = request.session_id
        try:
            raw = self._do_agent_api_call(
                "POST",
                "api/invocations",
                payload,
                request.session_id,
                timeout_seconds=request.timeout,
                app_url=app_url,
            )
        except DatabricksAgentInvocationTimeout:
            raise
        except AirflowException as exc:
            cause = exc.__cause__ or exc.__context__
            if isinstance(cause, RetryError):
                transient = cause.last_attempt.exception()
                if transient is not None:
                    raise transient from None
            if isinstance(cause, requests.HTTPError) and (
                cause.response.status_code == 429 or cause.response.status_code >= 500
            ):
                raise cause from None
            raise ManagedAgentInvocationError(
                "Databricks managed-agent request failed; inspect the app logs"
            ) from exc
        if not isinstance(raw, dict) or raw.get("status") != "completed":
            raise ManagedAgentInvocationError(
                f"Databricks agent invocation {invocation_id} did not complete; inspect the app logs"
            )
        output = raw.get("output")
        text = output if isinstance(output, str) else "" if output is None else json.dumps(output)
        if isinstance(output, dict) and isinstance(output.get("output"), str):
            text = output["output"]
        return ManagedAgentResponse(
            text=text,
            raw=raw,
            structured=None if isinstance(output, str) else output,
            session_id=raw.get("session_id", request.session_id),
            trace_ref=raw.get("id", invocation_id),
        )

    def _validate_oauth(self) -> None:
        conn = self.databricks_conn
        if not conn.extra_dejson.get("service_principal_oauth") or not conn.login or not conn.password:
            raise ValueError("Databricks Apps requires service_principal_oauth with a client ID and secret")

    def _get_headers(self, token: str, session_id: str | None) -> dict[str, str]:
        headers = {**self.user_agent_header, "Authorization": f"Bearer {token}"}
        if session_id is not None:
            headers["X-Routing-Key"] = session_id
        return headers

    def create_invocation(
        self,
        invocation_id: str,
        input: Any,
        session_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Start a background invocation; retrying the same UUID and input is idempotent.

        :param invocation_id: UUID identifying the invocation.
        :param input: JSON-serializable input whose schema is defined by the agent.
        :param session_id: Conversation ID, also sent as the routing key. Defaults to ``None``.
        :return: Complete JSON submission response returned by the agent server.
        """
        invocation_id = str(UUID(invocation_id))
        payload = {"id": invocation_id, "input": input, "background": True}
        if session_id is not None:
            payload["session_id"] = session_id
        return self._do_agent_api_call("POST", "api/invocations", payload, session_id)

    def get_invocation(
        self, invocation_id: str, session_id: str | None = None, *, timeout_seconds: float | None = None
    ) -> dict[str, Any]:
        """
        Get the status and output of an invocation.

        :param invocation_id: UUID identifying the invocation.
        :param session_id: Conversation ID sent as the routing key. Defaults to ``None``.
        :param timeout_seconds: Optional polling request/retry budget in seconds. Defaults to ``None``,
            which uses the hook's HTTP timeout and retry settings. OAuth refresh uses the base hook's
            token timeout; if it exhausts this budget, no invocation request is sent.
        :return: Complete JSON invocation response, including output when available.
        """
        invocation_id = str(UUID(invocation_id))
        return self._do_agent_api_call(
            "GET", f"api/invocations/{invocation_id}", None, session_id, timeout_seconds=timeout_seconds
        )

    def _do_agent_api_call(
        self,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None,
        session_id: str | None,
        *,
        timeout_seconds: float | None = None,
        app_url: str | None = None,
    ) -> dict[str, Any]:
        app_url = self._validate_app_url(app_url if app_url is not None else self.app_url)
        self._validate_oauth()
        if timeout_seconds is not None and timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        deadline = time.monotonic() + timeout_seconds if timeout_seconds is not None else None
        retry = self._get_retry_object()
        if timeout_seconds is not None:
            original_stop = retry.stop
            budget_stop = stop_before_delay(timeout_seconds)
            retry.stop = lambda state: original_stop(state) or budget_stop(state)
        try:
            for attempt in retry:
                with attempt:
                    if deadline is not None and time.monotonic() >= deadline:
                        raise DatabricksAgentInvocationTimeout(
                            f"Timed out polling Databricks agent {endpoint}"
                        )
                    token = self._get_sp_token(self._get_oidc_token_service_url())
                    request_timeout: float = self.timeout_seconds
                    if deadline is not None:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise DatabricksAgentInvocationTimeout(
                                f"Timed out polling Databricks agent {endpoint}"
                            )
                        request_timeout = min(request_timeout, remaining)
                    response = requests.request(
                        method,
                        f"{app_url}/{endpoint}",
                        json=payload,
                        headers=self._get_headers(token, session_id),
                        timeout=request_timeout,
                        allow_redirects=False,
                        **self._get_requests_kwargs(),
                    )
                    if 300 <= response.status_code < 400:
                        raise DatabricksApiError("Databricks agent returned an unexpected redirect")
                    response.raise_for_status()
                    if deadline is not None and time.monotonic() >= deadline:
                        raise DatabricksAgentInvocationTimeout(
                            f"Timed out polling Databricks agent {endpoint}"
                        )
                    return response.json()
        except RetryError as e:
            if deadline is not None and (
                time.monotonic() >= deadline or retry.statistics["attempt_number"] < self.retry_limit
            ):
                raise DatabricksAgentInvocationTimeout(
                    f"Timed out polling Databricks agent {endpoint}"
                ) from e
            raise DatabricksApiError("Databricks agent request exhausted its HTTP retries") from e
        except requests.HTTPError as e:
            raise DatabricksApiError(
                f"Databricks agent request failed with HTTP {e.response.status_code}",
                http_status_code=e.response.status_code,
            ) from e
        raise DatabricksApiError("Databricks agent request returned no response")

    @staticmethod
    def _retryable_error(exception: BaseException) -> bool:
        return BaseDatabricksHook._retryable_error(exception) or isinstance(
            exception, (aiohttp.ServerDisconnectedError, aiohttp.ClientOSError, ConnectionResetError)
        )

    async def a_get_invocation(self, invocation_id: str, session_id: str | None = None) -> dict[str, Any]:
        """
        Asynchronously get an invocation inside the hook's async context manager.

        :param invocation_id: UUID identifying the invocation.
        :param session_id: Conversation ID sent as the routing key. Defaults to ``None``.
        :return: Complete JSON invocation response, including output when available.
        """
        invocation_id = str(UUID(invocation_id))
        await self._a_cache_databricks_conn()
        self._validate_oauth()
        url = f"{self._validate_app_url(self.app_url)}/api/invocations/{invocation_id}"
        try:
            async for attempt in self._a_get_retry_object():
                with attempt:
                    token = await self._a_get_sp_token(self._get_oidc_token_service_url())
                    async with self._session.get(
                        url,
                        headers=self._get_headers(token, session_id),
                        timeout=self.timeout_seconds,
                        allow_redirects=False,
                        **self._get_aiohttp_kwargs(url),
                    ) as response:
                        if 300 <= response.status < 400:
                            raise DatabricksApiError("Databricks agent returned an unexpected redirect")
                        response.raise_for_status()
                        return await response.json()
        except RetryError as e:
            raise DatabricksApiError("Databricks agent request exhausted its HTTP retries") from e
        except aiohttp.ClientResponseError as e:
            raise DatabricksApiError(
                f"Databricks agent request failed with HTTP {e.status}", http_status_code=e.status
            ) from e
        raise DatabricksApiError("Databricks agent request returned no response")
