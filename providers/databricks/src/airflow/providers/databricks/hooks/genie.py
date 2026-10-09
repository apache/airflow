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
"""Hook for Databricks Genie conversations."""

from __future__ import annotations

import json
import time
from typing import TYPE_CHECKING, Any
from urllib.parse import quote, urlsplit

import requests
from requests.auth import HTTPBasicAuth
from tenacity import RetryError

from airflow.providers.common.compat.sdk import (
    AirflowException,
    AirflowOptionalProviderFeatureException,
)
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

if TYPE_CHECKING:
    from airflow.providers.common.ai.managed_agents.base import (
        BaseManagedAgentHook,
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
        ManagedAgentResponse,
    )
else:
    try:
        from airflow.providers.common.ai.managed_agents.base import (
            BaseManagedAgentHook,
            ManagedAgentCapabilities,
            ManagedAgentRef,
            ManagedAgentRequest,
            ManagedAgentResponse,
        )
    except ImportError:

        def _needs_common_ai(*args: Any, **kwargs: Any) -> Any:
            raise AirflowOptionalProviderFeatureException(
                "Databricks Genie managed-agent integration needs the 'common.ai' extra of the "
                "Databricks provider: pip install 'apache-airflow-providers-databricks[common.ai]'."
            )

        class BaseManagedAgentHook:
            """Stand-in for the optional Common AI contract."""

            agent = _needs_common_ai

        ManagedAgentCapabilities = ManagedAgentRef = ManagedAgentRequest = ManagedAgentResponse = (
            _needs_common_ai
        )


_MAX_RESPONSE_BYTES = 1024 * 1024
_MAX_RESULT_BYTES = 48 * 1024
_MAX_RESULT_ROWS = 100
_POLL_INTERVAL_SECONDS = 1.0
_PENDING_STATUSES = frozenset(
    {
        "FETCHING_METADATA",
        "FILTERING_CONTEXT",
        "ASKING_AI",
        "PENDING_WAREHOUSE",
        "EXECUTING_QUERY",
        "SUBMITTED",
    }
)


class DatabricksGenieError(AirflowException):
    """A Genie request failed or returned a result that could not be used."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


class DatabricksGenieHook(BaseDatabricksHook, BaseManagedAgentHook):
    """
    Consult a Genie space using the Databricks Workspace REST API.

    The Databricks connection supplies authentication. Genie start/send requests are sent once:
    their outcome may be unknown after a timeout or server error, so the hook never retries them.
    Message and query-result reads use the hook's configured retry policy.
    """

    agent_platform = "databricks.genie"

    def __init__(self, *args: Any, poll_interval: float = _POLL_INTERVAL_SECONDS, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if poll_interval <= 0:
            raise ValueError("poll_interval must be greater than zero")
        self.poll_interval = poll_interval

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        if not isinstance(agent, str) or not agent.strip():
            raise ValueError("A Databricks Genie space ID must be a non-empty string.")
        return ManagedAgentRef(platform=self.agent_platform, name=agent)

    def get_agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        self.resolve_agent(agent)
        return ManagedAgentCapabilities(sessions=True, structured_output=True)

    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        self.resolve_agent(agent)
        if request.vendor_options:
            raise ValueError(
                f"Databricks Genie does not support vendor_options: {sorted(request.vendor_options)}"
            )
        prompt = request.prompt if request.prompt is not None else _messages_to_prompt(request.as_messages())
        timeout = request.timeout if request.timeout is not None else self.timeout_seconds
        result = self.consult(agent, prompt, conversation_id=request.session_id, timeout=timeout)
        return ManagedAgentResponse(
            text=json.dumps(result, ensure_ascii=False, separators=(",", ":")),
            raw=result,
            structured=result,
            session_id=result["conversation_id"],
            trace_ref=result["message_id"],
        )

    def consult(
        self,
        space_id: str,
        prompt: str,
        *,
        conversation_id: str | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Submit one question and return a bounded, structured Genie result."""
        self.resolve_agent(space_id)
        if not prompt.strip():
            raise ValueError("A Genie question must not be empty.")
        if timeout is not None and timeout <= 0:
            raise ValueError("timeout must be greater than zero")
        if conversation_id is not None and not conversation_id.strip():
            raise ValueError("conversation_id must not be empty")

        deadline = time.monotonic() + (timeout if timeout is not None else self.timeout_seconds)
        space = quote(space_id, safe="")
        if conversation_id is None:
            endpoint = f"spaces/{space}/start-conversation"
            body = {"content": prompt, "enable_visualization": False}
        else:
            conversation = quote(conversation_id, safe="")
            endpoint = f"spaces/{space}/conversations/{conversation}/messages"
            body = {"content": prompt, "enable_visualization": False}

        created = self._request("POST", endpoint, body=body, retry_read=False, deadline=deadline)
        resolved_conversation_id = created.get("conversation_id")
        message_id = created.get("message_id") or created.get("id")
        if not isinstance(resolved_conversation_id, str) or not isinstance(message_id, str):
            raise DatabricksGenieError(
                "Genie accepted the consultation but returned no conversation or message ID; "
                "the request will not be repeated."
            )

        while True:
            message = self._request(
                "GET",
                f"spaces/{space}/conversations/{quote(resolved_conversation_id, safe='')}/messages/"
                f"{quote(message_id, safe='')}",
                retry_read=True,
                deadline=deadline,
            )
            status = message.get("status")
            if status == "COMPLETED":
                break
            if status == "FAILED":
                error = message.get("error") or {}
                detail = error.get("error") if isinstance(error, dict) else str(error)
                raise DatabricksGenieError(
                    f"Genie could not answer the question: {detail or 'unknown error'}"
                )
            if status in {"CANCELLED", "QUERY_RESULT_EXPIRED"}:
                raise DatabricksGenieError(f"Genie consultation ended with status {status}.")
            if status not in _PENDING_STATUSES:
                raise DatabricksGenieError(f"Genie returned an unknown message status: {status!r}.")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DatabricksGenieError(
                    f"Timed out waiting for Genie message {message_id}; "
                    f"conversation {resolved_conversation_id} "
                    "is available to inspect."
                )
            time.sleep(min(self.poll_interval, remaining))

        structured = self._summarize_result(
            space_id, space, resolved_conversation_id, message_id, message, deadline
        )
        return _truncate_result(structured)

    def _summarize_result(
        self,
        space_id: str,
        space_path: str,
        conversation_id: str,
        message_id: str,
        message: dict[str, Any],
        deadline: float,
    ) -> dict[str, Any]:
        attachments = message.get("attachments") or []
        answers: list[str] = []
        queries: list[dict[str, Any]] = []
        query_results: list[dict[str, Any]] = []
        for attachment in attachments:
            if not isinstance(attachment, dict):
                continue
            text_attachment = attachment.get("text")
            if isinstance(text_attachment, dict) and isinstance(text_attachment.get("content"), str):
                answers.append(text_attachment["content"])
            query = attachment.get("query")
            if isinstance(query, dict):
                queries.append(
                    {
                        key: query[key]
                        for key in ("title", "query", "description", "query_result_metadata")
                        if key in query
                    }
                )
                attachment_id = attachment.get("attachment_id")
                if isinstance(attachment_id, str) and attachment_id:
                    response = self._request(
                        "GET",
                        f"spaces/{space_path}/conversations/"
                        f"{quote(conversation_id, safe='')}/messages/"
                        f"{quote(message_id, safe='')}/attachments/"
                        f"{quote(attachment_id, safe='')}/query-result",
                        retry_read=True,
                        deadline=deadline,
                    )
                    query_results.append(_summarize_query_result(response))
        result: dict[str, Any] = {
            "space_id": space_id,
            "conversation_id": conversation_id,
            "message_id": message_id,
            "status": message.get("status"),
            "answer": "\n\n".join(answers),
        }
        if queries:
            result["queries"] = queries
        if query_results:
            result["query_results"] = query_results
        return result

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        body: dict[str, Any] | None = None,
        retry_read: bool,
        deadline: float | None = None,
    ) -> dict[str, Any]:
        """Call one Genie endpoint; only GET requests are eligible for transport retries."""
        url = self._endpoint_url(f"api/2.0/genie/{endpoint}")
        parsed_url = urlsplit(url)
        is_loopback = parsed_url.hostname in {"localhost", "127.0.0.1", "::1"}
        if parsed_url.scheme != "https" and not is_loopback:
            raise DatabricksGenieError(
                "Databricks Genie requires HTTPS so connection credentials are not sent in cleartext."
            )
        headers = {**self.user_agent_header, **self._get_aad_headers()}
        token = self._get_token()
        auth = None
        if token:
            headers["Authorization"] = f"Bearer {token}"
        else:
            auth = HTTPBasicAuth(self._get_connection_attr("login"), self.databricks_conn.password)

        def send() -> dict[str, Any]:
            response = requests.request(
                method,
                url,
                json=body if method == "POST" else None,
                auth=auth,
                headers=headers,
                timeout=self._bounded_timeout(deadline),
                stream=True,
                **self._get_requests_kwargs(),
            )
            try:
                if not response.ok:
                    if (
                        method == "GET"
                        and retry_read
                        and (response.status_code >= 500 or response.status_code == 429)
                    ):
                        response.raise_for_status()
                    raise self._http_error(response.status_code)
                payload = bytearray()
                for chunk in response.iter_content(16 * 1024):
                    payload.extend(chunk)
                    if len(payload) > _MAX_RESPONSE_BYTES:
                        raise DatabricksGenieError(
                            f"Genie response exceeded the {_MAX_RESPONSE_BYTES}-byte safety limit."
                        )
                if not payload:
                    return {}
                try:
                    decoded = json.loads(payload)
                except json.JSONDecodeError as exc:
                    raise DatabricksGenieError("Databricks Genie returned invalid JSON.") from exc
                if not isinstance(decoded, dict):
                    raise DatabricksGenieError("Genie returned a non-object response.")
                return decoded
            finally:
                response.close()

        if not retry_read:
            try:
                return send()
            except (requests.exceptions.RequestException, TimeoutError) as exc:
                raise DatabricksGenieError(
                    "The Genie request may have been accepted, but Databricks did not confirm its "
                    "result. The hook did not retry it; inspect the Genie conversation before "
                    "submitting again."
                ) from exc
            except DatabricksGenieError as exc:
                if exc.status_code is None or exc.status_code >= 500:
                    raise DatabricksGenieError(
                        f"{exc} The request may have been accepted; its outcome is unknown "
                        "and the hook did not retry it."
                    ) from exc
                raise

        try:
            for attempt in self._get_retry_object():
                with attempt:
                    if deadline is not None and time.monotonic() >= deadline:
                        raise DatabricksGenieError("Timed out waiting for a Genie read request.")
                    return send()
        except RetryError as exc:
            last_error = exc.last_attempt.exception()
            response = getattr(last_error, "response", None)
            if response is not None:
                raise self._http_error(response.status_code) from exc
            raise DatabricksGenieError(
                f"A Genie read request failed after {self.retry_limit} attempts."
            ) from exc
        except requests.exceptions.HTTPError as exc:
            response = exc.response
            if response is not None:
                raise self._http_error(response.status_code) from exc
            raise
        raise AssertionError("retry loop must return or raise")

    def _bounded_timeout(self, deadline: float | None) -> float:
        if deadline is None:
            return self.timeout_seconds
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DatabricksGenieError("Timed out waiting for a Genie read request.")
        return min(float(self.timeout_seconds), remaining)

    @staticmethod
    def _http_error(status_code: int) -> DatabricksGenieError:
        if status_code == 401:
            message = (
                "Databricks authentication failed (HTTP 401); check the Databricks connection credentials."
            )
        elif status_code == 403:
            message = (
                "Databricks denied access to the Genie space or conversation (HTTP 403). "
                "Genie may also return 403 when a resource does not exist; "
                "verify the ID and permissions."
            )
        elif status_code == 404:
            message = "The Genie space, conversation, message, or result was not found (HTTP 404)."
        elif status_code == 429:
            message = "Databricks rate-limited the Genie request (HTTP 429); retry after the task delay."
        else:
            message = f"Databricks Genie returned HTTP {status_code}."
        return DatabricksGenieError(message, status_code=status_code)


def _messages_to_prompt(messages: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for message in messages:
        content = message.get("content", "")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for item in content:
                if (
                    isinstance(item, dict)
                    and item.get("type") == "text"
                    and isinstance(item.get("text"), str)
                ):
                    parts.append(item["text"])
    prompt = "\n".join(parts).strip()
    if not prompt:
        raise ValueError("Genie consultations require a text prompt.")
    return prompt


def _summarize_query_result(response: dict[str, Any]) -> dict[str, Any]:
    statement = response.get("statement_response", response)
    if not isinstance(statement, dict):
        return {"result": "unavailable"}
    manifest = statement.get("manifest") or {}
    result = statement.get("result") or {}
    columns = [
        column.get("name") for column in manifest.get("schema", {}).get("columns", []) if column.get("name")
    ]
    rows = result.get("data_array") or []
    return {
        "columns": columns[:100],
        "rows": rows[:_MAX_RESULT_ROWS],
        "total_row_count": manifest.get("total_row_count", len(rows)),
        "truncated": len(rows) > _MAX_RESULT_ROWS or bool(manifest.get("truncated")),
    }


def _truncate_result(result: dict[str, Any]) -> dict[str, Any]:
    """Bound tool output while keeping valid JSON and the consultation identifiers."""
    encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":"))
    if len(encoded.encode("utf-8")) <= _MAX_RESULT_BYTES:
        return result
    bounded = dict(result)
    bounded["truncated"] = True
    for key in ("query_results", "queries"):
        if key in bounded:
            bounded[key] = [{"truncated": True} for _ in bounded[key]]
    answer = bounded.get("answer", "")
    low, high = 0, len(answer)
    while low < high:
        middle = (low + high + 1) // 2
        bounded["answer"] = answer[:middle]
        encoded_size = len(json.dumps(bounded, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
        if encoded_size <= _MAX_RESULT_BYTES:
            low = middle
        else:
            high = middle - 1
    bounded["answer"] = answer[:low]
    return bounded
