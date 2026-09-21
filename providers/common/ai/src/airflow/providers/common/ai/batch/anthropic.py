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
"""
Anthropic batch adapter (Message Batches API).

Talks to the ``anthropic`` SDK directly, for the same reason ``batch/openai.py``
does. The SDK import is deferred to :func:`_build_client`.

The SDK's ``messages.batches.create`` has no batch-level ``metadata``
parameter, so the idempotency key lives only in each request's ``custom_id``
prefix. That is enough to recognize results, but not to find an orphaned
batch: see :meth:`AnthropicBatchAdapter.find_orphaned_batch`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import structlog

from airflow.providers.common.ai.batch.base import (
    BatchAdapter,
    BatchState,
    BatchStatus,
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
)
from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

log = structlog.get_logger(logger_name="task")

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchRequest
    from airflow.providers.common.ai.batch.output_schema import OutputSpec

#: Anthropic only reports "in_progress"/"canceling"/"ended" at the job level: there is no
#: separate "failed" or "expired" job status, and "ended" alone does not say whether the batch
#: was cancelled. See ``_batch_status``.
_STATUS_MAP: dict[str, BatchStatus] = {
    "in_progress": "in_progress",
    "canceling": "in_progress",
    "ended": "completed",
}


def _batch_status(batch: Any) -> BatchStatus:
    """
    Resolve ``batch.processing_status`` to this adapter's status vocabulary.

    A batch at ``"ended"`` with ``cancel_initiated_at`` set finished because it
    was cancelled; without the check it would be reported as ``"completed"``
    and its manifest would say ``partial`` instead of ``cancelled``.
    """
    if batch.processing_status == "ended" and batch.cancel_initiated_at is not None:
        return "cancelled"
    return _STATUS_MAP.get(batch.processing_status, "in_progress")


def _build_client(api_key: str | None, base_url: str | None) -> Any:
    try:
        from anthropic import Anthropic
    except ImportError as e:
        raise AirflowOptionalProviderFeatureException(
            "Anthropic batch requires the anthropic SDK. Install with: "
            "pip install 'apache-airflow-providers-common-ai[anthropic]'"
        ) from e
    kwargs: dict[str, Any] = {}
    if api_key:
        kwargs["api_key"] = api_key
    if base_url:
        kwargs["base_url"] = base_url
    return Anthropic(**kwargs)


class AnthropicBatchAdapter(BatchAdapter):
    """
    Batch adapter for Anthropic's Message Batches API.

    :param api_key: Passed straight to the ``anthropic.Anthropic`` client.
        ``None`` falls back to the SDK's own env-var resolution
        (``ANTHROPIC_API_KEY``).
    :param base_url: Passed straight to the ``anthropic.Anthropic`` client.
    :param client: Inject a pre-built client (or a test double) instead of
        constructing one from ``api_key``/``base_url``.
    """

    name = "anthropic"
    #: Anthropic's documented limit: 100,000 requests or 256 MB, whichever is reached first.
    max_requests = 100_000
    max_payload_bytes = 256_000_000
    #: Each request carries its own full Messages params, model included.
    allows_per_request_model = True

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Any | None = None,
    ) -> None:
        self._client = client if client is not None else _build_client(api_key, base_url)

    def _build_params(
        self,
        request: BatchRequest,
        *,
        model: str,
        request_index: int,
        system_prompt: str,
        max_tokens: int,
        request_params: dict[str, Any] | None,
        directive: dict[str, Any],
    ) -> dict[str, Any]:
        effective_system_prompt = request.get("system_prompt") or system_prompt
        resolved_model = self.resolve_request_model(
            request.get("model"), default_bare_model=model, request_index=request_index
        )
        # User params first, then the keys this adapter manages, so a stray "model" or
        # "messages" in request_params cannot bypass the model check or replace the prompt.
        params: dict[str, Any] = {
            **(request_params or {}),
            **(request.get("params") or {}),
            "model": resolved_model,
            "max_tokens": request.get("max_tokens") or max_tokens,
            "messages": [{"role": "user", "content": request["prompt"]}],
            **directive,
        }
        if effective_system_prompt:
            params["system"] = effective_system_prompt
        return params

    def _iter_requests(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        idempotency_key: str,
        directive: dict[str, Any],
        system_prompt: str,
        max_tokens: int,
        request_params: dict[str, Any] | None,
    ) -> Iterator[dict[str, Any]]:
        for index, request in enumerate(requests):
            yield {
                # "-" rather than ":" to fit Anthropic's ``^[a-zA-Z0-9_-]{1,64}$`` custom_id rule.
                "custom_id": f"{idempotency_key}-{index}",
                "params": self._build_params(
                    request,
                    model=model,
                    request_index=index,
                    system_prompt=system_prompt,
                    max_tokens=max_tokens,
                    request_params=request_params,
                    directive=directive,
                ),
            }

    def build_output_directive(self, spec: OutputSpec) -> dict[str, Any]:
        if not spec.is_structured:
            return {}
        # A single tool with a forced tool_choice turns "produce structured output" from
        # something the model can decline into a hard format constraint.
        return {
            "tools": [
                {
                    "name": spec.schema_name,
                    "description": "Return the result using this schema.",
                    "input_schema": spec.json_schema,
                }
            ],
            "tool_choice": {"type": "tool", "name": spec.schema_name},
        }

    def validate_requests(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        output_spec: OutputSpec,
        idempotency_key: str,
        system_prompt: str = "",
        max_tokens: int = 1024,
        request_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        self.check_custom_id_length(idempotency_key, len(requests))

        if len(requests) > self.max_requests:
            raise LLMBatchLimitExceededError(
                f"Anthropic batch accepts at most {self.max_requests} requests per batch; got "
                f"{len(requests)}. Split the input across mapped task instances with .expand() "
                f"so each instance submits at most {self.max_requests} requests."
            )

        directive = self.build_output_directive(output_spec)
        directive_bytes = len(json.dumps(directive))
        built = self._iter_requests(
            requests,
            model=model,
            idempotency_key="0" * 16,
            directive=directive,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            request_params=request_params,
        )
        total_bytes = 0
        for index, item in enumerate(built):
            total_bytes += len(json.dumps(item))
            if total_bytes > self.max_payload_bytes:
                raise LLMBatchLimitExceededError(
                    f"Anthropic batch accepts a payload of at most {self.max_payload_bytes} bytes; "
                    f"this input reached {total_bytes} bytes at request {index + 1} of "
                    f"{len(requests)} (the output_type schema adds ~{directive_bytes} bytes to "
                    "every request). Split the input with .expand()."
                )

    def submit(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        idempotency_key: str,
        input_fingerprint: str,
        output_spec: OutputSpec,
        system_prompt: str = "",
        max_tokens: int = 1024,
        request_params: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> SubmitResult:
        directive = self.build_output_directive(output_spec)
        built = list(
            self._iter_requests(
                requests,
                model=model,
                idempotency_key=idempotency_key,
                directive=directive,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                request_params=request_params,
            )
        )
        batch = self._client.messages.batches.create(requests=built)
        # Requests go inline in the create call; there is no uploaded file to track.
        return SubmitResult(batch_id=batch.id, provider_input_ref=None)

    def get_batch(self, batch_id: str) -> BatchState:
        batch = self._client.messages.batches.retrieve(batch_id)
        request_counts = batch.request_counts
        # Normalized to the {succeeded, errored, expired, cancelled} shape the trigger event
        # uses regardless of adapter; "processing" is what is left over while in progress.
        counts = {
            "succeeded": request_counts.succeeded,
            "errored": request_counts.errored,
            "expired": request_counts.expired,
            "cancelled": request_counts.canceled,
        }
        return BatchState(status=_batch_status(batch), counts=counts, error_message=None)

    def cancel_batch(self, batch_id: str) -> None:
        self._client.messages.batches.cancel(batch_id)

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        """
        Return ``None`` unconditionally: this adapter does not recover orphaned batches.

        The SDK's batch-create call has no ``metadata`` parameter and the list
        endpoint cannot filter by ``custom_id``. A batch could in principle be
        identified by listing recent batches and reading the ``custom_id``
        prefix of an ended batch's first result, but that scan costs a result
        download per candidate and cannot see a batch that is still running, so
        it is not implemented. A crash between "submit sent" and "response
        recorded" therefore falls through to ``on_orphaned_intent``.
        """
        return None

    def iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        # Return (not yield) so a bad batch_id raises at call time rather than on first iteration.
        return self._iter_results(batch_id)

    def _iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        for item in self._client.messages.batches.results(batch_id):
            try:
                yield self._parse_result_item(item)
            except (AttributeError, IndexError, ValueError) as e:
                # One malformed item must not abort the whole stream; results.py counts the
                # index as "missing" once it is absent from ``seen``.
                log.warning(
                    "Skipping unparsable result item for batch",
                    batch_id=batch_id,
                    custom_id=getattr(item, "custom_id", None),
                    error=str(e),
                )

    #: Anthropic's per-item ``result.type`` -> ``RawResultItem.provider_status``. "canceled"
    #: (Anthropic's spelling) is normalized to "cancelled"; the original spelling is kept in the
    #: row's ``error.type``.
    _RESULT_TYPE_TO_PROVIDER_STATUS: dict[str, str] = {
        "errored": "errored",
        "canceled": "cancelled",
        "expired": "expired",
    }

    @classmethod
    def _parse_result_item(cls, item: Any) -> RawResultItem:
        custom_id = item.custom_id
        index = int(custom_id.rsplit("-", 1)[1])
        result = item.result

        if result.type != "succeeded":
            provider_status = cls._RESULT_TYPE_TO_PROVIDER_STATUS.get(result.type, "errored")
            # An errored result carries ``error: ErrorResponse``, whose own ``error`` field is the
            # typed error object with ``type`` and ``message``. Expired and canceled results carry
            # no error object at all.
            error_response = getattr(result, "error", None)
            error_object = getattr(error_response, "error", None)
            message = getattr(error_object, "message", None) or f"batch item {result.type}"
            provider_code = getattr(error_object, "type", None)
            return RawResultItem(
                custom_id=custom_id,
                index=index,
                provider_status=provider_status,  # type: ignore[arg-type]
                model=None,
                usage=None,
                finish_reason=None,
                error={
                    "type": result.type,
                    "message": message,
                    "provider_code": provider_code,
                    "stage": "provider",
                },
                raw=None,
            )

        message = result.message
        usage = message.usage
        return RawResultItem(
            custom_id=custom_id,
            index=index,
            provider_status="success",
            model=message.model,
            usage={"input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens},
            finish_reason=message.stop_reason,
            error=None,
            raw=message.content,
        )

    def extract_output(self, raw: RawResultItem, spec: OutputSpec) -> ExtractedOutput:
        blocks = raw.raw or []
        if not spec.is_structured:
            text = "".join(block.text for block in blocks if block.type == "text")
            return ExtractedOutput(kind="text", text=text or None)

        text_parts = []
        for block in blocks:
            if block.type == "tool_use" and block.name == spec.schema_name:
                return ExtractedOutput(kind="json_value", value=block.input)
            if block.type == "text":
                text_parts.append(block.text)
        # A forced tool_choice still permits the model to answer in plain text; keep it for
        # diagnostics in the row's raw_output.
        return ExtractedOutput(kind="absent", text="".join(text_parts) or None)
