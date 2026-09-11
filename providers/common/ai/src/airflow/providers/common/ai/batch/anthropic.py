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

Talks to the ``anthropic`` SDK directly (D1, same reasoning as
``batch/openai.py``). The SDK import is deferred to
:meth:`AnthropicBatchAdapter.__init__`.

R8 (checked against ``anthropic`` 1.5.0's actual
``messages.batches.create`` signature -- ``requests``, ``user_profile_id``,
``workspace_id`` only): **the SDK has no batch-level ``metadata`` parameter.**
Unlike OpenAI, there is nowhere to put the idempotency key except the
``custom_id`` prefix every request already carries (§5.4) -- this is not a
gap, ``custom_id`` already covers the recognition need.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

import structlog

from airflow.providers.common.ai.batch.base import (
    BatchAdapter,
    BatchState,
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
)
from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError, UnsupportedBatchProviderError

log = structlog.get_logger(logger_name="task")

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchRequest
    from airflow.providers.common.ai.batch.output_schema import OutputSpec

#: Anthropic's Message Batches API only ever reports "in_progress"/"canceling"/"ended"
#: at the job level (checked against the SDK's ``MessageBatch.processing_status`` type) --
#: unlike OpenAI, there is no separate "failed"/"expired" job status, and "ended" alone does
#: not distinguish a normal completion from a cancelled one (B4) -- see ``_batch_status``.
_STATUS_MAP: dict[str, str] = {
    "in_progress": "in_progress",
    "canceling": "in_progress",
    "ended": "completed",
}


def _batch_status(batch: Any) -> str:
    """
    Resolve ``batch.processing_status`` to this adapter's status vocabulary (B4).

    Anthropic's own ``processing_status`` conflates "completed normally" and "completed after
    being cancelled" into the same ``"ended"`` value -- the only signal that a batch now sitting
    at ``"ended"`` was actually cancelled is ``cancel_initiated_at`` being set (a real
    ``MessageBatch`` field: ``id/archived_at/cancel_initiated_at/created_at/ended_at/expires_at/
    processing_status/request_counts/results_url/type``). Without this check, a cancelled
    Anthropic batch would be reported as ``"completed"`` and its manifest would say
    ``terminal_reason: "partial"`` instead of ``"cancelled"`` -- the same outcome reported two
    different ways depending on which provider ran it, exactly what §8's status matrix exists to
    avoid.
    """
    if batch.processing_status == "ended" and getattr(batch, "cancel_initiated_at", None) is not None:
        return "cancelled"
    return _STATUS_MAP.get(batch.processing_status, "in_progress")


def _build_client(api_key: str | None, base_url: str | None) -> Any:
    try:
        from anthropic import Anthropic
    except ImportError as e:
        raise UnsupportedBatchProviderError(
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
        constructing one from ``api_key``/``base_url``. Not part of the
        public ``@task.llm_batch`` surface -- only the operator and tests
        use this.
    """

    name = "anthropic"
    max_requests = 100_000
    #: N7: unconfirmed. The plan's source for this figure could not be re-located on Anthropic's
    #: current public docs page for the Message Batches API during this round's review; treated
    #: as a conservative placeholder, not a verified limit. Do not cite this number as
    #: authoritative without re-checking against Anthropic's docs first.
    max_payload_bytes = 256_000_000
    #: D3: Anthropic has no batch-level "one model per batch" requirement -- a
    #: per-request ``model`` override just goes into that request's own ``params``.
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
        # D3/M10: unlike OpenAI, Anthropic allows each request its own model -- but it must
        # still resolve to *this* adapter (raises LLMBatchModelMismatchError otherwise), and
        # the resolved value must be the bare model name, never the "anthropic:" prefixed form.
        resolved_model = self.resolve_request_model(
            request.get("model"), default_bare_model=model, request_index=request_index
        )
        params: dict[str, Any] = {
            "model": resolved_model,
            "max_tokens": request.get("max_tokens") or max_tokens,
            "messages": [{"role": "user", "content": request["prompt"]}],
            **(request_params or {}),
            **(request.get("params") or {}),
            **directive,
        }
        if effective_system_prompt:
            params["system"] = effective_system_prompt
        return params

    def _build_requests(
        self,
        requests: list[BatchRequest],
        *,
        model: str,
        idempotency_key: str,
        directive: dict[str, Any],
        system_prompt: str,
        max_tokens: int,
        request_params: dict[str, Any] | None,
    ) -> list[dict[str, Any]]:
        return [
            {
                # M9: not ":" -- Anthropic's "Create a Message Batch" API docs (custom_id:
                # pattern ^[a-zA-Z0-9_-]{1,64}$, minLength 1, maxLength 64; verified 2026-09-11)
                # exclude it; the installed SDK itself does not encode this regex, so the docs
                # page is the source of truth here, not the SDK. "-" keeps both adapters
                # consistent (R3-5).
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
            for index, request in enumerate(requests)
        ]

    def build_output_directive(self, spec: OutputSpec) -> dict[str, Any]:
        if not spec.is_structured:
            return {}
        # Single tool + forced tool_choice (§10.3): not giving the model a choice turns
        # "produce structured output" from something it can decline into a hard format
        # constraint. tool_choice="auto" or multiple tools would only raise the invalid_output
        # rate without buying anything.
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
        built = self._build_requests(
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
        built = self._build_requests(
            requests,
            model=model,
            idempotency_key=idempotency_key,
            directive=directive,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            request_params=request_params,
        )
        batch = self._client.messages.batches.create(requests=built)
        # No provider-side upload step for Anthropic -- requests go inline in the create
        # call, so there is no file id to track for cleanup.
        return SubmitResult(batch_id=batch.id, provider_input_ref=None)

    def get_batch(self, batch_id: str) -> BatchState:
        batch = self._client.messages.batches.retrieve(batch_id)
        counts = None
        request_counts = getattr(batch, "request_counts", None)
        if request_counts is not None:
            # Normalized to the {succeeded, errored, expired, cancelled} shape the trigger's
            # TriggerEvent uses regardless of adapter (§4); "processing" is dropped since a
            # get_batch() call only reaches here once the job itself has ended.
            counts = {
                "succeeded": request_counts.succeeded,
                "errored": request_counts.errored,
                "expired": request_counts.expired,
                "cancelled": request_counts.canceled,
            }
        return BatchState(
            status=_batch_status(batch),
            counts=counts,
            error_message=None,
        )

    def cancel_batch(self, batch_id: str) -> None:
        self._client.messages.batches.cancel(batch_id)

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        """
        Return ``None`` unconditionally -- Anthropic gives no way to recover a Phase A orphan (M3).

        Confirmed against the installed SDK: ``messages.batches.create()`` has no batch-level
        ``metadata`` parameter (R8), and ``messages.batches.list()`` has no way to filter or
        search by ``custom_id`` either. There is therefore no query that can distinguish "the
        batch this orphaned intent record was trying to submit" from any other batch on the
        account -- ``not_before`` (N1) would narrow candidates the same way OpenAI's adapter
        does, but there is nothing to narrow: with zero identifying signal, adding a time filter
        cannot turn "no way to find it" into "found it". This is a real, documented gap (unlike
        OpenAI's adapter, which recovers via ``metadata``): a crash between "submit sent" and
        "submit response received" for an Anthropic batch cannot be automatically reconciled,
        and will result in a duplicate submission on the next attempt. Tracked as an accepted
        limitation, not silently papered over with a lookup that cannot actually work.
        """
        return None

    def iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        # Return (not yield) so a bad batch_id raises immediately at call time (§3),
        # matching AnthropicHook.stream_batch_results' own convention.
        return self._iter_results(batch_id)

    def _iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        for item in self._client.messages.batches.results(batch_id):
            try:
                yield self._parse_result_item(item)
            except (AttributeError, IndexError, ValueError) as e:
                # A malformed custom_id must not abort the whole stream (M7): one bad item
                # would otherwise make this batch's manifest impossible to ever produce, even
                # on retry -- the provider returns the same malformed item every time.
                log.warning(
                    "Skipping unparsable result item for batch",
                    custom_id=getattr(item, "custom_id", None),
                    error=str(e),
                )

    #: Anthropic's per-item ``result.type`` ("errored"/"canceled"/"expired") -> this module's
    #: ``RawResultItem.provider_status`` vocabulary (N2). "canceled" (Anthropic's spelling, one
    #: "l") is normalized to "cancelled" (this codebase's spelling elsewhere) for the internal
    #: value; the original provider spelling is preserved in the row's ``error.type`` field so
    #: nothing about the provider's own wording is lost, only normalized for internal dispatch.
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
            # N2: "canceled"/"expired" get their own provider_status, not folded into "errored" --
            # a batch that was cancelled or hit its SLA still billed for, and reported, these
            # items distinctly from an actual provider-side failure (rate limit, bad request).
            # Collapsing them into "errored" is exactly the defect this fix addresses: it made
            # `merge_counts["missing"]` structurally unable to ever be nonzero for Anthropic,
            # which made the expired/cancelled relabeling in assemble_manifest permanently inert.
            provider_status = cls._RESULT_TYPE_TO_PROVIDER_STATUS.get(result.type, "errored")
            message = getattr(getattr(result, "error", None), "message", None) or f"batch item {result.type}"
            return RawResultItem(
                custom_id=custom_id,
                index=index,
                provider_status=provider_status,
                model=None,
                usage=None,
                finish_reason=None,
                error={"type": result.type, "message": message, "provider_code": None, "stage": "provider"},
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
        if not spec.is_structured:
            text = "".join(block.text for block in raw.raw if getattr(block, "type", None) == "text")
            return ExtractedOutput(kind="text", text=text)

        text_parts = []
        for block in raw.raw or []:
            if (
                getattr(block, "type", None) == "tool_use"
                and getattr(block, "name", None) == spec.schema_name
            ):
                return ExtractedOutput(kind="json_value", value=block.input)
            if getattr(block, "type", None) == "text":
                text_parts.append(block.text)
        # Forced tool_choice still permits the model to answer in plain text (§10.3) --
        # keep any text content so it lands in raw_output for diagnostics.
        return ExtractedOutput(kind="absent", text="".join(text_parts) or None)
