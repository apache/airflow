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
OpenAI batch adapter.

Talks to the ``openai`` SDK directly rather than through
``apache-airflow-providers-openai``: that provider's hook has no batch listing
or file-download methods, so reusing it would add a hard dependency without
the capabilities this needs. The SDK import is deferred to
:func:`_build_client` so importing this module never requires ``openai``.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterator
from datetime import datetime
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
from airflow.providers.common.ai.exceptions import LLMBatchLimitExceededError, LLMBatchModelMismatchError
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

log = structlog.get_logger(logger_name="task")

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchRequest
    from airflow.providers.common.ai.batch.output_schema import OutputSpec

_ENDPOINT = "/v1/chat/completions"

#: Cap on how many batches ``find_orphaned_batch`` scans looking for a metadata match. OpenAI's
#: list API has no server-side metadata filter, so the scan is client-side; the cap bounds the
#: worst case against an account with a very long batch history.
_ORPHAN_SCAN_LIMIT = 500

#: ``Batch.created_at`` is whole seconds while the intent record's write time carries
#: microseconds, so a batch created in the same second as the intent write could otherwise be
#: excluded by the ``not_before`` filter. The identity guarantee is the key plus fingerprint
#: match; ``not_before`` only trims how far back the scan looks.
_CLOCK_SKEW_SLACK_SECONDS = 300

#: OpenAI's batch-job statuses collapsed to the small set ``BatchState`` uses.
_STATUS_MAP: dict[str, BatchStatus] = {
    "validating": "in_progress",
    "in_progress": "in_progress",
    "finalizing": "in_progress",
    "cancelling": "in_progress",
    "completed": "completed",
    "failed": "failed",
    "expired": "expired",
    "cancelled": "cancelled",
}

#: Error-file ``error.code`` values OpenAI writes for requests it never ran, mapped to the
#: per-item status they mean. Anything else in the error file is a real per-request failure.
_ERROR_CODE_TO_PROVIDER_STATUS: dict[str, str] = {
    "batch_expired": "expired",
    "batch_cancelled": "cancelled",
}


def _build_client(api_key: str | None, base_url: str | None) -> Any:
    try:
        from openai import OpenAI
    except ImportError as e:
        raise AirflowOptionalProviderFeatureException(
            "OpenAI batch requires the openai SDK. Install with: "
            "pip install 'apache-airflow-providers-common-ai[openai]'"
        ) from e
    kwargs: dict[str, Any] = {}
    if api_key:
        kwargs["api_key"] = api_key
    if base_url:
        kwargs["base_url"] = base_url
    return OpenAI(**kwargs)


class OpenAIBatchAdapter(BatchAdapter):
    """
    Batch adapter for OpenAI's Batch API (``/v1/chat/completions``).

    :param api_key: Passed straight to the ``openai.OpenAI`` client. ``None``
        falls back to the SDK's own env-var resolution (``OPENAI_API_KEY``).
    :param base_url: Passed straight to the ``openai.OpenAI`` client. Pointing
        it at an OpenAI-compatible gateway that exposes ``/v1/files`` and
        ``/v1/batches`` routes the batch through that gateway.
    :param client: Inject a pre-built client (or a test double) instead of
        constructing one from ``api_key``/``base_url``.
    """

    name = "openai"
    max_requests = 50_000
    max_payload_bytes = 200_000_000
    allows_per_request_model = False

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        client: Any | None = None,
    ) -> None:
        self._client = client if client is not None else _build_client(api_key, base_url)

    def _iter_lines(
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
        """Yield one JSONL line per request; a generator so validation and submit never hold every line at once."""
        for index, request in enumerate(requests):
            messages = []
            effective_system_prompt = request.get("system_prompt") or system_prompt
            if effective_system_prompt:
                messages.append({"role": "system", "content": effective_system_prompt})
            messages.append({"role": "user", "content": request["prompt"]})
            resolved_model = self.resolve_request_model(
                request.get("model"), default_bare_model=model, request_index=index
            )
            # User params first, then the keys this adapter manages, so a stray "model" or
            # "messages" in request_params cannot bypass the model check or replace the prompt.
            # ``max_completion_tokens`` is the cap every current chat model accepts; ``max_tokens``
            # is deprecated and rejected by reasoning models such as gpt-5.
            body: dict[str, Any] = {
                **(request_params or {}),
                **(request.get("params") or {}),
                "model": resolved_model,
                "messages": messages,
                "max_completion_tokens": request.get("max_tokens") or max_tokens,
                **directive,
            }
            body.pop("max_tokens", None)
            yield {
                # "-" rather than ":" so both adapters share a separator that fits Anthropic's
                # ``^[a-zA-Z0-9_-]{1,64}$`` custom_id rule.
                "custom_id": f"{idempotency_key}-{index}",
                "method": "POST",
                "url": _ENDPOINT,
                "body": body,
            }

    def build_output_directive(self, spec: OutputSpec) -> dict[str, Any]:
        if not spec.is_structured:
            return {}
        # strict=False: strict mode requires rewriting the user's schema (every property
        # required, additionalProperties false), changing its meaning, and has no Anthropic
        # equivalent. Schema mismatches land as invalid_output rows on both providers alike.
        return {
            "response_format": {
                "type": "json_schema",
                "json_schema": {
                    "name": spec.schema_name,
                    "schema": spec.json_schema,
                    "strict": False,
                },
            }
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
                f"OpenAI batch accepts at most {self.max_requests} requests per batch; got "
                f"{len(requests)}. Split the input across mapped task instances with .expand() "
                f"so each instance submits at most {self.max_requests} requests."
            )

        # Resolve (and reject a foreign-provider override) before checking uniformity, so two
        # requests naming the same other provider's model cannot pass as "uniform".
        distinct_models = {
            self.resolve_request_model(r.get("model"), default_bare_model=model, request_index=i)
            for i, r in enumerate(requests)
        }
        if len(distinct_models) > 1:
            raise LLMBatchModelMismatchError(
                "OpenAI requires every request in a batch to use the same model; got "
                f"{len(distinct_models)} distinct models ({sorted(distinct_models)!r}). "
                "Per-request model overrides are supported by Anthropic only."
            )

        directive = self.build_output_directive(output_spec)
        directive_bytes = len(json.dumps(directive))
        total_bytes = 0
        lines = self._iter_lines(
            requests,
            model=model,
            idempotency_key="0" * 16,
            directive=directive,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            request_params=request_params,
        )
        for index, line in enumerate(lines):
            total_bytes += len(json.dumps(line).encode()) + 1
            if total_bytes > self.max_payload_bytes:
                raise LLMBatchLimitExceededError(
                    f"OpenAI batch accepts a payload of at most {self.max_payload_bytes} bytes; "
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
        completion_window: str = "24h",
        **kwargs: Any,
    ) -> SubmitResult:
        directive = self.build_output_directive(output_spec)
        lines = self._iter_lines(
            requests,
            model=model,
            idempotency_key=idempotency_key,
            directive=directive,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            request_params=request_params,
        )
        # Encode each line once, straight into the upload buffer: no intermediate list of dicts
        # and no intermediate str of the whole file.
        jsonl_bytes = b"".join(json.dumps(line).encode() + b"\n" for line in lines)
        uploaded = self._client.files.create(file=(f"{idempotency_key}.jsonl", jsonl_bytes), purpose="batch")
        batch = self._client.batches.create(
            input_file_id=uploaded.id,
            endpoint=_ENDPOINT,
            completion_window=completion_window,
            # Both values let find_orphaned_batch tell "this task instance" (idempotency_key,
            # stable across a clear) apart from "this exact input" (input_fingerprint). The
            # fingerprint is truncated to 16 hex characters; OpenAI caps metadata values at 512
            # characters, so the full digest would fit, but 16 is ample for a cross-check.
            metadata={"idempotency_key": idempotency_key, "input_fingerprint": input_fingerprint[:16]},
        )
        return SubmitResult(batch_id=batch.id, provider_input_ref=uploaded.id)

    def get_batch(self, batch_id: str) -> BatchState:
        batch = self._client.batches.retrieve(batch_id)
        counts = None
        request_counts = batch.request_counts
        if request_counts is not None:
            # OpenAI's breakdown is {completed, failed, total}: progress counters, not a
            # success/error split. Once the job has expired or been cancelled, whatever was
            # neither completed nor failed was never processed, so the remainder is attributed
            # to the job's own terminal status.
            remainder = max(request_counts.total - request_counts.completed - request_counts.failed, 0)
            counts = {
                "succeeded": request_counts.completed,
                "errored": request_counts.failed,
                "expired": remainder if batch.status == "expired" else 0,
                "cancelled": remainder if batch.status == "cancelled" else 0,
            }
        return BatchState(
            status=_STATUS_MAP.get(batch.status, "in_progress"),
            counts=counts,
            error_message=_format_batch_errors(batch.errors),
        )

    def cancel_batch(self, batch_id: str) -> None:
        self._client.batches.cancel(batch_id)

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        """
        Look for a batch whose metadata carries both identifiers, created at or after ``not_before``.

        :meth:`submit` records ``idempotency_key`` and ``input_fingerprint`` in
        the batch's metadata, so a batch whose submit response never made it
        back can be found and re-attached to. No server-side metadata filter
        exists, so this walks up to ``_ORPHAN_SCAN_LIMIT`` batches client-side;
        among every match the most recently created one wins.
        """
        not_before_epoch = (
            math.floor(datetime.fromisoformat(not_before).timestamp()) - _CLOCK_SKEW_SLACK_SECONDS
            if not_before
            else 0.0
        )
        best = None
        scanned = 0
        for batch in self._client.batches.list(limit=100):
            if scanned >= _ORPHAN_SCAN_LIMIT:
                log.warning(
                    "Stopped scanning for an orphaned batch after the scan limit; a matching batch may exist",
                    scan_limit=_ORPHAN_SCAN_LIMIT,
                    idempotency_key=idempotency_key,
                )
                break
            scanned += 1
            metadata = batch.metadata or {}
            if metadata.get("idempotency_key") != idempotency_key:
                continue
            if metadata.get("input_fingerprint") != input_fingerprint[:16]:
                continue
            if batch.created_at < not_before_epoch:
                continue
            if best is None or batch.created_at > best.created_at:
                best = batch
        return best.id if best is not None else None

    def iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        batch = self._client.batches.retrieve(batch_id)
        return self._iter_result_files(batch_id, batch.output_file_id, batch.error_file_id)

    def _iter_result_files(
        self, batch_id: str, output_file_id: str | None, error_file_id: str | None
    ) -> Iterator[RawResultItem]:
        # Stream the output file, then the error file, so both files' contents are never resident.
        if output_file_id:
            yield from self._iter_file_lines(batch_id, output_file_id)
        if error_file_id:
            yield from self._iter_file_lines(batch_id, error_file_id)

    def _iter_file_lines(self, batch_id: str, file_id: str) -> Iterator[RawResultItem]:
        # ``with_streaming_response`` is what makes the SDK stream the body; a plain
        # ``files.content()`` reads the whole file into memory before ``iter_lines`` runs.
        with self._client.files.with_streaming_response.content(file_id) as response:
            for line in response.iter_lines():
                if not line:
                    continue
                try:
                    yield self._parse_result_line(json.loads(line))
                except (KeyError, IndexError, ValueError, AttributeError, TypeError) as e:
                    # One malformed line must not abort the whole stream: the provider returns
                    # the same line on every retry, so the manifest could never be produced.
                    # results.py counts the index as "missing" once it is absent from ``seen``.
                    log.warning(
                        "Skipping unparsable result line for batch",
                        batch_id=batch_id,
                        file_id=file_id,
                        line=line[:200],
                        error=str(e),
                    )

    @staticmethod
    def _parse_result_line(payload: dict[str, Any]) -> RawResultItem:
        custom_id = payload["custom_id"]
        index = int(custom_id.rsplit("-", 1)[1])
        error = payload.get("error")
        response = payload.get("response") or {}
        body = response.get("body") or {}

        if error is not None or response.get("status_code") != 200:
            error_dict: dict[str, Any] = error if isinstance(error, dict) else {}
            raw_body_error = body.get("error")
            body_error: dict[str, Any] = raw_body_error if isinstance(raw_body_error, dict) else {}
            message = (
                error_dict.get("message")
                or body_error.get("message")
                or (str(error) if error is not None else f"HTTP {response.get('status_code')}")
            )
            code = error_dict.get("code") or body_error.get("code")
            provider_status = _ERROR_CODE_TO_PROVIDER_STATUS.get(str(code), "errored")
            return RawResultItem(
                custom_id=custom_id,
                index=index,
                provider_status=provider_status,  # type: ignore[arg-type]
                model=body.get("model"),
                usage=None,
                finish_reason=None,
                error={
                    "type": "provider_error" if provider_status == "errored" else provider_status,
                    "message": message,
                    "provider_code": str(code)
                    if code is not None
                    else str(response.get("status_code") or "") or None,
                    "stage": "provider",
                },
                raw=None,
            )

        choice = (body.get("choices") or [{}])[0]
        message_content = (choice.get("message") or {}).get("content")
        usage = body.get("usage") or {}
        return RawResultItem(
            custom_id=custom_id,
            index=index,
            provider_status="success",
            model=body.get("model"),
            usage={
                "input_tokens": usage.get("prompt_tokens", 0),
                "output_tokens": usage.get("completion_tokens", 0),
            },
            finish_reason=choice.get("finish_reason"),
            error=None,
            raw=message_content,
        )

    def extract_output(self, raw: RawResultItem, spec: OutputSpec) -> ExtractedOutput:
        if raw.raw is None:
            # A refusal, or a reasoning model that spent its whole token budget before emitting
            # visible output. Either way there is no content to hand back as a success.
            return ExtractedOutput(kind="absent")
        if not spec.is_structured:
            return ExtractedOutput(kind="text", text=raw.raw)
        return ExtractedOutput(kind="json_text", text=raw.raw)


def _format_batch_errors(errors: Any) -> str | None:
    """Render OpenAI's ``Batch.errors`` as ``code: message`` pairs instead of a pydantic repr."""
    data = getattr(errors, "data", None) or []
    rendered = "; ".join(
        f"{getattr(e, 'code', None) or 'error'}: {getattr(e, 'message', None) or ''}".rstrip(": ")
        for e in data
    )
    return rendered or None
