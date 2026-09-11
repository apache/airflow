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

Talks to the ``openai`` SDK directly (D1: common.ai does not depend on
``apache-airflow-providers-openai`` -- that provider's hook has no
``list_batches``/file-download methods and reuse would buy a hard dependency
without buying the capabilities this needs). The SDK import is deferred to
:meth:`OpenAIBatchAdapter.__init__` so importing this module never requires
``openai`` to be installed; see :func:`_build_client`.
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
    ExtractedOutput,
    RawResultItem,
    SubmitResult,
)
from airflow.providers.common.ai.exceptions import (
    LLMBatchLimitExceededError,
    LLMBatchModelMismatchError,
    UnsupportedBatchProviderError,
)

log = structlog.get_logger(logger_name="task")

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchRequest
    from airflow.providers.common.ai.batch.output_schema import OutputSpec

_ENDPOINT = "/v1/chat/completions"

#: Cap on how many batches ``find_orphaned_batch`` scans looking for a metadata match -- OpenAI's
#: list API has no server-side metadata filter (checked against the installed SDK's
#: ``Batches.list`` signature: only ``after``/``limit`` cursor params), so this is O(scanned
#: batches) client-side, not O(1). Default list ordering is not verified here (not documented in
#: the installed SDK's docstring); the cap exists purely to bound worst-case cost against an
#: account with a very long batch history, not because recency is assumed.
_ORPHAN_SCAN_LIMIT = 500

#: R3-1: OpenAI's ``Batch.created_at`` is whole seconds (checked against the installed SDK's
#: ``openai/types/batch.py`` -- ``created_at: int``), while ``not_before`` (the Phase A intent
#: record's write time) carries microsecond precision. Comparing them directly means a batch
#: created in the same second as (but a few hundred microseconds before) the intent write would
#: fail ``created_at < not_before_epoch`` and get silently excluded -- the exact orphan this
#: recovery exists to find, missed roughly half the time purely from truncation, not from it
#: actually being a different, older batch. Subtracting this slack does not reopen N1: the
#: identity guarantee against recovering an unrelated older batch is the idempotency_key +
#: input_fingerprint match above, not this timestamp filter -- ``not_before`` only trims how far
#: back the scan bothers looking among already-key-and-fingerprint-matched candidates.
_CLOCK_SKEW_SLACK_SECONDS = 300

#: OpenAI's batch-job statuses collapsed to the small set ``BatchState`` uses.
#: "validating"/"finalizing"/"cancelling" are all still-running from the
#: caller's point of view -- only "completed"/"failed"/"expired"/"cancelled"
#: are terminal.
_STATUS_MAP: dict[str, str] = {
    "validating": "in_progress",
    "in_progress": "in_progress",
    "finalizing": "in_progress",
    "cancelling": "in_progress",
    "completed": "completed",
    "failed": "failed",
    "expired": "expired",
    "cancelled": "cancelled",
}


def _build_client(api_key: str | None, base_url: str | None) -> Any:
    try:
        from openai import OpenAI
    except ImportError as e:
        raise UnsupportedBatchProviderError(
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
    :param base_url: Passed straight to the ``openai.OpenAI`` client.
    :param client: Inject a pre-built client (or a test double) instead of
        constructing one from ``api_key``/``base_url``. Not part of the
        public ``@task.llm_batch`` surface -- only the operator and tests
        use this.
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

    def _build_lines(
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
        lines = []
        for index, request in enumerate(requests):
            messages = []
            effective_system_prompt = request.get("system_prompt") or system_prompt
            if effective_system_prompt:
                messages.append({"role": "system", "content": effective_system_prompt})
            messages.append({"role": "user", "content": request["prompt"]})
            # D3/M10: a per-request model override must resolve to *this* adapter (raises
            # LLMBatchModelMismatchError otherwise) -- never passed through unresolved.
            resolved_model = self.resolve_request_model(
                request.get("model"), default_bare_model=model, request_index=index
            )
            body: dict[str, Any] = {
                "model": resolved_model,
                "messages": messages,
                "max_tokens": request.get("max_tokens") or max_tokens,
                **(request_params or {}),
                **(request.get("params") or {}),
                **directive,
            }
            lines.append(
                {
                    # M9: not ":" -- Anthropic's documented custom_id character set is
                    # ^[a-zA-Z0-9_-]{1,64}$ (could not independently verify from the installed
                    # SDK, which does not encode the server-side regex; treated as authoritative
                    # per the review finding rather than risk it). "-" keeps both adapters using
                    # the same separator and format.
                    "custom_id": f"{idempotency_key}-{index}",
                    "method": "POST",
                    "url": _ENDPOINT,
                    "body": body,
                }
            )
        return lines

    def build_output_directive(self, spec: OutputSpec) -> dict[str, Any]:
        if not spec.is_structured:
            return {}
        # v1 fixes strict=False deliberately (Non-goal 8 / plan §10.2): opting in would
        # require rewriting the user's schema to satisfy OpenAI's strict-mode constraints
        # (all properties required, additionalProperties: false), changing its declared
        # semantics, and it has no Anthropic equivalent -- keeping it off keeps both
        # providers exercising the same invalid_output path in the same proportion.
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

        # M10: resolve (and reject a foreign-provider override) before checking uniformity --
        # otherwise two requests that both (wrongly) name the same *other* provider's model
        # would pass the uniformity check and get silently submitted to OpenAI under that name.
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
        lines = self._build_lines(
            requests,
            model=model,
            idempotency_key="0" * 16,
            directive=directive,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            request_params=request_params,
        )
        for index, line in enumerate(lines):
            total_bytes += len(json.dumps(line).encode())
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
        lines = self._build_lines(
            requests,
            model=model,
            idempotency_key=idempotency_key,
            directive=directive,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            request_params=request_params,
        )
        jsonl_bytes = ("\n".join(json.dumps(line) for line in lines) + "\n").encode()
        uploaded = self._client.files.create(file=(f"{idempotency_key}.jsonl", jsonl_bytes), purpose="batch")
        batch = self._client.batches.create(
            input_file_id=uploaded.id,
            endpoint=_ENDPOINT,
            completion_window=completion_window,
            # N1: fingerprint alongside the key -- find_orphaned_batch must be able to tell "this
            # task instance" (idempotency_key, stable across a clear) apart from "this exact
            # input" (input_fingerprint). Truncated to 16 hex chars: OpenAI metadata values cap at
            # 512 characters (checked against ``hooks/openai.py:619``'s ``create_batch`` docstring),
            # so the full 64-char sha256 digest would fit, but 16 is already a vanishingly small
            # collision risk for a courtesy cross-check and keeps the metadata payload small.
            metadata={"idempotency_key": idempotency_key, "input_fingerprint": input_fingerprint[:16]},
        )
        return SubmitResult(batch_id=batch.id, provider_input_ref=uploaded.id)

    def get_batch(self, batch_id: str) -> BatchState:
        batch = self._client.batches.retrieve(batch_id)
        counts = None
        request_counts = getattr(batch, "request_counts", None)
        if request_counts is not None:
            # OpenAI's own breakdown is {completed, failed, total} -- progress counters, not a
            # success/error split (per-item outcome is only knowable from output_file_id/
            # error_file_id, which the trigger deliberately never downloads, per §4).
            # "completed"/"failed" map straight across. "expired"/"cancelled" have no dedicated
            # OpenAI-side per-item counter (N2) -- once the job itself has reached one of those
            # terminal statuses, whatever wasn't completed or failed was never processed, so the
            # remainder (total - completed - failed) is attributed to whichever terminal status
            # the job itself reached. Do not attribute it to both: only one of "expired"/
            # "cancelled" is ever nonzero for a given batch, matching ``batch.status``.
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
            error_message=str(batch.errors) if getattr(batch, "errors", None) else None,
        )

    def cancel_batch(self, batch_id: str) -> None:
        self._client.batches.cancel(batch_id)

    def find_orphaned_batch(
        self, idempotency_key: str, input_fingerprint: str, not_before: str
    ) -> str | None:
        """
        Look for a matching batch, created at or after ``not_before`` minus a clock-skew slack.

        (N1, R3-1 -- see ``_CLOCK_SKEW_SLACK_SECONDS``.) Recovery for a Phase A orphan (M3): the
        OpenAI adapter's own ``submit()`` sets
        ``metadata={"idempotency_key": ..., "input_fingerprint": ...}`` (see :meth:`submit`), so
        a batch that was submitted but whose response never made it back can still be found and
        re-attached to, instead of blindly resubmitted -- and, critically, *not* an older batch
        for the same task instance whose content has since changed (idempotency_key is stable
        across a ``clear`` with different prompts; input_fingerprint is not). No server-side
        metadata filter exists (see ``_ORPHAN_SCAN_LIMIT``), so this walks up to that many batches
        client-side; among every match, the most recently created one wins, since listing order
        is not guaranteed to be recency-first.
        """
        not_before_epoch = (
            math.floor(datetime.fromisoformat(not_before).timestamp()) - _CLOCK_SKEW_SLACK_SECONDS
            if not_before
            else 0.0
        )
        best: Any = None
        scanned = 0
        for batch in self._client.batches.list(limit=100):
            if scanned >= _ORPHAN_SCAN_LIMIT:
                break
            scanned += 1
            metadata = getattr(batch, "metadata", None) or {}
            if metadata.get("idempotency_key") != idempotency_key:
                continue
            if metadata.get("input_fingerprint") != input_fingerprint[:16]:
                continue
            created_at = getattr(batch, "created_at", 0) or 0
            if created_at < not_before_epoch:
                continue
            if best is None or created_at > getattr(best, "created_at", 0):
                best = batch
        return best.id if best is not None else None

    def iter_results(self, batch_id: str) -> Iterator[RawResultItem]:
        batch = self._client.batches.retrieve(batch_id)
        return self._iter_result_files(
            getattr(batch, "output_file_id", None), getattr(batch, "error_file_id", None)
        )

    def _iter_result_files(
        self, output_file_id: str | None, error_file_id: str | None
    ) -> Iterator[RawResultItem]:
        # Stream the output file, then the error file, one at a time -- the memory bound
        # (§6) means never holding both files' parsed contents at once.
        if output_file_id:
            yield from self._iter_file_lines(output_file_id)
        if error_file_id:
            yield from self._iter_file_lines(error_file_id)

    def _iter_file_lines(self, file_id: str) -> Iterator[RawResultItem]:
        response = self._client.files.content(file_id)
        for line in response.iter_lines():
            if not line:
                continue
            payload = json.loads(line)
            try:
                yield self._parse_result_line(payload)
            except (KeyError, IndexError, ValueError) as e:
                # A malformed custom_id must not abort the whole stream (M7): one bad line
                # would otherwise make this batch's manifest impossible to ever produce, even
                # on retry -- the provider will return the same malformed line every time.
                # results.py's reconciliation counts this index as "missing" once it is absent
                # from `seen`, same as if the provider had dropped the row entirely.
                log.warning(
                    "Skipping unparsable result line for batch",
                    custom_id=payload.get("custom_id"),
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
            message = error.get("message") if isinstance(error, dict) else str(error or response)
            return RawResultItem(
                custom_id=custom_id,
                index=index,
                provider_status="errored",
                model=body.get("model"),
                usage=None,
                finish_reason=None,
                error={
                    "type": "provider_error",
                    "message": message,
                    "provider_code": str(response.get("status_code")) if response else None,
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
        if not spec.is_structured:
            return ExtractedOutput(kind="text", text=raw.raw)
        if raw.raw is None:
            return ExtractedOutput(kind="absent")
        return ExtractedOutput(kind="json_text", text=raw.raw)
