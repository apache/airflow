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
Streaming result merge, JSONL landing, and XCom manifest assembly for ``@task.llm_batch``.

Memory-bounded streaming is a hard requirement, not an optimization: a batch
can have up to 100,000 results, each up to a few KB, so materializing the
full result set (``list()``, ``sorted()``, or otherwise) risks running the
worker out of memory. The only structure this module keeps resident for the
whole batch is ``seen``, a ``set[int]`` of indexes already written (~4MB for
100k ints) -- everything else is processed and written one item at a time, in
whatever order the provider streams it in (``ordered: false`` in the
manifest is an honest declaration of this, not an apology for it).
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import structlog

from airflow.providers.common.ai.batch.base import evaluate_batch_counts
from airflow.providers.common.ai.batch.output_schema import validate_extracted_output

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchAdapter, RawResultItem
    from airflow.providers.common.ai.batch.output_schema import OutputSpec
    from airflow.sdk import ObjectStoragePath

log = structlog.get_logger(logger_name="task")

STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
STATUS_INVALID_OUTPUT = "invalid_output"
STATUS_EXPIRED = "expired"
STATUS_CANCELLED = "cancelled"
STATUS_MISSING = "missing"

#: The per-item statuses this module ever writes to a JSONL row. ``expired``/``cancelled`` are
#: per-item statuses (N2) for a provider that reports them at that granularity (Anthropic does);
#: a provider that only reports them at the job level (OpenAI -- an expired/cancelled item is
#: simply absent from the result stream, indistinguishable from any other never-processed index)
#: still lands those requests in ``STATUS_MISSING`` here, and :func:`assemble_manifest` re-labels
#: the unexplained portion of ``missing`` from the job-level terminal event's own counts (M5/M8).
#: A batch that timed out (our own wall-clock budget, batch still ``in_progress``) fails the task
#: before any results are streamed at all and never reaches this module at all.
_MERGE_STATUS_KEYS = (
    STATUS_SUCCESS,
    STATUS_ERROR,
    STATUS_INVALID_OUTPUT,
    STATUS_EXPIRED,
    STATUS_CANCELLED,
    STATUS_MISSING,
)

#: ``RawResultItem.provider_status`` (base.py: ``"success" | "errored" | "expired" | "cancelled"``)
#: -> the row-status vocabulary above. Only "errored" spells differently ("error"); "expired"/
#: "cancelled" are already row statuses in both vocabularies, but named here anyway so this map
#: is the one place both need to stay in sync.
_PROVIDER_STATUS_TO_ROW_STATUS: dict[str, str] = {
    "errored": STATUS_ERROR,
    "expired": STATUS_EXPIRED,
    "cancelled": STATUS_CANCELLED,
}


def _row_for_success(raw: RawResultItem, output: Any) -> dict[str, Any]:
    return {
        "custom_id": raw.custom_id,
        "index": raw.index,
        "status": STATUS_SUCCESS,
        "output": output,
        "raw_output": None,
        "error": None,
        "model": raw.model,
        "usage": raw.usage,
        "finish_reason": raw.finish_reason,
    }


def _row_for_provider_terminal(raw: RawResultItem, *, status: str) -> dict[str, Any]:
    """Shared builder for ``"error"``/``"expired"``/``"cancelled"`` rows -- same shape, different status."""
    return {
        "custom_id": raw.custom_id,
        "index": raw.index,
        "status": status,
        "output": None,
        "raw_output": None,
        "error": raw.error,
        "model": raw.model,
        "usage": raw.usage,
        "finish_reason": raw.finish_reason,
    }


def _row_for_invalid_output(
    raw: RawResultItem, *, raw_output: str | None, error_message: str | None
) -> dict[str, Any]:
    return {
        "custom_id": raw.custom_id,
        "index": raw.index,
        "status": STATUS_INVALID_OUTPUT,
        "output": None,
        "raw_output": raw_output,
        "error": {
            "type": "output_validation_error",
            "message": error_message,
            "provider_code": None,
            "stage": "output_validation",
        },
        "model": raw.model,
        "usage": raw.usage,
        "finish_reason": raw.finish_reason,
    }


def _row_for_missing(index: int, *, custom_id_prefix: str) -> dict[str, Any]:
    return {
        "custom_id": f"{custom_id_prefix}-{index}",
        "index": index,
        "status": STATUS_MISSING,
        "output": None,
        "raw_output": None,
        "error": None,
        "model": None,
        "usage": None,
        "finish_reason": None,
    }


def build_result_row(raw: RawResultItem, adapter: BatchAdapter, spec: OutputSpec) -> dict[str, Any]:
    """
    Build one JSONL row from a provider-native result.

    Non-success outcomes (``raw.provider_status in ("errored", "expired", "cancelled")``) never go
    through output validation -- there is nothing to validate, the request itself never produced
    usable output (N2: ``"expired"``/``"cancelled"`` are their own per-item statuses, not folded
    into ``"errored"`` -- a provider that reports them at item granularity is telling you *why*
    that item has no output, and collapsing that into a generic provider error would make it
    indistinguishable from an actual API failure like rate limiting). Only a successful provider
    response is extracted and validated against ``spec``, and validation failure produces
    ``status: "invalid_output"``, never an exception: a model returning schema-non-conforming JSON
    is expected batch data, not a reason to abort the merge.
    """
    # RawResultItem.provider_status uses "errored" (base.py); the JSONL row status vocabulary
    # (§6) uses "error" -- translate, don't compare the two vocabularies directly.
    row_status = _PROVIDER_STATUS_TO_ROW_STATUS.get(raw.provider_status)
    if row_status is not None:
        return _row_for_provider_terminal(raw, status=row_status)

    extracted = adapter.extract_output(raw, spec)
    outcome = validate_extracted_output(extracted, spec)
    if outcome.ok:
        return _row_for_success(raw, outcome.value)
    return _row_for_invalid_output(raw, raw_output=outcome.raw_text, error_message=outcome.error_message)


@dataclass(frozen=True)
class MergeDiagnostics:
    """
    Anomalies encountered while merging (M7), surfaced separately from ``counts``.

    Never inflates the officially reconciled ``counts`` -- visible instead of silently dropped
    or silently double-counted.
    """

    duplicate_result_count: int = 0
    out_of_range_result_count: int = 0


def stream_results_to_jsonl(
    *,
    adapter: BatchAdapter,
    batch_id: str,
    output_spec: OutputSpec,
    request_count: int,
    custom_id_prefix: str,
    destination: ObjectStoragePath,
) -> tuple[dict[str, int], MergeDiagnostics]:
    """
    Stream every result for ``batch_id`` to ``destination`` as JSONL, one row per input index.

    Every index in ``range(request_count)`` gets exactly one row: results
    that never appeared in the provider stream (dropped by the provider, or
    genuinely never processed) are filled in as ``status: "missing"`` once
    the stream is exhausted. Validation happens inline, per item, as results
    arrive -- not after collecting them all -- so the memory bound holds for
    the invalid-output case too.

    Two defensive checks (M7) keep a single anomalous item from corrupting the whole batch's
    accounting or making the manifest impossible to ever produce:

    - An index the adapter yields **twice** is written once (the first occurrence); the repeat
      is dropped and counted in ``MergeDiagnostics.duplicate_result_count``, never double-counted
      into ``counts``.
    - An index **outside** ``range(request_count)`` (a corrupt/foreign ``custom_id``) is dropped
      entirely -- it cannot be rejoined to any input -- and counted in
      ``MergeDiagnostics.out_of_range_result_count``.

    Without this, either anomaly would inflate the total past ``request_count`` and make
    :func:`assemble_manifest`'s reconciliation check raise on *every* attempt to finalize this
    batch, including every retry -- the dirty data never goes away on its own.

    :return: a tuple of (per-status row counts for ``"success"``/``"error"``/``"invalid_output"``/
        ``"expired"``/``"cancelled"``/``"missing"``, diagnostics for anomalies excluded from those
        counts).
    """
    counts = dict.fromkeys(_MERGE_STATUS_KEYS, 0)
    seen: set[int] = set()
    duplicate_result_count = 0
    out_of_range_result_count = 0

    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w") as fh:
        for raw in adapter.iter_results(batch_id):
            if not (0 <= raw.index < request_count):
                out_of_range_result_count += 1
                log.warning(
                    "Dropping out-of-range result index for batch",
                    batch_id=batch_id,
                    index=raw.index,
                    request_count=request_count,
                    custom_id=raw.custom_id,
                )
                continue
            if raw.index in seen:
                duplicate_result_count += 1
                log.warning(
                    "Dropping duplicate result index for batch",
                    batch_id=batch_id,
                    index=raw.index,
                    custom_id=raw.custom_id,
                )
                continue

            row = build_result_row(raw, adapter, output_spec)
            fh.write(json.dumps(row) + "\n")
            seen.add(raw.index)
            counts[row["status"]] += 1

        for index in sorted(set(range(request_count)) - seen):
            row = _row_for_missing(index, custom_id_prefix=custom_id_prefix)
            fh.write(json.dumps(row) + "\n")
            counts[STATUS_MISSING] += 1

    return counts, MergeDiagnostics(
        duplicate_result_count=duplicate_result_count,
        out_of_range_result_count=out_of_range_result_count,
    )


def output_type_ref(spec: OutputSpec) -> str | None:
    if not spec.is_structured:
        return None
    module = getattr(spec.output_type, "__module__", None)
    qualname = getattr(spec.output_type, "__qualname__", None) or getattr(spec.output_type, "__name__", None)
    if module is None or qualname is None:
        return str(spec.output_type)
    return f"{module}.{qualname}"


def assemble_manifest(
    *,
    batch_id: str,
    adapter_name: str,
    llm_conn_id: str,
    model_id: str | None,
    output_spec: OutputSpec,
    result_uri: str,
    request_count: int,
    merge_counts: Mapping[str, int],
    extra_counts: Mapping[str, int] | None = None,
    merge_diagnostics: MergeDiagnostics | None = None,
    custom_id_prefix: str,
    submitted_at: str,
    completed_at: str,
) -> dict[str, Any]:
    """
    Assemble the XCom manifest (the sole XCom payload of ``@task.llm_batch`` -- results never are).

    ``merge_counts`` is the per-item breakdown from :func:`stream_results_to_jsonl` -- now
    including ``"expired"``/``"cancelled"`` when a provider reports those at item granularity
    (N2; Anthropic does). ``extra_counts`` carries the job-level ``{"expired", "cancelled"}``
    counts the trigger/operator reports for this batch's terminal event (M8) -- for a provider
    that has *no* per-item signal for these (OpenAI: an expired/cancelled item is simply absent
    from the result stream), this is the only source of that information. ``None``/absent keys
    default to ``0``.

    M5: an ``expired`` or ``cancelled`` terminal event still reaches this function (unlike a
    plain "timeout", which fails the task before any results are fetched) precisely so partial,
    already-billed results are not discarded.

    The two sources are combined without double-counting (N2): per-item counts
    (``merge_counts["expired"]``/``["cancelled"]``) are taken as-is; ``extra_counts`` only tops up
    whatever ``merge_counts["missing"]`` still has left *after* subtracting whatever the job-level
    figure already agrees was accounted for per-item. For OpenAI (no per-item signal, so
    ``merge_counts["expired"] == 0`` always) this reduces to the original "relabel from missing"
    behavior; for Anthropic (which does report per-item) it does not double-add on top of counts
    that already made it into a distinct row status via :func:`build_result_row`. Any part of
    ``extra_counts`` that ``merge_counts["missing"]`` cannot cover is dropped rather than allowed
    to break reconciliation -- the provider's self-reported figure and our own row-level count are
    not required to agree bit-for-bit, only to never overcount past ``request_count``.

    :raises ValueError: the counts do not reconcile against
        ``request_count`` -- every request must land in exactly one bucket.
    """
    extra = extra_counts or {}
    diagnostics = merge_diagnostics or MergeDiagnostics()

    merge_expired = merge_counts.get(STATUS_EXPIRED, 0)
    merge_cancelled = merge_counts.get(STATUS_CANCELLED, 0)
    unexplained = merge_counts.get(STATUS_MISSING, 0)

    expired_shortfall = max(extra.get("expired", 0) - merge_expired, 0)
    relabel_expired = min(unexplained, expired_shortfall)
    unexplained -= relabel_expired

    cancelled_shortfall = max(extra.get("cancelled", 0) - merge_cancelled, 0)
    relabel_cancelled = min(unexplained, cancelled_shortfall)
    unexplained -= relabel_cancelled

    full_counts = {
        "succeeded": merge_counts.get(STATUS_SUCCESS, 0),
        "errored": merge_counts.get(STATUS_ERROR, 0),
        "invalid_output": merge_counts.get(STATUS_INVALID_OUTPUT, 0),
        "expired": merge_expired + relabel_expired,
        "cancelled": merge_cancelled + relabel_cancelled,
        "missing": unexplained,
    }
    total = sum(full_counts.values())
    if total != request_count:
        raise ValueError(
            f"Batch {batch_id!r} result counts ({total}) do not reconcile against "
            f"request_count ({request_count}): {full_counts}"
        )

    if full_counts["expired"] > 0:
        terminal_reason = "expired"
    elif full_counts["cancelled"] > 0:
        terminal_reason = "cancelled"
    else:
        terminal_reason = "succeeded" if evaluate_batch_counts(full_counts) == "succeeded" else "partial"

    return {
        "schema_version": 1,
        "batch_id": batch_id,
        "adapter": adapter_name,
        "llm_conn_id": llm_conn_id,
        "model_id": model_id,
        "output_type_ref": output_type_ref(output_spec),
        "structured": output_spec.is_structured,
        "result_uri": result_uri,
        "request_count": request_count,
        "counts": full_counts,
        "duplicate_result_count": diagnostics.duplicate_result_count,
        "out_of_range_result_count": diagnostics.out_of_range_result_count,
        "ordered": False,
        "rejoin_key": "index",
        "custom_id_prefix": custom_id_prefix,
        "terminal_reason": terminal_reason,
        "submitted_at": submitted_at,
        "completed_at": completed_at,
    }


def missing_indexes(seen: Iterable[int], request_count: int) -> list[int]:
    """Return the sorted indexes in ``range(request_count)`` absent from ``seen``. Exposed for testing."""
    return sorted(set(range(request_count)) - set(seen))
