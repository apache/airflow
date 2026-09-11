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
Run-stable idempotency state for ``@task.llm_batch``, persisted outside XCom.

XCom is cleared at the start of every retry attempt (unlike a deferral
resume), so a ``batch_id`` pushed to XCom on submit is gone by the time a
retry's ``execute()`` runs -- the recovery this module exists for cannot rely
on XCom at all. State lives as a small JSON file on the same
:class:`~airflow.sdk.ObjectStoragePath` as the batch's results, at
``{result_path}/_airflow_batch_state/{key}.json``, following the durable-cache
pattern in ``durable/storage.py``.

Two invariants that must not be relaxed:

- The identity key never includes ``try_number`` -- that is the entire point
  of "run-stable": a retry must compute the *same* key as the attempt before
  it, so it can find and re-attach to that attempt's in-flight batch.
- The input fingerprint *does* include the output schema (see
  :func:`compute_fingerprint`) -- changing ``output_type`` without changing
  the prompts must still be treated as a different batch, since the schema is
  part of what gets sent to the provider.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
from dataclasses import asdict, dataclass
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.exceptions import LLMBatchStateReadError

if TYPE_CHECKING:
    from airflow.sdk import ObjectStoragePath

_STATE_DIR = "_airflow_batch_state"

#: Bumped if the on-disk record shape ever changes incompatibly.
SCHEMA_VERSION = 1


def compute_identity_key(*, dag_id: str, task_id: str, run_id: str, map_index: int) -> str:
    r"""
    Return the run-stable identity key for one task instance (excluding ``try_number``).

    Uses ``\x00`` rather than ``_`` to join the components -- a plain
    ``_``-joined string collides (Dag ``etl`` + task ``load_data`` and Dag
    ``etl_load`` + task ``data`` both yield ``etl_load_data``), which would
    let one task instance read or overwrite another's batch state. Mirrors
    ``durable/storage.py``'s ``DurableStorage`` identity hash verbatim, for
    the same reason.
    """
    identity = "\x00".join([dag_id, task_id, run_id, str(map_index)])
    return hashlib.sha256(identity.encode()).hexdigest()


def key16(key: str) -> str:
    """
    Return the short form of an identity key, used as the ``custom_id`` prefix and result filename stem.

    16 hex characters is 64 bits of the full sha256 -- collision risk across the
    (at most tens of thousands of requests in) a single batch, or across the
    handful of concurrent task instances writing under the same ``result_path``,
    is astronomically below the odds of a provider-side outage; a shorter,
    more manageable id is worth that trade for something humans read in
    filenames and provider dashboards.
    """
    return key[:16]


def _canonical_json(data: Any) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def compute_output_schema_digest(output_schema: Any) -> str:
    """
    Hash the output schema material on its own.

    Stored alongside the combined input fingerprint so a stale-state error
    message can say *which* part changed (prompts vs. the ``output_type``
    schema) instead of only "something changed".
    """
    return hashlib.sha256(_canonical_json(output_schema).encode()).hexdigest()


def compute_fingerprint(
    *,
    requests: list[Any],
    llm_conn_id: str,
    model_id: str | None,
    system_prompt: str,
    max_tokens: int,
    request_params: dict[str, Any] | None,
    output_schema: Any,
) -> str:
    """
    Hash everything that determines the request content and which account it is billed to.

    ``output_schema`` is caller-supplied (the literal string ``"str"`` for an
    unstructured batch, or an ``OutputSpec.json_schema`` dict otherwise) --
    this module does not know how to derive a schema from ``output_type``
    itself; that logic lives in ``batch/output_schema.py``, which this module
    does not import.

    Schema is included deliberately: it is sent to the provider as part of
    every request body (OpenAI's ``response_format``, Anthropic's
    ``tools[0].input_schema``), so a change to it -- an added field, an
    edited description -- is a change to the request content, not just to how
    the response gets parsed. Without this, clearing a task after editing its
    Pydantic ``output_type`` would silently re-attach to a batch whose results
    were produced under the *old* schema.

    ``llm_conn_id`` is included for a different but equally serious reason:
    two ``pydanticai`` connections can point at two different accounts (or
    even two different providers' API keys entirely). Without it, switching
    ``llm_conn_id`` and rerunning would silently re-attach to -- and return
    the results of -- a batch submitted under a completely different
    account than the one the current run is configured to use.
    """
    material = {
        "schema_version": SCHEMA_VERSION,
        "requests": requests,
        "llm_conn_id": llm_conn_id,
        "model_id": model_id,
        "system_prompt": system_prompt,
        "max_tokens": max_tokens,
        "request_params": request_params,
        "output_schema": output_schema,
    }
    return hashlib.sha256(_canonical_json(material).encode()).hexdigest()


@dataclass(frozen=True)
class BatchStateRecord:
    """
    The on-disk record at ``{result_path}/_airflow_batch_state/{key}.json``.

    Written in two phases (see :func:`write_intent` / :func:`write_submitted`)
    so a crash between "submit request sent" and "submit response received"
    leaves a trace, even though it cannot recover the orphaned batch id (see
    Risk R1 in the plan -- out of scope for v1).

    Re-attach eligibility is decided by comparing ``input_fingerprint``, never
    ``output_type_ref`` -- the ref is a human-readable label only and can
    stay identical while the schema underneath it changes.
    """

    schema_version: int
    key: str
    input_fingerprint: str
    output_schema_digest: str
    #: When the Phase A intent record was written (N1) -- lets orphan recovery reject a
    #: same-key/same-fingerprint candidate that was actually created *before* this attempt even
    #: started (a genuine earlier submission of identical content), instead of just taking
    #: whatever the provider's unordered listing happens to return first.
    intent_at: str | None
    adapter: str | None
    llm_conn_id: str | None
    model_id: str | None
    output_type_ref: str | None
    batch_id: str | None
    provider_input_ref: str | None
    request_count: int | None
    submitted_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> BatchStateRecord:
        return cls(
            schema_version=data["schema_version"],
            key=data["key"],
            input_fingerprint=data["input_fingerprint"],
            output_schema_digest=data["output_schema_digest"],
            intent_at=data.get("intent_at"),
            adapter=data.get("adapter"),
            llm_conn_id=data.get("llm_conn_id"),
            model_id=data.get("model_id"),
            output_type_ref=data.get("output_type_ref"),
            batch_id=data.get("batch_id"),
            provider_input_ref=data.get("provider_input_ref"),
            request_count=data.get("request_count"),
            submitted_at=data.get("submitted_at"),
        )


def _state_path(result_path: ObjectStoragePath, key: str) -> ObjectStoragePath:
    return result_path / _STATE_DIR / f"{key}.json"


def _write(result_path: ObjectStoragePath, key: str, record: BatchStateRecord) -> None:
    path = _state_path(result_path, key)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record.to_dict()))


def write_intent(
    result_path: ObjectStoragePath,
    *,
    key: str,
    input_fingerprint: str,
    output_schema_digest: str,
    intent_at: str,
) -> None:
    """
    Phase A: record intent to submit, before any network call.

    ``batch_id`` is ``null`` -- this record exists purely so a crash between
    "submit sent" and "submit response received" leaves a trace (see the
    module docstring and Risk R1). Overwritten by :func:`write_submitted` once
    the provider responds. ``intent_at`` feeds orphan recovery's ``not_before``
    check (N1) -- see :meth:`~airflow.providers.common.ai.batch.base.BatchAdapter.find_orphaned_batch`.
    """
    _write(
        result_path,
        key,
        BatchStateRecord(
            schema_version=SCHEMA_VERSION,
            key=key,
            input_fingerprint=input_fingerprint,
            output_schema_digest=output_schema_digest,
            intent_at=intent_at,
            adapter=None,
            llm_conn_id=None,
            model_id=None,
            output_type_ref=None,
            batch_id=None,
            provider_input_ref=None,
            request_count=None,
            submitted_at=None,
        ),
    )


def write_submitted(
    result_path: ObjectStoragePath,
    *,
    key: str,
    input_fingerprint: str,
    output_schema_digest: str,
    intent_at: str | None,
    adapter: str,
    llm_conn_id: str,
    model_id: str | None,
    output_type_ref: str | None,
    batch_id: str,
    provider_input_ref: str | None,
    request_count: int,
    submitted_at: str,
) -> None:
    """Phase B: overwrite the intent record with the full record, once the provider has accepted the batch."""
    _write(
        result_path,
        key,
        BatchStateRecord(
            schema_version=SCHEMA_VERSION,
            key=key,
            input_fingerprint=input_fingerprint,
            output_schema_digest=output_schema_digest,
            intent_at=intent_at,
            adapter=adapter,
            llm_conn_id=llm_conn_id,
            model_id=model_id,
            output_type_ref=output_type_ref,
            batch_id=batch_id,
            provider_input_ref=provider_input_ref,
            request_count=request_count,
            submitted_at=submitted_at,
        ),
    )


def read_state(result_path: ObjectStoragePath, key: str) -> BatchStateRecord | None:
    """
    Return the recorded state for ``key``, or ``None`` if no record exists.

    Only a genuine not-found (``FileNotFoundError``) reads as "no recorded
    batch" -- that is the one condition safe to treat as "go ahead and submit
    a new batch". Any other failure -- a transient object-storage error, or a
    state file that exists but is corrupt/malformed -- raises
    :class:`~airflow.providers.common.ai.exceptions.LLMBatchStateReadError`
    instead of degrading to ``None``. "Could not read the state" and
    "confirmed there is no state" are different facts; conflating them would
    turn a passing I/O blip, or a torn write, into a duplicate, billable
    submission the next time this runs. A ``JSONDecodeError`` is the most
    dangerous case of all: it means the file **exists** (a batch was very
    likely submitted), just unparsable -- silently reading that as "no
    batch" is exactly the bug this function exists to prevent.
    """
    path = _state_path(result_path, key)
    try:
        raw = path.read_text()
    except FileNotFoundError:
        return None
    except OSError as e:
        raise LLMBatchStateReadError(
            f"Failed to read batch state for key {key!r} at {path}: {e}. This is not the same "
            "as 'no recorded batch' -- retry rather than treating this as safe to submit a new one."
        ) from e

    try:
        return BatchStateRecord.from_dict(json.loads(raw))
    except (json.JSONDecodeError, KeyError, ValueError) as e:
        raise LLMBatchStateReadError(
            f"Batch state file for key {key!r} at {path} exists but is corrupt or malformed "
            f"({e}). This must not be treated as 'no recorded batch' -- a batch may already be "
            "in flight or billed under this key; investigate before resubmitting."
        ) from e


def delete_state(result_path: ObjectStoragePath, key: str) -> None:
    """
    Delete the recorded state for ``key``.

    Must only be called once a manifest has been successfully written --
    every "the batch is still alive" code path (in progress, deferred again,
    timed out but not cancelled, ...) must leave the state file in place so a
    later retry or manual clear can still re-attach.
    """
    path = _state_path(result_path, key)
    with contextlib.suppress(FileNotFoundError, OSError):
        path.unlink()
