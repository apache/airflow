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
"""Operator for ``@task.llm_batch``."""

from __future__ import annotations

import time
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.ai.batch import dispatch, results, state
from airflow.providers.common.ai.batch.base import IN_PROGRESS_STATUSES, TERMINAL_STATUS_MAP
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.exceptions import (
    LLMBatchInputError,
    LLMBatchJobError,
    LLMBatchOrphanedIntentError,
    LLMBatchPartialFailureError,
    LLMBatchStaleStateError,
    LLMBatchTimeoutError,
)
from airflow.providers.common.ai.triggers.llm_batch import MAX_CONSECUTIVE_POLL_FAILURES, LLMBatchTrigger
from airflow.providers.common.compat.sdk import BaseHook, BaseOperator, conf

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchAdapter, BatchRequest
    from airflow.providers.common.compat.sdk import Context

#: Below this, per-triggerer executor-thread contention (R2) makes polling unreliable
#: enough that the defer/poll cycle stops behaving predictably; batch jobs run on the
#: order of minutes to hours, so this is not a meaningful latency cost.
_MIN_POLL_INTERVAL = 30


class LLMBatchOperator(BaseOperator):
    """
    Submit prompts as a provider batch job and land results on object storage.

    Routes to the OpenAI or Anthropic batch API based on ``llm_conn_id``'s
    connection type and ``model_id``'s ``"<provider>:<model>"`` prefix (see
    :mod:`~airflow.providers.common.ai.batch.dispatch`). Unlike
    :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`, results
    **never** go to XCom -- they are written as JSONL to ``result_path``, and
    the XCom value is a small manifest describing where to find them and how
    many requests landed in each outcome bucket. This is a deliberate
    semantic reversal from ``LLMOperator.execute()``'s ``return output``.

    A retry (or a manual clear) of this task computes the same identity key
    as the attempt before it and re-attaches to that attempt's in-flight or
    already-completed batch instead of submitting a new one -- this is what
    makes ``retries`` safe to use here, unlike the vendor
    ``AnthropicBatchOperator``/``OpenAITriggerBatchOperator``, whose docs
    recommend ``retries=0``. Re-attach eligibility is decided by comparing a
    fingerprint of the request content, which includes the ``output_type``
    schema: editing a Pydantic ``output_type`` and clearing the task is
    treated as a new batch, never silently re-attached to results produced
    under the old schema. Clearing a task from the UI keeps the same
    ``run_id``/``map_index`` and therefore the same identity key -- clearing
    normally re-attaches; to force a fresh submission, delete the state file
    under ``{result_path}/_airflow_batch_state/``.

    A **cancelled** batch (via ``cancel_on_kill``/``cancel_on_timeout``, or
    cancelled out-of-band) is handled the same way as an **expired** one, not
    as a distinct dead end (N-M4): cancellation can happen after a
    meaningful fraction of requests have already completed and been billed,
    so those results are fetched, validated, and landed exactly like any
    other terminal batch -- discarding them would silently throw away
    results already paid for, the same money-safety reasoning ``expired``
    batches get. ``fail_on_partial_error`` decides whether a nonzero
    errored/invalid_output/expired/cancelled/missing count fails the task;
    the state file follows the same "keep it only if this run is about to
    raise for a partial failure" rule as every other terminal path (see
    ``_finalize``). A batch that merely failed or errored while polling
    leaves its state in place unconditionally, since the input may still be
    valid and worth re-attaching to.

    :param requests: The batch's inputs. A bare ``list[str]`` is a shorthand
        for ``list[BatchRequest]`` where each string becomes ``{"prompt": s}``.
    :param result_path: Directory URI (not a file path) where the JSONL
        results and internal state are written, e.g.
        ``s3://bucket/prefix``. Templated.
    :param llm_conn_id: Connection ID for the LLM provider. Its connection
        type selects OpenAI vs. Anthropic together with ``model_id``.
    :param model_id: Model identifier as ``"<provider>:<model>"`` (e.g.
        ``"openai:gpt-5"``). Required -- unlike
        :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`,
        there is no connection-level default for batch, because the prefix
        is also what selects the adapter.
    :param system_prompt: System-level instructions applied to every
        request that does not set its own ``system_prompt`` override.
    :param output_type: Expected output type. Default ``str``. Set to a
        Pydantic ``BaseModel`` subclass (or another type ``TypeAdapter``
        supports) for structured output. Not templated -- it is a Python
        class, not a string. There is no ``serialize_output`` parameter
        (unlike ``LLMOperator``): every result is written as a JSONL row,
        which is always plain JSON, so the model-dump-to-dict behavior that
        parameter opts into on the synchronous path is unconditional here.
    :param max_tokens: Default ``max_tokens`` for requests that do not set
        their own. Anthropic's Messages API requires this field; OpenAI
        does not and passes it through as an ordinary request parameter.
    :param request_params: Extra body parameters merged into every request
        (e.g. ``{"temperature": 0.2}``); a request's own ``params`` wins on
        conflicting keys.
    :param poll_interval: Seconds between status checks. Minimum 30 --
        batch jobs run on the order of minutes to hours, and polling faster
        than that mainly adds triggerer thread-pool contention (R2) with
        no benefit.
    :param timeout: Seconds from submission to allow the batch to reach a
        terminal state before this task times out. Default 24 hours. This is
        a *soft* budget enforced by this operator's own polling loop, which
        is meant to win gracefully (honoring ``cancel_on_timeout``) before
        Airflow's own ``execution_timeout`` (a hard, generic task timeout)
        would. If ``execution_timeout`` is set to less than
        ``timeout + poll_interval + 60``, it preempts this graceful path
        entirely and the task is killed with no chance for this operator's
        own timeout handling (or ``cancel_on_timeout``) to run at all.
    :param deferrable: Run in deferrable mode. Defaults to
        ``[operators] default_deferrable``.
    :param cancel_on_kill: Cancel the batch when the task is killed. In
        deferrable mode this is honored by the trigger's ``on_kill``, which
        only runs on Airflow 3.3+; on older Airflow a killed deferred task's
        batch is not cancelled automatically.
    :param cancel_on_timeout: Cancel the batch when ``timeout`` is reached
        without a terminal state (this includes giving up after persistent
        polling failures that coincide with the deadline having passed).
        A cancelled batch is finalized like any other terminal outcome
        (N-M4) -- its (possibly partial) results are still fetched and
        landed, and the recorded state is only cleared once that finalize
        succeeds without raising for ``fail_on_partial_error`` (see the
        class docstring). When ``False``, the task still fails, but the
        batch is left running (and billing) so a later retry can re-attach
        to it and its results without paying twice.
    :param fail_on_partial_error: Fail the task when any request either
        errored on the provider side, failed output validation, or expired
        without completing. Default ``False`` (succeed; the manifest's
        ``counts`` still records all three numbers so a downstream task can
        decide what to do about them).
    :param on_stale_state: What to do when a recorded batch for this task
        instance no longer matches the current input (prompts, model,
        ``output_type`` schema, ...): ``"cancel_and_resubmit"`` (default)
        best-effort cancels the stale batch and submits a new one;
        ``"fail"`` raises instead of spending on a fresh submission
        implicitly.
    :param on_orphaned_intent: What to do when a Phase A intent record exists
        (a previous attempt crashed between submit and recording the
        response) and provider-side recovery (:meth:`~airflow.providers.common.ai.batch.base.BatchAdapter.find_orphaned_batch`)
        finds no matching batch: ``"resubmit"`` (default) submits a new
        batch, same as if no record existed at all; ``"fail"`` raises
        instead. Unlike ``on_stale_state``, there is no old batch id to
        cancel here as a loss-limiting step -- if the original submission
        actually reached the provider and simply could not be recovered
        (e.g. Anthropic, which has no query capability for this at all),
        ``"resubmit"`` pays for both the original and the new batch.
    :param completion_window: OpenAI-specific completion window (currently
        only ``"24h"`` is offered by the API). Ignored by the Anthropic
        adapter, which logs once that it was ignored.
    """

    template_fields: Sequence[str] = ("requests", "result_path", "system_prompt", "model_id", "llm_conn_id")

    def __init__(
        self,
        *,
        requests: list[str] | list[BatchRequest],
        result_path: str,
        llm_conn_id: str,
        model_id: str | None = None,
        system_prompt: str = "",
        output_type: type = str,
        max_tokens: int = 1024,
        request_params: dict[str, Any] | None = None,
        poll_interval: int = 60,
        timeout: int = 86400,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        cancel_on_kill: bool = True,
        cancel_on_timeout: bool = True,
        fail_on_partial_error: bool = False,
        on_stale_state: Literal["cancel_and_resubmit", "fail"] = "cancel_and_resubmit",
        on_orphaned_intent: Literal["resubmit", "fail"] = "resubmit",
        completion_window: str = "24h",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.requests = requests
        self.result_path = result_path
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.system_prompt = system_prompt
        self.output_type = output_type
        self.max_tokens = max_tokens
        self.request_params = request_params
        self.poll_interval = poll_interval
        self.timeout = timeout
        self.deferrable = deferrable
        self.cancel_on_kill = cancel_on_kill
        self.cancel_on_timeout = cancel_on_timeout
        self.fail_on_partial_error = fail_on_partial_error
        self.on_stale_state = on_stale_state
        self.on_orphaned_intent = on_orphaned_intent
        self.completion_window = completion_window
        self.batch_id: str | None = None

    @staticmethod
    def _normalize_requests(requests: list[str] | list[BatchRequest]) -> list[BatchRequest]:
        return [{"prompt": r} if isinstance(r, str) else r for r in requests]

    def _bare_model(self) -> str:
        """Strip the ``"<provider>:"`` prefix -- adapters/SDKs want the provider's own model name."""
        _, _, bare_model = (self.model_id or "").partition(":")
        return bare_model

    def _build_adapter(self) -> tuple[str, BatchAdapter]:
        conn_type = BaseHook.get_connection(self.llm_conn_id).conn_type
        adapter_name = dispatch.resolve_adapter_name(conn_type, self.model_id)
        return adapter_name, dispatch.build_adapter(adapter_name, llm_conn_id=self.llm_conn_id)

    @staticmethod
    def _identity(context: Context) -> tuple[str, str]:
        ti = context["task_instance"]
        key = state.compute_identity_key(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index if ti.map_index is not None else -1,
        )
        return key, state.key16(key)

    @staticmethod
    def _is_not_found_error(exc: Exception) -> bool:
        """
        Best-effort, SDK-agnostic 404 detection.

        Both ``openai.NotFoundError`` and ``anthropic.NotFoundError`` use
        that exact class name, so matching on it lets this module tell "the
        provider has forgotten this batch id" apart from a transient poll
        failure without importing either SDK (D1).
        """
        return type(exc).__name__ == "NotFoundError"

    def execute(self, context: Context) -> dict[str, Any]:
        from airflow.sdk import ObjectStoragePath

        if self.poll_interval < _MIN_POLL_INTERVAL:
            raise LLMBatchInputError(
                f"poll_interval must be at least {_MIN_POLL_INTERVAL} seconds; got "
                f"{self.poll_interval}. Batch jobs run on the order of minutes to hours -- "
                "polling faster mainly adds triggerer thread-pool contention with no benefit."
            )

        requests = self._normalize_requests(self.requests)
        if not requests:
            raise LLMBatchInputError("LLMBatchOperator requires at least one request; got an empty list.")

        # Step 2 (§5.5): build the OutputSpec before the fingerprint -- the schema it produces
        # is part of the fingerprint's material (§5.2).
        spec = build_output_spec(self.output_type)
        output_schema: Any = spec.json_schema if spec.is_structured else "str"

        # Step 3: identity key (run-stable, no try_number) and content fingerprint.
        # M6: llm_conn_id is part of the fingerprint -- two connections can point at two
        # different accounts, and switching connections must never silently re-attach to a
        # batch billed to (and readable only from) a different one.
        key, key16 = self._identity(context)
        fingerprint = state.compute_fingerprint(
            requests=requests,
            llm_conn_id=self.llm_conn_id,
            model_id=self.model_id,
            system_prompt=self.system_prompt,
            max_tokens=self.max_tokens,
            request_params=self.request_params,
            output_schema=output_schema,
        )
        output_schema_digest = state.compute_output_schema_digest(output_schema)

        # Step 4: adapter, from conn_type x model_id prefix.
        adapter_name, adapter = self._build_adapter()

        # Step 5: reject over-limit input before any network call.
        adapter.validate_requests(
            requests,
            model=self._bare_model(),
            output_spec=spec,
            idempotency_key=key16,
            system_prompt=self.system_prompt,
            max_tokens=self.max_tokens,
            request_params=self.request_params,
        )

        result_path_osp = ObjectStoragePath(self.result_path)

        # Step 6: read back any recorded state for this task instance. Any failure other than
        # "no file" propagates as LLMBatchStateReadError (M2) -- never silently treated as "safe
        # to submit a new batch".
        record = state.read_state(result_path_osp, key)

        # M3: reattach target resolved from either a matching record, or (for a Phase A orphan)
        # a provider-side recovery lookup -- both funnel into the same get_batch/wait/finalize
        # path below so there is exactly one way this operator reattaches to a batch.
        reattach_batch_id: str | None = None
        # C5: anchor a resumed poll's timeout to when the batch was *actually* submitted, not to
        # "now" -- otherwise every reattach (a retry, or resuming a still-running batch) would
        # get a fresh full ``timeout`` budget, silently granting more wall-clock time than the
        # docstring's "seconds from submission" promises.
        reattach_submitted_at: str | None = None

        if record is not None and record.batch_id and record.input_fingerprint == fingerprint:
            reattach_batch_id = record.batch_id
            reattach_submitted_at = record.submitted_at
        elif record is not None and record.batch_id and record.input_fingerprint != fingerprint:
            if self.on_stale_state == "fail":
                reason = (
                    "the output_type schema changed"
                    if record.output_schema_digest != output_schema_digest
                    else "the prompts or other request content changed"
                )
                raise LLMBatchStaleStateError(
                    f"Recorded batch {record.batch_id!r} for this task instance no longer "
                    f"matches its input ({reason} since the recorded batch); "
                    "on_stale_state='fail' rejects it instead of submitting a new one. Delete "
                    "the state file under _airflow_batch_state/ to force a fresh submission, or "
                    "use on_stale_state='cancel_and_resubmit'."
                )
            self.log.warning(
                "Recorded batch %s for this task instance is stale; cancelling and resubmitting. "
                "If this happens on every retry of a @task.llm_batch-decorated task, the "
                "decorated function is likely non-deterministic across attempts (e.g. it embeds "
                "a wall-clock timestamp) -- each retry then pays for a brand new batch instead of "
                "reattaching to the previous one (A5).",
                record.batch_id,
            )
            try:
                adapter.cancel_batch(record.batch_id)
            except Exception as e:
                self.log.warning("Failed to cancel stale batch %s: %s", record.batch_id, e)
        elif record is not None and not record.batch_id:
            # Phase A orphan (M3): try provider-side recovery before assuming a fresh submit
            # is needed -- see BatchAdapter.find_orphaned_batch. N1: input_fingerprint is the
            # *current* fingerprint, not record.input_fingerprint -- if the input changed since
            # this intent was written, the provider-side search (keyed on the current
            # fingerprint) correctly finds nothing, since no batch was ever submitted with that
            # content's fingerprint; it will not recover a stale match. not_before rejects any
            # candidate created before this intent was recorded, so recovery cannot pick up an
            # unrelated older batch that happens to share both the key and the fingerprint.
            try:
                recovered_batch_id = adapter.find_orphaned_batch(
                    key16, fingerprint, not_before=record.intent_at or ""
                )
            except Exception as e:
                self.log.warning("Failed to search for an orphaned batch for this task instance: %s", e)
                recovered_batch_id = None
            if recovered_batch_id:
                self.log.warning(
                    "Recovered orphaned batch %s for this task instance (a previous attempt "
                    "likely crashed between submit and recording the response); re-attaching "
                    "instead of resubmitting and paying for the same requests twice.",
                    recovered_batch_id,
                )
                # R3-3: record.submitted_at is always None here (this is a Phase A record --
                # Phase B is what sets it), so falling back to "now" would anchor the C5 timeout
                # budget to recovery time instead of when the batch was actually submitted.
                # record.intent_at is the best available approximation of the true submit time.
                reattach_submitted_at = record.intent_at or datetime.now(timezone.utc).isoformat()
                state.write_submitted(
                    result_path_osp,
                    key=key,
                    input_fingerprint=fingerprint,
                    output_schema_digest=output_schema_digest,
                    intent_at=record.intent_at,
                    adapter=adapter_name,
                    llm_conn_id=self.llm_conn_id,
                    model_id=self.model_id,
                    output_type_ref=results.output_type_ref(spec),
                    batch_id=recovered_batch_id,
                    provider_input_ref=None,
                    request_count=len(requests),
                    submitted_at=reattach_submitted_at,
                )
                reattach_batch_id = recovered_batch_id
            elif self.on_orphaned_intent == "fail":
                # R3-6: unlike the stale-fingerprint branch, there is no old batch_id to cancel
                # here -- resubmitting risks paying twice if the original submit actually
                # succeeded and is simply unrecoverable (e.g. Anthropic, which has no
                # find_orphaned_batch query capability at all). on_orphaned_intent="fail" lets a
                # caller for whom that risk is unacceptable stop instead of resubmitting blindly.
                raise LLMBatchOrphanedIntentError(
                    "Found a submit-intent record for this task instance with no recorded "
                    "batch id (a previous attempt likely crashed between submit and recording "
                    "the response), and no matching batch was found on the provider. "
                    "on_orphaned_intent='fail' rejects resubmitting instead of risking a "
                    "duplicate, billable submission if the original request actually succeeded "
                    "on the provider side. Delete the state file under _airflow_batch_state/ to "
                    "force a fresh submission once you've confirmed no duplicate is in flight, "
                    "or use on_orphaned_intent='resubmit'."
                )
            else:
                self.log.warning(
                    "Found a submit-intent record with no batch id for this task instance "
                    "(a previous attempt likely crashed between submit and recording it), and no "
                    "matching in-flight batch was found on the provider; submitting a new batch. "
                    "This risks a duplicate, billable submission if the original request actually "
                    "reached the provider (on_orphaned_intent='resubmit', the default)."
                )

        if reattach_batch_id is not None:
            try:
                batch_state = adapter.get_batch(reattach_batch_id)
            except Exception as e:
                if not self._is_not_found_error(e):
                    raise
                self.log.warning(
                    "Batch %s (recorded for this task instance) was not found on the provider; "
                    "submitting a new batch.",
                    reattach_batch_id,
                )
            else:
                self.batch_id = reattach_batch_id
                self._push_batch_id_xcom(context)
                if batch_state.status not in IN_PROGRESS_STATUSES:
                    # Terminal already -- go straight to fetch/validate/merge/land. No submit
                    # call: this is the reattach case the whole design exists for. (Whether the
                    # status query just made above is itself billable is provider-dependent and
                    # not claimed here either way -- only "no new batch submitted" is verified.)
                    return self._handle_event(
                        context, self._event_from_state(self.batch_id, batch_state), key=key
                    )
                # Still running -- resume waiting without resubmitting.
                return self._wait(
                    context,
                    adapter_name=adapter_name,
                    adapter=adapter,
                    batch_id=self.batch_id,
                    key=key,
                    submitted_at=reattach_submitted_at or datetime.now(timezone.utc).isoformat(),
                )

        # Step 7: full submit. Phase A must land before the network call (§5.3) -- a crash
        # between "submit sent" and "submit response received" must leave a trace.
        intent_at = datetime.now(timezone.utc).isoformat()
        state.write_intent(
            result_path_osp,
            key=key,
            input_fingerprint=fingerprint,
            output_schema_digest=output_schema_digest,
            intent_at=intent_at,
        )
        submit_result = adapter.submit(
            requests,
            model=self._bare_model(),
            idempotency_key=key16,
            input_fingerprint=fingerprint,
            output_spec=spec,
            system_prompt=self.system_prompt,
            max_tokens=self.max_tokens,
            request_params=self.request_params,
            completion_window=self.completion_window,
        )
        submitted_at = datetime.now(timezone.utc).isoformat()
        state.write_submitted(
            result_path_osp,
            key=key,
            input_fingerprint=fingerprint,
            output_schema_digest=output_schema_digest,
            intent_at=intent_at,
            adapter=adapter_name,
            llm_conn_id=self.llm_conn_id,
            model_id=self.model_id,
            output_type_ref=results.output_type_ref(spec),
            batch_id=submit_result.batch_id,
            provider_input_ref=submit_result.provider_input_ref,
            request_count=len(requests),
            submitted_at=submitted_at,
        )
        self.batch_id = submit_result.batch_id
        # Observability only -- NOT the recovery mechanism (Discovery A: XCom is cleared at
        # the start of every retry attempt, so a retry cannot rely on reading this back).
        self._push_batch_id_xcom(context)
        self.log.info(
            "Submitted batch %s (%d requests) via %s adapter", self.batch_id, len(requests), adapter_name
        )

        return self._wait(
            context,
            adapter_name=adapter_name,
            adapter=adapter,
            batch_id=self.batch_id,
            key=key,
            submitted_at=submitted_at,
        )

    def _push_batch_id_xcom(self, context: Context) -> None:
        """Push the observability-only ``batch_id`` XCom key, honoring ``do_xcom_push`` (S3)."""
        if self.do_xcom_push:
            context["ti"].xcom_push(key="batch_id", value=self.batch_id)

    def _wait(
        self,
        context: Context,
        *,
        adapter_name: str,
        adapter: BatchAdapter,
        batch_id: str,
        key: str,
        submitted_at: str,
    ) -> dict[str, Any]:
        # C5: end_time is anchored to when the batch was actually submitted, not to "now" --
        # recomputing it fresh on every attempt (a retry, or a resumed still-running reattach)
        # would silently grant a full new ``timeout`` budget each time, contradicting this
        # class's own docstring ("seconds from submission").
        end_time = datetime.fromisoformat(submitted_at).timestamp() + self.timeout
        if self.deferrable:
            self.defer(
                # R3-4: a generic safety-net timeout, not a tight bound -- unlike the trigger's
                # own end_time (anchored to submitted_at as of C5), this duration is always
                # measured from "now" (this defer() call), so on a fresh submit it lands only
                # ~poll_interval+60s beyond the trigger's end_time, but on a resumed reattach to
                # a batch submitted long ago it lands considerably further beyond it -- both are
                # fine, since this only needs to fire *after* the trigger's own clean "timeout"
                # event (which honors cancel_on_timeout), never before it.
                timeout=self.execution_timeout or timedelta(seconds=self.timeout + self.poll_interval + 60),
                trigger=LLMBatchTrigger(
                    llm_conn_id=self.llm_conn_id,
                    adapter=adapter_name,
                    batch_id=batch_id,
                    poll_interval=self.poll_interval,
                    end_time=end_time,
                    cancel_on_kill=self.cancel_on_kill,
                    cancel_on_timeout=self.cancel_on_timeout,
                ),
                method_name="execute_complete",
            )
        event = self._poll_sync(adapter, batch_id, end_time=end_time)
        return self._handle_event(context, event, key=key)

    def _poll_sync(self, adapter: BatchAdapter, batch_id: str, *, end_time: float) -> dict[str, Any]:
        """Poll synchronously until terminal -- the ``deferrable=False`` counterpart of ``LLMBatchTrigger.run()``."""
        consecutive_failures = 0
        while True:
            try:
                batch_state = adapter.get_batch(batch_id)
            except Exception as e:
                consecutive_failures += 1
                timed_out = time.time() > end_time
                if consecutive_failures >= MAX_CONSECUTIVE_POLL_FAILURES or timed_out:
                    # M11: giving up because the wall-clock budget is actually exhausted is a
                    # real timeout (§8) and must honor cancel_on_timeout, same as the
                    # healthy-poll timeout path below. Giving up early from persistent poll
                    # failures alone (without exceeding end_time) stays "error" -- the batch's
                    # own health is unknown, so cancelling it would be presumptuous.
                    if timed_out and self.cancel_on_timeout:
                        try:
                            adapter.cancel_batch(batch_id)
                        except Exception as cancel_error:
                            self.log.warning(
                                "Failed to cancel batch %s on timeout: %s", batch_id, cancel_error
                            )
                    return {
                        "status": "timeout" if timed_out else "error",
                        "batch_id": batch_id,
                        "counts": None,
                        "message": str(e),
                    }
                self.log.warning("Polling batch %s failed (%s); retrying.", batch_id, e)
                time.sleep(self.poll_interval)
                continue

            consecutive_failures = 0
            if batch_state.status not in IN_PROGRESS_STATUSES:
                return self._event_from_state(batch_id, batch_state)

            if time.time() > end_time:
                if self.cancel_on_timeout:
                    try:
                        adapter.cancel_batch(batch_id)
                    except Exception as e:
                        self.log.warning("Failed to cancel batch %s on timeout: %s", batch_id, e)
                return {
                    "status": "timeout",
                    "batch_id": batch_id,
                    "counts": None,
                    "message": f"Batch {batch_id} did not reach a terminal status before the configured timeout.",
                }

            time.sleep(self.poll_interval)

    @staticmethod
    def _event_from_state(batch_id: str, batch_state: Any) -> dict[str, Any]:
        """Reshape a directly-polled ``BatchState`` into the same event shape the trigger yields."""
        return {
            "status": TERMINAL_STATUS_MAP.get(batch_state.status, "error"),
            "batch_id": batch_id,
            "counts": dict(batch_state.counts) if batch_state.counts is not None else None,
            "message": batch_state.error_message
            or f"Batch {batch_id} reached status {batch_state.status!r}.",
        }

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:
        """
        Resume after the trigger fires.

        This is a fresh operator instance (the deferred one released its
        worker slot), so ``batch_id`` comes from ``event``, not ``self``.
        """
        self.batch_id = event["batch_id"]
        key, _ = self._identity(context)
        return self._handle_event(context, event, key=key)

    def _handle_event(self, context: Context, event: dict[str, Any], *, key: str) -> dict[str, Any]:
        status = event["status"]
        if status in ("success", "expired", "cancelled"):
            # M5/N-M4: "expired" and "cancelled" are both terminal and may already be billed for
            # partially-completed requests -- both must go through the same fetch/validate/
            # merge/land path as "success", never be discarded. A cancelled batch is not treated
            # as a distinct dead end that unconditionally clears state and raises: whether the
            # task then fails is fail_on_partial_error's job, not this dispatch's (see the
            # extended check in _finalize). The state file's fate (kept vs. cleared) follows
            # _finalize's normal "keep it only if about to raise for a partial failure" rule,
            # same as every other terminal status.
            return self._finalize(
                context, batch_id=event["batch_id"], key=key, extra_counts=event.get("counts")
            )
        if status == "timeout":
            raise LLMBatchTimeoutError(event["message"])
        # "failed" (provider-reported) and "error" (polling gave up) both mean the job itself
        # never produced a usable result; the state file is left in place either way (§8) --
        # the input hasn't been proven unrecoverable, so a retry with unchanged input can still
        # legitimately re-attach and get the same (correct) answer.
        raise LLMBatchJobError(event["message"])

    def _finalize(
        self, context: Context, *, batch_id: str, key: str, extra_counts: dict[str, int] | None = None
    ) -> dict[str, Any]:
        """Fetch, validate, merge, and land results -- the one place that happens (§4)."""
        from airflow.sdk import ObjectStoragePath

        result_path_osp = ObjectStoragePath(self.result_path)
        record = state.read_state(result_path_osp, key)
        if record is None or record.request_count is None:
            raise LLMBatchJobError(
                f"No recorded state found for batch {batch_id!r} at finalize time; the state "
                "file may have been deleted externally between submit and completion."
            )

        # S5: use the connection/model actually recorded at submit time, not self.llm_conn_id/
        # self.model_id -- both are templated fields that are re-rendered on every execution
        # attempt (including a deferred resume), so relying on "now" instead of "then" could
        # drift from what was actually billed/submitted if a template's rendered value changes
        # between attempts.
        llm_conn_id = record.llm_conn_id or self.llm_conn_id
        model_id = record.model_id if record.model_id is not None else self.model_id
        adapter = dispatch.build_adapter(record.adapter, llm_conn_id=llm_conn_id)
        spec = build_output_spec(self.output_type)
        key16 = state.key16(key)
        destination = result_path_osp / f"{key16}.jsonl"

        merge_counts, merge_diagnostics = results.stream_results_to_jsonl(
            adapter=adapter,
            batch_id=batch_id,
            output_spec=spec,
            request_count=record.request_count,
            custom_id_prefix=key16,
            destination=destination,
        )
        manifest = results.assemble_manifest(
            batch_id=batch_id,
            adapter_name=record.adapter,
            llm_conn_id=llm_conn_id,
            model_id=model_id,
            output_spec=spec,
            result_uri=str(destination),
            request_count=record.request_count,
            merge_counts=merge_counts,
            extra_counts=extra_counts,
            merge_diagnostics=merge_diagnostics,
            custom_id_prefix=key16,
            submitted_at=record.submitted_at or "",
            completed_at=datetime.now(timezone.utc).isoformat(),
        )
        # Only delete once the manifest is durably built, and only if this run is not about to
        # raise for fail_on_partial_error (M1) -- a partial-failure raise must leave the state
        # file in place so a retry re-attaches to the *same* completed batch and reaches the
        # same conclusion, rather than paying for a brand new submission. Every "still alive"
        # path elsewhere in this class (in progress, deferred again, timed out without
        # cancelling, ...) already leaves the file in place too (§8's cross-cutting invariant).
        errored = manifest["counts"]["errored"]
        invalid_output = manifest["counts"]["invalid_output"]
        expired = manifest["counts"]["expired"]
        cancelled = manifest["counts"]["cancelled"]
        missing = manifest["counts"]["missing"]
        # N-M4: "cancelled" joins "expired" here for the same money-safety reason -- a cancelled
        # batch can still have completed (and billed) requests. N3: "missing" also counts -- a
        # batch that expired or was cancelled after completing only a small fraction of requests
        # must not report success just because none of those uncompleted requests individually
        # came back marked "errored"/"invalid_output"/"expired"/"cancelled".
        will_fail_partial = self.fail_on_partial_error and (
            errored or invalid_output or expired or cancelled or missing
        )

        if not will_fail_partial:
            state.delete_state(result_path_osp, key)

        if will_fail_partial:
            raise LLMBatchPartialFailureError(
                f"Batch {batch_id!r} had {errored} provider-side error(s), {invalid_output} "
                f"output-validation failure(s), {expired} expired (never-completed) request(s), "
                f"{cancelled} cancelled request(s), and {missing} request(s) with no recorded "
                f"outcome at all; fail_on_partial_error=True. Results are still available at "
                f"{manifest['result_uri']}."
            )
        return manifest

    def on_kill(self) -> None:
        """
        Cancel the batch if the (non-deferred) task is killed.

        Only fires while the worker process is alive. A killed *deferred*
        task is cancelled by the trigger's own ``on_kill`` instead (§8),
        which only Airflow 3.3+ calls.
        """
        if not (self.cancel_on_kill and self.batch_id):
            return
        try:
            _, adapter = self._build_adapter()
            adapter.cancel_batch(self.batch_id)
            self.log.info("on_kill: cancelled batch %s", self.batch_id)
        except Exception as e:
            self.log.warning("on_kill: failed to cancel batch %s: %s", self.batch_id, e)
