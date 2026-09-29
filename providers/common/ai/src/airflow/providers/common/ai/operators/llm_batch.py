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
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Literal

from airflow.providers.common.ai.batch import dispatch, results, state
from airflow.providers.common.ai.batch.output_schema import build_output_spec
from airflow.providers.common.ai.batch.polling import (
    MAX_CONSECUTIVE_POLL_FAILURES,
    BatchPoller,
    terminal_event,
)
from airflow.providers.common.ai.exceptions import (
    LLMBatchCancelledError,
    LLMBatchInputError,
    LLMBatchJobError,
    LLMBatchOrphanedIntentError,
    LLMBatchOrphanLookupError,
    LLMBatchPartialFailureError,
    LLMBatchStaleStateError,
    LLMBatchTimeoutError,
)
from airflow.providers.common.ai.triggers.llm_batch import LLMBatchTrigger
from airflow.providers.common.compat.sdk import BaseHook, BaseOperator, ObjectStoragePath, conf

if TYPE_CHECKING:
    from airflow.providers.common.ai.batch.base import BatchAdapter, BatchRequest, BatchState
    from airflow.providers.common.ai.batch.output_schema import OutputSpec
    from airflow.providers.common.ai.batch.state import BatchStateRecord
    from airflow.providers.common.compat.sdk import Connection, Context

#: Batch jobs run for minutes to hours; polling faster than this buys nothing and only adds
#: load on the provider's status endpoint and on the triggerer's thread pool.
_MIN_POLL_INTERVAL = 30

_STALE_STATE_POLICIES = ("cancel_and_resubmit", "fail")
_ORPHANED_INTENT_POLICIES = ("resubmit", "fail")


class LLMBatchOperator(BaseOperator):
    """
    Submit prompts as a provider batch job and land results on object storage.

    Routes to the OpenAI or Anthropic batch API based on ``model_id``'s
    ``"<provider>:<model>"`` prefix (see
    :mod:`~airflow.providers.common.ai.batch.dispatch`). Unlike
    :class:`~airflow.providers.common.ai.operators.llm.LLMOperator`, results
    never go to XCom: they are written as JSONL to ``result_path``, and the
    XCom value is a small manifest describing where to find them and how many
    requests landed in each outcome bucket. See the operator guide for the
    manifest and row schemas.

    A retry (or a manual clear) computes the same identity key as the attempt
    before it and re-attaches to that attempt's batch instead of submitting a
    new one, as long as the input still matches. The recorded state is kept
    after a successful landing too, so clearing a finished task re-lands the
    same results at no cost. Only two outcomes clear the state so that the
    next attempt submits fresh: a provider-side ``failed`` (input validation
    failed, nothing was billed) and a cancellation that lost requests.

    :param requests: The batch's inputs: a list of prompt strings, or a list
        of dicts with a ``"prompt"`` key and optional ``model``,
        ``system_prompt``, ``max_tokens`` and ``params`` overrides.
    :param result_path: Directory URI (not a file path) where the JSONL
        results and internal state are written, e.g. ``s3://bucket/prefix``.
        Templated, but must render to the same value on every attempt of a
        task instance; a retry looks for its recorded state there.
    :param llm_conn_id: Connection ID for the LLM provider. Must be a
        ``pydanticai`` connection today.
    :param model_id: Model identifier as ``"<provider>:<model>"`` (e.g.
        ``"openai:gpt-5"``). Falls back to the connection's Model field, as
        ``LLMOperator`` does.
    :param system_prompt: System-level instructions applied to every request
        that does not set its own ``system_prompt`` override.
    :param output_type: Expected output type. Default ``str``. Set to a
        Pydantic ``BaseModel`` subclass (or another type ``TypeAdapter``
        supports) for structured output; every result row is plain JSON.
    :param max_tokens: Output token cap for requests that do not set their
        own. Sent as ``max_tokens`` to Anthropic and as
        ``max_completion_tokens`` to OpenAI.
    :param request_params: Extra body parameters merged into every request
        (e.g. ``{"temperature": 0.2}``); a request's own ``params`` wins on
        conflicting keys. The keys the operator manages (``model``, the
        messages, the token cap and the structured-output directive) cannot
        be overridden from here.
    :param poll_interval: Seconds between status checks. Minimum 30.
    :param timeout: Seconds to wait for the batch to reach a terminal state,
        measured from submission. Default 24 hours. When a retry re-attaches
        to a batch whose budget has already elapsed (``cancel_on_timeout=False``),
        it gets a fresh ``timeout`` measured from the retry. If
        ``execution_timeout`` is set below ``timeout + poll_interval + 60``,
        Airflow's hard timeout preempts this graceful path and
        ``cancel_on_timeout`` never runs.
    :param deferrable: Run in deferrable mode. Defaults to
        ``[operators] default_deferrable``.
    :param cancel_on_kill: Cancel the batch when the task is killed. In
        deferrable mode this runs from the trigger's ``on_kill``, which only
        Airflow 3.3+ calls; there, clearing or marking a deferred task from
        the UI counts as a kill.
    :param cancel_on_timeout: Cancel the batch when ``timeout`` is reached
        without a terminal state. When ``False``, the task still fails, but
        the batch keeps running (and billing) and a later retry re-attaches
        to it.
    :param fail_on_partial_error: Fail the task when any request errored on
        the provider side, failed output validation, expired, was cancelled,
        or is missing from the results. Default ``False``: the task succeeds
        and the manifest's ``counts`` records each bucket. A batch in which
        every request failed still counts as partial.
    :param on_stale_state: What to do when a recorded batch for this task
        instance no longer matches the current input (prompts, model,
        ``output_type`` schema, ...): ``"cancel_and_resubmit"`` (default)
        best-effort cancels the stale batch and submits a new one;
        ``"fail"`` raises instead.
    :param on_orphaned_intent: What to do when a previous attempt recorded
        its intent to submit but no batch id, and the provider-side lookup
        finds no matching batch: ``"resubmit"`` (default) submits a new
        batch; ``"fail"`` raises instead. Anthropic offers no such lookup, so
        ``"resubmit"`` may pay twice there if the original request did reach
        the provider.
    :param completion_window: OpenAI-specific completion window. Only
        ``"24h"`` is offered by the API today. Ignored by Anthropic.
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
        if poll_interval < _MIN_POLL_INTERVAL:
            raise ValueError(
                f"poll_interval must be at least {_MIN_POLL_INTERVAL} seconds; got {poll_interval}. "
                "Batch jobs run for minutes to hours; polling faster has no benefit."
            )
        if timeout <= 0:
            raise ValueError(f"timeout must be a positive number of seconds; got {timeout}.")
        if on_stale_state not in _STALE_STATE_POLICIES:
            raise ValueError(
                f"on_stale_state must be one of {_STALE_STATE_POLICIES}; got {on_stale_state!r}."
            )
        if on_orphaned_intent not in _ORPHANED_INTENT_POLICIES:
            raise ValueError(
                f"on_orphaned_intent must be one of {_ORPHANED_INTENT_POLICIES}; got {on_orphaned_intent!r}."
            )
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
    def _normalize_requests(requests: Any) -> list[BatchRequest]:
        """
        Coerce ``requests`` to ``list[BatchRequest]``, rejecting anything that is not a list of prompts.

        A bare string is rejected rather than iterated: a decorated callable
        that returns ``"summarize this"`` instead of ``["summarize this"]``
        would otherwise submit one billable request per character.
        """
        if isinstance(requests, (str, bytes, Mapping)) or not isinstance(requests, Sequence):
            raise LLMBatchInputError(
                "requests must be a list of prompt strings or a list of {'prompt': ...} dicts; got "
                f"{type(requests).__name__}. A decorated function must return the whole list, not a "
                "single prompt."
            )
        normalized: list[BatchRequest] = []
        for index, item in enumerate(requests):
            if isinstance(item, str):
                normalized.append({"prompt": item})
            elif isinstance(item, Mapping) and isinstance(item.get("prompt"), str):
                normalized.append(dict(item))  # type: ignore[arg-type]
            else:
                raise LLMBatchInputError(
                    f"Request {index} must be a string or a dict with a string 'prompt'; got "
                    f"{type(item).__name__}. If the Dag sets render_template_as_native_obj=True, Jinja "
                    "converts numeric-looking prompts to numbers; return them wrapped in a dict."
                )
        if not normalized:
            raise LLMBatchInputError("LLMBatchOperator requires at least one request; got an empty list.")
        return normalized

    def _resolve_model_id(self, conn: Connection) -> str | None:
        """``model_id`` on the operator wins; otherwise the connection's Model field, as ``LLMOperator`` does."""
        if self.model_id:
            return self.model_id
        return conn.extra_dejson.get("model") or None

    def _connection(self) -> Connection:
        return BaseHook.get_connection(self.llm_conn_id)

    def _build_adapter(self, conn: Connection | None = None) -> tuple[str, BatchAdapter]:
        conn = conn or self._connection()
        adapter_cls = dispatch.get_adapter_class(conn.conn_type, self._resolve_model_id(conn))
        return adapter_cls.name, dispatch.build_adapter_from_connection(adapter_cls.name, conn)

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
        """SDK-agnostic 404 detection: both SDKs' ``NotFoundError`` carry ``status_code == 404``."""
        return getattr(exc, "status_code", None) == 404

    def execute(self, context: Context) -> dict[str, Any]:
        requests = self._normalize_requests(self.requests)
        spec = build_output_spec(self.output_type)
        output_schema: dict[str, Any] | str = spec.json_schema if spec.is_structured else "str"

        conn = self._connection()
        model_id = self._resolve_model_id(conn)
        adapter_name, adapter = self._build_adapter(conn)
        _, bare_model = dispatch.split_model_id(model_id)

        key, key16 = self._identity(context)
        fingerprint = state.compute_fingerprint(
            requests=requests,
            llm_conn_id=self.llm_conn_id,
            model_id=model_id,
            system_prompt=self.system_prompt,
            max_tokens=self.max_tokens,
            request_params=self.request_params,
            output_schema=output_schema,
        )
        output_schema_digest = state.compute_output_schema_digest(output_schema)

        try:
            adapter.validate_requests(
                requests,
                model=bare_model,
                output_spec=spec,
                idempotency_key=key16,
                system_prompt=self.system_prompt,
                max_tokens=self.max_tokens,
                request_params=self.request_params,
            )

            result_path_osp = ObjectStoragePath(self.result_path)
            record = state.read_state(result_path_osp, key)
            reattach_batch_id, reattach_submitted_at = self._resolve_reattach_target(
                record,
                adapter=adapter,
                adapter_name=adapter_name,
                fingerprint=fingerprint,
                output_schema_digest=output_schema_digest,
                key=key,
                key16=key16,
                model_id=model_id,
                spec=spec,
                request_count=len(requests),
                result_path_osp=result_path_osp,
            )

            if reattach_batch_id is not None:
                batch_state = self._get_recorded_batch(adapter, reattach_batch_id)
                if batch_state is not None:
                    self.batch_id = reattach_batch_id
                    self._push_batch_id_xcom(context)
                    if batch_state.status != "in_progress":
                        return self._handle_event(
                            context, terminal_event(self.batch_id, batch_state), key=key, spec=spec
                        )
                    return self._wait(
                        context,
                        adapter_name=adapter_name,
                        adapter=adapter,
                        batch_id=self.batch_id,
                        key=key,
                        spec=spec,
                        submitted_at=reattach_submitted_at or datetime.now(timezone.utc).isoformat(),
                        reattached=True,
                    )

            # Record the intent before the paid call so a crash between "request sent" and
            # "response recorded" leaves a trace the next attempt can act on.
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
                model=bare_model,
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
                model_id=model_id,
                output_type_ref=results.output_type_ref(spec),
                batch_id=submit_result.batch_id,
                provider_input_ref=submit_result.provider_input_ref,
                request_count=len(requests),
                submitted_at=submitted_at,
            )
            self.batch_id = submit_result.batch_id
            # Observability only. XCom is cleared at the start of every retry, so recovery
            # never relies on reading this back.
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
                spec=spec,
                submitted_at=submitted_at,
                reattached=False,
            )
        finally:
            adapter.close()

    def _resolve_reattach_target(
        self,
        record: BatchStateRecord | None,
        *,
        adapter: BatchAdapter,
        adapter_name: str,
        fingerprint: str,
        output_schema_digest: str,
        key: str,
        key16: str,
        model_id: str | None,
        spec: OutputSpec,
        request_count: int,
        result_path_osp: ObjectStoragePath,
    ) -> tuple[str | None, str | None]:
        """
        Decide whether this attempt re-attaches to a recorded batch, and which one.

        Returns ``(batch_id, submitted_at)`` to re-attach to, or ``(None, None)``
        to submit fresh. Handles the three recorded shapes: a matching batch,
        a stale batch (input changed), and an intent record with no batch id.
        """
        if record is None:
            return None, None

        if record.batch_id and record.input_fingerprint == fingerprint:
            self.log.info(
                "Re-attaching to batch %s submitted at %s by a previous attempt of this task instance; "
                "the input is unchanged, so nothing is resubmitted.",
                record.batch_id,
                record.submitted_at,
            )
            return record.batch_id, record.submitted_at

        if record.batch_id:
            reason = (
                "the output_type schema changed"
                if record.output_schema_digest != output_schema_digest
                else "the prompts or other request content changed"
            )
            if self.on_stale_state == "fail":
                raise LLMBatchStaleStateError(
                    f"Recorded batch {record.batch_id!r} for this task instance no longer matches its "
                    f"input ({reason} since the recorded batch); on_stale_state='fail' rejects it instead "
                    "of submitting a new one. Delete the state file under _airflow_batch_state/ to force "
                    "a fresh submission, or use on_stale_state='cancel_and_resubmit'."
                )
            self.log.warning(
                "Recorded batch %s for this task instance is stale (%s); cancelling it and resubmitting "
                "(on_stale_state='cancel_and_resubmit'). If this happens on every retry of a "
                "@task.llm_batch task, the decorated function is not deterministic across attempts and "
                "each retry pays for a new batch.",
                record.batch_id,
                reason,
            )
            self._cancel_recorded_batch(record, adapter, adapter_name)
            return None, None

        # Intent recorded, no batch id: a previous attempt crashed between submit and recording.
        try:
            recovered_batch_id = adapter.find_orphaned_batch(
                key16, fingerprint, not_before=record.intent_at or ""
            )
        except Exception as e:
            raise LLMBatchOrphanLookupError(
                "Found a submit-intent record for this task instance with no recorded batch id, and the "
                f"provider lookup for an orphaned batch failed ({e}). Whether a batch is already in flight "
                "is unknown, so this attempt does not resubmit; retry once the provider is reachable, or "
                "delete the state file under _airflow_batch_state/ after checking the provider's own "
                "batch listing."
            ) from e
        if recovered_batch_id:
            self.log.warning(
                "Recovered orphaned batch %s for this task instance (a previous attempt crashed between "
                "submit and recording the response); re-attaching instead of resubmitting.",
                recovered_batch_id,
            )
            submitted_at = record.intent_at or datetime.now(timezone.utc).isoformat()
            state.write_submitted(
                result_path_osp,
                key=key,
                input_fingerprint=fingerprint,
                output_schema_digest=output_schema_digest,
                intent_at=record.intent_at,
                adapter=adapter_name,
                llm_conn_id=self.llm_conn_id,
                model_id=model_id,
                output_type_ref=results.output_type_ref(spec),
                batch_id=recovered_batch_id,
                provider_input_ref=None,
                request_count=request_count,
                submitted_at=submitted_at,
            )
            return recovered_batch_id, submitted_at
        if self.on_orphaned_intent == "fail":
            raise LLMBatchOrphanedIntentError(
                "Found a submit-intent record for this task instance with no recorded batch id (a previous "
                "attempt crashed between submit and recording the response), and no matching batch was "
                "found on the provider. on_orphaned_intent='fail' rejects resubmitting instead of risking a "
                "duplicate, billable submission. Delete the state file under _airflow_batch_state/ to force "
                "a fresh submission once you've confirmed no duplicate is in flight, or use "
                "on_orphaned_intent='resubmit'."
            )
        self.log.warning(
            "Found a submit-intent record with no batch id for this task instance and no matching batch on "
            "the provider; submitting a new batch (on_orphaned_intent='resubmit'). This pays twice if the "
            "original request did reach the provider."
        )
        return None, None

    def _cancel_recorded_batch(
        self, record: BatchStateRecord, adapter: BatchAdapter, adapter_name: str
    ) -> None:
        """Cancel a stale batch through the connection that owns it, which may differ from the current one."""
        owner: BatchAdapter = adapter
        own_adapter = False
        if record.llm_conn_id and (record.llm_conn_id != self.llm_conn_id or record.adapter != adapter_name):
            try:
                owner = dispatch.build_adapter(record.adapter or adapter_name, llm_conn_id=record.llm_conn_id)
                own_adapter = True
            except Exception as e:
                self.log.warning(
                    "Could not build an adapter for connection %s to cancel stale batch %s: %s",
                    record.llm_conn_id,
                    record.batch_id,
                    e,
                )
                return
        try:
            owner.cancel_batch(record.batch_id)  # type: ignore[arg-type]
        except Exception as e:
            self.log.warning("Failed to cancel stale batch %s: %s", record.batch_id, e)
        finally:
            if own_adapter:
                owner.close()

    def _get_recorded_batch(self, adapter: BatchAdapter, batch_id: str) -> BatchState | None:
        """Fetch a recorded batch; ``None`` means the provider no longer knows it and a fresh submit follows."""
        try:
            return adapter.get_batch(batch_id)
        except Exception as e:
            if not self._is_not_found_error(e):
                raise
            self.log.warning(
                "Batch %s (recorded for this task instance) was not found on the provider; submitting a new batch.",
                batch_id,
            )
            return None

    def _push_batch_id_xcom(self, context: Context) -> None:
        """Push the observability-only ``batch_id`` XCom key, honoring ``do_xcom_push``."""
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
        spec: OutputSpec,
        submitted_at: str,
        reattached: bool,
    ) -> dict[str, Any]:
        # The budget is measured from submission, so a retry inside the budget does not get a
        # fresh one. A re-attach after the budget has elapsed (cancel_on_timeout=False left the
        # batch running) gets a new budget from now; otherwise it would time out on its first poll
        # and retries could never wait for the batch to finish.
        end_time = datetime.fromisoformat(submitted_at).timestamp() + self.timeout
        now = time.time()
        if reattached and now > end_time:
            self.log.info(
                "Re-attached to batch %s after its original timeout budget elapsed; waiting up to %ss more.",
                batch_id,
                self.timeout,
            )
            end_time = now + self.timeout
        if self.deferrable:
            self.defer(
                # A safety net only; the trigger's own end_time fires first and honors cancel_on_timeout.
                timeout=self.execution_timeout or timedelta(seconds=self.timeout + self.poll_interval + 60),
                trigger=LLMBatchTrigger(
                    llm_conn_id=self.llm_conn_id,
                    adapter=adapter_name,
                    batch_id=batch_id,
                    poll_interval=self.poll_interval,
                    end_time=end_time,
                    timeout=self.timeout,
                    cancel_on_kill=self.cancel_on_kill,
                    cancel_on_timeout=self.cancel_on_timeout,
                ),
                method_name="execute_complete",
            )
        event = self._poll_sync(adapter, batch_id, end_time=end_time)
        return self._handle_event(context, event, key=key, spec=spec)

    def _poll_sync(self, adapter: BatchAdapter, batch_id: str, *, end_time: float) -> dict[str, Any]:
        """Poll synchronously until terminal: the ``deferrable=False`` counterpart of ``LLMBatchTrigger.run()``."""
        poller = BatchPoller(
            batch_id=batch_id,
            end_time=end_time,
            timeout=self.timeout,
            cancel_on_timeout=self.cancel_on_timeout,
        )
        while True:
            try:
                batch_state = adapter.get_batch(batch_id)
            except Exception as e:
                outcome = poller.on_error(e, now=time.time())
                if outcome.event is None:
                    self.log.warning(
                        "Polling batch %s failed (attempt %d/%d): %s; retrying.",
                        batch_id,
                        poller.consecutive_failures,
                        MAX_CONSECUTIVE_POLL_FAILURES,
                        e,
                    )
            else:
                outcome = poller.on_state(batch_state, now=time.time())

            if outcome.event is None:
                time.sleep(self.poll_interval)
                continue
            if outcome.event["status"] != "timeout":
                return outcome.event

            cancelled = False
            cancel_error: str | None = None
            if outcome.cancel:
                try:
                    adapter.cancel_batch(batch_id)
                    cancelled = True
                    self.log.info(
                        "Cancelled batch %s: timeout=%ss exceeded with cancel_on_timeout=True",
                        batch_id,
                        self.timeout,
                    )
                except Exception as e:
                    cancel_error = str(e)
                    self.log.warning("Failed to cancel batch %s on timeout: %s", batch_id, e)
            return poller.finish_timeout(cancelled=cancelled, cancel_error=cancel_error)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> dict[str, Any]:
        """
        Resume after the trigger fires.

        This is a fresh operator instance (the deferred one released its
        worker slot), so ``batch_id`` comes from ``event``, not ``self``.
        """
        self.batch_id = event["batch_id"]
        key, _ = self._identity(context)
        return self._handle_event(context, event, key=key, spec=build_output_spec(self.output_type))

    def _handle_event(
        self, context: Context, event: dict[str, Any], *, key: str, spec: OutputSpec
    ) -> dict[str, Any]:
        status = event["status"]
        batch_id = event["batch_id"]
        result_path_osp = ObjectStoragePath(self.result_path)

        if status in ("success", "expired"):
            manifest = self._land(
                result_path_osp, batch_id=batch_id, key=key, spec=spec, extra_counts=event.get("counts")
            )
            self._log_landed(manifest)
            self._raise_if_partial(manifest)
            return manifest

        if status == "cancelled":
            manifest = self._land(
                result_path_osp, batch_id=batch_id, key=key, spec=spec, extra_counts=event.get("counts")
            )
            self._log_landed(manifest)
            lost = manifest["counts"]["cancelled"] + manifest["counts"]["missing"]
            if lost:
                # The cancel was deliberate (ours or out of band), so there is nothing in flight to
                # protect: clear the state so the next attempt submits fresh instead of re-landing
                # the same truncated results as a success.
                state.delete_state(result_path_osp, key)
                raise LLMBatchCancelledError(
                    f"Batch {batch_id!r} was cancelled before {lost} of {manifest['request_count']} requests "
                    f"completed (cancel_on_kill/cancel_on_timeout, or cancelled out of band). The results that "
                    f"did complete are at {manifest['result_uri']}. The recorded state was cleared, so a retry "
                    "submits a new batch."
                )
            self._raise_if_partial(manifest)
            return manifest

        if status == "timeout":
            raise LLMBatchTimeoutError(event["message"])

        if status == "failed":
            # The provider rejected the batch itself (OpenAI: input-file validation), so nothing ran and
            # nothing was billed. Keeping the state would make every retry re-attach to the same dead
            # batch; clear it so a retry resubmits.
            state.delete_state(result_path_osp, key)
            raise LLMBatchJobError(
                f"{event['message']} The provider reported the batch as failed before processing it; the "
                "recorded state was cleared so a retry submits a new batch."
            )

        # "error": polling gave up without knowing the batch's fate. Keep the state so a retry re-attaches.
        raise LLMBatchJobError(event["message"])

    def _log_landed(self, manifest: dict[str, Any]) -> None:
        counts = manifest["counts"]
        summary = ", ".join(f"{name}={value}" for name, value in counts.items() if value)
        self.log.info(
            "Landed %d results for batch %s at %s (%s; terminal_reason=%s)",
            manifest["request_count"],
            manifest["batch_id"],
            manifest["result_uri"],
            summary or "no results",
            manifest["terminal_reason"],
        )

    def _raise_if_partial(self, manifest: dict[str, Any]) -> None:
        if not self.fail_on_partial_error:
            return
        counts = manifest["counts"]
        buckets = {
            name: counts[name] for name in ("errored", "invalid_output", "expired", "cancelled", "missing")
        }
        if any(buckets.values()):
            detail = ", ".join(f"{name}={value}" for name, value in buckets.items() if value)
            raise LLMBatchPartialFailureError(
                f"Batch {manifest['batch_id']!r} did not produce a valid result for every request ({detail}); "
                f"fail_on_partial_error=True. Results are at {manifest['result_uri']}; the recorded state is "
                "kept, so a retry re-lands the same results without resubmitting."
            )

    def _land(
        self,
        result_path_osp: ObjectStoragePath,
        *,
        batch_id: str,
        key: str,
        spec: OutputSpec,
        extra_counts: dict[str, int] | None,
    ) -> dict[str, Any]:
        """Fetch, validate, merge and write the results; the one place that happens."""
        record = state.read_state(result_path_osp, key)
        if record is None or record.request_count is None:
            raise LLMBatchJobError(
                f"No recorded state found for batch {batch_id!r} at finalize time. Either the state file was "
                "deleted between submit and completion, or result_path rendered to a different value than "
                "at submit time; keep it stable across attempts of a task instance."
            )
        llm_conn_id = record.llm_conn_id or self.llm_conn_id
        model_id = record.model_id if record.model_id is not None else self.model_id
        adapter = dispatch.build_adapter(record.adapter or "", llm_conn_id=llm_conn_id)
        key16 = state.key16(key)
        destination = result_path_osp / f"{key16}.jsonl"
        try:
            merge_counts, merge_diagnostics = results.stream_results_to_jsonl(
                adapter=adapter,
                batch_id=batch_id,
                output_spec=spec,
                request_count=record.request_count,
                custom_id_prefix=key16,
                destination=destination,
            )
        finally:
            adapter.close()
        return results.assemble_manifest(
            batch_id=batch_id,
            adapter_name=record.adapter or "",
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

    def on_kill(self) -> None:
        """
        Cancel the batch if the (non-deferred) task is killed.

        Only fires while the worker process is alive. A killed *deferred*
        task is cancelled by the trigger's own ``on_kill`` instead, which only
        Airflow 3.3+ calls.
        """
        if not (self.cancel_on_kill and self.batch_id):
            return
        adapter = None
        try:
            _, adapter = self._build_adapter()
            adapter.cancel_batch(self.batch_id)
            self.log.info("on_kill: cancelled batch %s", self.batch_id)
        except Exception as e:
            self.log.warning("on_kill: failed to cancel batch %s: %s", self.batch_id, e)
        finally:
            if adapter is not None:
                adapter.close()
