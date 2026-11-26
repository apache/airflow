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
from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, Literal, NamedTuple

from opentelemetry import trace

from airflow.sdk._shared.observability.metrics import stats
from airflow.sdk.bases.operator import BaseOperatorMeta

if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.sdk.definitions.context import Context
    from airflow.sdk.types import Logger

tracer = trace.get_tracer(__name__)

# Read by the durable execution banner on the task logs page.
DURABLE_RECORD_KEY = "__airflow_durable__"


class DurableDecision(StrEnum):
    """What a try did with the external job; mirrored in the logs banner."""

    SUBMITTED = "submitted"
    RESUBMITTED = "resubmitted"
    RECONNECTED = "reconnected"
    ALREADY_SUCCEEDED = "already_succeeded"
    RESUMED = "resumed"


class ResumeDecision(NamedTuple):
    """
    Result of :meth:`ResumableJobMixin.get_resume_decision`.

    ``external_id`` is the stored job to reconnect to or take the result of. On ``resume`` it is the
    failed job to resume. On ``submit`` it is the previous job that ended without success, or ``None``
    when there is none.

    ``status`` is set only on ``resume``: the failed job's raw status from ``get_job_status``, which
    :meth:`ResumableJobMixin.resume_stored_job` passes to ``resume_failed_job``. It is ``None`` for
    every other action.
    """

    action: Literal["reconnect", "already_succeeded", "resume", "submit"]
    external_id: JsonValue | None = None
    status: str | None = None


class ResumableJobMixin(ABC):
    """
    Mixin for operators that submit one long-running job to an external system and poll for completion.

    **Purpose:** This mixin makes the synchronous operator path crash-safe. It is not a replacement
    for deferrable operators — deferrable remains the recommended approach for long-running tasks when
    a Triggerer is available and the async model fits the team. This mixin is for teams already running
    synchronous operators who want worker crashes to reconnect to the existing job rather than
    resubmitting a duplicate.

    **How it works:** On the first run, after submitting the job, the external ID (driver ID, YARN
    application ID, etc.) is persisted to ``task_state_store`` before polling starts. On retry, the mixin
    reads that ID back and reconnects to the already-running job instead of starting a new one.

    **What it does not do:** It does not free the worker slot during polling (use deferrable for that),
    and it does not stream logs from the remote system (the operator controls that separately).

    Usage: call ``execute_resumable(context)`` from the operator's ``execute()`` when reconnection
    is supported. An operator that cannot poll inline (e.g. one that defers) calls
    ``get_resume_decision(context)`` and ``persist_job_id(context, external_id)`` instead.

    Subclasses must implement all the methods specific to their external system. The mixin owns
    only ``execute_resumable()`` and the task_state_store read/write logic.

    Example::

        class MyOperator(ResumableJobMixin, BaseOperator):
            external_id_key = "my_job_id"

            def execute(self, context):
                return self.execute_resumable(context)

            def submit_job(self, context) -> JsonValue:
                return self.hook.submit(...)

            def get_job_status(self, external_id: JsonValue, context: Context) -> str:
                return self.hook.get_status(external_id)

            def is_job_active(self, status: str) -> bool:
                return status in ("RUNNING", "PENDING")

            def is_job_succeeded(self, status: str) -> bool:
                return status == "SUCCEEDED"

            def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
                self.hook.poll(external_id)

            def get_job_result(self, external_id: JsonValue, context: Context) -> Any:
                return None
    """

    if TYPE_CHECKING:
        # log comes from BaseOperator (via LoggingMixin) at runtime, but mypy cannot see
        # that because ResumableJobMixin does not inherit from it directly.
        log: Logger

    # Key used to store and retrieve the external job ID from task_state_store across retries.
    # Renaming this on a deployed operator breaks in-flight retries — the old key is already stored.
    external_id_key: str = "remote_job_id"

    resume_status_checks: int = 3
    resume_status_check_interval: float = 10.0

    # The mixin is not a BaseOperator subclass, but _apply_defaults is only ever called on concrete
    # operators that are BaseOperator subclasses. That is a runtime MRO guarantee not visible in the static
    # type signature here and hence we need the type ignore.
    @BaseOperatorMeta._apply_defaults  # type: ignore[type-var]
    def __init__(self, *, durable: bool = True, resume_on_retry: bool = True, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.durable = durable
        self.resume_on_retry = resume_on_retry

    def execute_resumable(self, context: Context) -> Any:
        """
        Core of the resumable execution logic. Call this from execute() when reconnection is supported.

        On initial run: submits the job, persists the external ID to task_state_store, then polls.

        Behaviour on retry:

        - On retry with active job: skips submission, reconnects to the running job.
        - On retry with succeeded job: skips submission and polling, returns result immediately.
        - On retry with failed job: if ``resume_on_retry`` is set, asks ``resume_failed_job`` to resume
          it, and resubmits fresh if it returns ``None`` or raises. Otherwise resubmits fresh.

        Known limitation: there is a small window between ``submit_job`` returning and
        ``task_state_store.set`` completing. If the worker dies in that gap, the next retry still
        holds the previous (terminal) ID and will resubmit a fresh job rather than reconnecting.
        Closing this window would require atomic "submit + persist", which is not possible across
        an external system boundary.
        """
        if not self.durable:
            external_id = self.submit_job(context)
            self.poll_until_complete(external_id, context)
            return self.get_job_result(external_id, context)

        decision = self.get_resume_decision(context)
        if decision.action == "reconnect":
            try:
                result = self.poll_until_complete(decision.external_id, context)
            except Exception:
                self._record_poll_failure(context, DurableDecision.RECONNECTED, decision.external_id)
                raise
            self._record_poll_outcome(context, DurableDecision.RECONNECTED, decision.external_id)
            return result
        if decision.action == "already_succeeded":
            return self.get_job_result(decision.external_id, context)
        if decision.action == "resume":
            resumed_id = self.resume_stored_job(context, decision)
            if resumed_id is not None:
                previous_id = decision.external_id if resumed_id != decision.external_id else None
                try:
                    self.poll_until_complete(resumed_id, context)
                except Exception:
                    self._record_poll_failure(
                        context, DurableDecision.RESUMED, resumed_id, previous_external_id=previous_id
                    )
                    raise
                self._record_poll_outcome(
                    context, DurableDecision.RESUMED, resumed_id, previous_external_id=previous_id
                )
                return self.get_job_result(resumed_id, context)

        # The replaced job's ID is kept in the history record; the banner does not show resubmits.
        failed_id = decision.external_id
        external_id = self.submit_job(context)
        self.persist_job_id(context, external_id)
        if external_id is not None:
            if failed_id is None:
                self._record_try(context, DurableDecision.SUBMITTED, external_id)
            else:
                self._record_try(
                    context, DurableDecision.RESUBMITTED, external_id, previous_external_id=failed_id
                )
        self.poll_until_complete(external_id, context)
        return self.get_job_result(external_id, context)

    def get_resume_decision(self, context: Context) -> ResumeDecision:
        """
        Decide whether to reconnect to the stored job, take its result, or submit a new one.

        Never submits, resumes or polls, so a deferrable operator can call it before ``defer()``. A
        reconnect or already succeeded decision is recorded for the logs banner here, so every caller gets
        it. On ``"resume"``, call :meth:`resume_stored_job` and submit a new job if it returns ``None``.
        """
        if not self.durable:
            return ResumeDecision("submit")

        ti = context.get("ti")
        stats_tags = {**(ti.stats_tags if ti is not None else {}), "operator": type(self).__name__}

        with tracer.start_as_current_span("resumable_job.resume_decision") as span:
            span.set_attribute("operator", type(self).__name__)
            span.set_attribute("resumable.external_id_key", self.external_id_key)

            task_state_store = context.get("task_state_store")
            if task_state_store is None:
                span.set_attribute("resumable.decision", "no_task_state_store")
                self.log.warning(
                    "task_state_store not available in context, crash recovery is disabled for this run"
                )
                return ResumeDecision("submit")

            external_id = task_state_store.get(self.external_id_key)
            if external_id is None:
                span.set_attribute("resumable.decision", "fresh_submit")
                stats.incr("resumable_job.fresh_submit", tags=stats_tags)
                self.log.debug(
                    "No stored external ID found; submitting fresh job",
                    external_id_key=self.external_id_key,
                )
                return ResumeDecision("submit")

            stats.incr("resumable_job.reconnect_attempt", tags=stats_tags)
            status = self.get_job_status(external_id, context)
            span.set_attribute("resumable.external_id", str(external_id))
            span.set_attribute("resumable.prior_status", status)

            if self.is_job_active(status):
                span.set_attribute("resumable.decision", "reconnect")
                stats.incr("resumable_job.reconnect_success", tags=stats_tags)
                self.log.info(
                    "Reconnecting to existing job",
                    external_id_key=self.external_id_key,
                    external_id=external_id,
                    status=status,
                )
                self._record_try(context, DurableDecision.RECONNECTED, external_id, status="running")
                return ResumeDecision("reconnect", external_id)

            if self.is_job_succeeded(status):
                span.set_attribute("resumable.decision", "already_succeeded")
                stats.incr("resumable_job.already_succeeded", tags=stats_tags)
                self.log.info(
                    "Job already completed successfully, skipping resubmission",
                    external_id_key=self.external_id_key,
                    external_id=external_id,
                )
                self._record_try(context, DurableDecision.ALREADY_SUCCEEDED, external_id, status="success")
                return ResumeDecision("already_succeeded", external_id)

            if self.resume_on_retry and self._can_resume_failed_job():
                span.set_attribute("resumable.decision", "resume")
                return ResumeDecision("resume", external_id, status)

            self._record_terminal_resubmit(external_id, status, stats_tags)
            span.set_attribute("resumable.decision", "terminal_resubmit")
            return ResumeDecision("submit", external_id)

    def resume_stored_job(self, context: Context, decision: ResumeDecision) -> JsonValue | None:
        """
        Resume the failed job from a ``"resume"`` decision and return the ID to poll.

        Returns ``None`` when the operator declines, or when the resume raised and the job still looks
        failed, so the caller submits a new job. A new ID is stored so the next retry finds it.
        """
        ti = context.get("ti")
        stats_tags = {**(ti.stats_tags if ti is not None else {}), "operator": type(self).__name__}
        external_id, status = decision.external_id, decision.status or ""
        resumed_id, error = self._resume_failed_job_safely(external_id, status, context)
        if error is not None:
            resumed_id = self._find_job_resumed_despite_error(external_id, context, error, stats_tags)
        if resumed_id is None:
            self._record_terminal_resubmit(external_id, status, stats_tags)
            return None
        if error is None:
            stats.incr("resumable_job.resumed", tags=stats_tags)
            self.log.info(
                "Resuming failed job from where it stopped",
                external_id_key=self.external_id_key,
                external_id=external_id,
                resumed_id=resumed_id,
                status=status,
            )
        if resumed_id != external_id:
            self.persist_job_id(context, resumed_id)
        self._record_try(
            context,
            DurableDecision.RESUMED,
            resumed_id,
            status="running",
            previous_external_id=external_id if resumed_id != external_id else None,
        )
        return resumed_id

    def _record_terminal_resubmit(self, external_id: JsonValue, status: str, stats_tags: dict) -> None:
        stats.incr("resumable_job.terminal_resubmit", tags=stats_tags)
        self.log.warning(
            "Prior job in terminal state, resubmitting fresh",
            external_id_key=self.external_id_key,
            external_id=external_id,
            status=status,
        )

    def _can_resume_failed_job(self) -> bool:
        return type(self).resume_failed_job is not ResumableJobMixin.resume_failed_job

    def persist_job_id(self, context: Context, external_id: JsonValue) -> None:
        """Store a newly submitted job's external ID so a retry can reconnect to it."""
        if not self.durable or external_id is None:
            return
        task_state_store = context.get("task_state_store")
        if task_state_store is None:
            return
        task_state_store.set(self.external_id_key, external_id)
        self.log.debug(
            "Persisted external ID to task store",
            external_id_key=self.external_id_key,
            external_id=external_id,
        )

    def resume_failed_job(self, external_id: JsonValue, status: str, context: Context) -> JsonValue | None:
        """
        Ask the external system to resume a failed job from where it stopped, instead of starting over.

        Called on a retry when the stored job is in a terminal failed state. Return the ID to poll,
        which may be the same ID (for example a Databricks run repaired in place) or a new one. Return
        ``None`` to submit a fresh job, which is the default.

        Only called when ``resume_on_retry`` is set. A cleared task reaches it only when the clear kept
        task state, because clearing otherwise discards the stored ID.

        If this raises, the mixin re-checks the stored job's status before submitting a fresh job. An
        engine that resumes under a new ID must handle a lost reply itself.
        """
        return None

    def _resume_failed_job_safely(
        self, external_id: JsonValue, status: str, context: Context
    ) -> tuple[JsonValue | None, Exception | None]:
        """Return the ID to poll, or ``None``, and the error the resume raised, if any."""
        try:
            return self.resume_failed_job(external_id, status, context), None
        except Exception as error:
            self.log.warning(
                "Resume of failed job raised, checking whether it started anyway",
                external_id_key=self.external_id_key,
                external_id=external_id,
                exc_info=True,
            )
            return None, error

    def _find_job_resumed_despite_error(
        self, external_id: JsonValue, context: Context, error: Exception, stats_tags: dict[str, str]
    ) -> JsonValue | None:
        """
        Return ``external_id`` if the job is active or succeeded although its resume raised.

        Return ``None`` if it still looks failed after every check, so a fresh job is safe to submit.
        Re-raise ``error`` if the status cannot be read, because guessing could start a second job.
        """
        for check in range(self.resume_status_checks):
            if check:
                # The engine can take a few seconds to show an accepted resume as active.
                time.sleep(self.resume_status_check_interval)
            try:
                status = self.get_job_status(external_id, context)
            except Exception:
                self.log.exception(
                    "Could not read job status after its resume raised, failing this try",
                    external_id_key=self.external_id_key,
                    external_id=external_id,
                )
                raise error
            if self.is_job_active(status) or self.is_job_succeeded(status):
                stats.incr("resumable_job.resume_reply_lost", tags=stats_tags)
                self.log.warning(
                    "Resume raised but the job resumed anyway, watching it instead of submitting a new one",
                    external_id_key=self.external_id_key,
                    external_id=external_id,
                    status=status,
                )
                return external_id
        self.log.warning(
            "Job still failed after its resume raised, resubmitting fresh",
            external_id_key=self.external_id_key,
            external_id=external_id,
        )
        return None

    def _record_poll_outcome(
        self,
        context: Context,
        decision: DurableDecision,
        external_id: JsonValue,
        *,
        previous_external_id: JsonValue = None,
    ) -> None:
        """Record the watched job's status once polling returns, which may be before the job ends."""
        try:
            job_status = self.get_job_status(external_id, context)
        except Exception:
            self.log.warning("Could not read job status for durable execution history", exc_info=True)
            return
        # A poll that returned without raising accepted the job's end state (e.g. SageMaker ``Stopped``).
        if not self.is_job_active(job_status):
            self._record_try(
                context, decision, external_id, status="success", previous_external_id=previous_external_id
            )

    def _record_poll_failure(
        self,
        context: Context,
        decision: DurableDecision,
        external_id: JsonValue,
        *,
        previous_external_id: JsonValue = None,
    ) -> None:
        """Record ``failed`` only if the job ended; a wait timeout raises while the job still runs."""
        try:
            job_status = self.get_job_status(external_id, context)
        except Exception:
            self.log.warning("Could not read job status for durable execution history", exc_info=True)
            return
        if not self.is_job_active(job_status) and not self.is_job_succeeded(job_status):
            self._record_try(
                context, decision, external_id, status="failed", previous_external_id=previous_external_id
            )

    def _record_try(
        self,
        context: Context,
        decision: DurableDecision,
        external_id: JsonValue,
        *,
        status: Literal["running", "success", "failed"] | None = None,
        previous_external_id: JsonValue = None,
    ) -> None:
        """
        Record this try for the logs banner.

        ``status`` is an Airflow state rather than the provider's raw status so the UI can render it.
        A reconnect that ends in success stores the job's run time as saved.
        """
        task_state_store = context.get("task_state_store")
        if task_state_store is None:
            return
        try:
            now = datetime.now(tz=UTC)
            stored = task_state_store.get(DURABLE_RECORD_KEY)
            record: dict[str, Any] = stored if isinstance(stored, dict) else {}
            try_number = getattr(context.get("ti"), "try_number", None)
            tries = [t for t in record.get("tries", []) if t.get("try_number") != try_number]
            submitted_at: str | None
            if decision in (DurableDecision.SUBMITTED, DurableDecision.RESUBMITTED):
                submitted_at = now.isoformat()
            else:
                # Per try, so a later resubmit cannot change the submit time an earlier try shows.
                submitted_at = next(
                    (
                        t["submitted_at"]
                        for t in reversed(tries)
                        if t.get("external_id") == external_id and t.get("submitted_at")
                    ),
                    None,
                )
            saved_seconds = None
            if decision == DurableDecision.RECONNECTED and status == "success" and submitted_at:
                saved_seconds = (now - datetime.fromisoformat(submitted_at)).total_seconds()
            tries.append(
                {
                    "try_number": try_number,
                    "decision": decision.value,
                    "external_id": external_id,
                    "status": status,
                    "submitted_at": submitted_at,
                    "saved_seconds": saved_seconds,
                    **({"previous_external_id": previous_external_id} if previous_external_id else {}),
                }
            )
            task_state_store.set(DURABLE_RECORD_KEY, {"tries": tries})
        except Exception:
            # Only feeds the UI, so it must never fail the task.
            self.log.warning("Could not record durable execution history", exc_info=True)

    @abstractmethod
    def submit_job(self, context: Context) -> JsonValue:
        """
        Submit the job to the external system. Return its external ID.

        The returned ID must not be ``None``, a ``None`` return is treated as
        "no ID available" and the ID will not be persisted to task state.
        """
        raise NotImplementedError

    @abstractmethod
    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        """
        Query the external system for the current job status.

        ``context`` is provided so implementations can use it if needed to implement advanced features such as:

        - cache terminal status to ``task_state_store`` when the remote resource may be
          ephemeral (e.g. a K8s driver pod that gets garbage-collected after completion),
        """
        raise NotImplementedError

    @abstractmethod
    def is_job_active(self, status: str) -> bool:
        """
        Return True if the job is still running and can be reconnected to.

        ``status`` is a raw string returned by the external system — not an Airflow enum.
        Its values are backend-specific (e.g. ``"RUNNING"``, ``"Pending"``, ``"ContainerCreating"``).
        """
        raise NotImplementedError

    @abstractmethod
    def is_job_succeeded(self, status: str) -> bool:
        """
        Return True if the job completed successfully.

        ``status`` is a raw string returned by the external system — not an Airflow enum.
        Its values are backend-specific (e.g. ``"FINISHED"``, ``"Succeeded"``).
        """
        raise NotImplementedError

    @abstractmethod
    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        """Block until the job reaches a terminal state. Raise on failure."""
        raise NotImplementedError

    @abstractmethod
    def get_job_result(self, external_id: JsonValue, context: Context) -> Any:
        """Return the job result after completion. Return None if not applicable."""
        raise NotImplementedError
