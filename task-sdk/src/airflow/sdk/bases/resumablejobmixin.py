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

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from opentelemetry import trace

from airflow.sdk._shared.observability.metrics import stats
from airflow.sdk.bases.operator import BaseOperatorMeta

if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.sdk.definitions.context import Context
    from airflow.sdk.types import Logger

tracer = trace.get_tracer(__name__)


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
    is supported.

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

    # The mixin is not a BaseOperator subclass, but _apply_defaults is only ever called on concrete
    # operators that are BaseOperator subclasses. That is a runtime MRO guarantee not visible in the static
    # type signature here and hence we need the type ignore.
    @BaseOperatorMeta._apply_defaults  # type: ignore[type-var]
    def __init__(self, *, durable: bool = True, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.durable = durable

    def execute_resumable(self, context: Context) -> Any:
        """
        Core of the resumable execution logic. Call this from execute() when reconnection is supported.

        On initial run: submits the job, persists the external ID to task_state_store, then polls.

        Behaviour on retry:
        - On retry with active job: skips submission, reconnects to the running job.
        - On retry with succeeded job: skips submission and polling, returns result immediately.
        - On retry with failed job: falls through and resubmits fresh.

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

        stats_tags = {"operator": type(self).__name__}
        # The task is team-scoped in multi-team deployments; surface team_name on the
        # resumable_job metrics via the running task instance's stats tags (omitted when
        # not multi-team or the task has no team).
        ti = context.get("ti")
        if ti is not None and (team_name := ti.stats_tags.get("team_name")):
            stats_tags["team_name"] = team_name

        reconnect_to: Any = None
        already_succeeded_id: Any = None

        with tracer.start_as_current_span("resumable_job.resume_decision") as span:
            span.set_attribute("operator", type(self).__name__)
            span.set_attribute("resumable.external_id_key", self.external_id_key)

            task_state_store = context.get("task_state_store")

            if task_state_store is None:
                span.set_attribute("resumable.decision", "no_task_state_store")
                self.log.warning(
                    "task_state_store not available in context, crash recovery is disabled for this run"
                )
            else:
                external_id = task_state_store.get(self.external_id_key)
                if external_id:
                    stats.incr("resumable_job.reconnect_attempt", tags=stats_tags)

                    status = self.get_job_status(external_id, context)

                    span.set_attribute("resumable.external_id", str(external_id))
                    span.set_attribute("resumable.prior_status", status)

                    if self.is_job_active(status):
                        # Job is still running, skip submission and reconnect to it.
                        span.set_attribute("resumable.decision", "reconnect")
                        stats.incr("resumable_job.reconnect_success", tags=stats_tags)
                        self.log.info(
                            "Reconnecting to existing job",
                            external_id_key=self.external_id_key,
                            external_id=external_id,
                            status=status,
                        )
                        reconnect_to = external_id
                    elif self.is_job_succeeded(status):
                        # Job already finished successfully, skip polling and return result directly.
                        span.set_attribute("resumable.decision", "already_succeeded")
                        stats.incr("resumable_job.already_succeeded", tags=stats_tags)
                        self.log.info(
                            "Job already completed successfully, skipping resubmission",
                            external_id_key=self.external_id_key,
                            external_id=external_id,
                        )
                        already_succeeded_id = external_id
                    else:
                        # Job is in a terminal failed state, fall through and submit a new job.
                        span.set_attribute("resumable.decision", "terminal_resubmit")
                        stats.incr("resumable_job.terminal_resubmit", tags=stats_tags)
                        self.log.warning(
                            "Prior job in terminal state, resubmitting fresh",
                            external_id_key=self.external_id_key,
                            external_id=external_id,
                            status=status,
                        )
                else:
                    span.set_attribute("resumable.decision", "fresh_submit")
                    stats.incr("resumable_job.fresh_submit", tags=stats_tags)
                    self.log.debug(
                        "No stored external ID found; submitting fresh job",
                        external_id_key=self.external_id_key,
                    )

        if reconnect_to is not None:
            return self.poll_until_complete(reconnect_to, context)
        if already_succeeded_id is not None:
            return self.get_job_result(already_succeeded_id, context)
        external_id = self.submit_job(context)

        if task_state_store is not None and external_id is not None:
            task_state_store.set(self.external_id_key, external_id)
            self.log.debug(
                "Persisted external ID to task store",
                external_id_key=self.external_id_key,
                external_id=external_id,
            )

        self.poll_until_complete(external_id, context)
        return self.get_job_result(external_id, context)

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
