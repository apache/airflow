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

from typing import TYPE_CHECKING, Any

from opentelemetry import trace

from airflow.sdk._shared.observability.metrics import stats
from airflow.sdk.bases.operator import BaseOperatorMeta

if TYPE_CHECKING:
    from collections.abc import Callable

    from pydantic import JsonValue
    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.definitions.context import Context
    from airflow.sdk.types import Logger

tracer = trace.get_tracer(__name__)


class ResumableJobMixin:
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

    Subclasses must implement the methods specific to their external system. The mixin owns
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
        Crash-safe submit-and-poll for synchronous operators. Call from ``execute()``.

        Binds the operator's ``submit_job`` / ``get_job_status`` / … methods to
        :func:`resume_or_submit`, which owns the persist + three-state reconnect logic. See that
        function for the behaviour and its known submit-vs-persist limitation.
        """
        stats_tags = {"operator": type(self).__name__}
        # The task is team-scoped in multi-team deployments; surface team_name on the
        # resumable_job metrics via the running task instance's stats tags (omitted when
        # not multi-team or the task has no team).
        ti = context.get("ti")
        if ti is not None and (team_name := ti.stats_tags.get("team_name")):
            stats_tags["team_name"] = team_name

        return resume_or_submit(
            durable=self.durable,
            external_id_key=self.external_id_key,
            task_state_store=context.get("task_state_store"),
            submit=lambda: self.submit_job(context),
            get_status=lambda external_id: self.get_job_status(external_id, context),
            is_active=self.is_job_active,
            is_succeeded=self.is_job_succeeded,
            poll=lambda external_id: self.poll_until_complete(external_id, context),
            get_result=lambda external_id: self.get_job_result(external_id, context),
            log=self.log,
            operator_name=type(self).__name__,
            stats_tags=stats_tags,
        )

    def submit_job(self, context: Context) -> JsonValue:
        """
        Submit the job to the external system. Return its external ID.

        The returned ID must not be ``None``, a ``None`` return is treated as
        "no ID available" and the ID will not be persisted to task state.
        """
        raise NotImplementedError

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        """
        Query the external system for the current job status.

        ``context`` is provided so implementations can use it if needed to implement advanced features such as:

        - cache terminal status to ``task_state_store`` when the remote resource may be
          ephemeral (e.g. a K8s driver pod that gets garbage-collected after completion),
        """
        raise NotImplementedError

    def is_job_active(self, status: str) -> bool:
        """
        Return True if the job is still running and can be reconnected to.

        ``status`` is a raw string returned by the external system — not an Airflow enum.
        Its values are backend-specific (e.g. ``"RUNNING"``, ``"Pending"``, ``"ContainerCreating"``).
        """
        raise NotImplementedError

    def is_job_succeeded(self, status: str) -> bool:
        """
        Return True if the job completed successfully.

        ``status`` is a raw string returned by the external system — not an Airflow enum.
        Its values are backend-specific (e.g. ``"FINISHED"``, ``"Succeeded"``).
        """
        raise NotImplementedError

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        """Block until the job reaches a terminal state. Raise on failure."""
        raise NotImplementedError

    def get_job_result(self, external_id: JsonValue, context: Context) -> Any:
        """Return the job result after completion. Return None if not applicable."""
        raise NotImplementedError


def resume_or_submit(
    *,
    durable: bool,
    external_id_key: str,
    task_state_store: Any,
    submit: Callable[[], JsonValue],
    get_status: Callable[[JsonValue], str],
    is_active: Callable[[str], bool],
    is_succeeded: Callable[[str], bool],
    poll: Callable[[JsonValue], Any],
    get_result: Callable[[JsonValue], Any],
    log: FilteringBoundLogger,
    operator_name: str,
    stats_tags: dict[str, str],
) -> Any:
    """
    Submit an external job and poll for it, reconnecting to a prior run on retry.

    The reusable core of crash-safe submit-and-poll execution, independent of any operator.
    ``ResumableJobMixin`` wraps it for synchronous operators, but it can equally be driven from the
    task runner for operators whose wait happens outside ``execute()`` (e.g. ``TriggerDagRunOperator``,
    which raises an exception and is polled by the runner).

    The callbacks are the external-system bindings: ``submit`` starts the job and returns its external
    id, ``get_status`` reads the raw backend status, ``is_active`` / ``is_succeeded`` classify it,
    ``poll`` blocks until terminal (raising on failure), ``get_result`` returns the result. On the
    first run the external id is persisted to ``task_state_store`` before polling; on retry it is read
    back and the job is reconnected (active), returned (succeeded), or resubmitted (terminal / missing).

    Known limitation: there is a small window between ``submit`` returning and the persist completing.
    A crash in that gap resubmits fresh on the next retry rather than reconnecting; closing it would
    require an atomic "submit + persist", which is not possible across an external system boundary.
    """
    if not durable:
        external_id = submit()
        poll(external_id)
        return get_result(external_id)

    reconnect_to: Any = None
    already_succeeded_id: Any = None

    with tracer.start_as_current_span("resumable_job.resume_decision") as span:
        span.set_attribute("operator", operator_name)
        span.set_attribute("resumable.external_id_key", external_id_key)

        if task_state_store is None:
            span.set_attribute("resumable.decision", "no_task_state_store")
            log.warning("task_state_store not available in context, crash recovery is disabled for this run")
        else:
            external_id = task_state_store.get(external_id_key)
            if external_id:
                stats.incr("resumable_job.reconnect_attempt", tags=stats_tags)

                status = get_status(external_id)

                span.set_attribute("resumable.external_id", str(external_id))
                span.set_attribute("resumable.prior_status", status)

                if is_active(status):
                    span.set_attribute("resumable.decision", "reconnect")
                    stats.incr("resumable_job.reconnect_success", tags=stats_tags)
                    log.info(
                        "Reconnecting to existing job",
                        external_id_key=external_id_key,
                        external_id=external_id,
                        status=status,
                    )
                    reconnect_to = external_id
                elif is_succeeded(status):
                    span.set_attribute("resumable.decision", "already_succeeded")
                    stats.incr("resumable_job.already_succeeded", tags=stats_tags)
                    log.info(
                        "Job already completed successfully, skipping resubmission",
                        external_id_key=external_id_key,
                        external_id=external_id,
                    )
                    already_succeeded_id = external_id
                else:
                    span.set_attribute("resumable.decision", "terminal_resubmit")
                    stats.incr("resumable_job.terminal_resubmit", tags=stats_tags)
                    log.warning(
                        "Prior job in terminal state, resubmitting fresh",
                        external_id_key=external_id_key,
                        external_id=external_id,
                        status=status,
                    )
            else:
                span.set_attribute("resumable.decision", "fresh_submit")
                stats.incr("resumable_job.fresh_submit", tags=stats_tags)
                log.debug(
                    "No stored external ID found; submitting fresh job",
                    external_id_key=external_id_key,
                )

    if reconnect_to is not None:
        return poll(reconnect_to)
    if already_succeeded_id is not None:
        return get_result(already_succeeded_id)

    external_id = submit()
    if task_state_store is not None and external_id is not None:
        task_state_store.set(external_id_key, external_id)
        log.debug(
            "Persisted external ID to task store",
            external_id_key=external_id_key,
            external_id=external_id,
        )
    poll(external_id)
    return get_result(external_id)
