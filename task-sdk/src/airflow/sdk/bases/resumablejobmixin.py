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

if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.sdk.definitions.context import Context
    from airflow.sdk.types import Logger


class ResumableJobMixin:
    """
    Mixin for operators that submit one long-running job to an external system and poll for completion.

    **Purpose:** This mixin makes the synchronous operator path crash-safe. It is not a replacement
    for deferrable operators — deferrable remains the recommended approach for long-running tasks when
    a Triggerer is available and the async model fits the team. This mixin is for teams already running
    synchronous operators who want worker crashes to reconnect to the existing job rather than
    resubmitting a duplicate.

    **How it works:** On the first run, after submitting the job, the external ID (driver ID, YARN
    application ID, etc.) is persisted to ``task_store`` before polling starts. On retry, the mixin
    reads that ID back and reconnects to the already-running job instead of starting a new one.

    **What it does not do:** It does not free the worker slot during polling (use deferrable for that),
    and it does not stream logs from the remote system (the operator controls that separately).

    Usage: call ``execute_resumable(context)`` from the operator's ``execute()`` when reconnection
    is supported.

    Subclasses must implement the methods specific to their external system. The mixin owns
    only ``execute_resumable()`` and the task_store read/write logic.

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

    # Key used to store and retrieve the external job ID from task_store across retries.
    # Renaming this on a deployed operator breaks in-flight retries — the old key is already stored.
    external_id_key: str = "remote_job_id"

    def execute_resumable(self, context: Context) -> Any:
        """
        Core of the resumable execution logic. Call this from execute() when reconnection is supported.

        On initial run: submits the job, persists the external ID to task_store, then polls.

        Behaviour on retry:
        - On retry with active job: skips submission, reconnects to the running job.
        - On retry with succeeded job: skips submission and polling, returns result immediately.
        - On retry with failed job: falls through and resubmits fresh.

        Known limitation: there is a small window between ``submit_job`` returning and
        ``task_store.set`` completing. If the worker dies in that gap, the next retry still
        holds the previous (terminal) ID and will resubmit a fresh job rather than reconnecting.
        Closing this window would require atomic "submit + persist", which is not possible across
        an external system boundary.
        """
        task_store = context.get("task_store")

        if task_store is not None:
            external_id = task_store.get(self.external_id_key)
            if external_id:
                status = self.get_job_status(external_id, context)
                if self.is_job_active(status):
                    self.log.info(
                        "Reconnecting to existing job identified by: %s (status: %s)", external_id, status
                    )
                    return self.poll_until_complete(external_id, context)
                if self.is_job_succeeded(status):
                    self.log.info(
                        "Job with identifier: %s already completed successfully, skipping resubmission",
                        external_id,
                    )
                    return self.get_job_result(external_id, context)
                self.log.info(
                    "Prior job with identifier: %s in terminal state %s, resubmitting fresh",
                    external_id,
                    status,
                )

        external_id = self.submit_job(context)

        if task_store is not None and external_id is not None:
            task_store.set(self.external_id_key, external_id)

        self.poll_until_complete(external_id, context)
        return self.get_job_result(external_id, context)

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

        - cache terminal status to ``task_store`` when the remote resource may be
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
