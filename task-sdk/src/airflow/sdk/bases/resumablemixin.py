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

import structlog

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context

log = structlog.get_logger(__name__)


class ResumableJobMixin:
    """
    Mixin for operators that submit one long-running job to an external system and poll for completion.

    On retry, reads the persisted external ID from task_state and reconnects to the existing job
    instead of resubmitting from scratch. Reconnect can mean different for different kind of jobs.

    Usage: call ``execute_resumable(context)`` from the operator's ``execute()`` when reconnection
    is supported.

    Subclasses must implement the methods specific to their external system. The mixin owns
    only ``execute_resumable()`` and the task_state read/write logic.

    Example::

        class MyOperator(ResumableJobMixin, BaseOperator):
            external_id_key = "my_job_id"

            def execute(self, context):
                return self.execute_resumable(context)

            def submit_job(self, context) -> str:
                return self.hook.submit(...)

            def get_job_status(self, external_id: str) -> str:
                return self.hook.get_status(external_id)

            def is_job_active(self, status: str) -> bool:
                return status in ("RUNNING", "PENDING")

            def is_job_succeeded(self, status: str) -> bool:
                return status == "SUCCEEDED"

            def poll_until_complete(self, external_id: str, context: Context) -> None:
                self.hook.poll(external_id)

            def get_job_result(self, external_id: str, context: Context) -> Any:
                return None
    """

    external_id_key: str = "remote_job_id"

    def execute_resumable(self, context: Context) -> Any:
        """
        Core of the resumable execution logic. Call this from execute() when reconnection is supported.

        On initial run: submits the job, persists the external ID to task_state, then polls.

        Behaviour on retry:
        - On retry with active job: skips submission, reconnects to the running job.
        - On retry with succeeded job: skips submission and polling, returns result immediately.
        - On retry with failed job: falls through and resubmits fresh.
        """
        task_state = context.get("task_state")

        if task_state is not None:
            external_id = task_state.get(self.external_id_key)
            if external_id:
                status = self.get_job_status(external_id)
                if self.is_job_active(status):
                    log.info(
                        "Reconnecting to existing job identified by: %s (status: %s)", external_id, status
                    )
                    return self.poll_until_complete(external_id, context)
                if self.is_job_succeeded(status):
                    log.info(
                        "Job with identifier: %s already completed successfully, skipping resubmission",
                        external_id,
                    )
                    return self.get_job_result(external_id, context)
                log.info(
                    "Prior job with identifier: %s in terminal state %s, resubmitting fresh",
                    external_id,
                    status,
                )

        external_id = self.submit_job(context)

        if task_state is not None:
            task_state.set(self.external_id_key, external_id)

        self.poll_until_complete(external_id, context)
        return self.get_job_result(external_id, context)

    def submit_job(self, context: Context) -> str:
        """Submit the job to the external system. Return its external ID."""
        raise NotImplementedError

    def get_job_status(self, external_id: str) -> str:
        """Query the external system for the current job status."""
        raise NotImplementedError

    def is_job_active(self, status: str) -> bool:
        """Return True if the job is still running and can be reconnected to."""
        raise NotImplementedError

    def is_job_succeeded(self, status: str) -> bool:
        """Return True if the job completed successfully."""
        raise NotImplementedError

    def poll_until_complete(self, external_id: str, context: Context) -> None:
        """Block until the job reaches a terminal state. Raise on failure."""
        raise NotImplementedError

    def get_job_result(self, external_id: str, context: Context) -> Any:
        """Return the job result after completion. Return None if not applicable."""
        raise NotImplementedError
