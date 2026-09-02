#
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

import datetime
import time
import warnings
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from airbyte_api.models import JobStatusEnum

from airflow.providers.airbyte.hooks.airbyte import AirbyteHook
from airflow.providers.airbyte.triggers.airbyte import AirbyteSyncTrigger
from airflow.providers.common.compat.sdk import AirflowException, BaseOperator, conf

_DURABLE_UNSET: object = object()


def _warn_and_disable_durable_pre_3_3(durable: object) -> bool:
    if durable is not _DURABLE_UNSET:
        warnings.warn(
            "`durable` has no effect on Airflow versions below 3.3.",
            UserWarning,
            stacklevel=3,
        )
    return False


try:
    from airflow.sdk import ResumableJobMixin
except ImportError:

    class ResumableJobMixin:  # type: ignore[no-redef]
        """Submit a fresh job on Airflow versions before 3.3."""

        external_id_key: str = "airbyte_job_id"

        def __init__(self, *, durable: Any = _DURABLE_UNSET, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.durable = _warn_and_disable_durable_pre_3_3(durable)

        def execute_resumable(self, context: Context) -> Any:
            external_id = self.submit_job(context=context)
            self.poll_until_complete(external_id=external_id, context=context)
            return self.get_job_result(external_id=external_id, context=context)


if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.providers.common.compat.sdk import Context


class AirbyteTriggerSyncOperator(ResumableJobMixin, BaseOperator):
    """
    Submits a job to an Airbyte server to run a integration process between your source and destination.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AirbyteTriggerSyncOperator`

    :param airbyte_conn_id: Optional. The name of the Airflow connection to get connection
        information for Airbyte. Defaults to "airbyte_default".
    :param connection_id: Required. The Airbyte ConnectionId UUID between a source and destination.
    :param asynchronous: Optional. Flag to get job_id after submitting the job to the Airbyte API.
        This is useful for submitting long running jobs and
        waiting on them asynchronously using the AirbyteJobSensor. Defaults to False.
    :param deferrable: Run operator in the deferrable mode.
    :param api_version: Optional. Airbyte API version. Defaults to "v1".
    :param wait_seconds: Optional. Number of seconds between checks. Only used when ``asynchronous`` is False.
        Defaults to 3 seconds.
    :param timeout: Optional. The amount of time, in seconds, to wait for the request to complete.
        Only used when ``asynchronous`` is False.  This limits how long the operator waits for the
        job to complete and does not imply job cancellation. Task-level timeouts should be
        enforced via ``execution_timeout``. Defaults to 3600 seconds (or 1 hour).
    :param execution_timeout: Maximum time allowed for the task to run. If exceeded, the Airbyte
        Job will be cancelled and the task will fail. When both ``execution_timeout`` and
        ``timeout`` are set, the earlier deadline takes precedence.
    :param durable: When True (the default), synchronous non-deferrable execution persists the
        Airbyte job id before polling. A retry reconnects to an active job, returns a succeeded
        job, or submits a new job after terminal failure. Requires Airflow 3.3+ and has no effect
        on earlier versions.
    """

    template_fields: Sequence[str] = ("connection_id",)
    ui_color = "#6C51FD"
    external_id_key: str = "airbyte_job_id"

    def __init__(
        self,
        connection_id: str,
        airbyte_conn_id: str = "airbyte_default",
        asynchronous: bool = False,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        api_version: str = "v1",
        wait_seconds: float = 3,
        timeout: float = 3600,
        durable: bool | None = None,
        **kwargs: Any,
    ) -> None:
        if durable is not None:
            kwargs["durable"] = durable
        super().__init__(**kwargs)
        self.airbyte_conn_id = airbyte_conn_id
        self.connection_id = connection_id
        self.timeout = timeout
        self.api_version = api_version
        self.wait_seconds = wait_seconds
        self.asynchronous = asynchronous
        self.deferrable = deferrable

    @cached_property
    def hook(self) -> AirbyteHook:
        return AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)

    def execute(self, context: Context) -> int | None:
        """Create Airbyte Job and wait to finish."""
        trigger_poll_interval = 60

        if not self.asynchronous and not self.deferrable:
            self.execute_resumable(context=context)
            return self.job_id

        job_object = self._submit_sync_connection()
        state = job_object.status
        now = time.time()

        # Derive absolute deadlines for deferrable execution.
        # execution_timeout is a hard task-level limit (cancels the job),
        # while timeout only limits how long we wait for the job to finish.
        # If both are set, the earliest deadline wins.
        end_time = now + self.timeout
        execution_deadline = None

        defer_timeout: datetime.timedelta | None = None

        if self.execution_timeout is not None:
            execution_deadline = now + self.execution_timeout.total_seconds()

            # Allow two trigger polling cycles for timeout events to be
            # processed and Airbyte job cancellation to be initiated before
            # the framework defer timeout expires.
            poll_buffer = 2 * trigger_poll_interval
            defer_timeout = datetime.timedelta(seconds=self.execution_timeout.total_seconds() + poll_buffer)

        if self.asynchronous:
            self.log.info("Async Task returning job_id %s", self.job_id)
            return self.job_id

        self.log.debug("Running in deferrable mode in job state %s...", state)
        if state in (JobStatusEnum.RUNNING, JobStatusEnum.PENDING, JobStatusEnum.INCOMPLETE):
            self.defer(
                timeout=defer_timeout,
                trigger=AirbyteSyncTrigger(
                    conn_id=self.airbyte_conn_id,
                    job_id=self.job_id,
                    end_time=end_time,
                    execution_deadline=execution_deadline,
                    poll_interval=trigger_poll_interval,
                ),
                method_name="execute_complete",
            )
        elif state == JobStatusEnum.SUCCEEDED:
            self.log.info("Job %s completed successfully", self.job_id)
            return None
        elif state == JobStatusEnum.FAILED:
            raise AirflowException(f"Job failed:\n{self.job_id}")
        elif state == JobStatusEnum.CANCELLED:
            raise AirflowException(f"Job was cancelled:\n{self.job_id}")
        else:
            raise AirflowException(f"Encountered unexpected state `{state}` for job_id `{self.job_id}")

        return None

    def _submit_sync_connection(self) -> Any:
        job_object = self.hook.submit_sync_connection(connection_id=self.connection_id)
        self.job_id = job_object.job_id
        self.log.info("Job %s was submitted to Airbyte Server", self.job_id)
        return job_object

    def submit_job(self, context: Context) -> JsonValue:
        self._submit_sync_connection()
        return self.job_id

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        self.job_id = cast("int", external_id)
        return self.hook.get_job_status(job_id=self.job_id)

    def is_job_active(self, status: str) -> bool:
        return status in (JobStatusEnum.RUNNING, JobStatusEnum.PENDING, JobStatusEnum.INCOMPLETE)

    def is_job_succeeded(self, status: str) -> bool:
        return status == JobStatusEnum.SUCCEEDED

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        self.job_id = cast("int", external_id)
        self.hook.wait_for_job(
            job_id=self.job_id,
            wait_seconds=self.wait_seconds,
            timeout=self.timeout,
        )

    def get_job_result(self, external_id: JsonValue, context: Context) -> int:
        self.job_id = cast("int", external_id)
        return self.job_id

    def execute_complete(self, context: Context, event: Any = None) -> None:
        """
        Invoke this callback when the trigger fires; return immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event["status"] == "error":
            self.log.debug("Error occurred with context: %s", context)
            raise RuntimeError(event["message"])

        if event["status"] == "cancelled":
            self.log.debug("Job cancelled with context: %s", context)
            raise RuntimeError(event["message"])

        job_id = event.get("job_id")
        if event["status"] == "timeout":
            hook = AirbyteHook(airbyte_conn_id=self.airbyte_conn_id, api_version=self.api_version)

            if job_id:
                self.log.info("Cancelling Airbyte job %s due to execution timeout", job_id)
                try:
                    hook.cancel_job(job_id=job_id)
                except AirflowException:
                    self.log.warning(
                        "Failed to cancel Airbyte job %s after timeout",
                        job_id,
                        exc_info=True,
                    )
            else:
                self.log.warning("No job_id found; skipping cancellation")

            raise RuntimeError(event["message"])

        self.log.info("%s completed successfully.", self.task_id)
        return None

    def on_kill(self) -> None:
        """Cancel the job if task is cancelled."""
        self.log.debug(
            "Job status for job_id %s prior to canceling is: %s",
            self.job_id,
            self.hook.get_job_status(job_id=self.job_id),
        )
        if self.job_id:
            self.log.info("on_kill: cancel the airbyte Job %s", self.job_id)
            try:
                self.hook.cancel_job(job_id=self.job_id)
            except Exception:
                self.log.warning(
                    "Failed to cancel Airbyte job %s during on_kill",
                    self.job_id,
                    exc_info=True,
                )
