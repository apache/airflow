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
import warnings
from collections.abc import Sequence
from datetime import timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from snowflake.connector.errors import ProgrammingError

from airflow.providers.common.compat.sdk import conf
from airflow.providers.common.compat.standard.operators import BaseOperator
from airflow.providers.common.sql.hooks.handlers import fetch_one_handler
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.triggers.snowpark_containers import SnowparkContainerJobTrigger
from airflow.providers.snowflake.utils.snowpark_containers import (
    NON_TERMINAL_STATUSES,
    NOT_FOUND_STATUS,
    OBJECT_NOT_EXIST_ERROR_CODE,
    TERMINAL_STATUSES,
    SnowparkContainerJobStatus,
)

_DURABLE_UNSET = object()


def _warn_and_disable_durable_pre_3_3(durable: Any) -> bool:
    """Disable durable below 3.3, warning if it was explicitly set."""
    if durable is not _DURABLE_UNSET:
        warnings.warn(
            "`durable` has no effect on Airflow versions below 3.3.",
            UserWarning,
            stacklevel=3,
        )
    return False


# ResumableJobMixin only exists on Airflow 3.3+ and this provider still targets >=2.11. Drop this
# fallback once the provider's minimum Airflow version is >=3.3.
try:
    from airflow.sdk import ResumableJobMixin
except ImportError:

    class ResumableJobMixin:  # type: ignore[no-redef]
        """Airflow <3.3 stub, task_state_store unavailable, always submits fresh."""

        external_id_key: str = "snowpark_container_job_name"

        def __init__(self, *, durable: Any = _DURABLE_UNSET, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            self.durable = _warn_and_disable_durable_pre_3_3(durable)

        def execute_resumable(self, context):
            external_id = self.submit_job(context)
            self.poll_until_complete(external_id, context)
            return self.get_job_result(external_id, context)


if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.providers.common.compat.sdk import Context


class SnowparkContainerJobOperator(ResumableJobMixin, BaseOperator):
    """
    Execute a job on Snowpark Container Services.

    Submits a container job to a compute pool via ``EXECUTE JOB SERVICE``,
    optionally polls for completion, retrieves container logs, and
    drops the job service on success.

    .. seealso::
        `Snowpark Container Services <https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview>`_

    :param compute_pool: name of the compute pool to run the job on
    :param container_name: container name as defined in the service specification file,
        used for retrieving container logs
    :param spec: spec filename on the stage (e.g. ``'spec.yaml'``).
        Must be provided together with ``spec_stage``
    :param spec_stage: stage where the spec file is stored (e.g. ``'@my_stage'``).
        Must be provided together with ``spec``
    :param spec_text: inline YAML spec text, as an alternative to ``spec``/``spec_stage``.
        The text is wrapped in ``$$`` delimiters automatically
    :param name: (Optional) job service name. If not provided, Snowflake
        auto-generates a name
    :param query_warehouse: (Optional) warehouse for SQL queries run inside the container.
        This is separate from the ``warehouse`` parameter used by the operator's
        own SQL commands
    :param replicas: (Optional) number of job replicas to run. (default value: 1)
    :param external_access_integrations: (Optional) Names of the external access
        integrations that allow your job to access external sites. Names are
        case-sensitive (default value: None)
    :param wait_for_completion: poll until the job reaches a terminal state.
        When disabled, the job is submitted and the operator returns
        immediately. (default value: True)
    :param drop_on_completion: drop the job service after the job finishes
        successfully or on a timeout. Failed jobs are not dropped, allowing
        inspection in Snowflake. (default value: True)
    :param poll_interval: the interval in seconds to poll the query status.
        (default value: 10)
    :param snowflake_conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param deferrable: Run the operator in deferrable mode. Only effective when
        ``wait_for_completion`` is True. With ``wait_for_completion=False`` the
        operator submits the job and returns immediately without deferring.
        (default value: False)
    :param durable: When ``True``, the submitted job name is persisted to
        task state before polling begins. A worker crash on retry reconnects to the existing
        job instead of resubmitting the SQL. Set to ``False`` to always submit fresh on
        retry. Requires Airflow 3.3+; ignored on earlier versions.
        With ``wait_for_completion=False`` or ``deferrable=True`` durable has no effect. (default value: True)
    :param timeout: Maximum seconds to wait for the job to reach a terminal
        state. When it elapses the task fails. (default value: 86400)
    :param database: name of database (will overwrite database defined
        in connection)
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON). Used for the operator's
        own SQL commands, not for the container's queries
    """

    external_id_key = "snowpark_container_job_name"
    template_fields: Sequence[str] = (
        "compute_pool",
        "spec",
        "spec_stage",
        "container_name",
        "spec_text",
        "name",
        "query_warehouse",
        "snowflake_conn_id",
        "external_access_integrations",
    )

    def __init__(
        self,
        *,
        compute_pool: str,
        container_name: str,
        spec: str | None = None,
        spec_stage: str | None = None,
        spec_text: str | None = None,
        name: str | None = None,
        query_warehouse: str | None = None,
        replicas: int = 1,
        external_access_integrations: list[str] | None = None,
        wait_for_completion: bool = True,
        drop_on_completion: bool = True,
        poll_interval: int = 10,
        snowflake_conn_id: str = "snowflake_default",
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        durable: bool | None = None,
        timeout: int = 24 * 60 * 60,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        warehouse: str | None = None,
        **kwargs: Any,
    ) -> None:
        if spec_text is not None and (spec is not None or spec_stage is not None):
            raise ValueError("Cannot specify both 'spec_text' and 'spec'/'spec_stage'")
        # durable is a named parameter here (not left to **kwargs) so default_args={"durable": ...}
        # reaches it on every supported Airflow version.
        if durable is not None:
            kwargs["durable"] = durable
        super().__init__(**kwargs)
        self.compute_pool = compute_pool
        self.container_name = container_name
        self.spec = spec
        self.spec_stage = spec_stage
        self.spec_text = spec_text
        self.name = name
        self.query_warehouse = query_warehouse
        self.replicas = replicas
        self.external_access_integrations = external_access_integrations
        self.wait_for_completion = wait_for_completion
        self.drop_on_completion = drop_on_completion
        self.poll_interval = poll_interval
        self.snowflake_conn_id = snowflake_conn_id
        self.deferrable = deferrable
        self.timeout = timeout
        self.database = database
        self.schema = schema
        self.role = role
        self.warehouse = warehouse
        # Set after the job is submitted, parsed from the job submission response.
        self.job_name: str | None = None
        # On a fresh submit the mixin runs both poll_until_complete and get_job_result.
        # poll_until_complete sets this so get_job_result does not finalize a second time.
        self._poll_until_complete_ran = False

        if self.deferrable and not self.wait_for_completion:
            self.log.warning("deferrable has no effect when wait_for_completion is False.")

    @cached_property
    def _hook(self) -> SnowflakeHook:
        return SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
            role=self.role,
        )

    def _build_sql(self) -> str:
        """Build the execute job SQL statement."""
        sql = f"EXECUTE JOB SERVICE IN COMPUTE POOL {self.compute_pool}"
        if self.name:
            sql += f" NAME = {self.name}"
        sql += " ASYNC = TRUE"
        if self.replicas > 1:
            sql += f" REPLICAS = {self.replicas}"
        if self.query_warehouse:
            sql += f" QUERY_WAREHOUSE = {self.query_warehouse}"
        if self.external_access_integrations:
            eais = ", ".join(self.external_access_integrations)
            sql += f" EXTERNAL_ACCESS_INTEGRATIONS = ({eais})"
        if self.spec_text:
            sql += f" FROM SPECIFICATION $${self.spec_text}$$"
        else:
            sql += f" FROM {self.spec_stage} SPEC = '{self.spec}'"
        return sql

    def _run_one(self, sql: str, return_dictionaries: bool = False) -> Any:
        """Run a single statement that returns one row via fetch_one_handler."""
        return self._hook.run(sql, handler=fetch_one_handler, return_dictionaries=return_dictionaries)

    def _describe_status(self, external_id: JsonValue) -> str:
        """Describe the job's current status."""
        response = self._run_one(f"DESCRIBE SERVICE {external_id}", return_dictionaries=True)
        return response.get("status")

    def submit_job(self, context: Context) -> str:
        """Submit the job and return the name."""
        response = self._run_one(self._build_sql())
        self.job_name = response[0].split("'")[1]
        if not self.job_name:
            raise RuntimeError("Job name was not returned")
        return self.job_name

    def get_job_status(self, external_id: JsonValue, context: Context) -> str:
        """Return NOT_FOUND when the service no longer exists, otherwise the current status."""
        try:
            return self._describe_status(external_id)
        except ProgrammingError as e:
            if e.errno == OBJECT_NOT_EXIST_ERROR_CODE:
                return NOT_FOUND_STATUS
            raise

    def is_job_active(self, status: str) -> bool:
        """Return True while the job is still running."""
        return status in NON_TERMINAL_STATUSES

    def is_job_succeeded(self, status: str) -> bool:
        """Return True when the job has completed successfully."""
        return status == SnowparkContainerJobStatus.DONE

    def poll_until_complete(self, external_id: JsonValue, context: Context) -> None:
        """Poll the job until it reaches a terminal state and handle the final status."""
        # On reconnect the mixin skips submit_job, so set the job name from the external id here.
        self.job_name = cast("str", external_id)

        status = None
        end_time = time.monotonic() + self.timeout
        while True:
            if time.monotonic() >= end_time:
                self._log_container_output(status)
                if self.drop_on_completion:
                    self._drop_service()
                raise TimeoutError(f"Job {self.job_name} did not reach a terminal status before the timeout.")
            status = self._describe_status(self.job_name)
            if status in TERMINAL_STATUSES:
                # get_job_result is skipped when the mixin reconnects to a still-running job, so
                # finalize here.
                self._handle_final_status(status=status)
                self._poll_until_complete_ran = True
                return
            if status not in NON_TERMINAL_STATUSES:
                raise RuntimeError(f"Job {self.job_name} returned unexpected status: {status}")
            time.sleep(self.poll_interval)

    def get_job_result(self, external_id: JsonValue, context: Context) -> None:
        """Finalize the completed job unless poll_until_complete already did."""
        self.job_name = cast("str", external_id)
        if self._poll_until_complete_ran:
            return
        # The mixin only reaches this path when the job is DONE, so the status is hardcoded.
        self._handle_final_status(status=SnowparkContainerJobStatus.DONE)

    def _log_container_output(self, status: str | None) -> None:
        """Fetch and log container output for all replicas. Best-effort so it never blocks cleanup."""
        for instance_id in range(self.replicas):
            sql = f"SELECT SYSTEM$GET_SERVICE_LOGS('{self.job_name}', {instance_id}, '{self.container_name}')"
            try:
                response = self._run_one(sql)[0]
            except Exception as e:
                self.log.warning("Could not retrieve logs for instance_id %d: %s", instance_id, e)
                continue
            if not response:
                self.log.info("No logs returned for instance_id %d", instance_id)
                continue
            if status != SnowparkContainerJobStatus.DONE:
                self.log.error("Logs for instance_id %d:\n%s", instance_id, response)
            else:
                self.log.info("Logs for instance_id %d:\n%s", instance_id, response)

    def _drop_service(self) -> None:
        """Best-effort drop of the job service."""
        try:
            self._hook.run(f"DROP SERVICE IF EXISTS {self.job_name}")
        except Exception as e:
            self.log.error("Error dropping service %s: %s", self.job_name, e)

    def on_kill(self) -> None:
        """Drop the running service on task kill."""
        if self.job_name:
            self._drop_service()

    def _handle_final_status(self, status: str) -> None:
        """Log container output, fail unless the job is DONE, and optionally drop the service on success."""
        self._log_container_output(status)
        if status != SnowparkContainerJobStatus.DONE:
            raise RuntimeError(f"Job '{self.job_name}' finished with status: {status}")
        if self.drop_on_completion:
            # Job already succeeded, so a cleanup failure is logged rather than raised
            # to avoid marking a successful job as failed.
            self._drop_service()

    def execute(self, context: Context) -> str:
        """Submit and optionally wait for a Snowpark Container Services job."""
        if not self.spec_text and not (self.spec and self.spec_stage):
            raise ValueError("Must provide either 'spec_text' or both 'spec' and 'spec_stage'")

        if not self.wait_for_completion:
            return self.submit_job(context)

        if self.deferrable:
            job_name = self.submit_job(context)
            # timeout and execution_timeout give the trigger two separate deadlines. timeout caps
            # how long the job is polled, and execution_timeout, when set, enforces the task-level
            # limit. The trigger times out on whichever is reached first.
            now = time.time()
            poll_buffer = timedelta(seconds=self.poll_interval + 60)
            execution_deadline = None
            defer_timeout = timedelta(seconds=self.timeout) + poll_buffer
            if self.execution_timeout is not None:
                # Hand the execution deadline to the trigger so it emits a timeout event that drops the
                # service. The framework's defer timeout would otherwise kill the task with no cleanup.
                execution_deadline = (
                    context["ti"].start_date.timestamp() + self.execution_timeout.total_seconds()
                )
                # Pad the backstop past that deadline so the trigger fires first.
                defer_timeout = self.execution_timeout + poll_buffer
            self.defer(
                trigger=SnowparkContainerJobTrigger(
                    job_name=job_name,
                    snowflake_conn_id=self.snowflake_conn_id,
                    poll_interval=self.poll_interval,
                    end_time=now + self.timeout,
                    execution_deadline=execution_deadline,
                    database=self.database,
                    schema=self.schema,
                    role=self.role,
                    warehouse=self.warehouse,
                ),
                timeout=defer_timeout,
                method_name="execute_complete",
            )
        self.execute_resumable(context)
        return cast("str", self.job_name)

    def execute_complete(self, context: Context, event: dict[str, Any]) -> str:
        """Resume after the trigger fires."""
        self.job_name = event["job_name"]
        status = event["status"]
        if status == "timeout":
            self._log_container_output(status)
            if self.drop_on_completion:
                self._drop_service()
            raise TimeoutError(event.get("message", f"Job '{self.job_name}' did not complete: {status}"))
        if status == "error":
            raise RuntimeError(event.get("message", f"Job '{self.job_name}' did not complete: {status}"))
        self._handle_final_status(status)
        return self.job_name
