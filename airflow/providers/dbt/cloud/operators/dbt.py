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

import json
import time
import warnings
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.dbt.cloud.hooks.dbt import (
    DbtCloudHook,
    DbtCloudJobRunException,
    DbtCloudJobRunStatus,
    JobRunInfo,
)
from airflow.providers.dbt.cloud.triggers.dbt import DbtCloudRunJobTrigger
from airflow.providers.dbt.cloud.utils.openlineage import generate_openlineage_events_from_dbt_cloud_run

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from airflow.utils.context import Context


class DbtCloudRunJobOperatorLink(BaseOperatorLink):
    """Allows users to monitor the triggered job run directly in dbt Cloud."""

    name = "Monitor Job Run"

    def get_link(self, operator: BaseOperator, *, ti_key=None):
        return XCom.get_value(key="job_run_url", ti_key=ti_key)


class DbtCloudRunJobOperator(BaseOperator):
    """
    Executes a dbt Cloud job.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DbtCloudRunJobOperator`

    :param dbt_cloud_conn_id: The connection ID for connecting to dbt Cloud.
    :param job_id: The ID of a dbt Cloud job.
    :param account_id: Optional. The ID of a dbt Cloud account.
    :param trigger_reason: Optional. Description of the reason to trigger the job.
        Defaults to "Triggered via Apache Airflow by task <task_id> in the <dag_id> DAG."
    :param steps_override: Optional. List of dbt commands to execute when triggering the job instead of those
        configured in dbt Cloud.
    :param schema_override: Optional. Override the destination schema in the configured target for this job.
    :param wait_for_termination: Flag to wait on a job run's termination.  By default, this feature is
        enabled but could be disabled to perform an asynchronous wait for a long-running job run execution
        using the ``DbtCloudJobRunSensor``.
    :param timeout: Time in seconds to wait for a job run to reach a terminal status for non-asynchronous
        waits. Used only if ``wait_for_termination`` is True. Defaults to 7 days.
    :param check_interval: Time in seconds to check on a job run's status for non-asynchronous waits.
        Used only if ``wait_for_termination`` is True. Defaults to 60 seconds.
    :param additional_run_config: Optional. Any additional parameters that should be included in the API
        request when triggering the job.
    :param deferrable: Run operator in the deferrable mode
    :return: The ID of the triggered dbt Cloud job run.
    """

    template_fields = (
        "dbt_cloud_conn_id",
        "job_id",
        "account_id",
        "trigger_reason",
        "steps_override",
        "schema_override",
        "additional_run_config",
    )

    operator_extra_links = (DbtCloudRunJobOperatorLink(),)

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        job_id: int,
        account_id: int | None = None,
        trigger_reason: str | None = None,
        steps_override: list[str] | None = None,
        schema_override: str | None = None,
        wait_for_termination: bool = True,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        additional_run_config: dict[str, Any] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.account_id = account_id
        self.job_id = job_id
        self.trigger_reason = trigger_reason
        self.steps_override = steps_override
        self.schema_override = schema_override
        self.wait_for_termination = wait_for_termination
        self.timeout = timeout
        self.check_interval = check_interval
        self.additional_run_config = additional_run_config or {}
        self.run_id: int | None = None
        self.deferrable = deferrable

    def execute(self, context: Context):
        if self.trigger_reason is None:
            self.trigger_reason = (
                f"Triggered via Apache Airflow by task {self.task_id!r} in the {self.dag.dag_id} DAG."
            )

        trigger_job_response = self.hook.trigger_job_run(
            account_id=self.account_id,
            job_id=self.job_id,
            cause=self.trigger_reason,
            steps_override=self.steps_override,
            schema_override=self.schema_override,
            additional_run_config=self.additional_run_config,
        )
        self.run_id = trigger_job_response.json()["data"]["id"]
        job_run_url = trigger_job_response.json()["data"]["href"]
        # Push the ``job_run_url`` value to XCom regardless of what happens during execution so that the job
        # run can be monitored via the operator link.
        context["ti"].xcom_push(key="job_run_url", value=job_run_url)

        if self.wait_for_termination and isinstance(self.run_id, int):
            if self.deferrable is False:
                self.log.info("Waiting for job run %s to terminate.", self.run_id)

                if self.hook.wait_for_job_run_status(
                    run_id=self.run_id,
                    account_id=self.account_id,
                    expected_statuses=DbtCloudJobRunStatus.SUCCESS.value,
                    check_interval=self.check_interval,
                    timeout=self.timeout,
                ):
                    self.log.info("Job run %s has completed successfully.", self.run_id)
                else:
                    raise DbtCloudJobRunException(f"Job run {self.run_id} has failed or has been cancelled.")

                return self.run_id
            else:
                end_time = time.time() + self.timeout
                job_run_info = JobRunInfo(account_id=self.account_id, run_id=self.run_id)
                job_run_status = self.hook.get_job_run_status(**job_run_info)
                if not DbtCloudJobRunStatus.is_terminal(job_run_status):
                    self.defer(
                        timeout=self.execution_timeout,
                        trigger=DbtCloudRunJobTrigger(
                            conn_id=self.dbt_cloud_conn_id,
                            run_id=self.run_id,
                            end_time=end_time,
                            account_id=self.account_id,
                            poll_interval=self.check_interval,
                        ),
                        method_name="execute_complete",
                    )
                elif job_run_status == DbtCloudJobRunStatus.SUCCESS.value:
                    self.log.info("Job run %s has completed successfully.", self.run_id)
                    return self.run_id
                elif job_run_status in (
                    DbtCloudJobRunStatus.CANCELLED.value,
                    DbtCloudJobRunStatus.ERROR.value,
                ):
                    raise DbtCloudJobRunException(f"Job run {self.run_id} has failed or has been cancelled.")
        else:
            if self.deferrable is True:
                warnings.warn(
                    "Argument `wait_for_termination` is False and `deferrable` is True , hence "
                    "`deferrable` parameter doesn't have any effect",
                )
            return self.run_id

    def execute_complete(self, context: Context, event: dict[str, Any]) -> int:
        """
        Callback for when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info(event["message"])
        self.run_id = event["run_id"]
        return int(event["run_id"])

    def on_kill(self) -> None:
        if self.run_id:
            self.hook.cancel_job_run(account_id=self.account_id, run_id=self.run_id)

            if self.hook.wait_for_job_run_status(
                run_id=self.run_id,
                account_id=self.account_id,
                expected_statuses=DbtCloudJobRunStatus.CANCELLED.value,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                self.log.info("Job run %s has been cancelled successfully.", self.run_id)

    @cached_property
    def hook(self):
        """Returns DBT Cloud hook."""
        return DbtCloudHook(self.dbt_cloud_conn_id)

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        """
        Implementing _on_complete because job_run needs to be triggered first in execute method.

        This should send additional events only if operator `wait_for_termination` is set to True.
        """
        from airflow.providers.openlineage.extractors import OperatorLineage

        if isinstance(self.run_id, int) and self.wait_for_termination is True:
            return generate_openlineage_events_from_dbt_cloud_run(operator=self, task_instance=task_instance)
        return OperatorLineage()


class DbtCloudGetJobRunArtifactOperator(BaseOperator):
    """
    Download artifacts from a dbt Cloud job run.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DbtCloudGetJobRunArtifactOperator`

    :param dbt_cloud_conn_id: The connection ID for connecting to dbt Cloud.
    :param run_id: The ID of a dbt Cloud job run.
    :param path: The file path related to the artifact file. Paths are rooted at the target/ directory.
        Use "manifest.json", "catalog.json", or "run_results.json" to download dbt-generated artifacts
        for the run.
    :param account_id: Optional. The ID of a dbt Cloud account.
    :param step: Optional. The index of the Step in the Run to query for artifacts. The first step in the
        run has the index 1. If the step parameter is omitted, artifacts for the last step in the run will
        be returned.
    :param output_file_name: Optional. The desired file name for the download artifact file.
        Defaults to <run_id>_<path> (e.g. "728368_run_results.json").
    """

    template_fields = ("dbt_cloud_conn_id", "run_id", "path", "account_id", "output_file_name")

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        run_id: int,
        path: str,
        account_id: int | None = None,
        step: int | None = None,
        output_file_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.run_id = run_id
        self.path = path
        self.account_id = account_id
        self.step = step
        self.output_file_name = output_file_name or f"{self.run_id}_{self.path}".replace("/", "-")

    def execute(self, context: Context) -> str:
        hook = DbtCloudHook(self.dbt_cloud_conn_id)
        response = hook.get_job_run_artifact(
            run_id=self.run_id, path=self.path, account_id=self.account_id, step=self.step
        )

        output_file_path = Path(self.output_file_name)
        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        with output_file_path.open(mode="w") as file:
            self.log.info(
                "Writing %s artifact for job run %s to %s.", self.path, self.run_id, self.output_file_name
            )
            if self.path.endswith(".json"):
                json.dump(response.json(), file)
            else:
                file.write(response.text)

        return self.output_file_name


class DbtCloudListJobsOperator(BaseOperator):
    """
    List jobs in a dbt Cloud project.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:DbtCloudListJobsOperator`

    Retrieves metadata for all jobs tied to a specified dbt Cloud account. If a ``project_id`` is
    supplied, only jobs pertaining to this project id will be retrieved.

    :param account_id: Optional. If an account ID is not provided explicitly,
        the account ID from the dbt Cloud connection will be used.
    :param order_by: Optional. Field to order the result by. Use '-' to indicate reverse order.
        For example, to use reverse order by the run ID use ``order_by=-id``.
    :param project_id: Optional. The ID of a dbt Cloud project.
    """

    template_fields = (
        "account_id",
        "project_id",
    )

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        account_id: int | None = None,
        project_id: int | None = None,
        order_by: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.account_id = account_id
        self.project_id = project_id
        self.order_by = order_by

    def execute(self, context: Context) -> list:
        hook = DbtCloudHook(self.dbt_cloud_conn_id)
        list_jobs_response = hook.list_jobs(
            account_id=self.account_id, order_by=self.order_by, project_id=self.project_id
        )
        buffer = []
        for job_metadata in list_jobs_response:
            for job in job_metadata.json()["data"]:
                buffer.append(job["id"])
        self.log.info("Jobs in the specified dbt Cloud account are: %s", ", ".join(map(str, buffer)))
        return buffer
