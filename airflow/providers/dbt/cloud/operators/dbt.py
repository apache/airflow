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
from typing import TYPE_CHECKING, Any

from airflow.models import BaseOperator, BaseOperatorLink, XCom
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DbtCloudRunJobOperatorLink(BaseOperatorLink):
    """
    Operator link for DbtCloudRunJobOperator. This link allows users to monitor the triggered job run
    directly in dbt Cloud.
    """

    name = "Monitor Job Run"

    def get_link(self, operator, dttm=None, *, ti_key=None):
        if ti_key is not None:
            job_run_url = XCom.get_value(key="job_run_url", ti_key=ti_key)
        else:
            assert dttm
            job_run_url = XCom.get_one(
                dag_id=operator.dag.dag_id, task_id=operator.task_id, execution_date=dttm, key="job_run_url"
            )

        return job_run_url


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
        self.hook: DbtCloudHook
        self.run_id: int

    def execute(self, context: Context) -> int:
        if self.trigger_reason is None:
            self.trigger_reason = (
                f"Triggered via Apache Airflow by task {self.task_id!r} in the {self.dag.dag_id} DAG."
            )

        self.hook = DbtCloudHook(self.dbt_cloud_conn_id)
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

        if self.wait_for_termination:
            self.log.info("Waiting for job run %s to terminate.", str(self.run_id))

            if self.hook.wait_for_job_run_status(
                run_id=self.run_id,
                account_id=self.account_id,
                expected_statuses=DbtCloudJobRunStatus.SUCCESS.value,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                self.log.info("Job run %s has completed successfully.", str(self.run_id))
            else:
                raise DbtCloudJobRunException(f"Job run {self.run_id} has failed or has been cancelled.")

        return self.run_id

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
                self.log.info("Job run %s has been cancelled successfully.", str(self.run_id))


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

    def execute(self, context: Context) -> None:
        hook = DbtCloudHook(self.dbt_cloud_conn_id)
        response = hook.get_job_run_artifact(
            run_id=self.run_id, path=self.path, account_id=self.account_id, step=self.step
        )

        with open(self.output_file_name, "w") as file:
            if self.path.endswith(".json"):
                json.dump(response.json(), file)
            else:
                file.write(response.text)


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
