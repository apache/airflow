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

import json
from typing import TYPE_CHECKING, Optional

from airflow.models import BaseOperator, BaseOperatorLink, TaskInstance
from airflow.providers.dbt.cloud.hooks.dbt import DbtCloudHook, DbtCloudJobRunException, DbtCloudJobRunStatus

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DbtCloudRunJobOperatorLink(BaseOperatorLink):
    name = "Monitor Job Run"

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        job_run_url = ti.xcom_pull(task_ids=operator.task_id, key="job_run_url")

        return job_run_url


class DbtCloudRunJobOperator(BaseOperator):
    template_fields = ("dbt_cloud_conn_id", "job_id", "account_id", "trigger_reason")

    operator_extra_links = (DbtCloudRunJobOperatorLink(),)

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        job_id: int,
        account_id: Optional[int] = None,
        trigger_reason: Optional[str] = None,
        wait_for_termination: bool = False,
        timeout: int = 60 * 60 * 24 * 7,
        check_interval: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.hook = DbtCloudHook(self.dbt_cloud_conn_id)
        self.account_id = account_id
        self.job_id = job_id
        self.trigger_reason = trigger_reason or ""
        self.wait_for_termination = wait_for_termination
        self.timeout = timeout
        self.check_interval = check_interval
        self.run_id: int

    def execute(self, context: "Context") -> int:
        trigger_job_response = self.hook.trigger_job_run(
            account_id=self.account_id, job_id=self.job_id, cause=self.trigger_reason
        )
        self.run_id = trigger_job_response.json()["data"]["id"]
        job_run_url = trigger_job_response.json()["data"]["href"]

        context["ti"].xcom_push(key="job_run_url", value=job_run_url)

        if self.wait_for_termination:
            self.log.info(f"Waiting for job run {self.run_id} to terminate.")

            if self.hook.wait_for_job_run_status(
                run_id=self.run_id,
                account_id=self.account_id,
                expected_statuses=DbtCloudJobRunStatus.SUCCESS.value,
                check_interval=self.check_interval,
                timeout=self.timeout,
            ):
                self.log.info(f"Job run {self.run_id} has completed successfully.")
            else:
                raise DbtCloudJobRunException(f"Job run {self.run_id} has failed or has been cancelled.")

        return self.run_id

    def on_kill(self) -> None:
        if self.run_id:
            self.hook.cancel_job_run(account_id=self.account_id, run_id=self.run_id)
            self.log.info(f"Job run {self.run_id} has been cancelled successfully.")


class DbtCloudGetJobRunArtifactOperator(BaseOperator):
    template_fields = ("dbt_cloud_conn_id", "run_id", "path", "account_id", "output_file_name")

    def __init__(
        self,
        *,
        dbt_cloud_conn_id: str = DbtCloudHook.default_conn_name,
        run_id: int,
        path: str,
        account_id: Optional[int] = None,
        step: Optional[int] = None,
        output_file_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.dbt_cloud_conn_id = dbt_cloud_conn_id
        self.hook = DbtCloudHook(self.dbt_cloud_conn_id)
        self.run_id = run_id
        self.path = path
        self.account_id = account_id
        self.step = step
        self.output_file_name = output_file_name

    def execute(self, context: "Context") -> None:
        artifact_data = self.hook.get_job_run_artifact(
            run_id=self.run_id, path=self.path, account_id=self.account_id, step=self.step
        )

        if not self.output_file_name:
            self.output_file_name = f"{self.run_id}_{self.path}"

        with open(self.output_file_name, "w") as file:
            json.dump(artifact_data.json(), file)
