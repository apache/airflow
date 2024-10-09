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

import time
from functools import cached_property
from typing import TYPE_CHECKING, Any

from deprecated.classic import deprecated

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import BaseOperator
from airflow.providers.alibaba.cloud.hooks.emr import AppState, EmrServerlessSparkHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class EmrServerlessSparkStartJobRunOperator(BaseOperator):
    """Operator for users to submit EMR Serverless Spark jobs."""

    def __init__(
        self,
        *,
        emr_serverless_spark_conn_id: str = "emr_serverless_spark_default",
        region: str | None = None,
        polling_interval: int = 0,
        workspace_id: str,
        resource_queue_id: str,
        code_type: str,
        name: str,
        engine_release_version: str | None = None,
        entry_point: str,
        entry_point_args: list[str],
        spark_submit_parameters: str,
        is_prod: bool,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        self._emr_serverless_spark_conn_id = emr_serverless_spark_conn_id
        self._region = region

        self.workspace_id: str = workspace_id
        self.job_run_id: str | None = None
        self.polling_interval = polling_interval
        self.resource_queue_id: str = resource_queue_id
        self.code_type: str = code_type
        self.name: str = name
        self.engine_release_version: str | None = engine_release_version
        self.entry_point: str = entry_point
        self.entry_point_args: list[str] = entry_point_args
        self.spark_submit_parameters: str = spark_submit_parameters
        self.is_prod: bool = is_prod

    @cached_property
    def hook(self) -> EmrServerlessSparkHook:
        """Get valid hook."""
        return EmrServerlessSparkHook(
            emr_serverless_spark_conn_id=self._emr_serverless_spark_conn_id,
            region=self._region,
            workspace_id=self.workspace_id,
        )

    @deprecated(reason="use `hook` property instead.", category=AirflowProviderDeprecationWarning)
    def get_hook(self) -> EmrServerlessSparkHook:
        """Get valid hook."""
        return self.hook

    def execute(self, context: Context) -> Any:
        submit_response = self.hook.start_job_run(
            resource_queue_id=self.resource_queue_id,
            code_type=self.code_type,
            name=self.name,
            engine_release_version=self.engine_release_version,
            entry_point=self.entry_point,
            entry_point_args=self.entry_point_args,
            spark_submit_parameters=self.spark_submit_parameters,
            is_prod=self.is_prod,
        )
        self.job_run_id = submit_response.body.job_run_id
        self.poll_job_run_state()

    def poll_job_run_state(self):
        self.log.info("Polling job run state for job - %s", self.job_run_id)

        if self.polling_interval > 0:
            self.poll_for_termination(self.job_run_id)

    def poll_for_termination(self, job_run_id: str) -> None:
        state = self.hook.get_job_run_state(job_run_id)
        while AppState(state) not in EmrServerlessSparkHook.TERMINAL_STATES:
            self.log.info("Job run with id %s is in state: %s", job_run_id, state)
            time.sleep(self.polling_interval)
            state = self.hook.get_job_run_state(job_run_id)
        self.log.info("Job run with id %s terminated with state: %s", job_run_id, state)
        self.log.info("Trying to fetch job run details...")
        self.print_job_run_details(job_run_id)
        if AppState(state) != AppState.SUCCESS:
            raise AirflowException(f"Job run {job_run_id} did not succeed")

    def print_job_run_details(self, job_run_id: str) -> None:
        job_run = self.hook.get_job_run(job_run_id)
        self.log.info("Spark UI link for job run - %s: %s", job_run_id, job_run.web_ui)
        self.log.info("Spark logs for job run - %s: %s", job_run_id, job_run.log)

    def on_kill(self) -> None:
        self.kill()

    def kill(self) -> None:
        """Delete the specified application."""
        if self.job_run_id is not None:
            self.hook.cancel_job_run(self.job_run_id)
