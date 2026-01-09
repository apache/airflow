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

import asyncio
import json
from collections.abc import Collection, Iterable, Sequence
from datetime import datetime
from typing import Any

from dateutil import parser
from google.api_core.exceptions import NotFound
from google.cloud.orchestration.airflow.service_v1.types import ExecuteAirflowCommandResponse

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent


class CloudComposerExecutionTrigger(BaseTrigger):
    """The trigger handles the async communication with the Google Cloud Composer."""

    def __init__(
        self,
        project_id: str,
        region: str,
        operation_name: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        pooling_period_seconds: int = 30,
    ):
        super().__init__()
        self.project_id = project_id
        self.region = region
        self.operation_name = operation_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.pooling_period_seconds = pooling_period_seconds

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.cloud.triggers.cloud_composer.CloudComposerExecutionTrigger",
            {
                "project_id": self.project_id,
                "region": self.region,
                "operation_name": self.operation_name,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "pooling_period_seconds": self.pooling_period_seconds,
            },
        )

    def _get_async_hook(self) -> CloudComposerAsyncHook:
        return CloudComposerAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    async def run(self):
        self.gcp_hook = self._get_async_hook()
        while True:
            operation = await self.gcp_hook.get_operation(operation_name=self.operation_name)
            if operation.done:
                break
            elif operation.error.message:
                raise AirflowException(f"Cloud Composer Environment error: {operation.error.message}")
            await asyncio.sleep(self.pooling_period_seconds)
        yield TriggerEvent(
            {
                "operation_name": operation.name,
                "operation_done": operation.done,
            }
        )


class CloudComposerAirflowCLICommandTrigger(BaseTrigger):
    """The trigger wait for the Airflow CLI command result."""

    def __init__(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        execution_cmd_info: dict,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        poll_interval: int = 10,
    ):
        super().__init__()
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.execution_cmd_info = execution_cmd_info
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.cloud.triggers.cloud_composer.CloudComposerAirflowCLICommandTrigger",
            {
                "project_id": self.project_id,
                "region": self.region,
                "environment_id": self.environment_id,
                "execution_cmd_info": self.execution_cmd_info,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "poll_interval": self.poll_interval,
            },
        )

    def _get_async_hook(self) -> CloudComposerAsyncHook:
        return CloudComposerAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    async def run(self):
        self.gcp_hook = self._get_async_hook()
        try:
            result = await self.gcp_hook.wait_command_execution_result(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                execution_cmd_info=self.execution_cmd_info,
                poll_interval=self.poll_interval,
            )
        except AirflowException as ex:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(ex),
                }
            )
            return

        exit_code = result.get("exit_info", {}).get("exit_code")

        if exit_code == 0:
            yield TriggerEvent(
                {
                    "status": "success",
                    "result": result,
                }
            )
            return

        error_output = "".join(line["content"] for line in result.get("error", []))
        message = f"Airflow CLI command failed with exit code {exit_code}.\nError output:\n{error_output}"
        yield TriggerEvent(
            {
                "status": "error",
                "message": message,
            }
        )
        return


class CloudComposerDAGRunTrigger(BaseTrigger):
    """The trigger wait for the DAG run completion."""

    def __init__(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        composer_dag_id: str,
        start_date: datetime,
        end_date: datetime,
        allowed_states: list[str],
        composer_dag_run_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        poll_interval: int = 10,
        composer_airflow_version: int = 2,
        use_rest_api: bool = False,
    ):
        super().__init__()
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.composer_dag_id = composer_dag_id
        self.start_date = start_date
        self.end_date = end_date
        self.allowed_states = allowed_states
        self.composer_dag_run_id = composer_dag_run_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.poll_interval = poll_interval
        self.composer_airflow_version = composer_airflow_version
        self.use_rest_api = use_rest_api

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.cloud.triggers.cloud_composer.CloudComposerDAGRunTrigger",
            {
                "project_id": self.project_id,
                "region": self.region,
                "environment_id": self.environment_id,
                "composer_dag_id": self.composer_dag_id,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "allowed_states": self.allowed_states,
                "composer_dag_run_id": self.composer_dag_run_id,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "poll_interval": self.poll_interval,
                "composer_airflow_version": self.composer_airflow_version,
                "use_rest_api": self.use_rest_api,
            },
        )

    async def _pull_dag_runs(self) -> list[dict]:
        """Pull the list of dag runs."""
        if self.use_rest_api:
            try:
                environment = await self.gcp_hook.get_environment(
                    project_id=self.project_id,
                    region=self.region,
                    environment_id=self.environment_id,
                )
            except NotFound as not_found_err:
                self.log.info("The Composer environment %s does not exist.", self.environment_id)
                raise AirflowException(not_found_err)
            composer_airflow_uri = environment.config.airflow_uri

            self.log.info(
                "Pulling the DAG %s runs from the %s environment...",
                self.composer_dag_id,
                self.environment_id,
            )
            dag_runs_response = await self.gcp_hook.get_dag_runs(
                composer_airflow_uri=composer_airflow_uri,
                composer_dag_id=self.composer_dag_id,
            )
            dag_runs = dag_runs_response["dag_runs"]
        else:
            cmd_parameters = (
                ["-d", self.composer_dag_id, "-o", "json"]
                if self.composer_airflow_version < 3
                else [self.composer_dag_id, "-o", "json"]
            )
            dag_runs_cmd = await self.gcp_hook.execute_airflow_command(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                command="dags",
                subcommand="list-runs",
                parameters=cmd_parameters,
            )
            cmd_result = await self.gcp_hook.wait_command_execution_result(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
                execution_cmd_info=ExecuteAirflowCommandResponse.to_dict(dag_runs_cmd),
            )
            dag_runs = json.loads(cmd_result["output"][0]["content"])
        return dag_runs

    def _check_dag_runs_states(
        self,
        dag_runs: list[dict],
        start_date: datetime,
        end_date: datetime,
    ) -> bool:
        for dag_run in dag_runs:
            if (
                start_date.timestamp()
                < parser.parse(
                    dag_run["execution_date" if self.composer_airflow_version < 3 else "logical_date"]
                ).timestamp()
                < end_date.timestamp()
            ) and dag_run["state"] not in self.allowed_states:
                return False
        return True

    def _get_async_hook(self) -> CloudComposerAsyncHook:
        return CloudComposerAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    def _check_composer_dag_run_id_states(self, dag_runs: list[dict]) -> bool:
        for dag_run in dag_runs:
            if (
                dag_run["dag_run_id" if self.use_rest_api else "run_id"] == self.composer_dag_run_id
                and dag_run["state"] in self.allowed_states
            ):
                return True
        return False

    async def run(self):
        self.gcp_hook: CloudComposerAsyncHook = self._get_async_hook()
        try:
            while True:
                if datetime.now(self.end_date.tzinfo).timestamp() > self.end_date.timestamp():
                    dag_runs = await self._pull_dag_runs()

                    if len(dag_runs) == 0:
                        self.log.info("Dag runs are empty. Sensor waits for dag runs...")
                        self.log.info("Sleeping for %s seconds.", self.poll_interval)
                        await asyncio.sleep(self.poll_interval)
                        continue

                    if self.composer_dag_run_id:
                        self.log.info(
                            "Sensor waits for allowed states %s for specified RunID: %s",
                            self.allowed_states,
                            self.composer_dag_run_id,
                        )
                        if self._check_composer_dag_run_id_states(dag_runs=dag_runs):
                            yield TriggerEvent({"status": "success"})
                            return
                    else:
                        self.log.info("Sensor waits for allowed states: %s", self.allowed_states)
                        if self._check_dag_runs_states(
                            dag_runs=dag_runs,
                            start_date=self.start_date,
                            end_date=self.end_date,
                        ):
                            yield TriggerEvent({"status": "success"})
                            return
                self.log.info("Sleeping for %s seconds.", self.poll_interval)
                await asyncio.sleep(self.poll_interval)
        except AirflowException as ex:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(ex),
                }
            )
            return


class CloudComposerExternalTaskTrigger(BaseTrigger):
    """The trigger wait for the external task completion."""

    def __init__(
        self,
        project_id: str,
        region: str,
        environment_id: str,
        start_date: datetime,
        end_date: datetime,
        allowed_states: list[str],
        skipped_states: list[str],
        failed_states: list[str],
        composer_external_dag_id: str,
        composer_external_task_ids: Collection[str] | None = None,
        composer_external_task_group_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        poll_interval: int = 10,
        composer_airflow_version: int = 2,
    ):
        super().__init__()
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.start_date = start_date
        self.end_date = end_date
        self.allowed_states = allowed_states
        self.skipped_states = skipped_states
        self.failed_states = failed_states
        self.composer_external_dag_id = composer_external_dag_id
        self.composer_external_task_ids = composer_external_task_ids
        self.composer_external_task_group_id = composer_external_task_group_id
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.poll_interval = poll_interval
        self.composer_airflow_version = composer_airflow_version

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow.providers.google.cloud.triggers.cloud_composer.CloudComposerExternalTaskTrigger",
            {
                "project_id": self.project_id,
                "region": self.region,
                "environment_id": self.environment_id,
                "start_date": self.start_date,
                "end_date": self.end_date,
                "allowed_states": self.allowed_states,
                "skipped_states": self.skipped_states,
                "failed_states": self.failed_states,
                "composer_external_dag_id": self.composer_external_dag_id,
                "composer_external_task_ids": self.composer_external_task_ids,
                "composer_external_task_group_id": self.composer_external_task_group_id,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "poll_interval": self.poll_interval,
                "composer_airflow_version": self.composer_airflow_version,
            },
        )

    async def _get_task_instances(self, start_date: str, end_date: str) -> list[dict]:
        """Get the list of task instances."""
        try:
            environment = await self.gcp_hook.get_environment(
                project_id=self.project_id,
                region=self.region,
                environment_id=self.environment_id,
            )
        except NotFound as not_found_err:
            self.log.info("The Composer environment %s does not exist.", self.environment_id)
            raise AirflowException(not_found_err)
        composer_airflow_uri = environment.config.airflow_uri

        self.log.info(
            "Pulling the DAG '%s' task instances from the '%s' environment...",
            self.composer_external_dag_id,
            self.environment_id,
        )
        task_instances_response = await self.gcp_hook.get_task_instances(
            composer_airflow_uri=composer_airflow_uri,
            composer_dag_id=self.composer_external_dag_id,
            query_parameters={
                "execution_date_gte" if self.composer_airflow_version < 3 else "logical_date_gte": start_date,
                "execution_date_lte" if self.composer_airflow_version < 3 else "logical_date_lte": end_date,
            },
        )
        task_instances = task_instances_response["task_instances"]

        if self.composer_external_task_ids:
            task_instances = [
                task_instance
                for task_instance in task_instances
                if task_instance["task_id"] in self.composer_external_task_ids
            ]
        elif self.composer_external_task_group_id:
            task_instances = [
                task_instance
                for task_instance in task_instances
                if self.composer_external_task_group_id in task_instance["task_id"].split(".")
            ]

        return task_instances

    def _check_task_instances_states(
        self,
        task_instances: list[dict],
        start_date: datetime,
        end_date: datetime,
        states: Iterable[str],
    ) -> bool:
        for task_instance in task_instances:
            if (
                start_date.timestamp()
                < parser.parse(
                    task_instance["execution_date" if self.composer_airflow_version < 3 else "logical_date"]
                ).timestamp()
                < end_date.timestamp()
            ) and task_instance["state"] not in states:
                return False
        return True

    def _get_async_hook(self) -> CloudComposerAsyncHook:
        return CloudComposerAsyncHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

    async def run(self):
        self.gcp_hook: CloudComposerAsyncHook = self._get_async_hook()
        try:
            while True:
                task_instances = await self._get_task_instances(
                    start_date=self.start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    end_date=self.end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                )

                if len(task_instances) == 0:
                    self.log.info("Task Instances are empty. Sensor waits for task instances...")
                    self.log.info("Sleeping for %s seconds.", self.poll_interval)
                    await asyncio.sleep(self.poll_interval)
                    continue

                if self.failed_states and self._check_task_instances_states(
                    task_instances=task_instances,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    states=self.failed_states,
                ):
                    yield TriggerEvent({"status": "failed"})
                    return

                if self.skipped_states and self._check_task_instances_states(
                    task_instances=task_instances,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    states=self.skipped_states,
                ):
                    yield TriggerEvent({"status": "skipped"})
                    return

                self.log.info("Sensor waits for allowed states: %s", self.allowed_states)
                if self._check_task_instances_states(
                    task_instances=task_instances,
                    start_date=self.start_date,
                    end_date=self.end_date,
                    states=self.allowed_states,
                ):
                    yield TriggerEvent({"status": "success"})
                    return

                self.log.info("Sleeping for %s seconds.", self.poll_interval)
                await asyncio.sleep(self.poll_interval)
        except AirflowException as ex:
            yield TriggerEvent(
                {
                    "status": "error",
                    "message": str(ex),
                }
            )
            return
