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
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from dateutil import parser
from google.cloud.orchestration.airflow.service_v1.types import ExecuteAirflowCommandResponse

from airflow.exceptions import AirflowException
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
            },
        )

    async def _pull_dag_runs(self) -> list[dict]:
        """Pull the list of dag runs."""
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
            if dag_run["run_id"] == self.composer_dag_run_id and dag_run["state"] in self.allowed_states:
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
