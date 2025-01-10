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
"""This module contains a Cloud Composer sensor."""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from dateutil import parser
from google.cloud.orchestration.airflow.service_v1.types import ExecuteAirflowCommandResponse

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.cloud_composer import CloudComposerHook
from airflow.providers.google.cloud.triggers.cloud_composer import CloudComposerDAGRunTrigger
from airflow.providers.google.common.consts import GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CloudComposerDAGRunSensor(BaseSensorOperator):
    """
    Check if a DAG run has completed.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param region: Required. The ID of the Google Cloud region that the service belongs to.
    :param environment_id: The name of the Composer environment.
    :param composer_dag_id: The ID of executable DAG.
    :param allowed_states: Iterable of allowed states, default is ``['success']``.
    :param execution_range: execution DAGs time range. Sensor checks DAGs states only for DAGs which were
        started in this time range. For yesterday, use [positive!] datetime.timedelta(days=1).
        For future, use [negative!] datetime.timedelta(days=-1). For specific time, use list of
        datetimes [datetime(2024,3,22,11,0,0), datetime(2024,3,22,12,0,0)].
        Or [datetime(2024,3,22,0,0,0)] in this case sensor will check for states from specific time in the
        past till current time execution.
        Default value datetime.timedelta(days=1).
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :param poll_interval: Optional: Control the rate of the poll for the result of deferrable run.
    :param deferrable: Run sensor in deferrable mode.
    """

    template_fields = (
        "project_id",
        "region",
        "environment_id",
        "composer_dag_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        project_id: str,
        region: str,
        environment_id: str,
        composer_dag_id: str,
        allowed_states: Iterable[str] | None = None,
        execution_range: timedelta | list[datetime] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poll_interval: int = 10,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.region = region
        self.environment_id = environment_id
        self.composer_dag_id = composer_dag_id
        self.allowed_states = list(allowed_states) if allowed_states else [TaskInstanceState.SUCCESS.value]
        self.execution_range = execution_range
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def _get_logical_dates(self, context) -> tuple[datetime, datetime]:
        if isinstance(self.execution_range, timedelta):
            if self.execution_range < timedelta(0):
                return context["logical_date"], context["logical_date"] - self.execution_range
            else:
                return context["logical_date"] - self.execution_range, context["logical_date"]
        elif isinstance(self.execution_range, list) and len(self.execution_range) > 0:
            return self.execution_range[0], self.execution_range[1] if len(
                self.execution_range
            ) > 1 else context["logical_date"]
        else:
            return context["logical_date"] - timedelta(1), context["logical_date"]

    def poke(self, context: Context) -> bool:
        start_date, end_date = self._get_logical_dates(context)

        if datetime.now(end_date.tzinfo) < end_date:
            return False

        dag_runs = self._pull_dag_runs()

        self.log.info("Sensor waits for allowed states: %s", self.allowed_states)
        allowed_states_status = self._check_dag_runs_states(
            dag_runs=dag_runs,
            start_date=start_date,
            end_date=end_date,
        )

        return allowed_states_status

    def _pull_dag_runs(self) -> list[dict]:
        """Pull the list of dag runs."""
        hook = CloudComposerHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        dag_runs_cmd = hook.execute_airflow_command(
            project_id=self.project_id,
            region=self.region,
            environment_id=self.environment_id,
            command="dags",
            subcommand="list-runs",
            parameters=["-d", self.composer_dag_id, "-o", "json"],
        )
        cmd_result = hook.wait_command_execution_result(
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
                < parser.parse(dag_run["logical_date"]).timestamp()
                < end_date.timestamp()
            ) and dag_run["state"] not in self.allowed_states:
                return False
        return True

    def execute(self, context: Context) -> None:
        if self.deferrable:
            start_date, end_date = self._get_logical_dates(context)
            self.defer(
                trigger=CloudComposerDAGRunTrigger(
                    project_id=self.project_id,
                    region=self.region,
                    environment_id=self.environment_id,
                    composer_dag_id=self.composer_dag_id,
                    start_date=start_date,
                    end_date=end_date,
                    allowed_states=self.allowed_states,
                    gcp_conn_id=self.gcp_conn_id,
                    impersonation_chain=self.impersonation_chain,
                    poll_interval=self.poll_interval,
                ),
                method_name=GOOGLE_DEFAULT_DEFERRABLE_METHOD_NAME,
            )
        super().execute(context)

    def execute_complete(self, context: Context, event: dict):
        if event and event["status"] == "error":
            raise AirflowException(event["message"])
        self.log.info("DAG %s has executed successfully.", self.composer_dag_id)
