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

from collections.abc import Collection
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.utils.state import DagRunState

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class MwaaDagRunCompletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when an MWAA Dag Run is complete.

    :param external_env_name: The external MWAA environment name that contains the DAG Run you want to wait for
        (templated)
    :param external_dag_id: The DAG ID in the external MWAA environment that contains the DAG Run you want to wait for
        (templated)
    :param external_dag_run_id: The DAG Run ID in the external MWAA environment that you want to wait for (templated)
    :param success_states: Collection of DAG Run states that would make this task marked as successful, default is
        ``{airflow.utils.state.DagRunState.SUCCESS}`` (templated)
    :param failure_states: Collection of DAG Run states that would make this task marked as failed and raise an
        AirflowException, default is ``{airflow.utils.state.DagRunState.FAILED}`` (templated)
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 60)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 720)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        external_env_name: str,
        external_dag_id: str,
        external_dag_run_id: str,
        success_states: Collection[str] | None = None,
        failure_states: Collection[str] | None = None,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 720,
        aws_conn_id: str | None = None,
    ) -> None:
        self.success_states = set(success_states) if success_states else {DagRunState.SUCCESS.value}
        self.failure_states = set(failure_states) if failure_states else {DagRunState.FAILED.value}

        if len(self.success_states & self.failure_states):
            raise ValueError("success_states and failure_states must not have any values in common")

        in_progress_states = {s.value for s in DagRunState} - self.success_states - self.failure_states

        super().__init__(
            serialized_fields={
                "external_env_name": external_env_name,
                "external_dag_id": external_dag_id,
                "external_dag_run_id": external_dag_run_id,
                "success_states": success_states,
                "failure_states": failure_states,
            },
            waiter_name="mwaa_dag_run_complete",
            waiter_args={
                "Name": external_env_name,
                "Path": f"/dags/{external_dag_id}/dagRuns/{external_dag_run_id}",
                "Method": "GET",
            },
            failure_message=f"The DAG run {external_dag_run_id} of DAG {external_dag_id} in MWAA environment {external_env_name} failed with state",
            status_message="State of DAG run",
            status_queries=["RestApiResponse.state"],
            return_key="dag_run_id",
            return_value=external_dag_run_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            waiter_config_overrides={
                "acceptors": _build_waiter_acceptors(
                    success_states=self.success_states,
                    failure_states=self.failure_states,
                    in_progress_states=in_progress_states,
                )
            },
        )

    def hook(self) -> AwsGenericHook:
        return MwaaHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )


def _build_waiter_acceptors(
    success_states: set[str], failure_states: set[str], in_progress_states: set[str]
) -> list:
    acceptors = []
    for state_set, state_waiter_category in (
        (success_states, "success"),
        (failure_states, "failure"),
        (in_progress_states, "retry"),
    ):
        for dag_run_state in state_set:
            acceptors.append(
                {
                    "matcher": "path",
                    "argument": "RestApiResponse.state",
                    "expected": dag_run_state,
                    "state": state_waiter_category,
                }
            )

    return acceptors
