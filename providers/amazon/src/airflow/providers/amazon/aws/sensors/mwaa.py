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

from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any, Literal

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.mwaa import MwaaDagRunCompletedTrigger, MwaaTaskCompletedTrigger
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MwaaDagRunSensor(AwsBaseSensor[MwaaHook]):
    """
    Waits for a DAG Run in an MWAA Environment to complete.

    If the DAG Run fails, an AirflowException is thrown.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:MwaaDagRunSensor`

    :param external_env_name: The external MWAA environment name that contains the DAG Run you want to wait for
        (templated)
    :param external_dag_id: The DAG ID in the external MWAA environment that contains the DAG Run you want to wait for
        (templated)
    :param external_dag_run_id: The DAG Run ID in the external MWAA environment that you want to wait for (templated)
    :param success_states: Collection of DAG Run states that would make this task marked as successful, default is
        ``{airflow.utils.state.DagRunState.SUCCESS}`` (templated)
    :param failure_states: Collection of DAG Run states that would make this task marked as failed and raise an
        AirflowException, default is ``{airflow.utils.state.DagRunState.FAILED}`` (templated)
    :param airflow_version: The Airflow major version the MWAA environment runs.
            This parameter is only used if the local web token method is used to call Airflow API. (templated)
    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 60)
    :param max_retries: Number of times before returning the current state. (default: 720)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = MwaaHook
    template_fields: Sequence[str] = aws_template_fields(
        "external_env_name",
        "external_dag_id",
        "external_dag_run_id",
        "success_states",
        "failure_states",
        "airflow_version",
        "deferrable",
        "max_retries",
        "poke_interval",
    )

    def __init__(
        self,
        *,
        external_env_name: str,
        external_dag_id: str,
        external_dag_run_id: str,
        success_states: Collection[str] | None = None,
        failure_states: Collection[str] | None = None,
        airflow_version: Literal[2, 3] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poke_interval: int = 60,
        max_retries: int = 720,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.success_states = set(success_states) if success_states else {DagRunState.SUCCESS.value}
        self.failure_states = set(failure_states) if failure_states else {DagRunState.FAILED.value}

        if len(self.success_states & self.failure_states):
            raise ValueError("success_states and failure_states must not have any values in common")

        self.external_env_name = external_env_name
        self.external_dag_id = external_dag_id
        self.external_dag_run_id = external_dag_run_id
        self.airflow_version = airflow_version
        self.deferrable = deferrable
        self.poke_interval = poke_interval
        self.max_retries = max_retries

    def poke(self, context: Context) -> bool:
        self.log.info(
            "Poking for DAG run %s of DAG %s in MWAA environment %s",
            self.external_dag_run_id,
            self.external_dag_id,
            self.external_env_name,
        )
        response = self.hook.invoke_rest_api(
            env_name=self.external_env_name,
            path=f"/dags/{self.external_dag_id}/dagRuns/{self.external_dag_run_id}",
            method="GET",
            airflow_version=self.airflow_version,
        )

        # If RestApiStatusCode == 200, the RestApiResponse must have the "state" key, otherwise something terrible has
        # happened in the API and KeyError would be raised
        # If RestApiStatusCode >= 300, a botocore exception would've already been raised during the
        # self.hook.invoke_rest_api call
        # The scope of this sensor is going to only be raising AirflowException due to failure of the DAGRun

        state = response["RestApiResponse"]["state"]

        if state in self.failure_states:
            raise AirflowException(
                f"The DAG run {self.external_dag_run_id} of DAG {self.external_dag_id} in MWAA environment {self.external_env_name} "
                f"failed with state: {state}"
            )

        return state in self.success_states

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        return None

    def execute(self, context: Context):
        if self.deferrable:
            self.defer(
                trigger=MwaaDagRunCompletedTrigger(
                    external_env_name=self.external_env_name,
                    external_dag_id=self.external_dag_id,
                    external_dag_run_id=self.external_dag_run_id,
                    success_states=self.success_states,
                    failure_states=self.failure_states,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context=context)


class MwaaTaskSensor(AwsBaseSensor[MwaaHook]):
    """
    Waits for a task in an MWAA Environment to complete.

    If the task fails, an AirflowException is thrown.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:MwaaTaskSensor`

    :param external_env_name: The external MWAA environment name that contains the Task Instance you want to wait for
        (templated)
    :param external_dag_id: The DAG ID in the external MWAA environment that contains the Task Instance you want to wait for
        (templated)
    :param external_dag_run_id: The DAG Run ID in the external MWAA environment that you want to wait for (templated)
    :param external_task_id: The Task ID in the external MWAA environment that you want to wait for (templated)
    :param success_states: Collection of task instance states that would make this task marked as successful, default is
        ``{airflow.utils.state.TaskInstanceState.SUCCESS}`` (templated)
    :param failure_states: Collection of task instance states that would make this task marked as failed and raise an
        AirflowException, default is ``{airflow.utils.state.TaskInstanceState.FAILED}`` (templated)
    :param airflow_version: The Airflow major version the MWAA environment runs.
            This parameter is only used if the local web token method is used to call Airflow API. (templated)
    :param deferrable: If True, the sensor will operate in deferrable mode. This mode requires aiobotocore
        module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    :param poke_interval: Polling period in seconds to check for the status of the job. (default: 60)
    :param max_retries: Number of times before returning the current state. (default: 720)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    aws_hook_class = MwaaHook
    template_fields: Sequence[str] = aws_template_fields(
        "external_env_name",
        "external_dag_id",
        "external_dag_run_id",
        "external_task_id",
        "success_states",
        "failure_states",
        "airflow_version",
        "deferrable",
        "max_retries",
        "poke_interval",
    )

    def __init__(
        self,
        *,
        external_env_name: str,
        external_dag_id: str,
        external_dag_run_id: str | None = None,
        external_task_id: str,
        success_states: Collection[str] | None = None,
        failure_states: Collection[str] | None = None,
        airflow_version: Literal[2, 3] | None = None,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poke_interval: int = 60,
        max_retries: int = 720,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.success_states = set(success_states) if success_states else {TaskInstanceState.SUCCESS.value}
        self.failure_states = set(failure_states) if failure_states else {TaskInstanceState.FAILED.value}

        if len(self.success_states & self.failure_states):
            raise ValueError("success_states and failure_states must not have any values in common")

        self.external_env_name = external_env_name
        self.external_dag_id = external_dag_id
        self.external_dag_run_id = external_dag_run_id
        self.external_task_id = external_task_id
        self.airflow_version = airflow_version
        self.deferrable = deferrable
        self.poke_interval = poke_interval
        self.max_retries = max_retries

    def poke(self, context: Context) -> bool:
        self.log.info(
            "Poking for task %s of DAG run %s of DAG %s in MWAA environment %s",
            self.external_task_id,
            self.external_dag_run_id,
            self.external_dag_id,
            self.external_env_name,
        )

        response = self.hook.invoke_rest_api(
            env_name=self.external_env_name,
            path=f"/dags/{self.external_dag_id}/dagRuns/{self.external_dag_run_id}/taskInstances/{self.external_task_id}",
            method="GET",
            airflow_version=self.airflow_version,
        )
        # If RestApiStatusCode == 200, the RestApiResponse must have the "state" key, otherwise something terrible has
        # happened in the API and KeyError would be raised
        # If RestApiStatusCode >= 300, a botocore exception would've already been raised during the
        # self.hook.invoke_rest_api call
        # The scope of this sensor is going to only be raising AirflowException due to failure of the task

        state = response["RestApiResponse"]["state"]

        if state in self.failure_states:
            raise AirflowException(
                f"The task {self.external_task_id} of DAG run {self.external_dag_run_id} of DAG {self.external_dag_id} in MWAA environment {self.external_env_name} "
                f"failed with state: {state}"
            )

        return state in self.success_states

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        return None

    def execute(self, context: Context):
        if self.external_dag_run_id is None:
            response = self.hook.invoke_rest_api(
                env_name=self.external_env_name,
                path=f"/dags/{self.external_dag_id}/dagRuns",
                method="GET",
                airflow_version=self.airflow_version,
            )
            self.external_dag_run_id = response["RestApiResponse"]["dag_runs"][-1]["dag_run_id"]

        if self.deferrable:
            self.defer(
                trigger=MwaaTaskCompletedTrigger(
                    external_env_name=self.external_env_name,
                    external_dag_id=self.external_dag_id,
                    external_dag_run_id=self.external_dag_run_id,
                    external_task_id=self.external_task_id,
                    success_states=self.success_states,
                    failure_states=self.failure_states,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        else:
            super().execute(context=context)
