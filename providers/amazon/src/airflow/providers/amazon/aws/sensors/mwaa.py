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
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.utils.state import State

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
        ``airflow.utils.state.State.success_states`` (templated)
    :param failure_states: Collection of DAG Run states that would make this task marked as failed and raise an
        AirflowException, default is ``airflow.utils.state.State.failed_states`` (templated)
    """

    aws_hook_class = MwaaHook
    template_fields: Sequence[str] = aws_template_fields(
        "external_env_name",
        "external_dag_id",
        "external_dag_run_id",
        "success_states",
        "failure_states",
    )

    def __init__(
        self,
        *,
        external_env_name: str,
        external_dag_id: str,
        external_dag_run_id: str,
        success_states: Collection[str] | None = None,
        failure_states: Collection[str] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.success_states = set(success_states if success_states else State.success_states)
        self.failure_states = set(failure_states if failure_states else State.failed_states)

        if len(self.success_states & self.failure_states):
            raise AirflowException("allowed_states and failed_states must not have any values in common")

        self.external_env_name = external_env_name
        self.external_dag_id = external_dag_id
        self.external_dag_run_id = external_dag_run_id

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
        )

        # If RestApiStatusCode == 200, the RestApiResponse must have the "state" key, otherwise something terrible has
        # happened in the API and KeyError would be raised
        # If RestApiStatusCode >= 300, a botocore exception would've already been raised during the
        # self.hook.invoke_rest_api call
        # The scope of this sensor is going to only be raising AirflowException due to failure of the DAGRun

        state = response["RestApiResponse"]["state"]
        if state in self.success_states:
            return True

        if state in self.failure_states:
            raise AirflowException(
                f"The DAG run {self.external_dag_run_id} of DAG {self.external_dag_id} in MWAA environment {self.external_env_name} "
                f"failed with state {state}."
            )
        return False
