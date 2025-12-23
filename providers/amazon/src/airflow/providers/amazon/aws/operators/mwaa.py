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
"""This module contains AWS MWAA operators."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.mwaa import MwaaHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.mwaa import MwaaDagRunCompletedTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MwaaTriggerDagRunOperator(AwsBaseOperator[MwaaHook]):
    """
    Trigger a Dag Run for a Dag in an Amazon MWAA environment.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:MwaaTriggerDagRunOperator`

    :param env_name: The MWAA environment name (templated)
    :param trigger_dag_id: The ID of the DAG to be triggered (templated)
    :param trigger_run_id: The Run ID. This together with trigger_dag_id are a unique key. (templated)
    :param logical_date: The logical date (previously called execution date). This is the time or interval
        covered by this DAG run, according to the DAG definition. This together with trigger_dag_id are a
        unique key. This field is required if your environment is running with Airflow 3. (templated)
    :param data_interval_start: The beginning of the interval the DAG run covers
    :param data_interval_end: The end of the interval the DAG run covers
    :param conf: Additional configuration parameters. The value of this field can be set only when creating
        the object. (templated)
    :param note: Contains manually entered notes by the user about the DagRun. (templated)
    :param airflow_version: The Airflow major version the MWAA environment runs.
            This parameter is only used if the local web token method is used to call Airflow API. (templated)

    :param wait_for_completion: Whether to wait for DAG run to stop. (default: False)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 120)
    :param waiter_max_attempts: Maximum number of attempts to check for DAG run completion. (default: 720)
    :param deferrable: If True, the operator will wait asynchronously for the DAG run to stop.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
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
        "env_name",
        "trigger_dag_id",
        "trigger_run_id",
        "logical_date",
        "data_interval_start",
        "data_interval_end",
        "conf",
        "note",
        "airflow_version",
    )
    template_fields_renderers = {"conf": "json"}

    def __init__(
        self,
        *,
        env_name: str,
        trigger_dag_id: str,
        trigger_run_id: str | None = None,
        logical_date: str | None = None,
        data_interval_start: str | None = None,
        data_interval_end: str | None = None,
        conf: dict | None = None,
        note: str | None = None,
        airflow_version: Literal[2, 3] | None = None,
        wait_for_completion: bool = False,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 20,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.env_name = env_name
        self.trigger_dag_id = trigger_dag_id
        self.trigger_run_id = trigger_run_id
        self.logical_date = logical_date
        self.data_interval_start = data_interval_start
        self.data_interval_end = data_interval_end
        self.conf = conf if conf else {}
        self.note = note
        self.airflow_version = airflow_version
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict:
        validated_event = validate_execute_complete_event(event)
        if validated_event["status"] != "success":
            raise AirflowException(f"DAG run failed: {validated_event}")

        dag_run_id = validated_event["dag_run_id"]
        self.log.info("DAG run %s of DAG %s completed", dag_run_id, self.trigger_dag_id)
        return self.hook.invoke_rest_api(
            env_name=self.env_name,
            path=f"/dags/{self.trigger_dag_id}/dagRuns/{dag_run_id}",
            method="GET",
            airflow_version=self.airflow_version,
        )

    def execute(self, context: Context) -> dict:
        """
        Trigger a Dag Run for the Dag in the Amazon MWAA environment.

        :param context: the Context object
        :return: dict with information about the Dag run
            For details of the returned dict, see :py:meth:`botocore.client.MWAA.invoke_rest_api`
        """
        response = self.hook.invoke_rest_api(
            env_name=self.env_name,
            path=f"/dags/{self.trigger_dag_id}/dagRuns",
            method="POST",
            body={
                "dag_run_id": self.trigger_run_id,
                "logical_date": self.logical_date,
                "data_interval_start": self.data_interval_start,
                "data_interval_end": self.data_interval_end,
                "conf": self.conf,
                "note": self.note,
            },
            airflow_version=self.airflow_version,
        )

        dag_run_id = response["RestApiResponse"]["dag_run_id"]
        self.log.info("DAG run %s of DAG %s created", dag_run_id, self.trigger_dag_id)

        task_description = f"DAG run {dag_run_id} of DAG {self.trigger_dag_id} to complete"
        if self.deferrable:
            self.log.info("Deferring for %s", task_description)
            self.defer(
                trigger=MwaaDagRunCompletedTrigger(
                    external_env_name=self.env_name,
                    external_dag_id=self.trigger_dag_id,
                    external_dag_run_id=dag_run_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )
        elif self.wait_for_completion:
            self.log.info("Waiting for %s", task_description)
            api_kwargs = {
                "Name": self.env_name,
                "Path": f"/dags/{self.trigger_dag_id}/dagRuns/{dag_run_id}",
                "Method": "GET",
            }
            self.hook.get_waiter("mwaa_dag_run_complete").wait(
                **api_kwargs,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return response
