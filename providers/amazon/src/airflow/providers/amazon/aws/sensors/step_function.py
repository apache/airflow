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
from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields

if TYPE_CHECKING:
    from airflow.utils.context import Context


class StepFunctionExecutionSensor(AwsBaseSensor[StepFunctionHook]):
    """
    Poll the Step Function State Machine Execution until it reaches a terminal state; fails if the task fails.

    On successful completion of the Execution the Sensor will do an XCom Push
    of the State Machine's output to `output`

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:StepFunctionExecutionSensor`

    :param execution_arn: execution_arn to check the state of
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

    INTERMEDIATE_STATES = ("RUNNING",)
    FAILURE_STATES = (
        "FAILED",
        "TIMED_OUT",
        "ABORTED",
    )
    SUCCESS_STATES = ("SUCCEEDED",)

    aws_hook_class = StepFunctionHook
    template_fields: Sequence[str] = aws_template_fields("execution_arn")
    ui_color = "#66c3ff"

    def __init__(self, *, execution_arn: str, **kwargs):
        super().__init__(**kwargs)
        self.execution_arn = execution_arn

    def poke(self, context: Context):
        execution_status = self.hook.describe_execution(self.execution_arn)
        state = execution_status["status"]
        output = json.loads(execution_status["output"]) if "output" in execution_status else None

        if state in self.FAILURE_STATES:
            raise AirflowException(f"Step Function sensor failed. State Machine Output: {output}")

        if state in self.INTERMEDIATE_STATES:
            return False

        self.log.info("Doing xcom_push of output")

        context["ti"].xcom_push(key="output", value=output)
        return True
