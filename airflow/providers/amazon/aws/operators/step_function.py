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
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Sequence

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.triggers.step_function import StepFunctionsExecutionCompleteTrigger

if TYPE_CHECKING:
    from airflow.utils.context import Context


class StepFunctionStartExecutionOperator(BaseOperator):
    """
    An Operator that begins execution of an AWS Step Function State Machine.

    Additional arguments may be specified and are passed down to the underlying BaseOperator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:StepFunctionStartExecutionOperator`

    :param state_machine_arn: ARN of the Step Function State Machine
    :param name: The name of the execution.
    :param state_machine_input: JSON data input to pass to the State Machine
    :param aws_conn_id: aws connection to uses
    :param do_xcom_push: if True, execution_arn is pushed to XCom with key execution_arn.
    :param waiter_max_attempts: Maximum number of attempts to poll the execution.
    :param waiter_delay: Number of seconds between polling the state of the execution.
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
    """

    template_fields: Sequence[str] = ("state_machine_arn", "name", "input")
    template_ext: Sequence[str] = ()
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        state_machine_arn: str,
        name: str | None = None,
        state_machine_input: dict | str | None = None,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        waiter_max_attempts: int = 30,
        waiter_delay: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.state_machine_arn = state_machine_arn
        self.name = name
        self.input = state_machine_input
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context):
        hook = StepFunctionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

        execution_arn = hook.start_execution(self.state_machine_arn, self.name, self.input)

        if execution_arn is None:
            raise AirflowException(f"Failed to start State Machine execution for: {self.state_machine_arn}")

        self.log.info("Started State Machine execution for %s: %s", self.state_machine_arn, execution_arn)
        if self.deferrable:
            self.defer(
                trigger=StepFunctionsExecutionCompleteTrigger(
                    execution_arn=execution_arn,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
            )
        return execution_arn

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        if event is None or event["status"] != "success":
            raise AirflowException(f"Trigger error: event is {event}")

        self.log.info("State Machine execution completed successfully")
        return event["execution_arn"]


class StepFunctionGetExecutionOutputOperator(BaseOperator):
    """
    An Operator that returns the output of an AWS Step Function State Machine execution.

    Additional arguments may be specified and are passed down to the underlying BaseOperator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:StepFunctionGetExecutionOutputOperator`

    :param execution_arn: ARN of the Step Function State Machine Execution
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    """

    template_fields: Sequence[str] = ("execution_arn",)
    template_ext: Sequence[str] = ()
    ui_color = "#f9c915"

    def __init__(
        self,
        *,
        execution_arn: str,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.execution_arn = execution_arn
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    def execute(self, context: Context):
        hook = StepFunctionHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)

        execution_status = hook.describe_execution(self.execution_arn)
        response = None
        if "output" in execution_status:
            response = json.loads(execution_status["output"])
        elif "error" in execution_status:
            response = json.loads(execution_status["error"])

        self.log.info("Got State Machine Execution output for %s", self.execution_arn)

        return response
