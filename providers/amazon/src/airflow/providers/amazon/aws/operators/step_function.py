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
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.links.step_function import (
    StateMachineDetailsLink,
    StateMachineExecutionsDetailsLink,
)
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.step_function import StepFunctionsExecutionCompleteTrigger
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Context


class StepFunctionStartExecutionOperator(AwsBaseOperator[StepFunctionHook]):
    """
    An Operator that begins execution of an AWS Step Function State Machine.

    Additional arguments may be specified and are passed down to the underlying BaseOperator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:StepFunctionStartExecutionOperator`

    :param state_machine_arn: ARN of the Step Function State Machine
    :param name: The name of the execution.
    :param is_redrive_execution: Restarts unsuccessful executions of Standard workflows that did not
            complete successfully in the last 14 days.
    :param state_machine_input: JSON data input to pass to the State Machine
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is None or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param do_xcom_push: if True, execution_arn is pushed to XCom with key execution_arn.
    :param waiter_max_attempts: Maximum number of attempts to poll the execution.
    :param waiter_delay: Number of seconds between polling the state of the execution.
    :param deferrable: If True, the operator will wait asynchronously for the job to complete.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False, but can be overridden in config file by setting default_deferrable to True)
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

    aws_hook_class = StepFunctionHook
    template_fields: Sequence[str] = aws_template_fields(
        "state_machine_arn", "name", "input", "is_redrive_execution"
    )
    ui_color = "#f9c915"
    operator_extra_links = (StateMachineDetailsLink(), StateMachineExecutionsDetailsLink())

    def __init__(
        self,
        *,
        state_machine_arn: str,
        name: str | None = None,
        is_redrive_execution: bool = False,
        state_machine_input: dict | str | None = None,
        waiter_max_attempts: int = 30,
        waiter_delay: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.state_machine_arn = state_machine_arn
        self.name = name
        self.is_redrive_execution = is_redrive_execution
        self.input = state_machine_input
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context):
        StateMachineDetailsLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            state_machine_arn=self.state_machine_arn,
        )

        if not (
            execution_arn := self.hook.start_execution(
                self.state_machine_arn, self.name, self.input, self.is_redrive_execution
            )
        ):
            raise AirflowException(f"Failed to start State Machine execution for: {self.state_machine_arn}")

        StateMachineExecutionsDetailsLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            execution_arn=execution_arn,
        )

        self.log.info("Started State Machine execution for %s: %s", self.state_machine_arn, execution_arn)
        if self.deferrable:
            self.defer(
                trigger=StepFunctionsExecutionCompleteTrigger(
                    execution_arn=execution_arn,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    botocore_config=self.botocore_config,
                    verify=self.verify,
                ),
                method_name="execute_complete",
                timeout=timedelta(seconds=self.waiter_max_attempts * self.waiter_delay),
            )
        return execution_arn

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event["status"] != "success":
            raise AirflowException(f"Trigger error: event is {validated_event}")

        self.log.info("State Machine execution completed successfully")
        return validated_event["execution_arn"]


class StepFunctionGetExecutionOutputOperator(AwsBaseOperator[StepFunctionHook]):
    """
    An Operator that returns the output of an AWS Step Function State Machine execution.

    Additional arguments may be specified and are passed down to the underlying BaseOperator.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:StepFunctionGetExecutionOutputOperator`

    :param execution_arn: ARN of the Step Function State Machine Execution
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

    aws_hook_class = StepFunctionHook
    template_fields: Sequence[str] = aws_template_fields("execution_arn")
    ui_color = "#f9c915"
    operator_extra_links = (StateMachineExecutionsDetailsLink(),)

    def __init__(self, *, execution_arn: str, **kwargs):
        super().__init__(**kwargs)
        self.execution_arn = execution_arn

    def execute(self, context: Context):
        StateMachineExecutionsDetailsLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            execution_arn=self.execution_arn,
        )

        execution_status = self.hook.describe_execution(self.execution_arn)
        response = None
        if "output" in execution_status:
            response = json.loads(execution_status["output"])
        elif "error" in execution_status:
            response = json.loads(execution_status["error"])

        self.log.info("Got State Machine Execution output for %s", self.execution_arn)

        return response
