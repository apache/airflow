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

from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StepFunctionHook(AwsBaseHook):
    """
    Interact with an AWS Step Functions State Machine.

    Provide thin wrapper around :external+boto3:py:class:`boto3.client("stepfunctions") <SFN.Client>`.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        - :class:`airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "stepfunctions"
        super().__init__(*args, **kwargs)

    def start_execution(
        self,
        state_machine_arn: str,
        name: str | None = None,
        state_machine_input: dict | str | None = None,
        is_redrive_execution: bool = False,
    ) -> str:
        """
        Start Execution of the State Machine.

        .. seealso::
            - :external+boto3:py:meth:`SFN.Client.start_execution`

        :param state_machine_arn: AWS Step Function State Machine ARN.
        :param is_redrive_execution: Restarts unsuccessful executions of Standard workflows that did not
            complete successfully in the last 14 days.
        :param name: The name of the execution.
        :param state_machine_input: JSON data input to pass to the State Machine.
        :return: Execution ARN.
        """
        if is_redrive_execution:
            if not name:
                raise AirflowFailException(
                    "Execution name is required to start RedriveExecution for %s.",
                    state_machine_arn,
                )
            elements = state_machine_arn.split(":stateMachine:")
            execution_arn = f"{elements[0]}:execution:{elements[1]}:{name}"
            self.conn.redrive_execution(executionArn=execution_arn)
            self.log.info(
                "Successfully started RedriveExecution for Step Function State Machine: %s.",
                state_machine_arn,
            )
            return execution_arn

        execution_args = {"stateMachineArn": state_machine_arn}
        if name is not None:
            execution_args["name"] = name
        if state_machine_input is not None:
            if isinstance(state_machine_input, str):
                execution_args["input"] = state_machine_input
            elif isinstance(state_machine_input, dict):
                execution_args["input"] = json.dumps(state_machine_input)

        self.log.info("Executing Step Function State Machine: %s", state_machine_arn)

        response = self.conn.start_execution(**execution_args)
        return response.get("executionArn")

    def describe_execution(self, execution_arn: str) -> dict:
        """
        Describe a State Machine Execution.

        .. seealso::
            - :external+boto3:py:meth:`SFN.Client.describe_execution`

        :param execution_arn: ARN of the State Machine Execution.
        :return: Dict with execution details.
        """
        return self.get_conn().describe_execution(executionArn=execution_arn)
