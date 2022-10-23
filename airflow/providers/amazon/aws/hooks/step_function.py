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

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class StepFunctionHook(AwsBaseHook):
    """
    Interact with an AWS Step Functions State Machine.

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`
    """

    def __init__(self, *args, **kwargs) -> None:
        kwargs["client_type"] = "stepfunctions"
        super().__init__(*args, **kwargs)

    def start_execution(
        self,
        state_machine_arn: str,
        name: str | None = None,
        state_machine_input: dict | str | None = None,
    ) -> str:
        """
        Start Execution of the State Machine.
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.start_execution

        :param state_machine_arn: AWS Step Function State Machine ARN
        :param name: The name of the execution.
        :param state_machine_input: JSON data input to pass to the State Machine
        :return: Execution ARN
        :rtype: str
        """
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
        Describes a State Machine Execution
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/stepfunctions.html#SFN.Client.describe_execution

        :param execution_arn: ARN of the State Machine Execution
        :return: Dict with Execution details
        :rtype: dict
        """
        return self.get_conn().describe_execution(executionArn=execution_arn)
