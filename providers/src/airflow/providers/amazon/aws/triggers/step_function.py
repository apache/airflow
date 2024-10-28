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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.step_function import StepFunctionHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class StepFunctionsExecutionCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Trigger to poll for the completion of a Step Functions execution.

    :param execution_arn: ARN of the state machine to poll
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        execution_arn: str,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 30,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={
                "execution_arn": execution_arn,
                "region_name": region_name,
            },
            waiter_name="step_function_succeeded",
            waiter_args={"executionArn": execution_arn},
            failure_message="Step function failed",
            status_message="Status of step function execution is",
            status_queries=["status", "error", "cause"],
            return_key="execution_arn",
            return_value=execution_arn,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return StepFunctionHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
