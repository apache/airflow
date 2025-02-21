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

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class LambdaCreateFunctionCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Trigger to poll for the completion of a Lambda function creation.

    :param function_name: The function name
    :param function_arn: The function ARN
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        function_name: str,
        function_arn: str,
        waiter_delay: int = 60,
        waiter_max_attempts: int = 30,
        aws_conn_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"function_name": function_name, "function_arn": function_arn},
            waiter_name="function_active_v2",
            waiter_args={"FunctionName": function_name},
            failure_message="Lambda function creation failed",
            status_message="Status of Lambda function creation is",
            status_queries=[
                "Configuration.LastUpdateStatus",
                "Configuration.LastUpdateStatusReason",
                "Configuration.LastUpdateStatusReasonCode",
            ],
            return_key="function_arn",
            return_value=function_arn,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return LambdaHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
