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

from functools import cached_property
from typing import TYPE_CHECKING, Any, AsyncIterator

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.triggers.base import BaseTrigger, TriggerEvent

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


class LambdaInvokeFunctionCompleteTrigger(BaseTrigger):
    """
    LambdaInvokeFunctionCompleteTrigger is fired as deferred class with params to run the task in triggerer.

    :param function_name: The name of the AWS Lambda function, version, or alias.
    :param log_type: Set to Tail to include the execution log in the response and task logs.
        Otherwise, set to "None". Applies to synchronously invoked functions only,
        and returns the last 4 KB of the execution log.
    :param qualifier: Specify a version or alias to invoke a published version of the function.
    :param invocation_type: AWS Lambda invocation type (RequestResponse, Event, DryRun)
    :param client_context: Data about the invoking client to pass to the function in the context object
    :param payload: JSON provided as input to the Lambda function

    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: region name to use in AWS Hook
    :param verify: Whether to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    def __init__(
        self,
        function_name: str,
        invocation_type: str | None = None,
        log_type: str | None = None,
        client_context: str | None = None,
        payload: bytes | str | None = None,
        qualifier: str | None = None,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__()
        self.function_name = function_name
        self.payload = payload
        self.log_type = log_type
        self.qualifier = qualifier
        self.invocation_type = invocation_type
        self.client_context = client_context

        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.verify = verify
        self.botocore_config = botocore_config

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "function_name": self.function_name,
                "payload": self.payload,
                "log_type": self.log_type,
                "qualifier": self.qualifier,
                "invocation_type": self.invocation_type,
                "client_context": self.client_context,
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "verify": self.verify,
                "botocore_config": self.botocore_config,
            },
        )

    @cached_property
    def hook(self) -> LambdaHook:
        return LambdaHook(aws_conn_id=self.aws_conn_id,
                          region_name=self.region_name,
                          verify=self.verify,
                          config=self.botocore_config,
                          )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        response = await self.hook.invoke_lambda_async(
            function_name=self.function_name,
            invocation_type=self.invocation_type,
            log_type=self.log_type,
            client_context=self.client_context,
            payload=self.payload,
            qualifier=self.qualifier,
        )

        payload = await response.get("Payload").read()
        yield TriggerEvent({"status": "success", "response": response, "payload": payload.decode()})
