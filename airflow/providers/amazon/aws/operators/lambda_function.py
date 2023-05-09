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

import json
from typing import TYPE_CHECKING, Sequence

from airflow.compat.functools import cached_property
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class LambdaCreateFunctionOperator(BaseOperator):
    """
    Creates an AWS Lambda function.

    More information regarding parameters of this operator can be found here
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.create_function

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LambdaCreateFunctionOperator`

    :param function_name: The name of the AWS Lambda function, version, or alias.
    :param runtime: The identifier of the function's runtime. Runtime is required if the deployment package
        is a .zip file archive.
    :param role: The Amazon Resource Name (ARN) of the function's execution role.
    :param handler: The name of the method within your code that Lambda calls to run your function.
        Handler is required if the deployment package is a .zip file archive.
    :param code: The code for the function.
    :param description: A description of the function.
    :param timeout: The amount of time (in seconds) that Lambda allows a function to run before stopping it.
    :param config: Optional dictionary for arbitrary parameters to the boto API create_lambda call.
    :param wait_for_completion: If True, the operator will wait until the function is active.
    :param aws_conn_id: The AWS connection ID to use
    """

    template_fields: Sequence[str] = (
        "function_name",
        "runtime",
        "role",
        "handler",
        "code",
        "config",
    )
    ui_color = "#ff7300"

    def __init__(
        self,
        *,
        function_name: str,
        runtime: str | None = None,
        role: str,
        handler: str | None = None,
        code: dict,
        description: str | None = None,
        timeout: int | None = None,
        config: dict = {},
        wait_for_completion: bool = False,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.function_name = function_name
        self.runtime = runtime
        self.role = role
        self.handler = handler
        self.code = code
        self.description = description
        self.timeout = timeout
        self.config = config
        self.wait_for_completion = wait_for_completion
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self) -> LambdaHook:
        return LambdaHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context):
        self.log.info("Creating AWS Lambda function: %s", self.function_name)
        response = self.hook.create_lambda(
            function_name=self.function_name,
            runtime=self.runtime,
            role=self.role,
            handler=self.handler,
            code=self.code,
            description=self.description,
            timeout=self.timeout,
            **self.config,
        )
        self.log.info("Lambda response: %r", response)

        if self.wait_for_completion:
            self.log.info("Wait for Lambda function to be active")
            waiter = self.hook.conn.get_waiter("function_active_v2")
            waiter.wait(
                FunctionName=self.function_name,
            )

        return response.get("FunctionArn")


class LambdaInvokeFunctionOperator(BaseOperator):
    """
    Invokes an AWS Lambda function. You can invoke a function synchronously (and wait for the response),
    or asynchronously.
    To invoke a function asynchronously, set `invocation_type` to `Event`. For more details,
    review the boto3 Lambda invoke docs.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:LambdaInvokeFunctionOperator`

    :param function_name: The name of the AWS Lambda function, version, or alias.
    :param log_type: Set to Tail to include the execution log in the response. Otherwise, set to "None".
    :param qualifier: Specify a version or alias to invoke a published version of the function.
    :param invocation_type: AWS Lambda invocation type (RequestResponse, Event, DryRun)
    :param client_context: Data about the invoking client to pass to the function in the context object
    :param payload: JSON provided as input to the Lambda function
    :param aws_conn_id: The AWS connection ID to use
    """

    template_fields: Sequence[str] = ("function_name", "payload", "qualifier", "invocation_type")
    ui_color = "#ff7300"

    def __init__(
        self,
        *,
        function_name: str,
        log_type: str | None = None,
        qualifier: str | None = None,
        invocation_type: str | None = None,
        client_context: str | None = None,
        payload: str | None = None,
        aws_conn_id: str = "aws_default",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.function_name = function_name
        self.payload = payload
        self.log_type = log_type
        self.qualifier = qualifier
        self.invocation_type = invocation_type
        self.client_context = client_context
        self.aws_conn_id = aws_conn_id

    @cached_property
    def hook(self) -> LambdaHook:
        return LambdaHook(aws_conn_id=self.aws_conn_id)

    def execute(self, context: Context):
        """
        Invokes the target AWS Lambda function from Airflow.

        :return: The response payload from the function, or an error object.
        """
        success_status_codes = [200, 202, 204]
        self.log.info("Invoking AWS Lambda function: %s with payload: %s", self.function_name, self.payload)
        response = self.hook.invoke_lambda(
            function_name=self.function_name,
            invocation_type=self.invocation_type,
            log_type=self.log_type,
            client_context=self.client_context,
            payload=self.payload,
            qualifier=self.qualifier,
        )
        self.log.info("Lambda response metadata: %r", response.get("ResponseMetadata"))
        if response.get("StatusCode") not in success_status_codes:
            raise ValueError("Lambda function did not execute", json.dumps(response.get("ResponseMetadata")))
        payload_stream = response.get("Payload")
        payload = payload_stream.read().decode()
        if "FunctionError" in response:
            raise ValueError(
                "Lambda function execution resulted in error",
                {"ResponseMetadata": response.get("ResponseMetadata"), "Payload": payload},
            )
        self.log.info("Lambda function invocation succeeded: %r", response.get("ResponseMetadata"))
        return payload
