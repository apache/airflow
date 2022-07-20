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

import json
from typing import TYPE_CHECKING, Optional, Sequence

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AwsLambdaInvokeFunctionOperator(BaseOperator):
    """
    Invokes an AWS Lambda function.
    You can invoke a function synchronously (and wait for the response),
    or asynchronously.
    To invoke a function asynchronously,
    set `invocation_type` to `Event`. For more details,
    review the boto3 Lambda invoke docs.

    :param function_name: The name of the AWS Lambda function, version, or alias.
    :param payload: The JSON string that you want to provide to your Lambda function as input.
    :param log_type: Set to Tail to include the execution log in the response. Otherwise, set to "None".
    :param qualifier: Specify a version or alias to invoke a published version of the function.
    :param aws_conn_id: The AWS connection ID to use

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AwsLambdaInvokeFunctionOperator`

    """

    template_fields: Sequence[str] = ('function_name', 'payload', 'qualifier', 'invocation_type')
    ui_color = '#ff7300'

    def __init__(
        self,
        *,
        function_name: str,
        log_type: Optional[str] = None,
        qualifier: Optional[str] = None,
        invocation_type: Optional[str] = None,
        client_context: Optional[str] = None,
        payload: Optional[str] = None,
        aws_conn_id: str = 'aws_default',
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

    def execute(self, context: 'Context'):
        """
        Invokes the target AWS Lambda function from Airflow.

        :return: The response payload from the function, or an error object.
        """
        hook = LambdaHook(aws_conn_id=self.aws_conn_id)
        success_status_codes = [200, 202, 204]
        self.log.info("Invoking AWS Lambda function: %s with payload: %s", self.function_name, self.payload)
        response = hook.invoke_lambda(
            function_name=self.function_name,
            invocation_type=self.invocation_type,
            log_type=self.log_type,
            client_context=self.client_context,
            payload=self.payload,
            qualifier=self.qualifier,
        )
        self.log.info("Lambda response metadata: %r", response.get("ResponseMetadata"))
        if response.get("StatusCode") not in success_status_codes:
            raise ValueError('Lambda function did not execute', json.dumps(response.get("ResponseMetadata")))
        payload_stream = response.get("Payload")
        payload = payload_stream.read().decode()
        if "FunctionError" in response:
            raise ValueError(
                'Lambda function execution resulted in error',
                {"ResponseMetadata": response.get("ResponseMetadata"), "Payload": payload},
            )
        self.log.info('Lambda function invocation succeeded: %r', response.get("ResponseMetadata"))
        return payload
