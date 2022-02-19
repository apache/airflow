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

"""This module contains AWS Lambda hook"""
import warnings

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class LambdaHook(AwsBaseHook):
    """
    Interact with AWS Lambda

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param function_name: AWS Lambda Function Name
    :param log_type: Tail Invocation Request
    :param qualifier: AWS Lambda Function Version or Alias Name
    :param invocation_type: AWS Lambda Invocation Type (RequestResponse, Event etc)
    """

    def __init__(
        self,
        *args,
        **kwargs,
    ) -> None:
        kwargs["client_type"] = "lambda"
        super().__init__(*args, **kwargs)

    def invoke_lambda(self, function_name: str, **kwargs):
        """Invoke Lambda Function"""
        response = self.conn.invoke(
            FunctionName=function_name, **{k: v for k, v in kwargs.items() if v is not None}
        )
        return response

    def create_lambda(
        self, function_name: str, runtime: str, role: str, handler: str, code: dict, **kwargs
    ) -> dict:
        """Create a Lambda Function"""
        response = self.conn.create_function(
            FunctionName=function_name,
            Runtime=runtime,
            Role=role,
            Handler=handler,
            Code=code,
            **{k: v for k, v in kwargs.items() if v is not None},
        )
        return response


class AwsLambdaHook(LambdaHook):
    """
    This hook is deprecated.
    Please use :class:`airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            "This hook is deprecated. "
            "Please use :class:`airflow.providers.amazon.aws.hooks.lambda_function.LambdaHook`.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
