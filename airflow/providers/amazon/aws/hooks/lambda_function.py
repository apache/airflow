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
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class AwsLambdaHook(AwsBaseHook):
    """
    Interact with AWS Lambda

    Additional arguments (such as ``aws_conn_id``) may be specified and
    are passed down to the underlying AwsBaseHook.

    .. seealso::
        :class:`~airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook`

    :param function_name: AWS Lambda Function Name
    :type function_name: str
    :param log_type: Tail Invocation Request
    :type log_type: str
    :param qualifier: AWS Lambda Function Version or Alias Name
    :type qualifier: str
    :param invocation_type: AWS Lambda Invocation Type (RequestResponse, Event etc)
    :type invocation_type: str
    """

    def __init__(
        self,
        function_name: str,
        log_type: str = 'None',
        qualifier: str = '$LATEST',
        invocation_type: str = 'RequestResponse',
        *args,
        **kwargs,
    ) -> None:
        self.function_name = function_name
        self.log_type = log_type
        self.invocation_type = invocation_type
        self.qualifier = qualifier
        kwargs["client_type"] = "lambda"
        super().__init__(*args, **kwargs)

    def invoke_lambda(self, payload: str) -> str:
        """Invoke Lambda Function"""
        response = self.conn.invoke(
            FunctionName=self.function_name,
            InvocationType=self.invocation_type,
            LogType=self.log_type,
            Payload=payload,
            Qualifier=self.qualifier,
        )

        return response
