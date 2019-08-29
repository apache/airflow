# -*- coding: utf-8 -*-
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
#

"""
Execute AWS Lambda functions.
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook


class AwsLambdaExecutionError(Exception):
    """
    Raised when there is an error executing the function.
    """


class AwsLambdaPayloadError(Exception):
    """
    Raised when there is an error with the Payload object in the response.
    """


class AwsLambdaInvokeFunctionOperator(BaseOperator):
    """
    Invoke AWS Lambda functions with a JSON payload.

    The check_success_function signature should be a single param which will receive a dict.
    The dict will be the "Response Structure" described in
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/lambda.html#Lambda.Client.invoke.
    It may be necessary to read the Payload see the actual response from the Lambda function e.g.,

    ```
    def succeeded(response):
        payload = json.loads(response['Payload'].read())
        # do something with payload
    ```

    :param function_name: The name of the Lambda function.
    :type function_name: str
    :param region_name: AWS region e.g., eu-west-1, ap-southeast-1, etc.
    :type region_name: str
    :param payload: The JSON to submit as input to a Lambda function.
    :type payload: str
    :param check_success_function: A function to check the Lambda response and determine success or failure.
    :type check_success_function: function
    :param log_type: Set to Tail to include the execution log in the response. Otherwise, set to "None".
    :type log_type: str
    :param qualifier: A version or alias name for the Lambda.
    :type qualifier: str
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
        (http://boto3.readthedocs.io/en/latest/guide/configuration.html).
    :type aws_conn_id: str
    """

    @apply_defaults
    def __init__(
        self,
        function_name,
        region_name,
        payload,
        check_success_function,
        log_type="None",
        qualifier="$LATEST",
        aws_conn_id=None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.function_name = function_name
        self.region_name = region_name
        self.payload = payload
        self.log_type = log_type
        self.qualifier = qualifier
        self.check_success_function = check_success_function
        self.aws_conn_id = aws_conn_id

    def get_hook(self):
        """
        Initialises an AWS Lambda hook

        :return: airflow.contrib.hooks.AwsLambdaHook
        """
        return AwsLambdaHook(
            self.function_name,
            self.region_name,
            self.log_type,
            self.qualifier,
            aws_conn_id=self.aws_conn_id,
        )

    def execute(self, context):
        self.log.info("AWS Lambda: invoking %s", self.function_name)

        response = self.get_hook().invoke_lambda(self.payload)

        try:
            self._validate_lambda_api_response(response)
            self._validate_lambda_response_payload(response)
        except (AwsLambdaExecutionError, AwsLambdaPayloadError) as e:
            self.log.error(response)
            raise e

        self.log.info("AWS Lambda: %s succeeded!", self.function_name)

    def _validate_lambda_api_response(self, response):
        """
        Check whether the AWS Lambda function executed without errors.

        :param response: HTTP Response from AWS Lambda.
        :type response: dict
        :return: None
        """

        if "FunctionError" in response or response["StatusCode"] >= 300:
            raise AwsLambdaExecutionError("AWS Lambda: error occurred during execution")

    def _validate_lambda_response_payload(self, response):
        """
        Call a user provided function to validate the Payload object for errors.

        :param response: HTTP Response from AWS Lambda.
        :type response: dict
        :return: None
        """
        if not self.check_success_function(response):
            raise AwsLambdaPayloadError(
                "AWS Lambda: error validating response payload!"
            )
