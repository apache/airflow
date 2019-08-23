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

import json
import unittest
from unittest import mock

from airflow.contrib.operators.aws_lambda_operator import (
    AwsLambdaInvokeFunctionOperator,
    AwsLambdaExecutionError,
    AwsLambdaPayloadError,
)


class TestAwsLambdaInvokeFunctionOperator(unittest.TestCase):
    @mock.patch(
        "airflow.contrib.operators.aws_lambda_operator.AwsLambdaHook.invoke_lambda",
        return_value={"StatusCode": 200},
    )
    def test__execute__given_check_success_function_fails__raises_an_exception(
        self, mock_aws_lambda_hook
    ):
        operator = AwsLambdaInvokeFunctionOperator(
            task_id="foo",
            function_name="foo",
            region_name="eu-west-1",
            payload=json.dumps({"foo": "bar"}),
            check_success_function=lambda r: False,
        )

        with self.assertRaises(AwsLambdaPayloadError):
            operator.execute(None)

    @mock.patch(
        "airflow.contrib.operators.aws_lambda_operator.AwsLambdaHook.invoke_lambda"
    )
    def test__execute__given_an_invalid_lambda_api_response__raises_an_exception(
        self, mock_aws_lambda_hook
    ):
        responses = [{"StatusCode": 500}, {"FunctionError": "foo"}]

        for response in responses:
            mock_aws_lambda_hook.return_value = response

            operator = AwsLambdaInvokeFunctionOperator(
                task_id="foo",
                function_name="foo",
                region_name="eu-west-1",
                payload=json.dumps({"foo": "bar"}),
                check_success_function=lambda r: True,
            )

            with self.assertRaises(AwsLambdaExecutionError):
                operator.execute(None)


if __name__ == "__main__":
    unittest.main()
