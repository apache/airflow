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

import io
import json
import unittest
import zipfile

from moto import mock_iam, mock_lambda, mock_sts

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator


@mock_lambda
@mock_sts
@mock_iam
class TestAwsLambdaInvokeFunctionOperator(unittest.TestCase):
    def test_init(self):
        lambda_operator = AwsLambdaInvokeFunctionOperator(
            task_id="test",
            function_name="test",
            payload=json.dumps({"TestInput": "Testdata"}),
            log_type="None",
            aws_conn_id="aws_conn_test",
        )
        assert lambda_operator.task_id == "test"
        assert lambda_operator.function_name == "test"
        assert lambda_operator.payload == json.dumps({"TestInput": "Testdata"})
        assert lambda_operator.log_type == "None"
        assert lambda_operator.aws_conn_id == "aws_conn_test"

    def create_zip(self, body):
        code = body
        zip_output = io.BytesIO()
        zip_file = zipfile.ZipFile(zip_output, "w", zipfile.ZIP_DEFLATED)
        zip_file.writestr("lambda_function.py", code)
        zip_file.close()
        zip_output.seek(0)
        return zip_output.read()

    def create_iam_role(self, role_name: str):
        iam = AwsBaseHook("aws_conn_test", client_type="iam")
        resp = iam.conn.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "lambda.amazonaws.com"},
                            "Action": "sts:AssumeRole",
                        }
                    ],
                }
            ),
            Description="IAM role for Lambda execution.",
        )
        return resp["Role"]["Arn"]

    def create_lambda_function(self, function_name: str):
        code = """def handler(event, context):
            return event
        """
        role_name = "LambdaRole"
        role_arn = self.create_iam_role(role_name)
        zipped_code = self.create_zip(code)
        lambda_client = LambdaHook(aws_conn_id="aws_conn_test")
        resp = lambda_client.create_lambda(
            function_name=function_name,
            runtime="python3.7",
            role=role_arn,
            code={
                "ZipFile": zipped_code,
            },
            handler="lambda_function.handler",
        )
        return resp

    def test_invoke_lambda(self):
        self.create_lambda_function('test')
        test_event_input = {"TestInput": "Testdata"}
        lambda_invoke_function = AwsLambdaInvokeFunctionOperator(
            task_id="task_test",
            function_name="test",
            log_type='None',
            payload=json.dumps(test_event_input),
        )
        value = lambda_invoke_function.execute(None)
        assert json.dumps(json.loads(value)) == json.dumps(test_event_input)
