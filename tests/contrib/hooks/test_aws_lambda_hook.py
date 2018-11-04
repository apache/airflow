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

import unittest
import io
import json
import textwrap
import zipfile

from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook

try:
    from moto import mock_lambda
except ImportError:
    mock_lambda = None


class TestAwsLambdaHook(unittest.TestCase):

    @unittest.skipIf(mock_lambda is None, 'mock_lambda package not present')
    @mock_lambda
    def test_get_conn_returns_a_boto3_connection(self):
        hook = AwsLambdaHook(aws_conn_id='aws_default',
                             function_name="test_function", region_name="us-east-1")
        self.assertIsNotNone(hook.get_conn())

    @staticmethod
    def lambda_function():
        code = textwrap.dedent("""
def lambda_handler(event, context):
    return event
        """)
        zip_output = io.BytesIO()
        zip_file = zipfile.ZipFile(zip_output, 'w', zipfile.ZIP_DEFLATED)
        zip_file.writestr('lambda_function.zip', code)
        zip_file.close()
        zip_output.seek(0)
        return zip_output.read()

    @unittest.skipIf(mock_lambda is None, 'mock_lambda package not present')
    @mock_lambda
    def test_invoke_lambda_function(self):

        hook = AwsLambdaHook(aws_conn_id='aws_default',
                             function_name="test_function", region_name="us-east-1")

        hook.get_conn().create_function(
            FunctionName='test_function',
            Runtime='python2.7',
            Role='test-iam-role',
            Handler='lambda_function.lambda_handler',
            Code={
                'ZipFile': self.lambda_function(),
            },
            Description='test lambda function',
            Timeout=3,
            MemorySize=128,
            Publish=True,
        )

        input = {'hello': 'airflow'}
        response = hook.invoke_lambda(payload=json.dumps(input))

        self.assertEquals(response["StatusCode"], 202)


if __name__ == '__main__':
    unittest.main()
