# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from airflow.contrib.hooks.aws_hook import AwsHook


class AwsLambdaHook(AwsHook):
    """
    Interact with AWS Lambda

    :param function_name: AWS Lambda Function Name
    :type function_name: str
    :param region_name: AWS Region Name (example: us-west-2)
    :type region_name: str
    :param log_type: Tail Invocation Request
    :type log_type: str
    :param qualifier: AWS Lambda Function Version or Alias Name
    :type qualifier: str
    :param invocation_type: AWS Lambda Invocation Type (RequestResponse, Event etc)
    :type invocation_type: str
    """

    def __init__(self, function_name, region_name=None, log_type='None', qualifier='$LATEST',
                 invocation_type='RequestResponse', *args, **kwargs):
        self.function_name = function_name
        self.region_name = region_name
        self.log_type = log_type
        self.invocation_type = invocation_type
        self.qualifier = qualifier
        super(AwsLambdaHook, self).__init__(*args, **kwargs)

    def get_conn(self):
        self.conn = self.get_client_type('lambda', self.region_name)
        return self.conn

    def invoke_lambda(self, payload):
        """
        Invoke Lambda Function
        """

        awslambda_conn = self.get_conn()

        response = awslambda_conn.invoke(
            FunctionName=self.function_name,
            InvocationType=self.invocation_type,
            LogType=self.log_type,
            Payload=payload,
            Qualifier=self.qualifier
        )

        return response
