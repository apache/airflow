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

import unittest

from unittest import mock

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker_model import SageMakerModelOperator

role = 'arn:aws:iam:role/test-role'

bucket = 'test-bucket'

model_name = 'test-model-name'

image = 'test-image'

output_url = 's3://{}/test/output'.format(bucket)
create_model_params = {
    'ModelName': model_name,
    'PrimaryContainer': {
        'Image': image,
        'ModelDataUrl': output_url,
    },
    'ExecutionRoleArn': role,
}


class TestSageMakerModelOperator(unittest.TestCase):
    def setUp(self):
        self.sagemaker = SageMakerModelOperator(
            task_id='test_sagemaker_operator', aws_conn_id='sagemaker_test_id', config=create_model_params
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    def test_execute(self, mock_model, mock_client):
        mock_model.return_value = {'ModelArn': 'testarn', 'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(create_model_params)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    def test_execute_with_failure(self, mock_model, mock_client):
        mock_model.return_value = {'ModelArn': 'testarn', 'ResponseMetadata': {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)
