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

from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker_endpoint import SageMakerEndpointOperator

role = 'arn:aws:iam:role/test-role'
bucket = 'test-bucket'
image = 'test-image'
output_url = f's3://{bucket}/test/output'
model_name = 'test-model-name'
config_name = 'test-endpoint-config-name'
endpoint_name = 'test-endpoint-name'

create_model_params = {
    'ModelName': model_name,
    'PrimaryContainer': {
        'Image': image,
        'ModelDataUrl': output_url,
    },
    'ExecutionRoleArn': role,
}

create_endpoint_config_params = {
    'EndpointConfigName': config_name,
    'ProductionVariants': [
        {
            'VariantName': 'AllTraffic',
            'ModelName': model_name,
            'InitialInstanceCount': '1',
            'InstanceType': 'ml.c4.xlarge',
        }
    ],
}

create_endpoint_params = {'EndpointName': endpoint_name, 'EndpointConfigName': config_name}

config = {
    'Model': create_model_params,
    'EndpointConfig': create_endpoint_config_params,
    'Endpoint': create_endpoint_params,
}


class TestSageMakerEndpointOperator(unittest.TestCase):
    def setUp(self):
        self.sagemaker = SageMakerEndpointOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=config,
            wait_for_completion=False,
            check_interval=5,
            operation='create',
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        for variant in self.sagemaker.config['EndpointConfig']['ProductionVariants']:
            self.assertEqual(variant['InitialInstanceCount'], int(variant['InitialInstanceCount']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    def test_execute(self, mock_endpoint, mock_endpoint_config, mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'testarn', 'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(create_model_params)
        mock_endpoint_config.assert_called_once_with(create_endpoint_config_params)
        mock_endpoint.assert_called_once_with(
            create_endpoint_params, wait_for_completion=False, check_interval=5, max_ingestion_time=None
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    def test_execute_with_failure(self, mock_endpoint, mock_endpoint_config, mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'testarn', 'ResponseMetadata': {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    @mock.patch.object(SageMakerHook, 'update_endpoint')
    def test_execute_with_duplicate_endpoint_creation(
        self, mock_endpoint_update, mock_endpoint, mock_endpoint_config, mock_model, mock_client
    ):
        response = {
            "Error": {"Code": "ValidationException", "Message": "Cannot create already existing endpoint."}
        }
        mock_endpoint.side_effect = ClientError(error_response=response, operation_name="CreateEndpoint")
        mock_endpoint_update.return_value = {
            'EndpointArn': 'testarn',
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }
        self.sagemaker.execute(None)
