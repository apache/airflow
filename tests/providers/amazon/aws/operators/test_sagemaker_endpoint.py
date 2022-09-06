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
from typing import Dict, List
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerEndpointOperator

CREATE_MODEL_PARAMS: Dict = {
    'ModelName': 'model_name',
    'PrimaryContainer': {'Image': 'image_name', 'ModelDataUrl': 'output_path'},
    'ExecutionRoleArn': 'arn:aws:iam:role/test-role',
}
CREATE_ENDPOINT_CONFIG_PARAMS: Dict = {
    'EndpointConfigName': 'config_name',
    'ProductionVariants': [
        {
            'VariantName': 'AllTraffic',
            'ModelName': 'model_name',
            'InitialInstanceCount': '1',
            'InstanceType': 'ml.c4.xlarge',
        }
    ],
}
CREATE_ENDPOINT_PARAMS: Dict = {'EndpointName': 'endpoint_name', 'EndpointConfigName': 'config_name'}

CONFIG: Dict = {
    'Model': CREATE_MODEL_PARAMS,
    'EndpointConfig': CREATE_ENDPOINT_CONFIG_PARAMS,
    'Endpoint': CREATE_ENDPOINT_PARAMS,
}

EXPECTED_INTEGER_FIELDS: List[List[str]] = [['EndpointConfig', 'ProductionVariants', 'InitialInstanceCount']]


class TestSageMakerEndpointOperator(unittest.TestCase):
    def setUp(self):
        self.sagemaker = SageMakerEndpointOperator(
            task_id='test_sagemaker_operator',
            config=CONFIG,
            wait_for_completion=False,
            check_interval=5,
            operation='create',
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    @mock.patch.object(sagemaker, 'serialize', return_value="")
    def test_integer_fields(self, serialize, mock_endpoint, mock_endpoint_config, mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'test_arn', 'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for variant in self.sagemaker.config['EndpointConfig']['ProductionVariants']:
            assert variant['InitialInstanceCount'] == int(variant['InitialInstanceCount'])

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    @mock.patch.object(sagemaker, 'serialize', return_value="")
    def test_execute(self, serialize, mock_endpoint, mock_endpoint_config, mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'test_arn', 'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(CREATE_MODEL_PARAMS)
        mock_endpoint_config.assert_called_once_with(CREATE_ENDPOINT_CONFIG_PARAMS)
        mock_endpoint.assert_called_once_with(
            CREATE_ENDPOINT_PARAMS, wait_for_completion=False, check_interval=5, max_ingestion_time=None
        )
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for variant in self.sagemaker.config['EndpointConfig']['ProductionVariants']:
            assert variant['InitialInstanceCount'] == int(variant['InitialInstanceCount'])

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    def test_execute_with_failure(self, mock_endpoint, mock_endpoint_config, mock_model, mock_client):
        mock_endpoint.return_value = {'EndpointArn': 'test_arn', 'ResponseMetadata': {'HTTPStatusCode': 404}}
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    @mock.patch.object(SageMakerHook, 'create_endpoint')
    @mock.patch.object(SageMakerHook, 'update_endpoint')
    @mock.patch.object(sagemaker, 'serialize', return_value="")
    def test_execute_with_duplicate_endpoint_creation(
        self, serialize, mock_endpoint_update, mock_endpoint, mock_endpoint_config, mock_model, mock_client
    ):
        response = {
            'Error': {'Code': 'ValidationException', 'Message': 'Cannot create already existing endpoint.'}
        }
        mock_endpoint.side_effect = ClientError(error_response=response, operation_name='CreateEndpoint')
        mock_endpoint_update.return_value = {
            'EndpointArn': 'test_arn',
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }
        self.sagemaker.execute(None)
