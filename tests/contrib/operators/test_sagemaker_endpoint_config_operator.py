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

import unittest
try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from airflow import configuration
from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.contrib.operators.sagemaker_endpoint_config_operator \
    import SageMakerEndpointConfigOperator
from airflow.exceptions import AirflowException

model_name = 'test-model-name'
config_name = 'test-config-name'

create_endpoint_config_params = {
    'EndpointConfigName': config_name,
    'ProductionVariants': [
        {
            'VariantName': 'AllTraffic',
            'ModelName': model_name,
            'InitialInstanceCount': '1',
            'InstanceType': 'ml.c4.xlarge'
        }
    ]
}


class TestSageMakerEndpointConfigOperator(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.sagemaker = SageMakerEndpointConfigOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=create_endpoint_config_params
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        for variant in self.sagemaker.config['ProductionVariants']:
            self.assertEqual(variant['InitialInstanceCount'],
                             int(variant['InitialInstanceCount']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_endpoint_config')
    def test_execute(self, mock_model, mock_client):
        mock_model.return_value = {
            'EndpointConfigArn': 'testarn',
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(create_endpoint_config_params)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    def test_execute_with_failure(self, mock_model, mock_client):
        mock_model.return_value = {
            'EndpointConfigArn': 'testarn',
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


if __name__ == '__main__':
    unittest.main()
