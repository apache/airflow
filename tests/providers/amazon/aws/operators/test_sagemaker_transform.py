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

import mock

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker_transform import SageMakerTransformOperator

role = 'arn:aws:iam:role/test-role'

bucket = 'test-bucket'

key = 'test/data'
data_url = 's3://{}/{}'.format(bucket, key)

job_name = 'test-job-name'

model_name = 'test-model-name'

image = 'test-image'

output_url = 's3://{}/test/output'.format(bucket)

create_transform_params = {
    'TransformJobName': job_name,
    'ModelName': model_name,
    'MaxConcurrentTransforms': '12',
    'MaxPayloadInMB': '6',
    'BatchStrategy': 'MultiRecord',
    'TransformInput': {
        'DataSource': {
            'S3DataSource': {
                'S3DataType': 'S3Prefix',
                'S3Uri': data_url
            }
        }
    },
    'TransformOutput': {
        'S3OutputPath': output_url,
    },
    'TransformResources': {
        'InstanceType': 'ml.m4.xlarge',
        'InstanceCount': '3'
    }
}

create_model_params = {
    'ModelName': model_name,
    'PrimaryContainer': {
        'Image': image,
        'ModelDataUrl': output_url,
    },
    'ExecutionRoleArn': role
}

config = {
    'Model': create_model_params,
    'Transform': create_transform_params
}


class TestSageMakerTransformOperator(unittest.TestCase):

    def setUp(self):
        self.sagemaker = SageMakerTransformOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=config,
            wait_for_completion=False,
            check_interval=5
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        test_config = self.sagemaker.config['Transform']
        self.assertEqual(test_config['TransformResources']['InstanceCount'],
                         int(test_config['TransformResources']['InstanceCount']))
        self.assertEqual(test_config['MaxConcurrentTransforms'],
                         int(test_config['MaxConcurrentTransforms']))
        self.assertEqual(test_config['MaxPayloadInMB'],
                         int(test_config['MaxPayloadInMB']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_execute(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {'TransformJobArn': 'testarn',
                                       'ResponseMetadata':
                                       {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(create_model_params)
        mock_transform.assert_called_once_with(create_transform_params,
                                               wait_for_completion=False,
                                               check_interval=5,
                                               max_ingestion_time=None
                                               )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_execute_with_failure(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {'TransformJobArn': 'testarn',
                                       'ResponseMetadata':
                                       {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)
