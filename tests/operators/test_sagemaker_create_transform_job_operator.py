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
from airflow.contrib.operators.sagemaker_create_transform_job_operator \
    import SageMakerCreateTransformJobOperator
from airflow.exceptions import AirflowException

role = 'test-role'

bucket = 'test-bucket'

key = 'test/data'
data_url = 's3://{}/{}'.format(bucket, key)

job_name = 'test-job-name'

model_name = 'test-model-name'

image = 'test-image'

output_url = 's3://{}/test/output'.format(bucket)

create_transform_params = \
    {
        'TransformJobName': job_name,
        'ModelName': model_name,
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
            'InstanceCount': 123
        }
    }

create_model_params = \
    {
        'ModelName': model_name,
        'PrimaryContainer': {
            'Image': image,
            'ModelDataUrl': output_url,
        },
        'ExecutionRoleArn': role
    }


class TestSageMakertransformOperator(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.sagemaker = SageMakerCreateTransformJobOperator(
            task_id='test_sagemaker_operator',
            sagemaker_conn_id='sagemaker_test_id',
            transform_job_config=create_transform_params,
            model_config=create_model_params,
            region_name='us-west-2',
            use_db_config=True,
            wait_for_completion=False,
            check_interval=5
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    @mock.patch.object(SageMakerHook, '__init__')
    def test_hook_init(self, hook_init, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {"TransformJobArn": "testarn",
                                       "ResponseMetadata":
                                       {"HTTPStatusCode": 200}}
        hook_init.return_value = None
        self.sagemaker.execute(None)
        hook_init.assert_called_once_with(
            sagemaker_conn_id='sagemaker_test_id',
            region_name='us-west-2',
            use_db_config=True,
            check_interval=5,
            max_ingestion_time=None
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_execute_without_failure(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {"TransformJobArn": "testarn",
                                       "ResponseMetadata":
                                       {"HTTPStatusCode": 200}}
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(create_model_params)
        mock_transform.assert_called_once_with(create_transform_params,
                                               wait_for_completion=False
                                               )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_execute_with_failure(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {"TransformJobArn": "testarn",
                                       "ResponseMetadata":
                                       {"HTTPStatusCode": 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


if __name__ == '__main__':
    unittest.main()
