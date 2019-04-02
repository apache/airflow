# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# 'License'); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
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
from airflow.contrib.operators.sagemaker_training_operator \
    import SageMakerTrainingOperator
from airflow.exceptions import AirflowException

role = 'arn:aws:iam:role/test-role'

bucket = 'test-bucket'

key = 'test/data'
data_url = 's3://{}/{}'.format(bucket, key)

job_name = 'test-job-name'

image = 'test-image'

output_url = 's3://{}/test/output'.format(bucket)
create_training_params = \
    {
        'AlgorithmSpecification': {
            'TrainingImage': image,
            'TrainingInputMode': 'File'
        },
        'RoleArn': role,
        'OutputDataConfig': {
            'S3OutputPath': output_url
        },
        'ResourceConfig': {
            'InstanceCount': '2',
            'InstanceType': 'ml.c4.8xlarge',
            'VolumeSizeInGB': '50'
        },
        'TrainingJobName': job_name,
        'HyperParameters': {
            'k': '10',
            'feature_dim': '784',
            'mini_batch_size': '500',
            'force_dense': 'True'
        },
        'StoppingCondition': {
            'MaxRuntimeInSeconds': '3600'
        },
        'InputDataConfig': [
            {
                'ChannelName': 'train',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': data_url,
                        'S3DataDistributionType': 'FullyReplicated'
                    }
                },
                'CompressionType': 'None',
                'RecordWrapperType': 'None'
            }
        ]
    }


class TestSageMakerTrainingOperator(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.sagemaker = SageMakerTrainingOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=create_training_params,
            wait_for_completion=False,
            check_interval=5
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        self.assertEqual(self.sagemaker.config['ResourceConfig']['InstanceCount'],
                         int(self.sagemaker.config['ResourceConfig']['InstanceCount']))
        self.assertEqual(self.sagemaker.config['ResourceConfig']['VolumeSizeInGB'],
                         int(self.sagemaker.config['ResourceConfig']['VolumeSizeInGB']))
        self.assertEqual(self.sagemaker.config['StoppingCondition']['MaxRuntimeInSeconds'],
                         int(self.sagemaker.config['StoppingCondition']['MaxRuntimeInSeconds']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_training_job')
    def test_execute(self, mock_training, mock_client):
        mock_training.return_value = {'TrainingJobArn': 'testarn',
                                      'ResponseMetadata':
                                          {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_training.assert_called_once_with(create_training_params,
                                              wait_for_completion=False,
                                              print_log=True,
                                              check_interval=5,
                                              max_ingestion_time=None
                                              )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_training_job')
    def test_execute_with_failure(self, mock_training, mock_client):
        mock_training.return_value = {'TrainingJobArn': 'testarn',
                                      'ResponseMetadata':
                                          {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


if __name__ == '__main__':
    unittest.main()
