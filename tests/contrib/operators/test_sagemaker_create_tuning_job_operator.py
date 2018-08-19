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
from airflow.contrib.operators.sagemaker_create_tuning_job_operator \
    import SageMakerCreateTuningJobOperator
from airflow.exceptions import AirflowException

role = 'test-role'

bucket = 'test-bucket'

key = 'test/data'
data_url = 's3://{}/{}'.format(bucket, key)

job_name = 'test-job-name'

image = 'test-image'

output_url = 's3://{}/test/output'.format(bucket)

create_tuning_params = {'HyperParameterTuningJobName': job_name,
                        'HyperParameterTuningJobConfig': {
                            'Strategy': 'Bayesian',
                            'HyperParameterTuningJobObjective': {
                                'Type': 'Maximize',
                                'MetricName': 'test_metric'
                            },
                            'ResourceLimits': {
                                'MaxNumberOfTrainingJobs': 123,
                                'MaxParallelTrainingJobs': 123
                            },
                            'ParameterRanges': {
                                'IntegerParameterRanges': [
                                    {
                                        'Name': 'k',
                                        'MinValue': '2',
                                        'MaxValue': '10'
                                    },
                                ]
                            }
                        },
                        'TrainingJobDefinition': {
                            'StaticHyperParameters':
                                {
                                    'k': '10',
                                    'feature_dim': '784',
                                    'mini_batch_size': '500',
                                    'force_dense': 'True'
                                },
                            'AlgorithmSpecification':
                                {
                                    'TrainingImage': image,
                                    'TrainingInputMode': 'File'
                                },
                            'RoleArn': 'string',
                            'InputDataConfig':
                                [
                                    {
                                        'ChannelName': 'train',
                                        'DataSource': {
                                            'S3DataSource': {
                                                'S3DataType': 'S3Prefix',
                                                'S3Uri': data_url,
                                                'S3DataDistributionType':
                                                    'FullyReplicated'
                                            }
                                        },
                                        'CompressionType': 'None',
                                        'RecordWrapperType': 'None'
                                    }
                                ],
                            'OutputDataConfig':
                                {
                                    'S3OutputPath': output_url
                                },
                            'ResourceConfig':
                                {
                                    'InstanceCount': 2,
                                    'InstanceType': 'ml.c4.8xlarge',
                                    'VolumeSizeInGB': 50
                                },
                            'StoppingCondition': dict(MaxRuntimeInSeconds=60 * 60)
                        }
                        }


class TestSageMakerTrainingOperator(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.sagemaker = SageMakerCreateTuningJobOperator(
            task_id='test_sagemaker_operator',
            sagemaker_conn_id='sagemaker_test_conn',
            tuning_job_config=create_tuning_params,
            region_name='us-east-1',
            use_db_config=False,
            wait_for_completion=False,
            check_interval=5
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    @mock.patch.object(SageMakerHook, '__init__')
    def test_hook_init(self, hook_init, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn',
                                    'ResponseMetadata':
                                    {'HTTPStatusCode': 200}}
        hook_init.return_value = None
        self.sagemaker.execute(None)
        hook_init.assert_called_once_with(
            sagemaker_conn_id='sagemaker_test_conn',
            region_name='us-east-1',
            use_db_config=False,
            check_interval=5,
            max_ingestion_time=None
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute_without_failure(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn',
                                    'ResponseMetadata':
                                    {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_tuning.assert_called_once_with(create_tuning_params,
                                            wait_for_completion=False)

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute_with_failure(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn',
                                    'ResponseMetadata':
                                    {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


if __name__ == '__main__':
    unittest.main()
