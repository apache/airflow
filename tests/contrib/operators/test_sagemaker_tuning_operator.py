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

from airflow import configuration
from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.contrib.operators.sagemaker_tuning_operator \
    import SageMakerTuningOperator
from airflow.exceptions import AirflowException
from tests.compat import mock

role = 'arn:aws:iam:role/test-role'

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
                                'MaxNumberOfTrainingJobs': '123',
                                'MaxParallelTrainingJobs': '123'
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
                            'RoleArn': role,
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
                                    'InstanceCount': '2',
                                    'InstanceType': 'ml.c4.8xlarge',
                                    'VolumeSizeInGB': '50'
                                },
                            'StoppingCondition': dict(MaxRuntimeInSeconds=60 * 60)
                        }
                        }


class TestSageMakerTuningOperator(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        self.sagemaker = SageMakerTuningOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_conn',
            config=create_tuning_params,
            wait_for_completion=False,
            check_interval=5
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        self.assertEqual(self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']
                         ['InstanceCount'],
                         int(self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']
                             ['InstanceCount']))
        self.assertEqual(self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']
                         ['VolumeSizeInGB'],
                         int(self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']
                             ['VolumeSizeInGB']))
        self.assertEqual(self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits']
                         ['MaxNumberOfTrainingJobs'],
                         int(self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits']
                             ['MaxNumberOfTrainingJobs']))
        self.assertEqual(self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits']
                         ['MaxParallelTrainingJobs'],
                         int(self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits']
                             ['MaxParallelTrainingJobs']))

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn',
                                    'ResponseMetadata':
                                    {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_tuning.assert_called_once_with(create_tuning_params,
                                            wait_for_completion=False,
                                            check_interval=5,
                                            max_ingestion_time=None
                                            )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute_with_failure(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn',
                                    'ResponseMetadata':
                                    {'HTTPStatusCode': 404}}
        self.assertRaises(AirflowException, self.sagemaker.execute, None)


if __name__ == '__main__':
    unittest.main()
