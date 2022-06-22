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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker_tuning import SageMakerTuningOperator

role = 'arn:aws:iam:role/test-role'

bucket = 'test-bucket'

key = 'test/data'
data_url = f's3://{bucket}/{key}'

job_name = 'test-job-name'

image = 'test-image'

output_url = f's3://{bucket}/test/output'

create_tuning_params = {
    'HyperParameterTuningJobName': job_name,
    'HyperParameterTuningJobConfig': {
        'Strategy': 'Bayesian',
        'HyperParameterTuningJobObjective': {'Type': 'Maximize', 'MetricName': 'test_metric'},
        'ResourceLimits': {'MaxNumberOfTrainingJobs': '123', 'MaxParallelTrainingJobs': '123'},
        'ParameterRanges': {
            'IntegerParameterRanges': [
                {'Name': 'k', 'MinValue': '2', 'MaxValue': '10'},
            ]
        },
    },
    'TrainingJobDefinition': {
        'StaticHyperParameters': {
            'k': '10',
            'feature_dim': '784',
            'mini_batch_size': '500',
            'force_dense': 'True',
        },
        'AlgorithmSpecification': {'TrainingImage': image, 'TrainingInputMode': 'File'},
        'RoleArn': role,
        'InputDataConfig': [
            {
                'ChannelName': 'train',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': data_url,
                        'S3DataDistributionType': 'FullyReplicated',
                    }
                },
                'CompressionType': 'None',
                'RecordWrapperType': 'None',
            }
        ],
        'OutputDataConfig': {'S3OutputPath': output_url},
        'ResourceConfig': {'InstanceCount': '2', 'InstanceType': 'ml.c4.8xlarge', 'VolumeSizeInGB': '50'},
        'StoppingCondition': dict(MaxRuntimeInSeconds=60 * 60),
    },
}


class TestSageMakerTuningOperator(unittest.TestCase):
    def setUp(self):
        self.sagemaker = SageMakerTuningOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_conn',
            config=create_tuning_params,
            wait_for_completion=False,
            check_interval=5,
        )

    def test_parse_config_integers(self):
        self.sagemaker.parse_config_integers()
        assert self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']['InstanceCount'] == int(
            self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']['InstanceCount']
        )
        assert self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']['VolumeSizeInGB'] == int(
            self.sagemaker.config['TrainingJobDefinition']['ResourceConfig']['VolumeSizeInGB']
        )
        assert self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits'][
            'MaxNumberOfTrainingJobs'
        ] == int(
            self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits'][
                'MaxNumberOfTrainingJobs'
            ]
        )
        assert self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits'][
            'MaxParallelTrainingJobs'
        ] == int(
            self.sagemaker.config['HyperParameterTuningJobConfig']['ResourceLimits'][
                'MaxParallelTrainingJobs'
            ]
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn', 'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_tuning.assert_called_once_with(
            create_tuning_params, wait_for_completion=False, check_interval=5, max_ingestion_time=None
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute_with_failure(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'testarn', 'ResponseMetadata': {'HTTPStatusCode': 404}}
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)
