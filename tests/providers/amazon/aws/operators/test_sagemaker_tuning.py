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

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTuningOperator

EXPECTED_INTEGER_FIELDS: List[List[str]] = [
    ['HyperParameterTuningJobConfig', 'ResourceLimits', 'MaxNumberOfTrainingJobs'],
    ['HyperParameterTuningJobConfig', 'ResourceLimits', 'MaxParallelTrainingJobs'],
    ['TrainingJobDefinition', 'ResourceConfig', 'InstanceCount'],
    ['TrainingJobDefinition', 'ResourceConfig', 'VolumeSizeInGB'],
    ['TrainingJobDefinition', 'StoppingCondition', 'MaxRuntimeInSeconds'],
]

CREATE_TUNING_PARAMS: Dict = {
    'HyperParameterTuningJobName': 'job_name',
    'HyperParameterTuningJobConfig': {
        'Strategy': 'Bayesian',
        'HyperParameterTuningJobObjective': {'Type': 'Maximize', 'MetricName': 'test_metric'},
        'ResourceLimits': {'MaxNumberOfTrainingJobs': '123', 'MaxParallelTrainingJobs': '123'},
        'ParameterRanges': {'IntegerParameterRanges': [{'Name': 'k', 'MinValue': '2', 'MaxValue': '10'}]},
    },
    'TrainingJobDefinition': {
        'StaticHyperParameters': {
            'k': '10',
            'feature_dim': '784',
            'mini_batch_size': '500',
            'force_dense': 'True',
        },
        'AlgorithmSpecification': {'TrainingImage': 'image_name', 'TrainingInputMode': 'File'},
        'RoleArn': 'arn:aws:iam:role/test-role',
        'InputDataConfig': [
            {
                'ChannelName': 'train',
                'DataSource': {
                    'S3DataSource': {
                        'S3DataType': 'S3Prefix',
                        'S3Uri': 's3_uri',
                        'S3DataDistributionType': 'FullyReplicated',
                    }
                },
                'CompressionType': 'None',
                'RecordWrapperType': 'None',
            }
        ],
        'OutputDataConfig': {'S3OutputPath': 'output_path'},
        'ResourceConfig': {'InstanceCount': '2', 'InstanceType': 'ml.c4.8xlarge', 'VolumeSizeInGB': '50'},
        'StoppingCondition': {'MaxRuntimeInSeconds': '3600'},
    },
}


class TestSageMakerTuningOperator(unittest.TestCase):
    def setUp(self):
        self.sagemaker = SageMakerTuningOperator(
            task_id='test_sagemaker_operator',
            config=CREATE_TUNING_PARAMS,
            wait_for_completion=False,
            check_interval=5,
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    @mock.patch.object(sagemaker, 'serialize', return_value="")
    def test_integer_fields(self, serialize, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'test_arn', 'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for (key1, key2, key3) in EXPECTED_INTEGER_FIELDS:
            assert self.sagemaker.config[key1][key2][key3] == int(self.sagemaker.config[key1][key2][key3])

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    @mock.patch.object(sagemaker, 'serialize', return_value="")
    def test_execute(self, serialize, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'test_arn', 'ResponseMetadata': {'HTTPStatusCode': 200}}
        self.sagemaker.execute(None)
        mock_tuning.assert_called_once_with(
            CREATE_TUNING_PARAMS, wait_for_completion=False, check_interval=5, max_ingestion_time=None
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_tuning_job')
    def test_execute_with_failure(self, mock_tuning, mock_client):
        mock_tuning.return_value = {'TrainingJobArn': 'test_arn', 'ResponseMetadata': {'HTTPStatusCode': 404}}
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)
