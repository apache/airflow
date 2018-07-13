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
#


import json
import unittest
import copy
try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

from airflow import configuration
from airflow import models
from airflow.utils import db
from airflow.contrib.hooks.sagemaker_hook import SageMakerHook
from airflow.hooks.S3_hook import S3Hook
from airflow.exceptions import AirflowException


role = "test-role"

bucket = "test-bucket"

key = "test/data"
data_url = "s3://{}/{}".format(bucket, key)

job_name = "test-job-name"

image = "test-image"

test_arn_return = {'TrainingJobArn': 'testarn'}

test_list_training_job_return = {
    'TrainingJobSummaries': [
        {
            'TrainingJobName': job_name,
            'TrainingJobStatus': 'InProgress'
        },
    ],
    'NextToken': 'test-token'
}

test_list_tuning_job_return = {
    'TrainingJobSummaries': [
        {
            'TrainingJobName': job_name,
            'TrainingJobArn': 'testarn',
            'TunedHyperParameters': {
                'k': '3'
            },
            'TrainingJobStatus': 'InProgress'
        },
    ],
    'NextToken': 'test-token'
}

output_url = "s3://{}/test/output".format(bucket)
create_training_params = \
    {
        "AlgorithmSpecification": {
            "TrainingImage": image,
            "TrainingInputMode": "File"
        },
        "RoleArn": role,
        "OutputDataConfig": {
            "S3OutputPath": output_url
        },
        "ResourceConfig": {
            "InstanceCount": 2,
            "InstanceType": "ml.c4.8xlarge",
            "VolumeSizeInGB": 50
        },
        "TrainingJobName": job_name,
        "HyperParameters": {
            "k": "10",
            "feature_dim": "784",
            "mini_batch_size": "500",
            "force_dense": "True"
        },
        "StoppingCondition": {
            "MaxRuntimeInSeconds": 60 * 60
        },
        "InputDataConfig": [
            {
                "ChannelName": "train",
                "DataSource": {
                    "S3DataSource": {
                        "S3DataType": "S3Prefix",
                        "S3Uri": data_url,
                        "S3DataDistributionType": "FullyReplicated"
                    }
                },
                "CompressionType": "None",
                "RecordWrapperType": "None"
            }
        ]
    }

create_tuning_params = {"HyperParameterTuningJobName": job_name,
                        "HyperParameterTuningJobConfig": {
                            "Strategy": "Bayesian",
                            "HyperParameterTuningJobObjective": {
                                "Type": "Maximize",
                                "MetricName": "test_metric"
                            },
                            "ResourceLimits": {
                                "MaxNumberOfTrainingJobs": 123,
                                "MaxParallelTrainingJobs": 123
                            },
                            "ParameterRanges": {
                                "IntegerParameterRanges": [
                                    {
                                        "Name": "k",
                                        "MinValue": "2",
                                        "MaxValue": "10"
                                    },
                                ]
                            }
                        },
                        "TrainingJobDefinition": {
                            "StaticHyperParameters":
                                create_training_params['HyperParameters'],
                            "AlgorithmSpecification":
                                create_training_params['AlgorithmSpecification'],
                            "RoleArn": "string",
                            "InputDataConfig":
                                create_training_params['InputDataConfig'],
                            "OutputDataConfig":
                                create_training_params['OutputDataConfig'],
                            "ResourceConfig":
                                create_training_params['ResourceConfig'],
                            "StoppingCondition": dict(MaxRuntimeInSeconds=60 * 60)
                        }
                        }

db_config = {"Re4sourceConfig": {
    "InstanceCount": 3,
    "InstanceType": "ml.c4.4config_test",
    "VolumeSizeInGB": 25
}
}


class TestSageMakerHook(unittest.TestCase):

    def setUp(self):
        configuration.load_test_config()
        db.merge_conn(
            models.Connection(
                conn_id='sagemaker_test_conn',
                conn_type='sagemaker',
                login='access_id',
                password='access_key',
                extra=json.dumps(db_config)
            )
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(S3Hook, 'check_for_key')
    @mock.patch.object(S3Hook, 'check_for_bucket')
    def test_check_for_url(self,
                           mock_check_bucket, mock_check_key, mock_client):
        mock_client.return_value = None
        hook = SageMakerHook()
        mock_check_bucket.side_effect = [False, True, True]
        mock_check_key.side_effect = [False, True]
        self.assertRaises(AirflowException,
                          hook.check_for_url, data_url)
        self.assertRaises(AirflowException,
                          hook.check_for_url, data_url)
        self.assertEqual(hook.check_for_url(data_url), True)

    @mock.patch.object(SageMakerHook, 'get_client_type')
    def test_conn(self, mock_get_client):
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test_conn',
                             region_name='us-east-1'
                             )
        self.assertEqual(hook.sagemaker_conn_id, 'sagemaker_test_conn')
        mock_get_client.assert_called_once_with('sagemaker',
                                                region_name='us-east-1'
                                                )

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_list_training_job(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'list_training_jobs.return_value':
                 test_list_training_job_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test_conn')
        response = hook.list_training_job(name_contains=job_name,
                                          status_equals='InProgress')
        mock_session.list_training_jobs. \
            assert_called_once_with(NameContains=job_name,
                                    StatusEquals='InProgress')
        self.assertEqual(response, test_list_training_job_return)

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_list_tuning_job(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'list_hyper_parameter_tuning_job.return_value':
                 test_list_tuning_job_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test_conn')
        response = hook.list_tuning_job(name_contains=job_name,
                                        status_equals='InProgress')
        mock_session.list_hyper_parameter_tuning_job. \
            assert_called_once_with(NameContains=job_name,
                                    StatusEquals='InProgress')
        self.assertEqual(response, test_list_tuning_job_return )

    @mock.patch.object(SageMakerHook, 'check_valid_training_input')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_training_job(self, mock_client, mock_check_training):
        mock_check_training.return_value = True
        mock_session = mock.Mock()
        attrs = {'create_training_job.return_value':
                 test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test_conn')
        response = hook.create_training_job(create_training_params)
        mock_session.create_training_job.assert_called_once_with(**create_training_params)
        self.assertEqual(response, test_arn_return)

    @mock.patch.object(SageMakerHook, 'check_valid_training_input')
    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_training_job_db_config(self, mock_client, mock_check_training):
        mock_check_training.return_value = True
        mock_session = mock.Mock()
        attrs = {'create_training_job.return_value':
                 test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook_use_db_config = SageMakerHook(sagemaker_conn_id='sagemaker_test_conn',
                                           use_db_config=True)
        response = hook_use_db_config.create_training_job(create_training_params)
        updated_config = copy.deepcopy(create_training_params)
        updated_config.update(db_config)
        mock_session.create_training_job.assert_called_once_with(**updated_config)
        self.assertEqual(response, test_arn_return)

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_create_tuning_job(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'create_hyper_parameter_tuning_job.return_value':
                 test_arn_return}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test_conn')
        response = hook.create_tuning_job(create_tuning_params)
        mock_session.create_hyper_parameter_tuning_job.\
            assert_called_once_with(**create_tuning_params)
        self.assertEqual(response, test_arn_return)

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_training_job(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'describe_training_job.return_value': 'InProgress'}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test_conn', job_name=job_name)
        response = hook.describe_training_job()
        mock_session.describe_training_job.\
            assert_called_once_with(TrainingJobName=job_name)
        self.assertEqual(response, 'InProgress')

    @mock.patch.object(SageMakerHook, 'get_conn')
    def test_describe_tuning_job(self, mock_client):
        mock_session = mock.Mock()
        attrs = {'describe_hyper_parameter_tuning_job.return_value':
                 'InProgress'}
        mock_session.configure_mock(**attrs)
        mock_client.return_value = mock_session
        hook = SageMakerHook(sagemaker_conn_id='sagemaker_test_conn', job_name=job_name)
        response = hook.describe_tuning_job()
        mock_session.describe_hyper_parameter_tuning_job.\
            assert_called_once_with(HyperParameterTuningJobName=job_name)
        self.assertEqual(response, 'InProgress')


if __name__ == '__main__':
    unittest.main()
