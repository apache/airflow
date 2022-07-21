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
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator

EXPECTED_INTEGER_FIELDS: List[List[str]] = [
    ['Transform', 'TransformResources', 'InstanceCount'],
    ['Transform', 'MaxConcurrentTransforms'],
    ['Transform', 'MaxPayloadInMB'],
]

CREATE_TRANSFORM_PARAMS: Dict = {
    'TransformJobName': 'job_name',
    'ModelName': 'model_name',
    'MaxConcurrentTransforms': '12',
    'MaxPayloadInMB': '6',
    'BatchStrategy': 'MultiRecord',
    'TransformInput': {'DataSource': {'S3DataSource': {'S3DataType': 'S3Prefix', 'S3Uri': 's3_uri'}}},
    'TransformOutput': {'S3OutputPath': 'output_path'},
    'TransformResources': {'InstanceType': 'ml.m4.xlarge', 'InstanceCount': '3'},
}

CREATE_MODEL_PARAMS: Dict = {
    'ModelName': 'model_name',
    'PrimaryContainer': {'Image': 'test_image', 'ModelDataUrl': 'output_path'},
    'ExecutionRoleArn': 'arn:aws:iam:role/test-role',
}

CONFIG: Dict = {'Model': CREATE_MODEL_PARAMS, 'Transform': CREATE_TRANSFORM_PARAMS}


class TestSageMakerTransformOperator(unittest.TestCase):
    def setUp(self):
        self.sagemaker = SageMakerTransformOperator(
            task_id='test_sagemaker_operator',
            aws_conn_id='sagemaker_test_id',
            config=CONFIG,
            wait_for_completion=False,
            check_interval=5,
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_integer_fields(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {
            'TransformJobArn': 'test_arn',
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }
        self.sagemaker.execute(None)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for (key1, key2, *key3) in EXPECTED_INTEGER_FIELDS:
            if key3:
                (key3,) = key3
                assert self.sagemaker.config[key1][key2][key3] == int(self.sagemaker.config[key1][key2][key3])
            else:
                self.sagemaker.config[key1][key2] == int(self.sagemaker.config[key1][key2])

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_execute(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {
            'TransformJobArn': 'test_arn',
            'ResponseMetadata': {'HTTPStatusCode': 200},
        }
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(CREATE_MODEL_PARAMS)
        mock_transform.assert_called_once_with(
            CREATE_TRANSFORM_PARAMS, wait_for_completion=False, check_interval=5, max_ingestion_time=None
        )

    @mock.patch.object(SageMakerHook, 'get_conn')
    @mock.patch.object(SageMakerHook, 'create_model')
    @mock.patch.object(SageMakerHook, 'create_transform_job')
    def test_execute_with_failure(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {
            'TransformJobArn': 'test_arn',
            'ResponseMetadata': {'HTTPStatusCode': 404},
        }
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)
