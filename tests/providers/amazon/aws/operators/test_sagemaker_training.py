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
from __future__ import annotations

from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTrainingOperator

EXPECTED_INTEGER_FIELDS: list[list[str]] = [
    ["ResourceConfig", "InstanceCount"],
    ["ResourceConfig", "VolumeSizeInGB"],
    ["StoppingCondition", "MaxRuntimeInSeconds"],
]

CREATE_TRAINING_PARAMS = {
    "AlgorithmSpecification": {"TrainingImage": "image_name", "TrainingInputMode": "File"},
    "RoleArn": "arn:aws:iam:role/test-role",
    "OutputDataConfig": {"S3OutputPath": "output_path"},
    "ResourceConfig": {"InstanceCount": "2", "InstanceType": "ml.c4.8xlarge", "VolumeSizeInGB": "50"},
    "TrainingJobName": "job_name",
    "HyperParameters": {"k": "10", "feature_dim": "784", "mini_batch_size": "500", "force_dense": "True"},
    "StoppingCondition": {"MaxRuntimeInSeconds": "3600"},
    "InputDataConfig": [
        {
            "ChannelName": "train",
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": "s3_uri",
                    "S3DataDistributionType": "FullyReplicated",
                }
            },
            "CompressionType": "None",
            "RecordWrapperType": "None",
        }
    ],
}


class TestSageMakerTrainingOperator:
    def setup_method(self):
        self.sagemaker = SageMakerTrainingOperator(
            task_id="test_sagemaker_operator",
            config=CREATE_TRAINING_PARAMS.copy(),
            wait_for_completion=False,
            check_interval=5,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_training_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_integer_fields(self, serialize, mock_training, mock_client):
        mock_training.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker._check_if_job_exists = mock.MagicMock()
        self.sagemaker.execute(None)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for (key1, key2) in EXPECTED_INTEGER_FIELDS:
            assert self.sagemaker.config[key1][key2] == int(self.sagemaker.config[key1][key2])

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_training_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_with_check_if_job_exists(self, serialize, mock_training, mock_client):
        mock_training.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker._check_if_job_exists = mock.MagicMock()
        self.sagemaker.execute(None)
        self.sagemaker._check_if_job_exists.assert_called_once()
        mock_training.assert_called_once_with(
            CREATE_TRAINING_PARAMS,
            wait_for_completion=False,
            print_log=True,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_training_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_without_check_if_job_exists(self, serialize, mock_training, mock_client):
        mock_training.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.check_if_job_exists = False
        self.sagemaker._check_if_job_exists = mock.MagicMock()
        self.sagemaker.execute(None)
        self.sagemaker._check_if_job_exists.assert_not_called()
        mock_training.assert_called_once_with(
            CREATE_TRAINING_PARAMS,
            wait_for_completion=False,
            print_log=True,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_training_job")
    def test_execute_with_failure(self, mock_training, mock_client):
        mock_training.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "list_training_jobs")
    def test_check_if_job_exists_increment(self, mock_list_training_jobs, mock_client):
        self.sagemaker.check_if_job_exists = True
        self.sagemaker.action_if_job_exists = "increment"
        mock_list_training_jobs.return_value = [{"TrainingJobName": "job_name"}]
        self.sagemaker._check_if_job_exists()

        expected_config = CREATE_TRAINING_PARAMS.copy()
        # Expect to see TrainingJobName suffixed with "-2" because we return one existing job
        expected_config["TrainingJobName"] = "job_name-2"
        assert self.sagemaker.config == expected_config

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "list_training_jobs")
    def test_check_if_job_exists_fail(self, mock_list_training_jobs, mock_client):
        self.sagemaker.check_if_job_exists = True
        self.sagemaker.action_if_job_exists = "fail"
        mock_list_training_jobs.return_value = [{"TrainingJobName": "job_name"}]
        with pytest.raises(AirflowException):
            self.sagemaker._check_if_job_exists()
