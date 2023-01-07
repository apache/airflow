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
from __future__ import annotations

import copy
from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator

EXPECTED_INTEGER_FIELDS: list[list[str]] = [
    ["Transform", "TransformResources", "InstanceCount"],
    ["Transform", "MaxConcurrentTransforms"],
    ["Transform", "MaxPayloadInMB"],
]

CREATE_TRANSFORM_PARAMS: dict = {
    "TransformJobName": "job_name",
    "ModelName": "model_name",
    "MaxConcurrentTransforms": "12",
    "MaxPayloadInMB": "6",
    "BatchStrategy": "MultiRecord",
    "TransformInput": {"DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": "s3_uri"}}},
    "TransformOutput": {"S3OutputPath": "output_path"},
    "TransformResources": {"InstanceType": "ml.m4.xlarge", "InstanceCount": "3"},
}
CREATE_TRANSFORM_PARAMS_INTEGER_FIELDS: dict = {
    "TransformJobName": "job_name",
    "ModelName": "model_name",
    "MaxConcurrentTransforms": 12,
    "MaxPayloadInMB": 6,
    "BatchStrategy": "MultiRecord",
    "TransformInput": {"DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": "s3_uri"}}},
    "TransformOutput": {"S3OutputPath": "output_path"},
    "TransformResources": {"InstanceType": "ml.m4.xlarge", "InstanceCount": 3},
}

CREATE_MODEL_PARAMS: dict = {
    "ModelName": "model_name",
    "PrimaryContainer": {"Image": "test_image", "ModelDataUrl": "output_path"},
    "ExecutionRoleArn": "arn:aws:iam:role/test-role",
}

CONFIG: dict = {"Model": CREATE_MODEL_PARAMS, "Transform": CREATE_TRANSFORM_PARAMS}


class TestSageMakerTransformOperator:
    def setup_method(self):
        self.sagemaker = SageMakerTransformOperator(
            task_id="test_sagemaker_operator",
            aws_conn_id="sagemaker_test_id",
            config=copy.deepcopy(CONFIG),
            wait_for_completion=False,
            check_interval=5,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_integer_fields(self, serialize, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.execute(None)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for (key1, key2, *key3) in EXPECTED_INTEGER_FIELDS:
            if key3:
                (key3,) = key3
                assert self.sagemaker.config[key1][key2][key3] == int(self.sagemaker.config[key1][key2][key3])
            else:
                self.sagemaker.config[key1][key2] == int(self.sagemaker.config[key1][key2])

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute(self, serialize, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(CREATE_MODEL_PARAMS)
        mock_transform.assert_called_once_with(
            CREATE_TRANSFORM_PARAMS_INTEGER_FIELDS,
            wait_for_completion=False,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    def test_execute_with_failure(self, mock_transform, mock_model, mock_client):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_with_check_if_job_exists(self, serialize, mock_transform, mock_client):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker._check_if_transform_job_exists = mock.MagicMock()
        self.sagemaker.execute(None)
        self.sagemaker._check_if_transform_job_exists.assert_called_once()
        mock_transform.assert_called_once_with(
            CREATE_TRANSFORM_PARAMS_INTEGER_FIELDS,
            wait_for_completion=False,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_without_check_if_job_exists(self, serialize, mock_transform, mock_client):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.check_if_job_exists = False
        self.sagemaker._check_if_transform_job_exists = mock.MagicMock()
        self.sagemaker.execute(None)
        self.sagemaker._check_if_transform_job_exists.assert_not_called()
        mock_transform.assert_called_once_with(
            CREATE_TRANSFORM_PARAMS_INTEGER_FIELDS,
            wait_for_completion=False,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "list_transform_jobs")
    def test_check_if_job_exists_increment(self, mock_list_transform_jobs, mock_client):
        self.sagemaker.check_if_job_exists = True
        self.sagemaker.action_if_job_exists = "increment"
        mock_list_transform_jobs.return_value = [{"TransformJobName": "job_name"}]
        self.sagemaker._check_if_transform_job_exists()

        expected_config = copy.deepcopy(CONFIG)
        # Expect to see TransformJobName suffixed with "-2" because we return one existing job
        expected_config["Transform"]["TransformJobName"] = "job_name-2"
        assert self.sagemaker.config == expected_config

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "list_transform_jobs")
    def test_check_if_job_exists_fail(self, mock_list_transform_jobs, mock_client):
        self.sagemaker.check_if_job_exists = True
        self.sagemaker.action_if_job_exists = "fail"
        mock_list_transform_jobs.return_value = [{"TransformJobName": "job_name"}]
        with pytest.raises(AirflowException):
            self.sagemaker._check_if_transform_job_exists()
