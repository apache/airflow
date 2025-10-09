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

from datetime import datetime
from unittest import mock

import pytest
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.sagemaker import LogState, SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerBaseOperator, SageMakerTrainingOperator
from airflow.providers.amazon.aws.triggers.sagemaker import (
    SageMakerTrigger,
)
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.openlineage.extractors import OperatorLineage

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

EXPECTED_INTEGER_FIELDS: list[list[str]] = [
    ["ResourceConfig", "InstanceCount"],
    ["ResourceConfig", "VolumeSizeInGB"],
    ["StoppingCondition", "MaxRuntimeInSeconds"],
]

CREATE_TRAINING_PARAMS = {
    "AlgorithmSpecification": {"TrainingImage": "image_name", "TrainingInputMode": "File"},
    "RoleArn": "arn:aws:iam:role/test-role",
    "OutputDataConfig": {"S3OutputPath": "output_path"},
    "ResourceConfig": {"InstanceCount": "2", "InstanceType": "ml.c6g.8xlarge", "VolumeSizeInGB": "50"},
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

    @mock.patch.object(SageMakerHook, "describe_training_job")
    @mock.patch.object(SageMakerHook, "create_training_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_integer_fields(self, _, mock_training, mock_desc):
        mock_desc.side_effect = [ClientError({"Error": {"Code": "ValidationException"}}, "op"), None]
        mock_training.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.execute(None)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for key1, key2 in EXPECTED_INTEGER_FIELDS:
            assert self.sagemaker.config[key1][key2] == int(self.sagemaker.config[key1][key2])

    @mock.patch.object(SageMakerHook, "describe_training_job")
    @mock.patch.object(SageMakerHook, "create_training_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_without_check_if_job_exists(self, _, mock_training, mock_desc):
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

        a = []
        a.sort()

    @mock.patch.object(SageMakerHook, "describe_training_job")
    @mock.patch.object(SageMakerHook, "create_training_job")
    def test_execute_with_failure(self, mock_training, mock_desc):
        mock_desc.side_effect = [ClientError({"Error": {"Code": "ValidationException"}}, "op")]
        mock_training.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)

    @mock.patch("airflow.providers.amazon.aws.operators.sagemaker.SageMakerTrainingOperator.defer")
    @mock.patch.object(
        SageMakerHook,
        "describe_training_job_with_log",
        return_value=(
            LogState.JOB_COMPLETE,
            {
                "TrainingJobStatus": "Completed",
                "ResourceConfig": {"InstanceCount": 1},
                "TrainingEndTime": datetime(2023, 5, 15),
                "TrainingStartTime": datetime(2023, 5, 16),
            },
            50,
        ),
    )
    @mock.patch.object(
        SageMakerHook,
        "describe_training_job",
        return_value={
            "TrainingJobStatus": "Completed",
            "ResourceConfig": {"InstanceCount": 1},
            "TrainingEndTime": datetime(2023, 5, 15),
            "TrainingStartTime": datetime(2023, 5, 16),
        },
    )
    @mock.patch.object(SageMakerHook, "create_training_job")
    def test_operator_complete_before_defer(
        self,
        mock_training,
        mock_describe_training_job,
        mock_describe_training_job_with_log,
        mock_defer,
    ):
        mock_training.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.deferrable = True
        self.sagemaker.wait_for_completion = True
        self.sagemaker.check_if_job_exists = False

        self.sagemaker.execute(context=None)
        assert not mock_defer.called

    @mock.patch.object(
        SageMakerHook,
        "describe_training_job_with_log",
        return_value=(
            LogState.WAIT_IN_PROGRESS,
            {
                "TrainingJobStatus": "Training",
                "ResourceConfig": {"InstanceCount": 1},
                "TrainingEndTime": datetime(2023, 5, 15),
                "TrainingStartTime": datetime(2023, 5, 16),
            },
            50,
        ),
    )
    @mock.patch.object(
        SageMakerHook,
        "describe_training_job",
        return_value={
            "TrainingJobStatus": "Training",
            "ResourceConfig": {"InstanceCount": 1},
            "TrainingEndTime": datetime(2023, 5, 15),
            "TrainingStartTime": datetime(2023, 5, 16),
        },
    )
    @mock.patch.object(SageMakerHook, "create_training_job")
    def test_operator_defer(
        self,
        mock_training,
        mock_describe_training_job,
        mock_describe_training_job_with_log,
    ):
        mock_training.return_value = {
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.deferrable = True
        self.sagemaker.wait_for_completion = True
        self.sagemaker.check_if_job_exists = False
        self.sagemaker.print_log = False

        with pytest.raises(TaskDeferred) as exc:
            self.sagemaker.execute(context=None)
        assert isinstance(exc.value.trigger, SageMakerTrigger), "Trigger is not a SagemakerTrigger"

    @mock.patch.object(
        SageMakerHook,
        "describe_training_job",
        return_value={
            "InputDataConfig": [
                {
                    "DataSource": {"S3DataSource": {"S3Uri": "s3://input-bucket/input-path"}},
                }
            ],
            "ModelArtifacts": {"S3ModelArtifacts": "s3://model-bucket/model-path"},
        },
    )
    @mock.patch.object(
        SageMakerHook,
        "create_training_job",
        return_value={
            "TrainingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        },
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_execute_openlineage_data(self, mock_exists, mock_training, mock_desc):
        self.sagemaker.execute(None)
        assert self.sagemaker.get_openlineage_facets_on_complete(None) == OperatorLineage(
            inputs=[Dataset(namespace="s3://input-bucket", name="input-path")],
            outputs=[Dataset(namespace="s3://model-bucket", name="model-path")],
        )

    def test_template_fields(self):
        validate_template_fields(self.sagemaker)
