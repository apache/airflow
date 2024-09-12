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
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerBaseOperator,
    SageMakerProcessingOperator,
)
from airflow.providers.amazon.aws.triggers.sagemaker import SageMakerTrigger
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.openlineage.extractors import OperatorLineage
from tests.providers.amazon.aws.utils.test_template_fields import validate_template_fields

CREATE_PROCESSING_PARAMS: dict = {
    "AppSpecification": {
        "ContainerArguments": ["container_arg"],
        "ContainerEntrypoint": ["container_entrypoint"],
        "ImageUri": "image_uri",
    },
    "Environment": {"key": "value"},
    "ExperimentConfig": {
        "ExperimentName": "experiment_name",
        "TrialComponentDisplayName": "trial_component_display_name",
        "TrialName": "trial_name",
    },
    "ProcessingInputs": [
        {
            "InputName": "analytics_input_name",
            "S3Input": {
                "LocalPath": "local_path",
                "S3CompressionType": "None",
                "S3DataDistributionType": "FullyReplicated",
                "S3DataType": "S3Prefix",
                "S3InputMode": "File",
                "S3Uri": "s3_uri",
            },
        }
    ],
    "ProcessingJobName": "job_name",
    "ProcessingOutputConfig": {
        "KmsKeyId": "kms_key_ID",
        "Outputs": [
            {
                "OutputName": "analytics_output_name",
                "S3Output": {
                    "LocalPath": "local_path",
                    "S3UploadMode": "EndOfJob",
                    "S3Uri": "s3_uri",
                },
            }
        ],
    },
    "ProcessingResources": {
        "ClusterConfig": {
            "InstanceCount": "2",
            "InstanceType": "ml.p2.xlarge",
            "VolumeSizeInGB": "30",
            "VolumeKmsKeyId": "kms_key",
        }
    },
    "RoleArn": "arn:aws:iam::0122345678910:role/SageMakerPowerUser",
    "Tags": [{"key": "value"}],
}

CREATE_PROCESSING_PARAMS_WITH_STOPPING_CONDITION: dict = CREATE_PROCESSING_PARAMS.copy()
CREATE_PROCESSING_PARAMS_WITH_STOPPING_CONDITION.update(StoppingCondition={"MaxRuntimeInSeconds": "3600"})

EXPECTED_INTEGER_FIELDS: list[list[str]] = [
    ["ProcessingResources", "ClusterConfig", "InstanceCount"],
    ["ProcessingResources", "ClusterConfig", "VolumeSizeInGB"],
]
EXPECTED_STOPPING_CONDITION_INTEGER_FIELDS: list[list[str]] = [["StoppingCondition", "MaxRuntimeInSeconds"]]


class TestSageMakerProcessingOperator:
    def setup_method(self):
        self.processing_config_kwargs = dict(
            task_id="test_sagemaker_operator",
            wait_for_completion=False,
            check_interval=5,
        )

        self.defer_processing_config_kwargs = dict(
            task_id="test_sagemaker_operator", wait_for_completion=True, check_interval=5, deferrable=True
        )

    @mock.patch.object(SageMakerHook, "describe_processing_job")
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_integer_fields_without_stopping_condition(self, _, __, ___, mock_desc):
        mock_desc.side_effect = [ClientError({"Error": {"Code": "ValidationException"}}, "op"), None]
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=CREATE_PROCESSING_PARAMS
        )

        sagemaker.execute(None)

        assert sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for key1, key2, key3 in EXPECTED_INTEGER_FIELDS:
            assert sagemaker.config[key1][key2][key3] == int(sagemaker.config[key1][key2][key3])

    @mock.patch.object(SageMakerHook, "describe_processing_job")
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_integer_fields_with_stopping_condition(self, _, __, ___, mock_desc):
        mock_desc.side_effect = [ClientError({"Error": {"Code": "ValidationException"}}, "op"), None]
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=CREATE_PROCESSING_PARAMS_WITH_STOPPING_CONDITION
        )
        sagemaker.execute(None)
        assert (
            sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS + EXPECTED_STOPPING_CONDITION_INTEGER_FIELDS
        )
        for key1, key2, *key3 in EXPECTED_INTEGER_FIELDS:
            if key3:
                (key3,) = key3
                assert sagemaker.config[key1][key2][key3] == int(sagemaker.config[key1][key2][key3])
            else:
                sagemaker.config[key1][key2] == int(sagemaker.config[key1][key2])

    @mock.patch.object(SageMakerHook, "describe_processing_job")
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute(self, _, mock_processing, __, mock_desc):
        mock_desc.side_effect = [ClientError({"Error": {"Code": "ValidationException"}}, "op"), None]
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=CREATE_PROCESSING_PARAMS
        )
        sagemaker.execute(None)
        mock_processing.assert_called_once_with(
            CREATE_PROCESSING_PARAMS, wait_for_completion=False, check_interval=5, max_ingestion_time=None
        )

    @mock.patch.object(SageMakerHook, "describe_processing_job")
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_with_stopping_condition(self, _, mock_processing, __, mock_desc):
        mock_desc.side_effect = [ClientError({"Error": {"Code": "ValidationException"}}, "op"), None]
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=CREATE_PROCESSING_PARAMS_WITH_STOPPING_CONDITION
        )
        sagemaker.execute(None)
        mock_processing.assert_called_once_with(
            CREATE_PROCESSING_PARAMS_WITH_STOPPING_CONDITION,
            wait_for_completion=False,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "describe_processing_job")
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 404}},
    )
    def test_execute_with_failure(self, _, mock_desc):
        mock_desc.side_effect = [ClientError({"Error": {"Code": "ValidationException"}}, "op"), None]
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=CREATE_PROCESSING_PARAMS
        )
        with pytest.raises(AirflowException):
            sagemaker.execute(None)

    @mock.patch.object(SageMakerHook, "describe_processing_job")
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=1)
    @mock.patch.object(
        SageMakerHook, "create_processing_job", return_value={"ResponseMetadata": {"HTTPStatusCode": 200}}
    )
    def test_execute_with_existing_job_timestamp(self, mock_create_processing_job, _, mock_desc):
        mock_desc.side_effect = [None, ClientError({"Error": {"Code": "ValidationException"}}, "op"), None]
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=CREATE_PROCESSING_PARAMS
        )
        sagemaker.action_if_job_exists = "timestamp"
        sagemaker.execute(None)

        expected_config = CREATE_PROCESSING_PARAMS.copy()
        # Expect to see ProcessingJobName suffixed because we return one existing job
        expected_config["ProcessingJobName"].startswith("job_name-")
        mock_create_processing_job.assert_called_once_with(
            expected_config,
            wait_for_completion=False,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "describe_processing_job")
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=1)
    @mock.patch.object(
        SageMakerHook, "create_processing_job", return_value={"ResponseMetadata": {"HTTPStatusCode": 200}}
    )
    def test_execute_with_existing_job_fail(self, _, __, ___):
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs, config=CREATE_PROCESSING_PARAMS
        )
        sagemaker.action_if_job_exists = "fail"
        with pytest.raises(AirflowException):
            sagemaker.execute(None)

    @mock.patch.object(SageMakerHook, "describe_processing_job")
    def test_action_if_job_exists_validation(self, mock_client):
        with pytest.raises(AirflowException):
            SageMakerProcessingOperator(
                **self.processing_config_kwargs,
                config=CREATE_PROCESSING_PARAMS,
                action_if_job_exists="not_fail_or_increment",
            )

    @mock.patch.object(
        SageMakerHook, "describe_processing_job", return_value={"ProcessingJobStatus": "InProgress"}
    )
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={
            "ProcessingJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        },
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_operator_defer(self, mock_job_exists, mock_processing, mock_describe):
        sagemaker_operator = SageMakerProcessingOperator(
            **self.defer_processing_config_kwargs,
            config=CREATE_PROCESSING_PARAMS,
        )
        sagemaker_operator.wait_for_completion = True
        with pytest.raises(TaskDeferred) as exc:
            sagemaker_operator.execute(context=None)
        assert isinstance(exc.value.trigger, SageMakerTrigger), "Trigger is not a SagemakerTrigger"

    @mock.patch("airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator.defer")
    @mock.patch.object(
        SageMakerHook, "describe_processing_job", return_value={"ProcessingJobStatus": "Completed"}
    )
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_operator_complete_before_defer(
        self, mock_job_exists, mock_processing, mock_describe, mock_defer
    ):
        sagemaker_operator = SageMakerProcessingOperator(
            **self.defer_processing_config_kwargs,
            config=CREATE_PROCESSING_PARAMS,
        )
        sagemaker_operator.execute(context=None)
        assert not mock_defer.called

    @mock.patch("airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator.defer")
    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={"ProcessingJobStatus": "Failed", "FailureReason": "It failed"},
    )
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_operator_failed_before_defer(
        self,
        mock_job_exists,
        mock_processing,
        mock_describe,
        mock_defer,
    ):
        sagemaker_operator = SageMakerProcessingOperator(
            **self.defer_processing_config_kwargs,
            config=CREATE_PROCESSING_PARAMS,
        )
        with pytest.raises(AirflowException):
            sagemaker_operator.execute(context=None)

        assert not mock_defer.called

    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={
            "ProcessingInputs": [{"S3Input": {"S3Uri": "s3://input-bucket/input-path"}}],
            "ProcessingOutputConfig": {
                "Outputs": [{"S3Output": {"S3Uri": "s3://output-bucket/output-path"}}]
            },
        },
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_operator_openlineage_data(self, check_job_exists, mock_processing, _, mock_desc):
        sagemaker = SageMakerProcessingOperator(
            **self.processing_config_kwargs,
            config=CREATE_PROCESSING_PARAMS,
            deferrable=True,
        )
        sagemaker.execute(context=None)
        assert sagemaker.get_openlineage_facets_on_complete(None) == OperatorLineage(
            inputs=[Dataset(namespace="s3://input-bucket", name="input-path")],
            outputs=[Dataset(namespace="s3://output-bucket", name="output-path")],
        )

    def test_template_fields(self):
        operator = SageMakerProcessingOperator(
            **self.processing_config_kwargs,
            config=CREATE_PROCESSING_PARAMS,
        )
        validate_template_fields(operator)
