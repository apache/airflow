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

from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerBaseOperator,
    SageMakerProcessingOperator,
)
from airflow.providers.amazon.aws.triggers.sagemaker import SageMakerTrigger
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.openlineage.extractors import OperatorLineage

from unit.amazon.aws.utils.test_template_fields import validate_template_fields

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
            "InstanceType": "ml.p3.2xlarge",
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
        for key1, key2, *key3_raw in EXPECTED_INTEGER_FIELDS:
            if key3_raw:
                (key3,) = key3_raw
                assert sagemaker.config[key1][key2][key3] == int(sagemaker.config[key1][key2][key3])
            else:
                assert sagemaker.config[key1][key2] == int(sagemaker.config[key1][key2])

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

    @mock.patch("airflow.providers.amazon.aws.operators.sagemaker.SageMakerProcessingOperator.defer")
    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={"ProcessingJobStatus": "Stopped", "FailureReason": "It stopped"},
    )
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_operator_stopped_before_defer(
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


PROCESSING_CONFIG_WITH_OUTPUT_FILES: dict = {
    **CREATE_PROCESSING_PARAMS,
    "ProcessingOutputConfig": {
        "Outputs": [
            {
                "OutputName": "evaluation",
                "S3Output": {
                    "LocalPath": "/opt/ml/processing/evaluation",
                    "S3UploadMode": "EndOfJob",
                    "S3Uri": "s3://my-bucket/eval-output",
                },
            }
        ],
    },
}

OUTPUT_FILES_TO_XCOM_CONFIG: list = [
    {
        "result_name": "EvaluationReport",
        "output_name": "evaluation",
        "file_name": "evaluation.json",
    }
]


class TestSageMakerProcessingOperatorOutputFiles:
    """Tests for the output_files_to_xcom feature in SageMakerProcessingOperator."""

    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={"ProcessingJobStatus": "Completed", "ProcessingJobName": "job_name"},
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_output_files_read_from_s3(self, _, mock_create, mock_count, mock_describe):
        """Output files are read from S3 and included in XCom return value."""
        evaluation_json = '{"metrics": {"mse": {"value": 5.33}}}'

        mock_s3_client = mock.MagicMock()
        mock_s3_client.get_object.return_value = {
            "Body": mock.MagicMock(read=mock.MagicMock(return_value=evaluation_json.encode("utf-8")))
        }

        with mock.patch.object(SageMakerHook, "get_session") as mock_session:
            mock_session.return_value.client.return_value = mock_s3_client

            operator = SageMakerProcessingOperator(
                task_id="test_task",
                config=PROCESSING_CONFIG_WITH_OUTPUT_FILES,
                output_files_to_xcom=OUTPUT_FILES_TO_XCOM_CONFIG,
            )
            result = operator.execute(context=None)

        assert "OutputFiles" in result
        assert result["OutputFiles"]["EvaluationReport"]["metrics"]["mse"]["value"] == 5.33
        mock_s3_client.get_object.assert_called_once_with(
            Bucket="my-bucket", Key="eval-output/evaluation.json"
        )

    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={"ProcessingJobStatus": "Completed", "ProcessingJobName": "job_name"},
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_no_output_files_returns_unchanged(self, _, mock_create, mock_count, mock_describe):
        """Without output_files_to_xcom kwarg, XCom return has no OutputFiles key."""
        operator = SageMakerProcessingOperator(
            task_id="test_task",
            config=CREATE_PROCESSING_PARAMS,
        )
        result = operator.execute(context=None)

        assert "Processing" in result
        assert "OutputFiles" not in result

    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={"ProcessingJobStatus": "Completed", "ProcessingJobName": "job_name"},
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_output_files_s3_error_does_not_fail_task(self, _, mock_create, mock_count, mock_describe):
        """S3 read failure sets _error key but does not raise."""
        mock_s3_client = mock.MagicMock()
        mock_s3_client.get_object.side_effect = Exception("NoSuchKey")

        with mock.patch.object(SageMakerHook, "get_session") as mock_session:
            mock_session.return_value.client.return_value = mock_s3_client

            operator = SageMakerProcessingOperator(
                task_id="test_task",
                config=PROCESSING_CONFIG_WITH_OUTPUT_FILES,
                output_files_to_xcom=OUTPUT_FILES_TO_XCOM_CONFIG,
            )
            result = operator.execute(context=None)

        assert "OutputFiles" in result
        assert "_error" in result["OutputFiles"]["EvaluationReport"]

    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={"ProcessingJobStatus": "Completed", "ProcessingJobName": "job_name"},
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_output_files_no_matching_output_logs_warning(self, _, mock_create, mock_count, mock_describe):
        """Output file referencing a non-existent output_name is skipped gracefully."""
        bad_output_files = [
            {
                "result_name": "MissingReport",
                "output_name": "nonexistent_output",
                "file_name": "report.json",
            }
        ]
        operator = SageMakerProcessingOperator(
            task_id="test_task",
            config=CREATE_PROCESSING_PARAMS,
            output_files_to_xcom=bad_output_files,
        )
        result = operator.execute(context=None)

        assert "OutputFiles" not in result

    def test_output_files_missing_required_keys_raises(self):
        """An output_files_to_xcom entry missing required keys raises when output files are read."""
        op = SageMakerProcessingOperator(
            task_id="test_task",
            config=CREATE_PROCESSING_PARAMS,
            output_files_to_xcom=[{"result_name": "Report", "output_name": "evaluation"}],  # no file_name
        )
        with mock.patch.object(SageMakerHook, "get_session"):
            with pytest.raises(ValueError, match="missing required keys"):
                op._read_output_files()

    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={"ProcessingJobStatus": "Completed", "ProcessingJobName": "job_name"},
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_output_files_invalid_json_sets_error(self, _, mock_create, mock_count, mock_describe):
        """A non-JSON file sets an 'Invalid JSON' error but does not fail the task."""
        mock_s3_client = mock.MagicMock()
        mock_s3_client.get_object.return_value = {
            "Body": mock.MagicMock(read=mock.MagicMock(return_value=b"col1,col2\n1,2\n"))
        }

        with mock.patch.object(SageMakerHook, "get_session") as mock_session:
            mock_session.return_value.client.return_value = mock_s3_client

            operator = SageMakerProcessingOperator(
                task_id="test_task",
                config=PROCESSING_CONFIG_WITH_OUTPUT_FILES,
                output_files_to_xcom=OUTPUT_FILES_TO_XCOM_CONFIG,
            )
            result = operator.execute(context=None)

        assert "Invalid JSON" in result["OutputFiles"]["EvaluationReport"]["_error"]

    @mock.patch.object(
        SageMakerHook,
        "describe_processing_job",
        return_value={"ProcessingJobStatus": "Completed", "ProcessingJobName": "job_name"},
    )
    @mock.patch.object(SageMakerHook, "count_processing_jobs_by_name", return_value=0)
    @mock.patch.object(
        SageMakerHook,
        "create_processing_job",
        return_value={"ProcessingJobArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}},
    )
    @mock.patch.object(SageMakerBaseOperator, "_check_if_job_exists", return_value=False)
    def test_output_files_not_found_sets_error(self, _, mock_create, mock_count, mock_describe):
        """A missing S3 object (NoSuchKey) sets a 'File not found' error but does not fail the task."""
        mock_s3_client = mock.MagicMock()
        mock_s3_client.get_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchKey", "Message": "The specified key does not exist."}},
            "GetObject",
        )

        with mock.patch.object(SageMakerHook, "get_session") as mock_session:
            mock_session.return_value.client.return_value = mock_s3_client

            operator = SageMakerProcessingOperator(
                task_id="test_task",
                config=PROCESSING_CONFIG_WITH_OUTPUT_FILES,
                output_files_to_xcom=OUTPUT_FILES_TO_XCOM_CONFIG,
            )
            result = operator.execute(context=None)

        assert "File not found" in result["OutputFiles"]["EvaluationReport"]["_error"]

    def test_output_files_skipped_when_not_waiting(self):
        """With wait_for_completion=False and output_files_to_xcom, init raises ValueError."""
        with pytest.raises(ValueError, match="output_files_to_xcom requires wait_for_completion=True"):
            SageMakerProcessingOperator(
                task_id="test_task",
                config=PROCESSING_CONFIG_WITH_OUTPUT_FILES,
                output_files_to_xcom=OUTPUT_FILES_TO_XCOM_CONFIG,
                wait_for_completion=False,
            )
