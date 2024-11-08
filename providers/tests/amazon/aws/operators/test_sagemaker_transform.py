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
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException, TaskDeferred
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerTransformOperator
from airflow.providers.amazon.aws.triggers.sagemaker import SageMakerTrigger
from airflow.providers.common.compat.openlineage.facet import Dataset
from airflow.providers.openlineage.extractors import OperatorLineage

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

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
            check_if_model_exists=False,
        )

    @mock.patch.object(SageMakerHook, "describe_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "describe_model")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_integer_fields(self, _, mock_create_transform, __, ___, mock_desc):
        mock_desc.side_effect = [
            ClientError({"Error": {"Code": "ValidationException"}}, "op"),
            {"ModelName": "model_name"},
        ]
        mock_create_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.execute(None)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for key1, key2, *key3 in EXPECTED_INTEGER_FIELDS:
            if key3:
                (key3,) = key3
                assert self.sagemaker.config[key1][key2][key3] == int(self.sagemaker.config[key1][key2][key3])
            else:
                self.sagemaker.config[key1][key2] == int(self.sagemaker.config[key1][key2])

    @mock.patch.object(SageMakerHook, "describe_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "describe_model")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute(self, _, mock_transform, __, mock_model, mock_desc):
        mock_desc.side_effect = [
            ClientError({"Error": {"Code": "ValidationException"}}, "op"),
            {"ModelName": "model_name"},
        ]
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

    @mock.patch.object(SageMakerHook, "describe_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    def test_execute_with_failure(self, mock_transform, _, mock_desc):
        mock_desc.side_effect = [
            ClientError({"Error": {"Code": "ValidationException"}}, "op"),
            None,
        ]
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 404},
        }
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)

    @mock.patch.object(SageMakerHook, "describe_transform_job")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "describe_model")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_with_check_if_job_exists(self, _, __, ___, mock_transform, mock_desc):
        mock_desc.side_effect = [
            ClientError({"Error": {"Code": "ValidationException"}}, "op"),
            {"ModelName": "model_name"},
        ]
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.execute(None)
        mock_transform.assert_called_once_with(
            CREATE_TRANSFORM_PARAMS_INTEGER_FIELDS,
            wait_for_completion=False,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch.object(SageMakerHook, "describe_transform_job")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "describe_model")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_without_check_if_job_exists(self, _, __, ___, mock_transform, ____):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.check_if_job_exists = False
        self.sagemaker.execute(None)
        mock_transform.assert_called_once_with(
            CREATE_TRANSFORM_PARAMS_INTEGER_FIELDS,
            wait_for_completion=False,
            check_interval=5,
            max_ingestion_time=None,
        )

    @mock.patch(  # since it is divided by 1000000, the added timestamp should be 2.
        "airflow.providers.amazon.aws.operators.sagemaker.time.time_ns", return_value=2000000
    )
    @mock.patch.object(SageMakerHook, "describe_transform_job", return_value={"ModelName": "model_name-2"})
    @mock.patch.object(
        SageMakerHook,
        "create_transform_job",
        return_value={
            "ResponseMetadata": {"HTTPStatusCode": 200},
        },
    )
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(
        SageMakerHook,
        "describe_model",
        side_effect=[
            None,
            ClientError({"Error": {"Code": "ValidationException"}}, "op"),
            "model_name-2",
        ],
    )
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_when_model_already_exists_it_should_add_timestamp_to_model_name(
        self, _, mock_describe_model, mock_create_model, __, ___, timestamp_mock
    ):
        self.sagemaker.check_if_job_exists = False
        self.sagemaker.check_if_model_exists = True
        model_config = {"ModelName": "model_name"}
        self.sagemaker.config["Model"] = model_config

        self.sagemaker.execute(None)

        mock_describe_model.assert_has_calls(
            [mock.call("model_name"), mock.call("model_name-2"), mock.call("model_name-2")]
        )
        mock_create_model.assert_called_once_with({"ModelName": "model_name-2"})

    @mock.patch.object(SageMakerHook, "describe_transform_job")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "describe_model")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_when_model_already_exists_it_should_raise_airflow_exception(
        self, _, mock_describe_model, mock_create_model, __, ___
    ):
        mock_describe_model.side_effect = [None]
        self.sagemaker.check_if_job_exists = False
        self.sagemaker.check_if_model_exists = True
        self.sagemaker.action_if_model_exists = "fail"
        model_config = {"ModelName": "model_name"}
        self.sagemaker.config["Model"] = model_config

        with pytest.raises(AirflowException) as context:
            self.sagemaker.execute(None)

        assert str(context.value) == "A SageMaker model with name model_name already exists."
        mock_describe_model.assert_called_once_with("model_name")
        mock_create_model.assert_not_called()

    @mock.patch.object(SageMakerHook, "describe_transform_job", return_value={"ModelName": "model_name"})
    @mock.patch.object(
        SageMakerHook,
        "create_transform_job",
        return_value={
            "ResponseMetadata": {"HTTPStatusCode": 200},
        },
    )
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "describe_model")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute_without_check_if_model_exists(self, _, mock_describe_model, mock_create_model, __, ___):
        self.sagemaker.check_if_job_exists = False
        self.sagemaker.check_if_model_exists = False
        model_config = {"ModelName": "model_name"}
        self.sagemaker.config["Model"] = model_config
        self.sagemaker._get_unique_model_name = mock.Mock()

        self.sagemaker.execute(None)

        mock_create_model.assert_called_once_with(model_config)
        mock_describe_model.assert_called_once_with("model_name")
        self.sagemaker._get_unique_model_name.assert_not_called()

    @mock.patch.object(SageMakerTransformOperator, "_get_unique_name")
    def test_get_unique_model_name_calls_get_unique_name_correctly(self, get_unique_name_mock):
        def describe_func():
            pass

        self.sagemaker._get_unique_model_name("model_name", True, describe_func)

        get_unique_name_mock.assert_called_once_with(
            "model_name",
            True,
            describe_func,
            self.sagemaker._check_if_model_exists,
            "model",
        )

    @mock.patch.object(SageMakerTransformOperator, "_check_if_resource_exists")
    def test_check_if_model_exists_calls_check_if_resource_exists_correctly(self, check_resource_exists_mock):
        def describe_func():
            pass

        self.sagemaker._check_if_model_exists("model_name", describe_func)

        check_resource_exists_mock.assert_called_once_with("model_name", "model", describe_func)

    @mock.patch("airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator.defer")
    @mock.patch.object(
        SageMakerHook,
        "describe_transform_job",
        return_value={"TransformJobStatus": "Failed", "FailureReason": "it failed"},
    )
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    def test_operator_failed_before_defer(self, _, mock_transform, mock_describe_transform_job, mock_defer):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.deferrable = True
        self.sagemaker.wait_for_completion = True
        self.sagemaker.check_if_job_exists = False

        with pytest.raises(AirflowException):
            self.sagemaker.execute(context=None)
        assert not mock_defer.called

    @mock.patch("airflow.providers.amazon.aws.operators.sagemaker.SageMakerTransformOperator.defer")
    @mock.patch.object(SageMakerHook, "describe_model")
    @mock.patch.object(
        SageMakerHook, "describe_transform_job", return_value={"TransformJobStatus": "Completed"}
    )
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    def test_operator_complete_before_defer(
        self, _, mock_transform, mock_describe_transform_job, mock_describe_model, mock_defer
    ):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        mock_describe_model.return_value = {"PrimaryContainer": {"ModelPackageName": "package-name"}}
        self.sagemaker.deferrable = True
        self.sagemaker.wait_for_completion = True
        self.sagemaker.check_if_job_exists = False

        self.sagemaker.execute(context=None)
        assert not mock_defer.called

    @mock.patch.object(
        SageMakerHook, "describe_transform_job", return_value={"TransformJobStatus": "InProgress"}
    )
    @mock.patch.object(SageMakerHook, "create_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    def test_operator_defer(self, _, mock_transform, mock_describe_transform_job):
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.deferrable = True
        self.sagemaker.wait_for_completion = True
        self.sagemaker.check_if_job_exists = False

        with pytest.raises(TaskDeferred) as exc:
            self.sagemaker.execute(context=None)

        assert isinstance(exc.value.trigger, SageMakerTrigger), "Trigger is not a SagemakerTrigger"

    @mock.patch.object(SageMakerHook, "describe_transform_job")
    @mock.patch.object(SageMakerHook, "create_model")
    @mock.patch.object(SageMakerHook, "describe_model")
    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_transform_job")
    def test_operator_lineage_data(self, mock_transform, mock_conn, mock_model, _, mock_desc):
        self.sagemaker.check_if_job_exists = False
        mock_conn.return_value.describe_model_package.return_value = {
            "InferenceSpecification": {"Containers": [{"ModelDataUrl": "s3://model-bucket/model-path"}]},
        }
        mock_model.return_value = {"PrimaryContainer": {"ModelPackageName": "package-name"}}
        mock_desc.return_value = {
            "TransformInput": {"DataSource": {"S3DataSource": {"S3Uri": "s3://input-bucket/input-path"}}},
            "TransformOutput": {"S3OutputPath": "s3://output-bucket/output-path"},
            "ModelName": "model_name",
        }
        mock_transform.return_value = {
            "TransformJobArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.execute(None)
        assert self.sagemaker.get_openlineage_facets_on_complete(None) == OperatorLineage(
            inputs=[
                Dataset(namespace="s3://input-bucket", name="input-path"),
                Dataset(namespace="s3://model-bucket", name="model-path"),
            ],
            outputs=[Dataset(namespace="s3://output-bucket", name="output-path")],
        )

    def test_template_fields(self):
        validate_template_fields(self.sagemaker)
