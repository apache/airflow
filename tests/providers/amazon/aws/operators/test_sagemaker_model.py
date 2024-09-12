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

from unittest import mock
from unittest.mock import patch

import pytest
from botocore.exceptions import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker import (
    ApprovalStatus,
    SageMakerDeleteModelOperator,
    SageMakerModelOperator,
    SageMakerRegisterModelVersionOperator,
)
from tests.providers.amazon.aws.utils.test_template_fields import validate_template_fields

CREATE_MODEL_PARAMS: dict = {
    "ModelName": "model_name",
    "PrimaryContainer": {
        "Image": "image_name",
        "ModelDataUrl": "output_path",
    },
    "ExecutionRoleArn": "arn:aws:iam:role/test-role",
}

EXPECTED_INTEGER_FIELDS: list[list[str]] = []


class TestSageMakerModelOperator:
    @patch.object(SageMakerHook, "describe_model", return_value="")
    @patch.object(SageMakerHook, "create_model")
    def test_execute(self, mock_create_model, _):
        sagemaker = SageMakerModelOperator(task_id="test_sagemaker_operator", config=CREATE_MODEL_PARAMS)
        mock_create_model.return_value = {"ModelArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}}

        sagemaker.execute(None)

        mock_create_model.assert_called_once_with(CREATE_MODEL_PARAMS)
        assert sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS

    @patch.object(SageMakerHook, "create_model")
    def test_execute_with_failure(self, mock_create_model):
        sagemaker = SageMakerModelOperator(task_id="test_sagemaker_operator", config=CREATE_MODEL_PARAMS)
        mock_create_model.return_value = {"ModelArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 404}}

        with pytest.raises(AirflowException):
            sagemaker.execute(None)


class TestSageMakerDeleteModelOperator:
    @patch.object(SageMakerHook, "delete_model")
    def test_execute(self, delete_model):
        op = SageMakerDeleteModelOperator(
            task_id="test_sagemaker_operator", config={"ModelName": "model_name"}
        )
        op.execute(None)
        delete_model.assert_called_once_with(model_name="model_name")

    def test_template_fields(self):
        op = SageMakerDeleteModelOperator(
            task_id="test_sagemaker_operator", config={"ModelName": "model_name"}
        )
        validate_template_fields(op)


class TestSageMakerRegisterModelVersionOperator:
    @patch.object(SageMakerHook, "create_model_package_group")
    @patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.conn", new_callable=mock.PropertyMock)
    def test_execute(self, conn_mock, create_group_mock):
        image = "257758044811.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost:1.2-1"
        model = "s3://your-bucket-name/model.tar.gz"
        group = "group-name"
        op = SageMakerRegisterModelVersionOperator(
            task_id="test",
            image_uri=image,
            model_url=model,
            package_group_name=group,
            model_approval=ApprovalStatus.APPROVED,
        )

        op.execute(None)

        create_group_mock.assert_called_once_with("group-name", "")
        conn_mock().create_model_package.assert_called_once()
        args_dict = conn_mock().create_model_package.call_args.kwargs
        assert args_dict["InferenceSpecification"]["Containers"][0]["Image"] == image
        assert args_dict["InferenceSpecification"]["Containers"][0]["ModelDataUrl"] == model
        assert args_dict["ModelPackageGroupName"] == group
        assert args_dict["ModelApprovalStatus"] == "Approved"

    @pytest.mark.parametrize("group_created", [True, False])
    @patch.object(SageMakerHook, "create_model_package_group")
    @patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.conn", new_callable=mock.PropertyMock)
    def test_group_deleted_if_error_when_adding_model(self, conn_mock, create_group_mock, group_created):
        group = "group-name"
        op = SageMakerRegisterModelVersionOperator(
            task_id="test",
            image_uri="257758044811.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost:1.2-1",
            model_url="s3://your-bucket-name/model.tar.gz",
            package_group_name=group,
            model_approval=ApprovalStatus.APPROVED,
        )
        create_group_mock.return_value = group_created
        conn_mock().create_model_package.side_effect = ClientError(
            error_response={"Error": {"Code": "ohno"}}, operation_name="empty"
        )

        with pytest.raises(ClientError):
            op.execute(None)

        if group_created:
            # delete group if it was created and there was an error in the second step (create model package)
            conn_mock().delete_model_package_group.assert_called_once_with(ModelPackageGroupName=group)
        else:
            # if the group already existed, we don't want to delete it in case of error on second step
            conn_mock().delete_model_package_group.assert_not_called()

    @patch.object(SageMakerHook, "create_model_package_group")
    @patch("airflow.providers.amazon.aws.hooks.sagemaker.SageMakerHook.conn", new_callable=mock.PropertyMock)
    def test_can_override_parameters_using_extras(self, conn_mock, _):
        response_type = ["test/test"]
        op = SageMakerRegisterModelVersionOperator(
            task_id="test",
            image_uri="257758044811.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost:1.2-1",
            model_url="s3://your-bucket-name/model.tar.gz",
            package_group_name="group-name",
            extras={"InferenceSpecification": {"SupportedResponseMIMETypes": response_type}},
        )

        op.execute(None)

        conn_mock().create_model_package.assert_called_once()
        args_dict = conn_mock().create_model_package.call_args.kwargs
        assert args_dict["InferenceSpecification"]["SupportedResponseMIMETypes"] == response_type

    def test_template_fields(self):
        response_type = ["test/test"]
        op = SageMakerRegisterModelVersionOperator(
            task_id="test",
            image_uri="257758044811.dkr.ecr.us-east-2.amazonaws.com/sagemaker-xgboost:1.2-1",
            model_url="s3://your-bucket-name/model.tar.gz",
            package_group_name="group-name",
            extras={"InferenceSpecification": {"SupportedResponseMIMETypes": response_type}},
        )
        validate_template_fields(op)
