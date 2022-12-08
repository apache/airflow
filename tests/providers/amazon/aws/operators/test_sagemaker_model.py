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

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.operators.sagemaker import (
    SageMakerDeleteModelOperator,
    SageMakerModelOperator,
)

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
    def setup_method(self):
        self.sagemaker = SageMakerModelOperator(task_id="test_sagemaker_operator", config=CREATE_MODEL_PARAMS)

    @mock.patch.object(SageMakerHook, "describe_model", return_value="")
    @mock.patch.object(SageMakerHook, "create_model")
    def test_execute(self, mock_create_model, _):
        mock_create_model.return_value = {"ModelArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 200}}
        self.sagemaker.execute(None)
        mock_create_model.assert_called_once_with(CREATE_MODEL_PARAMS)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS

    @mock.patch.object(SageMakerHook, "create_model")
    def test_execute_with_failure(self, mock_create_model):
        mock_create_model.return_value = {"ModelArn": "test_arn", "ResponseMetadata": {"HTTPStatusCode": 404}}
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)


class TestSageMakerDeleteModelOperator:
    @mock.patch.object(SageMakerHook, "delete_model")
    def test_execute(self, delete_model):
        op = SageMakerDeleteModelOperator(
            task_id="test_sagemaker_operator", config={"ModelName": "model_name"}
        )
        op.execute(None)
        delete_model.assert_called_once_with(model_name="model_name")
