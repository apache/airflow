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
from airflow.providers.amazon.aws.operators import sagemaker
from airflow.providers.amazon.aws.operators.sagemaker import SageMakerEndpointConfigOperator

from providers.tests.amazon.aws.utils.test_template_fields import validate_template_fields

CREATE_ENDPOINT_CONFIG_PARAMS: dict = {
    "EndpointConfigName": "config_name",
    "ProductionVariants": [
        {
            "VariantName": "AllTraffic",
            "ModelName": "model_name",
            "InitialInstanceCount": "1",
            "InstanceType": "ml.c4.xlarge",
        }
    ],
}

EXPECTED_INTEGER_FIELDS: list[list[str]] = [["ProductionVariants", "InitialInstanceCount"]]


class TestSageMakerEndpointConfigOperator:
    def setup_method(self):
        self.sagemaker = SageMakerEndpointConfigOperator(
            task_id="test_sagemaker_operator",
            config=CREATE_ENDPOINT_CONFIG_PARAMS,
        )

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_endpoint_config")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_integer_fields(self, serialize, mock_model, mock_client):
        mock_model.return_value = {
            "EndpointConfigArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.execute(None)
        assert self.sagemaker.integer_fields == EXPECTED_INTEGER_FIELDS
        for variant in self.sagemaker.config["ProductionVariants"]:
            assert variant["InitialInstanceCount"] == int(variant["InitialInstanceCount"])

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_endpoint_config")
    @mock.patch.object(sagemaker, "serialize", return_value="")
    def test_execute(self, serialize, mock_model, mock_client):
        mock_model.return_value = {
            "EndpointConfigArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        self.sagemaker.execute(None)
        mock_model.assert_called_once_with(CREATE_ENDPOINT_CONFIG_PARAMS)

    @mock.patch.object(SageMakerHook, "get_conn")
    @mock.patch.object(SageMakerHook, "create_model")
    def test_execute_with_failure(self, mock_model, mock_client):
        mock_model.return_value = {
            "EndpointConfigArn": "test_arn",
            "ResponseMetadata": {"HTTPStatusCode": 200},
        }
        with pytest.raises(AirflowException):
            self.sagemaker.execute(None)

    def test_template_fields(self):
        validate_template_fields(self.sagemaker)
