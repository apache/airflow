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

import json
from typing import TYPE_CHECKING, Generator
from unittest import mock

import pytest
from moto import mock_aws

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook, BedrockRuntimeHook
from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCustomizeModelOperator,
    BedrockInvokeModelOperator,
)

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection


class TestBedrockInvokeModelOperator:
    def setup_method(self):
        self.model_id = "meta.llama2-13b-chat-v1"
        self.prompt = "A very important question."
        self.generated_response = "An important answer."

    @pytest.fixture
    def mock_runtime_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockRuntimeHook, "conn") as _conn:
            _conn.invoke_model.return_value["body"].read.return_value = json.dumps(
                {
                    "generation": self.generated_response,
                    "prompt_token_count": len(self.prompt),
                    "generation_token_count": len(self.generated_response),
                    "stop_reason": "stop",
                }
            )
            yield _conn

    @pytest.fixture
    def runtime_hook(self) -> Generator[BedrockRuntimeHook, None, None]:
        with mock_aws():
            yield BedrockRuntimeHook(aws_conn_id="aws_default")

    def test_invoke_model_prompt_good_combinations(self, mock_runtime_conn):
        operator = BedrockInvokeModelOperator(
            task_id="test_task", model_id=self.model_id, input_data={"input_data": {"prompt": self.prompt}}
        )

        response = operator.execute({})

        assert response["generation"] == self.generated_response


class TestBedrockCustomizeModelOperator:
    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockHook, "conn") as _conn:
            _conn.create_model_customization_job.return_value = {
                "ResponseMetadata": {"HTTPStatusCode": 201},
                "jobArn": self.custom_job_arn,
            }
            _conn.get_model_customization_job.return_value = {
                "jobName": self.customize_model_job_name,
                "status": "InProgress",
            }
            yield _conn

    @pytest.fixture
    def bedrock_hook(self) -> Generator[BedrockHook, None, None]:
        with mock_aws():
            hook = BedrockHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.custom_job_arn = "valid_arn"
        self.customize_model_job_name = "testModelJob"

        self.operator = BedrockCustomizeModelOperator(
            task_id="test_task",
            job_name=self.customize_model_job_name,
            custom_model_name="testModelName",
            role_arn="valid_arn",
            base_model_id="base_model_id",
            hyperparameters={
                "epochCount": "1",
                "batchSize": "1",
                "learningRate": ".0005",
                "learningRateWarmupSteps": "0",
            },
            training_data_uri="s3://uri",
            output_data_uri="s3://uri/output",
        )
        self.operator.defer = mock.MagicMock()

    @pytest.mark.parametrize(
        "wait_for_completion, deferrable",
        [
            pytest.param(False, False, id="no_wait"),
            pytest.param(True, False, id="wait"),
            pytest.param(False, True, id="defer"),
        ],
    )
    @mock.patch.object(BedrockHook, "get_waiter")
    def test_customize_model_wait_combinations(
        self, _, wait_for_completion, deferrable, mock_conn, bedrock_hook
    ):
        self.operator.wait_for_completion = wait_for_completion
        self.operator.deferrable = deferrable

        response = self.operator.execute({})

        assert response == self.custom_job_arn
        assert bedrock_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    @pytest.mark.parametrize(
        "action_if_job_exists, succeeds",
        [
            pytest.param("timestamp", True, id="timestamp"),
            pytest.param("fail", True, id="fail"),
            pytest.param("call me maybe", False, id="invalid"),
        ],
    )
    def test_customize_model_validate_action_if_job_exists(self, action_if_job_exists, succeeds):
        exception = None
        operator = BedrockCustomizeModelOperator(
            task_id="test_task",
            job_name=self.customize_model_job_name,
            custom_model_name="testModelName",
            role_arn="valid_arn",
            base_model_id="base_model_id",
            hyperparameters={
                "epochCount": "1",
                "batchSize": "1",
                "learningRate": ".0005",
                "learningRateWarmupSteps": "0",
            },
            training_data_uri="s3://uri",
            output_data_uri="s3://uri/output",
            action_if_job_exists=action_if_job_exists,
        )

        try:
            operator._validate_action_if_job_exists()
        except Exception as e:
            exception = e

        if succeeds:
            assert operator.action_if_job_exists == action_if_job_exists
            assert exception is None
        else:
            assert isinstance(exception, AirflowException)
