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
from botocore.exceptions import ClientError
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook, BedrockRuntimeHook
from airflow.providers.amazon.aws.operators.bedrock import (
    BedrockCustomizeModelOperator,
    BedrockInvokeModelOperator,
)

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import BaseAwsConnection


class TestBedrockInvokeModelOperator:
    MODEL_ID = "meta.llama2-13b-chat-v1"
    TEST_PROMPT = "A very important question."
    GENERATED_RESPONSE = "An important answer."

    @pytest.fixture
    def mock_runtime_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockRuntimeHook, "conn") as _conn:
            _conn.invoke_model.return_value["body"].read.return_value = json.dumps(
                {
                    "generation": self.GENERATED_RESPONSE,
                    "prompt_token_count": len(self.TEST_PROMPT),
                    "generation_token_count": len(self.GENERATED_RESPONSE),
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
            task_id="test_task",
            model_id=self.MODEL_ID,
            input_data={"input_data": {"prompt": self.TEST_PROMPT}},
        )

        response = operator.execute({})

        assert response["generation"] == self.GENERATED_RESPONSE


class TestBedrockCustomizeModelOperator:
    CUSTOMIZE_JOB_ARN = "valid_arn"
    CUSTOMIZE_JOB_NAME = "testModelJob"

    @pytest.fixture
    def mock_conn(self) -> Generator[BaseAwsConnection, None, None]:
        with mock.patch.object(BedrockHook, "conn") as _conn:
            _conn.create_model_customization_job.return_value = {
                "ResponseMetadata": {"HTTPStatusCode": 201},
                "jobArn": self.CUSTOMIZE_JOB_ARN,
            }
            _conn.get_model_customization_job.return_value = {
                "jobName": self.CUSTOMIZE_JOB_NAME,
                "status": "InProgress",
            }
            yield _conn

    @pytest.fixture
    def bedrock_hook(self) -> Generator[BedrockHook, None, None]:
        with mock_aws():
            hook = BedrockHook(aws_conn_id="aws_default")
            yield hook

    def setup_method(self):
        self.operator = BedrockCustomizeModelOperator(
            task_id="test_task",
            job_name=self.CUSTOMIZE_JOB_NAME,
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

        assert response == self.CUSTOMIZE_JOB_ARN
        assert bedrock_hook.get_waiter.call_count == wait_for_completion
        assert self.operator.defer.call_count == deferrable

    conflict_msg = "The provided job name is currently in use."
    conflict_exception = ClientError(
        error_response={"Error": {"Message": conflict_msg, "Code": "ValidationException"}},
        operation_name="UnitTest",
    )
    success = {"ResponseMetadata": {"HTTPStatusCode": 201}, "jobArn": CUSTOMIZE_JOB_ARN}

    @pytest.mark.parametrize(
        "side_effect, ensure_unique_name",
        [
            pytest.param([conflict_exception, success], True, id="conflict_and_ensure_unique"),
            pytest.param([conflict_exception, success], False, id="conflict_and_not_ensure_unique"),
            pytest.param(
                [conflict_exception, conflict_exception, success],
                True,
                id="multiple_conflict_and_ensure_unique",
            ),
            pytest.param(
                [conflict_exception, conflict_exception, success],
                False,
                id="multiple_conflict_and_not_ensure_unique",
            ),
            pytest.param([success], True, id="no_conflict_and_ensure_unique"),
            pytest.param([success], False, id="no_conflict_and_not_ensure_unique"),
        ],
    )
    @mock.patch.object(BedrockHook, "get_waiter")
    def test_ensure_unique_job_name(self, _, side_effect, ensure_unique_name, mock_conn, bedrock_hook):
        mock_conn.create_model_customization_job.side_effect = side_effect
        expected_call_count = len(side_effect) if ensure_unique_name else 1
        self.operator.wait_for_completion = False

        response = self.operator.execute({})

        assert response == self.CUSTOMIZE_JOB_ARN
        mock_conn.create_model_customization_job.call_count == expected_call_count
        bedrock_hook.get_waiter.assert_not_called()
        self.operator.defer.assert_not_called()
