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
from typing import Generator
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.bedrock import BedrockRuntimeHook
from airflow.providers.amazon.aws.operators.bedrock import BedrockInvokeModelOperator

MODEL_ID = "meta.llama2-13b-chat-v1"
PROMPT = "A very important question."
GENERATED_RESPONSE = "An important answer."
MOCK_RESPONSE = json.dumps(
    {
        "generation": GENERATED_RESPONSE,
        "prompt_token_count": len(PROMPT),
        "generation_token_count": len(GENERATED_RESPONSE),
        "stop_reason": "stop",
    }
)


@pytest.fixture
def runtime_hook() -> Generator[BedrockRuntimeHook, None, None]:
    with mock_aws():
        yield BedrockRuntimeHook(aws_conn_id="aws_default")


class TestBedrockInvokeModelOperator:
    @mock.patch.object(BedrockRuntimeHook, "conn")
    def test_invoke_model_prompt_good_combinations(self, mock_conn):
        mock_conn.invoke_model.return_value["body"].read.return_value = MOCK_RESPONSE
        operator = BedrockInvokeModelOperator(
            task_id="test_task", model_id=MODEL_ID, input_data={"input_data": {"prompt": PROMPT}}
        )

        response = operator.execute({})

        assert response["generation"] == GENERATED_RESPONSE
