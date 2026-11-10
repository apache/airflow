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
"""Chain members stamp the responses they answer, so a replay can tell who answered."""

from __future__ import annotations

import pytest
from pydantic_ai import Agent
from pydantic_ai.exceptions import ModelAPIError, ModelHTTPError
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart
from pydantic_ai.models.fallback import FallbackModel
from pydantic_ai.models.function import AgentInfo, FunctionModel

from airflow.providers.common.ai.durable.member_stamp import (
    MEMBER_MODEL_ID_KEY,
    MEMBER_STAMP_KEY,
    MemberStampingModel,
    get_member_model_id,
)

pytestmark = pytest.mark.asyncio


def build_chain() -> FallbackModel:
    def fail(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        raise ModelHTTPError(500, "first")

    async def fail_stream(messages: list[ModelMessage], info: AgentInfo):
        raise ModelHTTPError(500, "first")
        yield ""

    def answer(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        return ModelResponse(parts=[TextPart("from second")])

    async def answer_stream(messages: list[ModelMessage], info: AgentInfo):
        yield "from second"

    first = FunctionModel(fail, stream_function=fail_stream, model_name="first")
    second = FunctionModel(answer, stream_function=answer_stream, model_name="second")
    return FallbackModel(
        MemberStampingModel(first), MemberStampingModel(second), fallback_on=(ModelAPIError,)
    )


class TestMemberStamp:
    async def test_a_response_names_the_member_that_answered(self):
        chain = build_chain()

        result = await Agent(chain).run("go")

        assert result.response.metadata[MEMBER_STAMP_KEY] == {MEMBER_MODEL_ID_KEY: "function:second"}
        assert get_member_model_id(result.response) == "function:second"

    async def test_a_streamed_response_names_the_member_that_answered(self):
        chain = build_chain()

        async with Agent(chain).run_stream("go") as streamed:
            await streamed.get_output()

        assert get_member_model_id(streamed.response) == "function:second"

    async def test_the_chain_keeps_its_own_identity(self):
        chain = build_chain()

        assert chain.model_id == "fallback:function:first,function:second"
