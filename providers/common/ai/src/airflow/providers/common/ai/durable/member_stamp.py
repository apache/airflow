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
"""Record which member of a ``FallbackModel`` chain answered a model request."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from pydantic_ai.models.wrapper import WrapperModel

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage, ModelResponse
    from pydantic_ai.models import ModelRequestParameters, StreamedResponse
    from pydantic_ai.settings import ModelSettings
    from pydantic_ai.tools import RunContext

MEMBER_STAMP_KEY = "__airflow_common_ai__"
MEMBER_MODEL_ID_KEY = "member_model_id"


def get_member_model_id(response: ModelResponse) -> str | None:
    """Return the ``model_id`` of the chain member that produced ``response``, if it was stamped."""
    stamp = (response.metadata or {}).get(MEMBER_STAMP_KEY)
    if not isinstance(stamp, dict):
        return None
    member_model_id = stamp.get(MEMBER_MODEL_ID_KEY)
    return member_model_id if isinstance(member_model_id, str) else None


class MemberStampingModel(WrapperModel):
    """
    Stamp each response with the ``model_id`` of the wrapped model.

    pydantic-ai's ``FallbackModel`` only records which member answered on suspended responses,
    so a durable replay cannot otherwise tell whether the recorded answer came from a model
    that is still in the chain. The stamp lives under its own key rather than the
    ``__pydantic_ai__`` namespace that pydantic-ai owns.
    """

    def _stamp(self, response: ModelResponse | StreamedResponse) -> None:
        if response.metadata is None:
            response.metadata = {}
        response.metadata[MEMBER_STAMP_KEY] = {MEMBER_MODEL_ID_KEY: self.wrapped.model_id}

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        response = await super().request(messages, model_settings, model_request_parameters)
        self._stamp(response)
        return response

    @asynccontextmanager
    async def request_stream(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
        run_context: RunContext[Any] | None = None,
    ) -> AsyncIterator[StreamedResponse]:
        async with super().request_stream(
            messages, model_settings, model_request_parameters, run_context
        ) as streamed_response:
            self._stamp(streamed_response)
            yield streamed_response
