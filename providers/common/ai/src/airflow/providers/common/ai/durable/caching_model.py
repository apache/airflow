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
"""Caching model wrapper for durable execution."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import structlog
from pydantic_ai.models.wrapper import WrapperModel

log = structlog.get_logger(logger_name="task")

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage, ModelResponse
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.settings import ModelSettings

    from airflow.providers.common.ai.durable.step_counter import DurableStepCounter
    from airflow.providers.common.ai.durable.storage import DurableStorage


@dataclass(init=False)
class CachingModel(WrapperModel):
    """
    Wraps a model to cache responses in ObjectStorage for durable execution.

    On each ``request()`` call, checks if a cached response exists for the
    current step index. If so, returns the cached response without calling
    the underlying model. Otherwise, calls the model and caches the response.
    """

    storage: DurableStorage = field(repr=False)
    counter: DurableStepCounter = field(repr=False)

    def __init__(
        self,
        wrapped: Any,
        *,
        storage: DurableStorage,
        counter: DurableStepCounter,
    ) -> None:
        super().__init__(wrapped)
        self.storage = storage
        self.counter = counter

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        step = self.counter.next_step()
        key = f"model_step_{step}"

        cached = self.storage.load_model_response(key)
        if cached is not None:
            self.counter.replayed_model += 1
            log.debug("Durable: replayed cached model response", step=step)
            return cached

        response = await self.wrapped.request(messages, model_settings, model_request_parameters)
        self.storage.save_model_response(key, response)
        self.counter.cached_model += 1
        log.debug("Durable: cached model response", step=step)
        return response
