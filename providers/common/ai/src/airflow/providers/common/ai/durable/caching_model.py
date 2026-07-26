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

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX
from airflow.providers.common.ai.durable.fingerprint import fingerprint_model_request

log = structlog.get_logger(logger_name="task")

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage, ModelResponse
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.settings import ModelSettings

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
    from airflow.providers.common.ai.durable.step_counter import DurableStepCounter


@dataclass(init=False)
class CachingModel(WrapperModel):
    """
    Wraps a model to cache responses in ObjectStorage for durable execution.

    On each ``request()`` call, checks if a cached response exists for the
    current step index and was produced by an equivalent request (same model,
    message history, settings, and tools -- compared via fingerprint). If so,
    returns the cached response without calling the underlying model.
    Otherwise, calls the model and caches the response. A fingerprint
    mismatch means the agent changed between attempts; the stale entry is
    discarded and the step re-runs live.
    """

    storage: DurableStorageProtocol = field(repr=False)
    counter: DurableStepCounter = field(repr=False)

    def __init__(
        self,
        wrapped: Any,
        *,
        storage: DurableStorageProtocol,
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
        key = f"{DURABLE_KEY_PREFIX}model_step_{step}"
        # Fingerprint the *prepared* request, not the raw arguments. Concrete
        # models call ``prepare_request()`` at the start of ``request()`` to merge
        # their model-level ``settings`` and apply profile-specific transforms
        # (thinking resolution, native-tool handling, output-mode defaults) before
        # the provider sees the request. Fingerprinting the raw arguments would
        # miss a change that lives only at the model level -- e.g. a different
        # temperature or thinking setting on the connection -- and replay a stale
        # response. The raw arguments are still passed to ``wrapped.request()``,
        # which re-runs ``prepare_request()`` itself (it is pure and idempotent).
        prepared_settings, prepared_parameters = self.wrapped.prepare_request(
            model_settings, model_request_parameters
        )
        fingerprint = fingerprint_model_request(
            f"{self.wrapped.system}:{self.wrapped.model_name}",
            messages,
            prepared_settings,
            prepared_parameters,
        )

        cached, cached_fingerprint = self.storage.load_model_response(key)
        if cached is not None:
            if cached_fingerprint == fingerprint:
                self.counter.replayed_model += 1
                log.debug("Durable: replayed cached model response", step=step)
                return cached
            log.warning(
                "Durable: cached model response does not match the current request; "
                "re-running this step instead of replaying",
                step=step,
                reason=(
                    "entry predates fingerprinting or the request could not be fingerprinted"
                    if fingerprint is None or cached_fingerprint is None
                    else "model, prompt, message history, settings, or tools changed since "
                    "the previous attempt"
                ),
            )

        response = await self.wrapped.request(messages, model_settings, model_request_parameters)
        self.storage.save_model_response(key, response, fingerprint=fingerprint)
        self.counter.cached_model += 1
        log.debug("Durable: cached model response", step=step)
        return response
