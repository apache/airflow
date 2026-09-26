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
from pydantic_ai.messages import ModelResponse, ToolCallPart
from pydantic_ai.models.wrapper import WrapperModel

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX
from airflow.providers.common.ai.durable.fingerprint import fingerprint_model_request

log = structlog.get_logger(logger_name="task")

if TYPE_CHECKING:
    from pydantic_ai.messages import ModelMessage
    from pydantic_ai.models import ModelRequestParameters
    from pydantic_ai.settings import ModelSettings

    from airflow.providers.common.ai.durable.base import DurableStorageProtocol
    from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger
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

    With a ``replay_usage`` ledger, a replay hit does not count toward the
    run's usage (see :class:`~airflow.providers.common.ai.durable.replay_usage.ReplayUsageLedger`).
    The cached response is returned unchanged -- it is part of what later
    steps' fingerprints hash, so zeroing its usage would make every step after
    this one diverge and re-run live -- and the ledger subtracts what the graph
    is about to add instead.
    """

    storage: DurableStorageProtocol = field(repr=False)
    counter: DurableStepCounter = field(repr=False)
    replay_usage: ReplayUsageLedger | None = field(default=None, repr=False)

    def __init__(
        self,
        wrapped: Any,
        *,
        storage: DurableStorageProtocol,
        counter: DurableStepCounter,
        replay_usage: ReplayUsageLedger | None = None,
    ) -> None:
        super().__init__(wrapped)
        self.storage = storage
        self.counter = counter
        self.replay_usage = replay_usage
        self._previous_suspended = False
        self._chain_function_calls = 0
        # Set by _credit_cached_successors when it loads the next model step's response
        # to decide whether to credit it; request() reuses it instead of loading again.
        self._peeked_model: tuple[str, ModelResponse, str | None] | None = None

    def credit_first_replay(self) -> None:
        """
        Credit the run's first model request if the cache holds a response for it.

        Called before the run starts, because pydantic-ai checks ``request_limit``
        before the first request reaches this model; without the credit, a retry whose
        seeded ``requests`` already equals ``request_limit`` could not start even when
        every step would replay for free.
        """
        if self.replay_usage is None:
            return
        cached, _ = self.storage.load_model_response(self._model_key(self.counter.total_steps))
        if cached is not None:
            self.replay_usage.credit_request()

    @staticmethod
    def _model_key(step: int) -> str:
        return f"{DURABLE_KEY_PREFIX}model_step_{step}"

    def _is_continuation(self, messages: list[ModelMessage]) -> bool:
        # pydantic-ai re-issues a suspended response (Anthropic ``pause_turn``, OpenAI
        # background mode) by sending it back as the last message; the graph counts
        # the whole chain as one request.
        return (
            self._previous_suspended
            and bool(messages)
            and isinstance(messages[-1], ModelResponse)
            and messages[-1].state == "suspended"
        )

    def _track_chain(self, response: ModelResponse, model_request_parameters: ModelRequestParameters) -> None:
        function_tools = {tool.name for tool in model_request_parameters.function_tools}
        self._chain_function_calls += sum(
            1
            for part in response.parts
            if isinstance(part, ToolCallPart) and part.tool_name in function_tools
        )
        self._previous_suspended = response.state == "suspended"

    def _credit_cached_successors(self, ledger: ReplayUsageLedger, step: int) -> None:
        """
        Credit what the replayed, complete response at ``step`` is followed by in the cache.

        The step counter is shared with ``CachingToolset``, so this response's
        function-tool calls took the next step indices when they first ran, and the
        next model request took the index after them. Tool indices with a cached result
        are credited; a missing one is a call that raised. The scan stops at the next
        model step, and looks no further than one index per function-tool call in this
        response (and in the earlier segments of its continuation chain) plus one.

        The next model step's response, if any, is kept on ``self`` so ``request()`` does
        not load and deserialize it from storage a second time when it reaches that step.
        """
        calls = self._chain_function_calls
        tool_steps: list[int] = []
        for index in range(step + 1, step + calls + 2):
            if index <= step + calls:
                found, _, _ = self.storage.load_tool_result(f"{DURABLE_KEY_PREFIX}tool_step_{index}")
                if found:
                    tool_steps.append(index)
                    continue
            next_key = self._model_key(index)
            next_model, next_fingerprint = self.storage.load_model_response(next_key)
            if next_model is not None:
                self._peeked_model = (next_key, next_model, next_fingerprint)
                ledger.credit_request()
                break
        ledger.credit_tool_steps(tool_steps)

    async def request(
        self,
        messages: list[ModelMessage],
        model_settings: ModelSettings | None,
        model_request_parameters: ModelRequestParameters,
    ) -> ModelResponse:
        step = self.counter.next_step()
        key = self._model_key(step)
        continuation = self._is_continuation(messages)
        if not continuation:
            self._chain_function_calls = 0
        had_request_credit = self.replay_usage.settle() if self.replay_usage is not None else False
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

        cached: ModelResponse | None
        if self._peeked_model is not None and self._peeked_model[0] == key:
            _, cached, cached_fingerprint = self._peeked_model
            self._peeked_model = None
        else:
            self._peeked_model = None
            cached, cached_fingerprint = self.storage.load_model_response(key)
        if cached is not None:
            if cached_fingerprint == fingerprint:
                self.counter.replayed_model += 1
                log.debug("Durable: replayed cached model response", step=step)
                self._track_chain(cached, model_request_parameters)
                if self.replay_usage is not None:
                    self.replay_usage.record_model_replay(cached, continuation=continuation)
                    if cached.state != "suspended":
                        self._credit_cached_successors(self.replay_usage, step)
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

        if self.replay_usage is not None:
            self.replay_usage.record_live_model_request(had_request_credit=had_request_credit)
        response = await self.wrapped.request(messages, model_settings, model_request_parameters)
        self.storage.save_model_response(key, response, fingerprint=fingerprint)
        self.counter.cached_model += 1
        log.debug("Durable: cached model response", step=step)
        self._track_chain(response, model_request_parameters)
        return response
