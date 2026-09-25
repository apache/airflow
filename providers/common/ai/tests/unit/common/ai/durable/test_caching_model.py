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

from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic_ai.exceptions import UsageLimitExceeded
from pydantic_ai.messages import ModelResponse, TextPart, ToolCallPart
from pydantic_ai.models import ModelRequestParameters
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.usage import RequestUsage, RunUsage, UsageLimits

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX as P
from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.fingerprint import fingerprint_model_request
from airflow.providers.common.ai.durable.replay_usage import ReplayUsageLedger
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.load_model_response.return_value = (None, None)
    return storage


@pytest.fixture
def counter():
    return DurableStepCounter()


@pytest.fixture
def mock_model():
    model = MagicMock()
    model.model_name = "test-model"
    model.system = "test"
    model.profile = MagicMock()
    model.settings = None
    # CachingModel fingerprints the prepared request; identity keeps prepared == raw.
    model.prepare_request = lambda settings, params: (settings, params)
    return model


@pytest.fixture(autouse=True)
def _patch_infer_model():
    """Prevent WrapperModel.__init__ from resolving the mock as a real model."""
    with patch("pydantic_ai.models.wrapper.infer_model", side_effect=lambda m: m):
        yield


@pytest.fixture
def sample_response():
    return ModelResponse(parts=[TextPart(content="Hello!")])


def request_fingerprint(messages=(), settings=None, params=None):
    """Fingerprint matching what CachingModel computes for the mock model."""
    return fingerprint_model_request(
        "test:test-model", list(messages), settings, params or ModelRequestParameters()
    )


class TestCachingModelCacheHit:
    @pytest.mark.asyncio
    async def test_returns_cached_response_without_calling_model(
        self, mock_model, mock_storage, counter, sample_response
    ):
        mock_storage.load_model_response.return_value = (sample_response, request_fingerprint())
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        result = await caching.request([], None, ModelRequestParameters())

        assert result is sample_response
        mock_model.request.assert_not_called()
        mock_storage.load_model_response.assert_called_once_with(f"{P}model_step_0")

    @pytest.mark.asyncio
    async def test_advances_counter_on_cache_hit(self, mock_model, mock_storage, counter, sample_response):
        mock_storage.load_model_response.return_value = (sample_response, request_fingerprint())
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        await caching.request([], None, ModelRequestParameters())

        assert counter.total_steps == 1

    @pytest.mark.asyncio
    async def test_replay_returns_cached_response_unchanged_and_nets_out_its_usage(
        self, mock_model, mock_storage, counter
    ):
        """The cached response is returned as-is -- it flows into the message history the
        next step's fingerprint hashes, so changing its usage would break every later
        replay -- and the ledger subtracts exactly what the graph adds back after this
        call returns (``requests += 1`` and ``incr(response.usage)``)."""
        cached_original = ModelResponse(
            parts=[TextPart(content="Hello!")],
            usage=RequestUsage(input_tokens=100, output_tokens=50, cost=Decimal("0.10")),
        )
        mock_storage.load_model_response.side_effect = lambda key: (
            (cached_original, request_fingerprint()) if key == f"{P}model_step_0" else (None, None)
        )
        seed = RunUsage(requests=1, input_tokens=100, output_tokens=50, cost=Decimal("0.10"))
        run_usage = RunUsage(requests=1, input_tokens=100, output_tokens=50, cost=Decimal("0.10"))
        ledger = ReplayUsageLedger(run_usage, None)
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter, replay_usage=ledger)

        result = await caching.request([], None, ModelRequestParameters())
        run_usage.requests += 1
        run_usage.incr(result.usage)

        assert result is cached_original
        assert result.usage == RequestUsage(input_tokens=100, output_tokens=50, cost=Decimal("0.10"))
        assert run_usage == seed

    @pytest.mark.asyncio
    async def test_replay_prices_an_unpriced_cached_response_before_subtracting(
        self, mock_model, mock_storage, counter
    ):
        """A live response is cached before pydantic-ai prices it, so the cached copy has
        ``cost=None``. The replay must price it the way the graph is about to, or the
        graph adds a cost the ledger never subtracted."""
        cached = ModelResponse(
            parts=[TextPart(content="Hello!")],
            usage=RequestUsage(input_tokens=100_000, output_tokens=10_000),
            model_name="gpt-4o",
            provider_name="openai",
        )
        expected_cost = cached.cost().total_price
        assert expected_cost > 0
        mock_storage.load_model_response.side_effect = lambda key: (
            (cached, request_fingerprint()) if key == f"{P}model_step_0" else (None, None)
        )
        run_usage = RunUsage(requests=1, cost=expected_cost)
        caching = CachingModel(
            mock_model, storage=mock_storage, counter=counter, replay_usage=ReplayUsageLedger(run_usage, None)
        )

        result = await caching.request([], None, ModelRequestParameters())

        assert result.usage.cost == expected_cost
        assert run_usage.cost == 0

    @pytest.mark.asyncio
    async def test_replay_without_ledger_returns_response_unchanged(self, mock_model, mock_storage, counter):
        """Without a ledger nothing is compensated, and the cached response's real usage
        is still returned unchanged (not zeroed) -- see the sibling test above for why."""
        cached = ModelResponse(parts=[TextPart(content="Hello!")], usage=RequestUsage(input_tokens=10))
        mock_storage.load_model_response.return_value = (cached, request_fingerprint())
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        result = await caching.request([], None, ModelRequestParameters())

        assert result is cached
        assert result.usage == RequestUsage(input_tokens=10)


class TestCachingModelReplayCredits:
    @pytest.mark.asyncio
    async def test_credit_first_replay_lets_a_fully_replayable_retry_start_at_request_limit(
        self, mock_model, mock_storage, counter, sample_response
    ):
        """pydantic-ai checks ``requests >= request_limit`` before the first request reaches
        ``CachingModel``. A retry seeded with ``requests == request_limit`` whose first step
        is cached must pass that check, since the replay is free."""
        mock_storage.load_model_response.side_effect = lambda key: (
            (sample_response, request_fingerprint()) if key == f"{P}model_step_0" else (None, None)
        )
        limits = UsageLimits(request_limit=2)
        run_usage = RunUsage(requests=2)
        caching = CachingModel(
            mock_model,
            storage=mock_storage,
            counter=counter,
            replay_usage=ReplayUsageLedger(run_usage, limits),
        )

        caching.credit_first_replay()
        limits.check_before_request(run_usage)
        await caching.request([], None, ModelRequestParameters())
        run_usage.requests += 1

        assert run_usage.requests == 2

    @pytest.mark.asyncio
    async def test_credited_request_that_runs_live_is_checked_again(
        self, mock_model, mock_storage, counter, sample_response
    ):
        """The credit assumed a free replay. When the fingerprint no longer matches, the
        request would be a real one past ``request_limit``, so it must not reach the model."""
        mock_storage.load_model_response.side_effect = lambda key: (
            (sample_response, "stale-fingerprint") if key == f"{P}model_step_0" else (None, None)
        )
        mock_model.request = AsyncMock(return_value=sample_response)
        limits = UsageLimits(request_limit=2)
        run_usage = RunUsage(requests=2)
        caching = CachingModel(
            mock_model,
            storage=mock_storage,
            counter=counter,
            replay_usage=ReplayUsageLedger(run_usage, limits),
        )

        caching.credit_first_replay()
        limits.check_before_request(run_usage)
        with pytest.raises(UsageLimitExceeded, match="request_limit"):
            await caching.request([], None, ModelRequestParameters())

        mock_model.request.assert_not_called()
        assert run_usage.requests == 2

    @pytest.mark.asyncio
    async def test_replay_credits_cached_tool_steps_and_the_next_request(
        self, mock_model, mock_storage, counter
    ):
        """A replayed response with two function-tool calls credits the cached tool
        results at the next step indices (a missing one -- a call that raised -- is
        skipped) and the next model request; a call to a tool that is not a function
        tool takes no index and is not credited."""
        response = ModelResponse(
            parts=[
                ToolCallPart(tool_name="tool_a", args={}, tool_call_id="a"),
                ToolCallPart(tool_name="tool_b", args={}, tool_call_id="b"),
                ToolCallPart(tool_name="final_result", args={}, tool_call_id="o"),
            ]
        )
        params = ModelRequestParameters(
            function_tools=[ToolDefinition(name="tool_a"), ToolDefinition(name="tool_b")],
            output_tools=[ToolDefinition(name="final_result")],
        )
        models = {
            f"{P}model_step_0": (response, request_fingerprint(params=params)),
            f"{P}model_step_3": (response, "x"),
        }
        mock_storage.load_model_response.side_effect = lambda key: models.get(key, (None, None))
        mock_storage.load_tool_result.side_effect = lambda key: (
            (True, "A", "fp") if key == f"{P}tool_step_2" else (False, None, None)
        )
        run_usage = RunUsage(requests=5, tool_calls=5)
        ledger = ReplayUsageLedger(run_usage, None)
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter, replay_usage=ledger)

        await caching.request([], None, params)

        # -1 for the replayed request itself, -1 for the credited next request.
        assert run_usage.requests == 3
        assert run_usage.tool_calls == 4
        ledger.settle()
        assert (run_usage.requests, run_usage.tool_calls) == (4, 5)

    @pytest.mark.asyncio
    async def test_credited_next_request_reuses_the_peeked_response_without_reloading(
        self, mock_model, mock_storage, counter
    ):
        """``_credit_cached_successors`` already loads the next model step's response from
        storage to decide whether to credit it. Before this fix, ``request()`` loaded and
        deserialized that same entry again when it reached that step: two replayed steps
        cost 4 ``load_model_response`` calls (each step's own load, plus each step's scan
        re-loading the next one). Reusing the scan's object drops that to 3 -- the second
        step's own load is skipped because the first step's scan already produced it; only
        its own successor-scan (querying step 2, which is not cached) still hits storage."""
        response_0 = ModelResponse(parts=[TextPart(content="Hello!")])
        response_1 = ModelResponse(parts=[TextPart(content="World!")])
        models = {
            f"{P}model_step_0": (response_0, request_fingerprint()),
            f"{P}model_step_1": (response_1, request_fingerprint()),
        }
        mock_storage.load_model_response.side_effect = lambda key: models.get(key, (None, None))
        run_usage = RunUsage(requests=5)
        caching = CachingModel(
            mock_model, storage=mock_storage, counter=counter, replay_usage=ReplayUsageLedger(run_usage, None)
        )

        result_0 = await caching.request([], None, ModelRequestParameters())
        calls_after_first_replay = mock_storage.load_model_response.call_count
        result_1 = await caching.request([], None, ModelRequestParameters())

        assert result_0 is response_0
        assert result_1 is response_1
        # Step 0's own load, plus its scan finding step 1 -- not yet the double-load this
        # test guards against, which would only show up once step 1's request() runs.
        assert calls_after_first_replay == 2
        # Without reuse this would be 4 (step 1 reloaded, then its scan queries step 2).
        assert mock_storage.load_model_response.call_count == 3


class TestCachingModelCacheMiss:
    @pytest.mark.asyncio
    async def test_calls_model_and_caches_on_miss(self, mock_model, mock_storage, counter, sample_response):
        mock_model.request = AsyncMock(return_value=sample_response)
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        result = await caching.request([], None, ModelRequestParameters())

        assert result is sample_response
        mock_model.request.assert_called_once()
        mock_storage.save_model_response.assert_called_once_with(
            f"{P}model_step_0", sample_response, fingerprint=request_fingerprint()
        )

    @pytest.mark.asyncio
    async def test_sequential_calls_use_incrementing_keys(self, mock_model, mock_storage, counter):
        response_1 = ModelResponse(parts=[TextPart(content="First")])
        response_2 = ModelResponse(parts=[TextPart(content="Second")])
        mock_model.request = AsyncMock(side_effect=[response_1, response_2])
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        await caching.request([], None, ModelRequestParameters())
        await caching.request([], None, ModelRequestParameters())

        keys = [call[0][0] for call in mock_storage.save_model_response.call_args_list]
        assert keys == [f"{P}model_step_0", f"{P}model_step_1"]


class TestCachingModelReplayVerification:
    @pytest.mark.asyncio
    async def test_fingerprint_mismatch_treated_as_miss(
        self, mock_model, mock_storage, counter, sample_response
    ):
        """A cached entry recorded for a different request must not be replayed."""
        stale = ModelResponse(parts=[TextPart(content="stale")])
        mock_storage.load_model_response.return_value = (stale, "fp_of_old_conversation")
        mock_model.request = AsyncMock(return_value=sample_response)
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        result = await caching.request([], None, ModelRequestParameters())

        assert result is sample_response
        mock_model.request.assert_called_once()
        assert counter.replayed_model == 0
        mock_storage.save_model_response.assert_called_once_with(
            f"{P}model_step_0", sample_response, fingerprint=request_fingerprint()
        )

    @pytest.mark.asyncio
    async def test_legacy_entry_without_fingerprint_treated_as_miss(
        self, mock_model, mock_storage, counter, sample_response
    ):
        """Pre-fingerprint cache entries cannot be verified, so they re-run."""
        stale = ModelResponse(parts=[TextPart(content="stale")])
        mock_storage.load_model_response.return_value = (stale, None)
        mock_model.request = AsyncMock(return_value=sample_response)
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        result = await caching.request([], None, ModelRequestParameters())

        assert result is sample_response
        mock_model.request.assert_called_once()

    @pytest.mark.asyncio
    async def test_fingerprint_uses_prepared_request_not_raw_arguments(
        self, mock_storage, counter, sample_response
    ):
        """Concrete models merge model-level settings in ``prepare_request`` before the
        provider sees the request. The fingerprint must reflect the prepared settings,
        so a model-level change (e.g. a different temperature on the connection) is not
        invisible behind identical raw ``request()`` arguments."""
        model = MagicMock()
        model.model_name = "test-model"
        model.system = "test"
        model.profile = MagicMock()
        model.settings = None
        model.request = AsyncMock(return_value=sample_response)
        # Simulate prepare_request merging a model-level temperature into settings.
        model.prepare_request = lambda settings, params: ({"temperature": 0.9}, params)
        caching = CachingModel(model, storage=mock_storage, counter=counter)

        await caching.request([], None, ModelRequestParameters())

        stored_fingerprint = mock_storage.save_model_response.call_args.kwargs["fingerprint"]
        # Reflects the prepared settings, not the raw ``None`` the agent passed in.
        assert stored_fingerprint == fingerprint_model_request(
            "test:test-model", [], {"temperature": 0.9}, ModelRequestParameters()
        )
        assert stored_fingerprint != request_fingerprint()
