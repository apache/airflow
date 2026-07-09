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

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models import ModelRequestParameters

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX as P
from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
from airflow.providers.common.ai.durable.fingerprint import fingerprint_tool_call
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.load_tool_result.return_value = (False, None, None)
    storage.load_model_response.return_value = (None, None)
    return storage


@pytest.fixture
def counter():
    return DurableStepCounter()


@pytest.fixture
def mock_toolset():
    toolset = MagicMock()
    toolset.call_tool = AsyncMock(return_value="fresh result")
    toolset.get_tools = AsyncMock(return_value={})
    toolset.__aenter__ = AsyncMock(return_value=toolset)
    toolset.__aexit__ = AsyncMock(return_value=None)
    return toolset


def ctx_for(tool_call_id: str | None = "call_1") -> SimpleNamespace:
    return SimpleNamespace(tool_call_id=tool_call_id)


class TestCachingToolsetCacheHit:
    @pytest.mark.asyncio
    async def test_returns_cached_result_without_calling_tool(self, mock_toolset, mock_storage, counter):
        fingerprint = fingerprint_tool_call("search", {"q": "foo"}, "call_1")
        mock_storage.load_tool_result.return_value = (True, "cached result", fingerprint)
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        result = await caching.call_tool("search", {"q": "foo"}, ctx_for("call_1"), MagicMock())

        assert result == "cached result"
        mock_toolset.call_tool.assert_not_called()
        mock_storage.load_tool_result.assert_called_once_with(f"{P}tool_step_0")

    @pytest.mark.asyncio
    async def test_advances_counter_on_cache_hit(self, mock_toolset, mock_storage, counter):
        fingerprint = fingerprint_tool_call("search", {}, "call_1")
        mock_storage.load_tool_result.return_value = (True, "cached", fingerprint)
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        await caching.call_tool("search", {}, ctx_for("call_1"), MagicMock())

        assert counter.total_steps == 1


class TestCachingToolsetCacheMiss:
    @pytest.mark.asyncio
    async def test_calls_tool_and_caches_on_miss(self, mock_toolset, mock_storage, counter):
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        result = await caching.call_tool("search", {"q": "foo"}, ctx_for("call_1"), MagicMock())

        assert result == "fresh result"
        mock_toolset.call_tool.assert_called_once()
        mock_storage.save_tool_result.assert_called_once_with(
            f"{P}tool_step_0",
            "fresh result",
            fingerprint=fingerprint_tool_call("search", {"q": "foo"}, "call_1"),
        )

    @pytest.mark.asyncio
    async def test_sequential_calls_use_incrementing_keys(self, mock_toolset, mock_storage, counter):
        mock_toolset.call_tool = AsyncMock(side_effect=["result_a", "result_b"])
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        await caching.call_tool("tool_a", {}, ctx_for(), MagicMock())
        await caching.call_tool("tool_b", {}, ctx_for(), MagicMock())

        keys = [call[0][0] for call in mock_storage.save_tool_result.call_args_list]
        assert keys == [f"{P}tool_step_0", f"{P}tool_step_1"]


class TestCachingToolsetReplayVerification:
    @pytest.mark.asyncio
    async def test_different_tool_call_treated_as_miss(self, mock_toolset, mock_storage, counter):
        """A cached result recorded for a different tool call must not be replayed."""
        stale_fingerprint = fingerprint_tool_call("lookup_order", {"id": "A1"}, "old_call")
        mock_storage.load_tool_result.return_value = (True, "stale result", stale_fingerprint)
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        result = await caching.call_tool("charge_card", {"amount": 5}, ctx_for("new_call"), MagicMock())

        assert result == "fresh result"
        mock_toolset.call_tool.assert_called_once()
        assert counter.replayed_tool == 0

    @pytest.mark.asyncio
    async def test_changed_tool_call_id_treated_as_miss(self, mock_toolset, mock_storage, counter):
        """Same name/args but a new model-issued call id means the conversation diverged."""
        stale_fingerprint = fingerprint_tool_call("search", {"q": "foo"}, "old_call")
        mock_storage.load_tool_result.return_value = (True, "stale result", stale_fingerprint)
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        result = await caching.call_tool("search", {"q": "foo"}, ctx_for("new_call"), MagicMock())

        assert result == "fresh result"
        mock_toolset.call_tool.assert_called_once()

    @pytest.mark.asyncio
    async def test_legacy_entry_without_fingerprint_treated_as_miss(
        self, mock_toolset, mock_storage, counter
    ):
        """Pre-fingerprint cache entries cannot be verified, so the tool re-runs."""
        mock_storage.load_tool_result.return_value = (True, "stale result", None)
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        result = await caching.call_tool("search", {"q": "foo"}, ctx_for("call_1"), MagicMock())

        assert result == "fresh result"
        mock_toolset.call_tool.assert_called_once()


class TestSharedCounter:
    @pytest.mark.asyncio
    async def test_model_and_toolset_share_counter(self, mock_toolset, mock_storage):
        """When CachingModel and CachingToolset share a counter, steps interleave correctly."""
        counter = DurableStepCounter()

        mock_model = MagicMock()
        mock_model.model_name = "test"
        mock_model.system = "test"
        mock_model.profile = MagicMock()
        mock_model.settings = None
        mock_model.prepare_request = lambda settings, params: (settings, params)

        response = ModelResponse(parts=[TextPart(content="response")])
        mock_model.request = AsyncMock(return_value=response)

        with patch("pydantic_ai.models.wrapper.infer_model", side_effect=lambda m: m):
            caching_model = CachingModel(mock_model, storage=mock_storage, counter=counter)
        caching_toolset = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        # Simulate: model call -> tool call -> model call
        await caching_model.request([], None, ModelRequestParameters())
        await caching_toolset.call_tool("search", {}, ctx_for(), MagicMock())
        await caching_model.request([], None, ModelRequestParameters())

        model_keys = [call[0][0] for call in mock_storage.save_model_response.call_args_list]
        tool_keys = [call[0][0] for call in mock_storage.save_tool_result.call_args_list]

        assert model_keys == [f"{P}model_step_0", f"{P}model_step_2"]
        assert tool_keys == [f"{P}tool_step_1"]
        assert counter.total_steps == 3
