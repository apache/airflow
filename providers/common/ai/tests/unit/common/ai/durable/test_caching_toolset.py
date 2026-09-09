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
from pydantic_ai.toolsets.abstract import ToolsetTool
from pydantic_ai.toolsets.combined import CombinedToolset
from pydantic_ai.toolsets.function import FunctionToolset

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX as P, DurableStorageProtocol
from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.caching_toolset import CachingToolset
from airflow.providers.common.ai.durable.fingerprint import fingerprint_tool_call
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter

from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS


@pytest.fixture
def mock_storage():
    storage = MagicMock(spec=DurableStorageProtocol)
    storage.load_tool_result.return_value = (False, None, None)
    storage.load_model_response.return_value = (None, None)
    storage.save_tool_result.return_value = True
    storage.save_model_response.return_value = True
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

    @pytest.mark.asyncio
    async def test_skipped_write_is_recorded_by_tool_name(self, mock_toolset, mock_storage, counter):
        """A result the backend did not store re-runs on retry, so it is not counted as cached."""
        mock_storage.save_tool_result.side_effect = [True, False]
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        await caching.call_tool("get_schema", {}, ctx_for("c1"), MagicMock())
        result = await caching.call_tool("run_query", {}, ctx_for("c2"), MagicMock())

        assert result == "fresh result"
        assert counter.cached_tool == 1
        assert counter.skipped_tools == ["run_query"]

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not AIRFLOW_V_3_1_PLUS, reason="cap_structlog needs airflow._shared, which lands in Airflow 3.1"
    )
    async def test_skipped_write_warns_by_tool_name(self, mock_toolset, mock_storage, counter, cap_structlog):
        """The warning names the tool on every path, not only in a successful run's summary."""
        mock_storage.save_tool_result.side_effect = [True, False]
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        await caching.call_tool("get_schema", {}, ctx_for("c1"), MagicMock())
        await caching.call_tool("run_query", {}, ctx_for("c2"), MagicMock())

        assert {"tool": "run_query", "step": 1, "log_level": "warning"} in cap_structlog
        assert {"tool": "get_schema", "log_level": "warning"} not in cap_structlog


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

    @pytest.mark.asyncio
    async def test_unverifiable_current_call_treated_as_miss(self, mock_toolset, mock_storage, counter):
        mock_storage.load_tool_result.return_value = (True, "stale result", None)
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)
        tool_args = {"value": object()}

        result = await caching.call_tool("search", tool_args, ctx_for("call_1"), MagicMock())

        assert result == "fresh result"
        mock_toolset.call_tool.assert_called_once()
        assert counter.replayed_tool == 0
        mock_storage.save_tool_result.assert_not_called()
        # Not written, so it counts as skipped, like a write the backend refused.
        assert (counter.cached_tool, counter.skipped_tools) == (0, ["search"])

    @pytest.mark.asyncio
    async def test_unverifiable_call_is_not_cached(self, mock_toolset, mock_storage, counter):
        """An entry stored without a fingerprint can never satisfy the replay guard, so none is written."""
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        result = await caching.call_tool("search", {"value": object()}, ctx_for("call_1"), MagicMock())

        assert result == "fresh result"
        mock_toolset.call_tool.assert_called_once()
        mock_storage.save_tool_result.assert_not_called()
        # Not written, so it counts as skipped, like a write the backend refused.
        assert (counter.cached_tool, counter.skipped_tools) == (0, ["search"])


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


class TestCachingToolsetReplayable:
    @pytest.mark.asyncio
    async def test_a_non_replayable_toolset_is_never_served_from_cache(
        self, mock_toolset, mock_storage, counter
    ):
        # A managed agent may have acted on a system Airflow cannot observe, so its toolset
        # declares replayable=False and a cached answer must not stand in for a fresh call.
        mock_toolset.replayable = False
        mock_storage.load_tool_result.return_value = (
            True,
            "stale cached result",
            fingerprint_tool_call("t", {}, "call_1"),
        )
        caching = CachingToolset(wrapped=mock_toolset, storage=mock_storage, counter=counter)

        tool = MagicMock(spec=ToolsetTool)
        tool.toolset = mock_toolset

        result = await caching.call_tool("t", {}, ctx_for(), tool=tool)

        assert result == "fresh result"
        mock_toolset.call_tool.assert_awaited_once()
        mock_storage.load_tool_result.assert_not_called()
        mock_storage.save_tool_result.assert_not_called()
        assert counter.replayed_tool == 0
        # The step is still consumed so later steps keep their keys.
        assert counter.next_step() == 1

    @pytest.mark.asyncio
    async def test_the_flag_is_read_per_tool_through_prefixed_and_combined_wrappers(
        self, mock_storage, counter
    ):
        # ``.prefixed()`` and ``CombinedToolset`` do not carry the attribute; the cache reads it off the
        # toolset each tool came from, so one non-replayable member does not stop its siblings replaying.
        class NonReplayable(FunctionToolset):
            replayable = False

        managed, plain = NonReplayable(), FunctionToolset()
        wrapped = CombinedToolset([plain, managed.prefixed("claims")])
        mock_storage.load_tool_result.return_value = (True, "stale", fingerprint_tool_call("t", {}, "call_1"))
        caching = CachingToolset(wrapped=wrapped, storage=mock_storage, counter=counter)
        from_managed, from_plain = MagicMock(spec=ToolsetTool), MagicMock(spec=ToolsetTool)
        from_managed.toolset, from_plain.toolset = managed, plain

        with patch.object(CombinedToolset, "call_tool", autospec=True, return_value="fresh") as call_tool:
            assert await caching.call_tool("t", {}, ctx_for(), tool=from_managed) == "fresh"
            assert await caching.call_tool("t", {}, ctx_for(), tool=from_plain) == "stale"

        call_tool.assert_awaited_once()
        mock_storage.load_tool_result.assert_called_once()
