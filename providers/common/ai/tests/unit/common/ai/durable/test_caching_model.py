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

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic_ai.messages import ModelResponse, TextPart
from pydantic_ai.models import ModelRequestParameters

from airflow.providers.common.ai.durable.base import DURABLE_KEY_PREFIX as P
from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.fingerprint import fingerprint_model_request
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
