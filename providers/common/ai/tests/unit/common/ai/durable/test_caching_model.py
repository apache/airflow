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

from airflow.providers.common.ai.durable.caching_model import CachingModel
from airflow.providers.common.ai.durable.step_counter import DurableStepCounter


@pytest.fixture
def mock_storage():
    storage = MagicMock()
    storage.load_model_response.return_value = None
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
    return model


@pytest.fixture(autouse=True)
def _patch_infer_model():
    """Prevent WrapperModel.__init__ from resolving the mock as a real model."""
    with patch("pydantic_ai.models.wrapper.infer_model", side_effect=lambda m: m):
        yield


@pytest.fixture
def sample_response():
    return ModelResponse(parts=[TextPart(content="Hello!")])


class TestCachingModelCacheHit:
    @pytest.mark.asyncio
    async def test_returns_cached_response_without_calling_model(
        self, mock_model, mock_storage, counter, sample_response
    ):
        mock_storage.load_model_response.return_value = sample_response
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        result = await caching.request([], None, MagicMock())

        assert result is sample_response
        mock_model.request.assert_not_called()
        mock_storage.load_model_response.assert_called_once_with("model_step_0")

    @pytest.mark.asyncio
    async def test_advances_counter_on_cache_hit(self, mock_model, mock_storage, counter, sample_response):
        mock_storage.load_model_response.return_value = sample_response
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        await caching.request([], None, MagicMock())

        assert counter.total_steps == 1


class TestCachingModelCacheMiss:
    @pytest.mark.asyncio
    async def test_calls_model_and_caches_on_miss(self, mock_model, mock_storage, counter, sample_response):
        mock_model.request = AsyncMock(return_value=sample_response)
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        result = await caching.request([], None, MagicMock())

        assert result is sample_response
        mock_model.request.assert_called_once()
        mock_storage.save_model_response.assert_called_once_with("model_step_0", sample_response)

    @pytest.mark.asyncio
    async def test_sequential_calls_use_incrementing_keys(self, mock_model, mock_storage, counter):
        response_1 = ModelResponse(parts=[TextPart(content="First")])
        response_2 = ModelResponse(parts=[TextPart(content="Second")])
        mock_model.request = AsyncMock(side_effect=[response_1, response_2])
        caching = CachingModel(mock_model, storage=mock_storage, counter=counter)

        await caching.request([], None, MagicMock())
        await caching.request([], None, MagicMock())

        keys = [call[0][0] for call in mock_storage.save_model_response.call_args_list]
        assert keys == ["model_step_0", "model_step_1"]
