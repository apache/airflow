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

from unittest.mock import Mock, patch

import pytest

from airflow.providers.openai.hooks.openai import OpenAIHook


@pytest.fixture
def openai_hook():
    with patch("airflow.providers.openai.hooks.openai.OpenAIHook._get_api_key"), patch(
        "airflow.providers.openai.hooks.openai.OpenAIHook._get_api_base"
    ) as _:
        yield OpenAIHook(conn_id="test_conn_id")


@pytest.fixture
def mock_embeddings_response():
    return {"data": [{"embedding": [0.1, 0.2, 0.3]}]}


@pytest.fixture
def mock_completions_response():
    return Mock(
        id="completion-id",
        object="completion",
        created=1234567890,
        model="text-davinci-002",
        usage={"prompt_tokens": 15, "completion_tokens": 32, "total_tokens": 47},
        choices=[Mock(text="the quick brown fox", finish_reason="stop", index=0)],
    )


def test_create_embeddings(openai_hook, mock_embeddings_response):
    text = "Sample text"
    with patch("openai.Embedding.create", return_value=mock_embeddings_response):
        embeddings = openai_hook.create_embeddings(text)
    assert embeddings == [0.1, 0.2, 0.3]


def test_get_api_key():
    mock_connection = Mock()
    mock_connection.password = "your_api_key"
    OpenAIHook.get_connection = Mock(return_value=mock_connection)
    api_key = OpenAIHook()._get_api_key()
    assert api_key == "your_api_key"
