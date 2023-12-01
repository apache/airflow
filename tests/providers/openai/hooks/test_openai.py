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

from unittest.mock import MagicMock, Mock, patch

import pytest
from openai import OpenAI
from openai.types import CreateEmbeddingResponse, Embedding

from airflow.providers.openai.hooks.openai import OpenAIHook


@pytest.fixture
def openai_hook():
    with patch("airflow.providers.openai.hooks.openai.OpenAI") as _:
        yield OpenAIHook(conn_id="test_conn_id")


@pytest.fixture
def mock_embeddings_response():
    return CreateEmbeddingResponse(
        data=[Embedding(embedding=[0.1, 0.2, 0.3], index=0, object="embedding")],
        model="text-embedding-ada-002-v2",
        object="list",
        usage={"prompt_tokens": 4, "total_tokens": 4},
    )


@patch("airflow.hooks.base.BaseHook.get_connection")
def test_create_embeddings(mock_get_connection, openai_hook, mock_embeddings_response):
    text = "Sample text"
    openai_hook.conn.embeddings.create.return_value = mock_embeddings_response
    embeddings = openai_hook.create_embeddings(text)
    assert embeddings == [0.1, 0.2, 0.3]


@patch("openai.OpenAI")
@patch("airflow.hooks.base.BaseHook.get_connection")
def test_openai_hook_get_conn(mock_get_connection, mock_openai):
    mock_connection = MagicMock()
    mock_connection.host = "http://example.com"
    mock_connection.password = "test-api-key"
    mock_get_connection.return_value = mock_connection
    OpenAIHook.get_connection = Mock(return_value=mock_connection)

    openai_hook = OpenAIHook(conn_id="test_conn")
    conn = openai_hook.conn

    assert isinstance(conn, OpenAI)
    assert conn.api_key == "test-api-key"
    assert conn.base_url == "http://example.com"


@patch("openai.OpenAI")
def test_openai_hook_test_connection(mock_openai, openai_hook):
    result, message = openai_hook.test_connection()
    assert result is True
    assert message == "Connection established!"
