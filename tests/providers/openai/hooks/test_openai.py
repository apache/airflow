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

import os
from unittest.mock import patch

import pytest
from openai.types import CreateEmbeddingResponse, Embedding

from airflow.models import Connection
from airflow.providers.openai.hooks.openai import OpenAIHook


@pytest.fixture
def mock_openai_connection():
    conn_id = "openai_conn"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    yield conn


@pytest.fixture
def mock_openai_hook(mock_openai_connection):
    with patch("airflow.providers.openai.hooks.openai.OpenAI"):
        yield OpenAIHook(conn_id=mock_openai_connection.conn_id)


@pytest.fixture
def mock_embeddings_response():
    return CreateEmbeddingResponse(
        data=[Embedding(embedding=[0.1, 0.2, 0.3], index=0, object="embedding")],
        model="text-embedding-ada-002-v2",
        object="list",
        usage={"prompt_tokens": 4, "total_tokens": 4},
    )


def test_create_embeddings(mock_openai_hook, mock_embeddings_response):
    text = "Sample text"
    mock_openai_hook.conn.embeddings.create.return_value = mock_embeddings_response
    embeddings = mock_openai_hook.create_embeddings(text)
    assert embeddings == [0.1, 0.2, 0.3]


def test_openai_hook_test_connection(mock_openai_hook):
    result, message = mock_openai_hook.test_connection()
    assert result is True
    assert message == "Connection established!"


@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_with_api_key_in_extra(mock_client):
    conn_id = "api_key_in_extra"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        extra={"openai_client_kwargs": {"api_key": "api_key_in_extra"}},
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    hook = OpenAIHook(conn_id=conn_id)
    hook.get_conn()
    mock_client.assert_called_once_with(
        api_key="api_key_in_extra",
        base_url=None,
    )


@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_with_api_key_in_password(mock_client):
    conn_id = "api_key_in_password"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        password="api_key_in_password",
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    hook = OpenAIHook(conn_id=conn_id)
    hook.get_conn()
    mock_client.assert_called_once_with(
        api_key="api_key_in_password",
        base_url=None,
    )


@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_with_base_url_in_extra(mock_client):
    conn_id = "base_url_in_extra"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        extra={"openai_client_kwargs": {"base_url": "base_url_in_extra", "api_key": "api_key_in_extra"}},
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    hook = OpenAIHook(conn_id=conn_id)
    hook.get_conn()
    mock_client.assert_called_once_with(
        api_key="api_key_in_extra",
        base_url="base_url_in_extra",
    )


@patch("airflow.providers.openai.hooks.openai.OpenAI")
def test_get_conn_with_openai_client_kwargs(mock_client):
    conn_id = "openai_client_kwargs"
    conn = Connection(
        conn_id=conn_id,
        conn_type="openai",
        extra={
            "openai_client_kwargs": {
                "api_key": "api_key_in_extra",
                "organization": "organization_in_extra",
            }
        },
    )
    os.environ[f"AIRFLOW_CONN_{conn.conn_id.upper()}"] = conn.get_uri()
    hook = OpenAIHook(conn_id=conn_id)
    hook.get_conn()
    mock_client.assert_called_once_with(
        api_key="api_key_in_extra",
        base_url=None,
        organization="organization_in_extra",
    )
