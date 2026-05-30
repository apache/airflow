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
from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.cohere.hooks.cohere import (
    CohereHook,
)


class TestCohereHook:
    """
    Test for CohereHook
    """

    def test__get_api_key(self):
        api_key = "test"
        base_url = "http://some_host.com"
        timeout = 150
        with (
            patch.object(
                CohereHook,
                "get_connection",
                return_value=Connection(conn_type="cohere", password=api_key, host=base_url),
            ),
            patch("cohere.ClientV2") as client,
        ):
            hook = CohereHook(timeout=timeout)
            _ = hook.get_conn()
            client.assert_called_once_with(api_key=api_key, timeout=timeout, base_url=base_url)

    def test_create_embeddings_passes_request_options_to_embed(self):
        embeddings = [[0.1, 0.2], [0.3, 0.4]]
        response = SimpleNamespace(embeddings=SimpleNamespace(float_=embeddings))
        client = MagicMock(spec=["embed"])
        client.embed.return_value = response
        request_options = {"max_retries": 2, "timeout_in_seconds": 30}

        hook = CohereHook(request_options=request_options)
        with patch.object(CohereHook, "get_conn", autospec=True, return_value=client) as get_conn:
            result = hook.create_embeddings(texts=["hello", "world"], model="embed-english-v3.0")

        assert result == embeddings
        get_conn.assert_called_once_with(hook)
        client.embed.assert_called_once_with(
            texts=["hello", "world"],
            model="embed-english-v3.0",
            input_type="search_document",
            embedding_types=["float"],
            request_options=request_options,
        )

    def test_create_embeddings_raises_when_float_embeddings_are_missing(self):
        response = SimpleNamespace(embeddings=SimpleNamespace(float_=None))
        client = MagicMock(spec=["embed"])
        client.embed.return_value = response

        hook = CohereHook()
        with (
            patch.object(CohereHook, "get_conn", autospec=True, return_value=client),
            pytest.raises(ValueError, match="Embeddings response is missing float_ field"),
        ):
            hook.create_embeddings(texts=["hello"])

    def test_connection_returns_success_tuple(self):
        client = MagicMock(spec=["chat"])
        messages = [{"role": "user", "content": "hello"}]

        hook = CohereHook()
        with patch.object(CohereHook, "get_conn", autospec=True, return_value=client) as get_conn:
            result = hook.test_connection(model="command-a-03-2025", messages=messages)

        assert result == (True, "Connection successfully established.")
        get_conn.assert_called_once_with(hook)
        client.chat.assert_called_once_with(model="command-a-03-2025", messages=messages)

    def test_connection_returns_failure_tuple_with_error_message(self):
        client = MagicMock(spec=["chat"])
        client.chat.side_effect = RuntimeError("invalid API key")
        messages = [{"role": "user", "content": "hello"}]

        hook = CohereHook()
        with patch.object(CohereHook, "get_conn", autospec=True, return_value=client) as get_conn:
            result = hook.test_connection(model="command-a-03-2025", messages=messages)

        assert result == (False, "Unexpected error: invalid API key")
        get_conn.assert_called_once_with(hook)
        client.chat.assert_called_once_with(model="command-a-03-2025", messages=messages)
