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

from unittest.mock import MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.cohere.operators.embedding import CohereEmbeddingOperator


@pytest.mark.parametrize(
    ("input_text", "expected_input"),
    [
        pytest.param(
            ["On Kernel-Target Alignment. We describe a family of global optimization procedures"],
            ["On Kernel-Target Alignment. We describe a family of global optimization procedures"],
            id="input-as-list",
        ),
        pytest.param(
            "On Kernel-Target Alignment. We describe a family of global optimization procedures",
            ["On Kernel-Target Alignment. We describe a family of global optimization procedures"],
            id="input-as-string",
        ),
    ],
)
@patch("airflow.providers.cohere.hooks.cohere.CohereHook.get_connection")
@patch("cohere.ClientV2")
def test_cohere_embedding_operator(cohere_client, get_connection, input_text, expected_input):
    """
    Test Cohere client is getting called with the correct key, that a string ``input_text`` is
    normalized to a list at execute time, and that the execute method returns expected response.
    """
    embedded_obj = [[1.0, 2.0, 3.0]]

    mock_response = MagicMock()
    mock_response.embeddings.float_ = embedded_obj

    api_key = "test"
    base_url = "http://some_host.com"
    timeout = 150
    request_options = None

    get_connection.return_value = Connection(conn_type="cohere", password=api_key, host=base_url)
    client_obj = MagicMock()
    cohere_client.return_value = client_obj
    client_obj.embed.return_value = mock_response

    op = CohereEmbeddingOperator(
        task_id="embed",
        conn_id="some_conn",
        input_text=input_text,
        timeout=timeout,
        request_options=request_options,
    )

    val = op.execute(context={})
    cohere_client.assert_called_once_with(api_key=api_key, base_url=base_url, timeout=timeout)
    client_obj.embed.assert_called_once()
    assert client_obj.embed.call_args.kwargs["texts"] == expected_input
    assert val == embedded_obj


@patch("airflow.providers.cohere.operators.embedding.CohereEmbeddingOperator.hook")
def test_single_string_input_text_normalized_at_execute(mock_hook):
    """
    A single-string ``input_text`` is wrapped into a list in ``execute()`` (after templating),
    not in ``__init__``. ``input_text`` is a template field, so wrapping it in the constructor
    would operate on the un-rendered Jinja expression.
    """
    op = CohereEmbeddingOperator(task_id="embed", conn_id="some_conn", input_text="single text")

    # __init__ keeps the value verbatim; the list normalization is deferred to execute().
    assert op.input_text == "single text"

    op.execute(context={})
    mock_hook.create_embeddings.assert_called_once_with(["single text"])


@patch("airflow.providers.cohere.operators.embedding.CohereEmbeddingOperator.hook")
def test_list_input_text_passed_through_at_execute(mock_hook):
    """A list ``input_text`` is passed to the hook unchanged."""
    texts = ["first", "second"]
    op = CohereEmbeddingOperator(task_id="embed", conn_id="some_conn", input_text=texts)

    op.execute(context={})
    mock_hook.create_embeddings.assert_called_once_with(texts)
