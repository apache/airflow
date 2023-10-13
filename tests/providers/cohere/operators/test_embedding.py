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

from airflow.exceptions import AirflowException
from airflow.providers.cohere.operators.embedding import CohereEmbeddingOperator


@patch("astronomer_providers_llm.providers.cohere.hooks.cohere.CohereHook._get_api_key")
@patch("cohere.Client")
def test_cohere_embedding_operator(cohere_client, _get_api_key):
    """
    Test Cohere client is getting called with the correct key and that
     the execute methods returns expected response.
    """
    embedded_obj = [1, 2, 3]

    class resp:
        embeddings = embedded_obj

    api_key = "test"
    texts = ["On Kernel-Target Alignment. We describe a family of global optimization procedures"]

    _get_api_key.return_value = api_key
    client_obj = MagicMock()
    cohere_client.return_value = client_obj
    client_obj.embed.return_value = resp

    op = CohereEmbeddingOperator(task_id="embed", conn_id="some_conn", input_text=texts)

    val = op.execute(context={})
    cohere_client.assert_called_once_with(api_key)
    assert val == embedded_obj


@pytest.mark.parametrize(
    "inputs, expected_exception, error_message",
    (
        (
            {"input_text": "test", "input_callable": "some_value"},
            RuntimeError,
            "Only one of 'input_text' and 'input_callable' is allowed",
        ),
        ({"input_callable": "str_type"}, AirflowException, "`input_callable` param must be callable"),
        ({}, RuntimeError, "Either one of 'input_text' and 'input_callable' must be provided"),
    ),
)
def test_validate_inputs(inputs, expected_exception, error_message):
    print(
        "inputs : ", inputs, " expected_exception : ", expected_exception, " error_message: ", error_message
    )
    with pytest.raises(expected_exception, match=error_message):
        CohereEmbeddingOperator(task_id="embed", conn_id="some_conn", **inputs)


@pytest.mark.parametrize(
    "inputs, expected_text",
    (
        ({"input_text": ["some text", "some text"]}, ["some text", "some text"]),
        (
            {"input_callable": lambda: ["some other text", "some other text"]},
            ["some other text", "some other text"],
        ),
    ),
)
def test_get_text(inputs, expected_text):
    op = CohereEmbeddingOperator(task_id="embed", conn_id="some_conn", **inputs)
    assert op.text_to_embed == expected_text
