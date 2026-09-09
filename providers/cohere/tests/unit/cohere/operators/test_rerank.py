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

from unittest import mock

from airflow.providers.cohere.hooks.cohere import CohereHook
from airflow.providers.cohere.operators.rerank import CohereRerankOperator


@mock.patch.object(CohereRerankOperator, "hook", new_callable=mock.PropertyMock)
def test_execute_coerces_templated_limits_and_forwards_model(mock_hook_property):
    expected = {"id": "rerank-id", "results": [{"index": 1, "relevance_score": 0.9}]}
    mock_hook = mock.create_autospec(CohereHook, instance=True)
    mock_hook_property.return_value = mock_hook
    mock_hook.rerank.return_value = expected
    documents = ["first", "second"]
    operator = CohereRerankOperator(
        task_id="rerank",
        query="Where is the capital?",
        documents=documents,
        model="rerank-v3.5",
        top_n="1",
        max_tokens_per_doc="512",
    )

    result = operator.execute(context={})

    mock_hook.rerank.assert_called_once_with(
        query="Where is the capital?",
        documents=documents,
        model="rerank-v3.5",
        top_n=1,
        max_tokens_per_doc=512,
    )
    assert result == expected


@mock.patch.object(CohereRerankOperator, "hook", new_callable=mock.PropertyMock)
def test_execute_omits_unset_optional_arguments(mock_hook_property):
    mock_hook = mock.create_autospec(CohereHook, instance=True)
    mock_hook_property.return_value = mock_hook
    operator = CohereRerankOperator(
        task_id="rerank",
        query="Where is the capital?",
        documents=["first", "second"],
    )

    operator.execute(context={})

    mock_hook.rerank.assert_called_once_with(query="Where is the capital?", documents=["first", "second"])


@mock.patch("airflow.providers.cohere.operators.rerank.CohereHook", autospec=True)
def test_hook_uses_operator_connection_options(mock_hook_class):
    request_options = {"timeout_in_seconds": 10}
    operator = CohereRerankOperator(
        task_id="rerank",
        query="Where is the capital?",
        documents=["first", "second"],
        conn_id="cohere_custom",
        timeout=30,
        request_options=request_options,
    )

    assert operator.hook == mock_hook_class.return_value
    mock_hook_class.assert_called_once_with(
        conn_id="cohere_custom",
        timeout=30,
        request_options=request_options,
    )
