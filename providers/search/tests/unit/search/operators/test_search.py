#
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

from airflow.providers.search.hooks.search import SearchHook
from airflow.providers.search.operators.search import (
    SearchAddDocumentOperator,
    SearchCreateIndexOperator,
    SearchQueryOperator,
)


@mock.patch.object(SearchHook, "search", autospec=True)
def test_query_operator(mock_search):
    op = SearchQueryOperator(task_id="q", query={"query": {}}, index_name="orders", search_conn_id="c")

    result = op.execute({})

    assert result is mock_search.return_value
    assert mock_search.call_args.args[1:] == ({"query": {}}, "orders")
    assert (op.hook.search_conn_id, op.hook.log_query) == ("c", True)


@mock.patch.object(SearchHook, "create_index", autospec=True)
def test_create_index_operator(mock_create):
    op = SearchCreateIndexOperator(task_id="c", index_name="orders", index_body={"mappings": {}})

    op.execute({})

    assert mock_create.call_args.args[1:] == ("orders", {"mappings": {}})


@mock.patch.object(SearchHook, "index_document", autospec=True)
def test_add_document_operator(mock_index):
    op = SearchAddDocumentOperator(task_id="a", index_name="orders", document={"n": 1}, doc_id=7)

    op.execute({})

    assert mock_index.call_args.args[1:] == ({"n": 1}, "orders")
    assert mock_index.call_args.kwargs == {"doc_id": 7}
