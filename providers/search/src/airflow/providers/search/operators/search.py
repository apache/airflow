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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.common.compat.sdk import BaseOperator
from airflow.providers.search.hooks.search import SearchHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class SearchQueryOperator(BaseOperator):
    """
    Run a search against Elasticsearch or OpenSearch and return the response.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SearchQueryOperator`

    :param query: Search request body, e.g. ``{"query": {"match": {"title": "airflow"}}}``.
    :param index_name: Index, pattern or comma-separated list to search.
    :param search_conn_id: The :ref:`search connection id <howto/connection:search>`.
    :param log_query: Log the query before running it.
    """

    template_fields: Sequence[str] = ("query", "index_name", "search_conn_id")
    template_fields_renderers = {"query": "json"}

    def __init__(
        self,
        *,
        query: dict[str, Any],
        index_name: str,
        search_conn_id: str = SearchHook.default_conn_name,
        log_query: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.index_name = index_name
        self.search_conn_id = search_conn_id
        self.log_query = log_query

    @cached_property
    def hook(self) -> SearchHook:
        return SearchHook(search_conn_id=self.search_conn_id, log_query=self.log_query)

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.search(self.query, self.index_name)


class SearchCreateIndexOperator(BaseOperator):
    """
    Create an index in Elasticsearch or OpenSearch.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SearchCreateIndexOperator`

    :param index_name: Name of the index to create.
    :param index_body: Optional ``settings``, ``mappings`` and ``aliases`` of the index.
    :param search_conn_id: The :ref:`search connection id <howto/connection:search>`.
    """

    template_fields: Sequence[str] = ("index_name", "index_body", "search_conn_id")
    template_fields_renderers = {"index_body": "json"}

    def __init__(
        self,
        *,
        index_name: str,
        index_body: dict[str, Any] | None = None,
        search_conn_id: str = SearchHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.index_name = index_name
        self.index_body = index_body
        self.search_conn_id = search_conn_id

    @cached_property
    def hook(self) -> SearchHook:
        return SearchHook(search_conn_id=self.search_conn_id)

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.create_index(self.index_name, self.index_body)


class SearchAddDocumentOperator(BaseOperator):
    """
    Add a document to an Elasticsearch or OpenSearch index, or replace the one with the same id.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:SearchAddDocumentOperator`

    :param index_name: Index to write to.
    :param document: The document.
    :param doc_id: Id of the document; the cluster assigns one when not given.
    :param search_conn_id: The :ref:`search connection id <howto/connection:search>`.
    """

    template_fields: Sequence[str] = ("index_name", "document", "doc_id", "search_conn_id")
    template_fields_renderers = {"document": "json"}

    def __init__(
        self,
        *,
        index_name: str,
        document: dict[str, Any],
        doc_id: str | int | None = None,
        search_conn_id: str = SearchHook.default_conn_name,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.index_name = index_name
        self.document = document
        self.doc_id = doc_id
        self.search_conn_id = search_conn_id

    @cached_property
    def hook(self) -> SearchHook:
        return SearchHook(search_conn_id=self.search_conn_id)

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.index_document(self.document, self.index_name, doc_id=self.doc_id)
