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

from airflow.providers.cohere.hooks.cohere import CohereHook
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from cohere.core.request_options import RequestOptions

    from airflow.providers.common.compat.sdk import Context


class CohereRerankOperator(BaseOperator):
    """
    Rerank documents by their relevance to a query using Cohere's Rerank API.

    :param query: The search query used to rank the documents.
    :param documents: Text documents to compare with the query.
    :param top_n: Maximum number of ranked documents to return. By default, all are returned.
    :param max_tokens_per_doc: Maximum number of tokens retained from each document.
    :param conn_id: Cohere connection id.
    :param timeout: Request timeout in seconds.
    :param request_options: Request-specific configuration passed to the Cohere client.
    """

    template_fields: Sequence[str] = ("query", "documents", "top_n", "max_tokens_per_doc")
    template_fields_renderers = {"documents": "json"}

    def __init__(
        self,
        *,
        query: str,
        documents: list[str],
        top_n: int | None = None,
        max_tokens_per_doc: int | None = None,
        conn_id: str = CohereHook.default_conn_name,
        timeout: int | None = None,
        request_options: RequestOptions | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.documents = documents
        self.top_n = top_n
        self.max_tokens_per_doc = max_tokens_per_doc
        self.conn_id = conn_id
        self.timeout = timeout
        self.request_options = request_options

    @cached_property
    def hook(self) -> CohereHook:
        """Return a Cohere hook."""
        return CohereHook(
            conn_id=self.conn_id,
            timeout=self.timeout,
            request_options=self.request_options,
        )

    def execute(self, context: Context) -> dict[str, Any]:
        """Rerank the documents and return an XCom-serializable response."""
        return self.hook.rerank(
            query=self.query,
            documents=self.documents,
            top_n=self.top_n,
            max_tokens_per_doc=self.max_tokens_per_doc,
        )
