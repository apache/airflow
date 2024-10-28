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

from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from opensearchpy import RequestsHttpConnection
from opensearchpy.exceptions import OpenSearchException

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.opensearch.hooks.opensearch import OpenSearchHook

if TYPE_CHECKING:
    from opensearchpy import Connection as OpenSearchConnectionClass

    from airflow.utils.context import Context


class OpenSearchQueryOperator(BaseOperator):
    """
    Run a query search against a given index on an OpenSearch cluster and returns results.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenSearchQueryOperator`

    :param query: A Dictionary OpenSearch DSL query.
    :param search_object: A Search object from opensearch-dsl.
    :param index_name: The name of the index to search for documents.
    :param opensearch_conn_id: opensearch connection to use
    :param opensearch_conn_class: opensearch connection class to use
    :param log_query: Whether to log the query used. Defaults to True and logs query used.
    """

    template_fields: Sequence[str] = ["query"]

    def __init__(
        self,
        *,
        query: dict | None = None,
        search_object: Any | None = None,
        index_name: str | None = None,
        opensearch_conn_id: str = "opensearch_default",
        opensearch_conn_class: type[OpenSearchConnectionClass]
        | None = RequestsHttpConnection,
        log_query: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query = query
        self.index_name = index_name
        self.opensearch_conn_id = opensearch_conn_id
        self.opensearch_conn_class = opensearch_conn_class
        self.log_query = log_query
        self.search_object = search_object

    @cached_property
    def hook(self) -> OpenSearchHook:
        """Get an instance of an OpenSearchHook."""
        return OpenSearchHook(
            open_search_conn_id=self.opensearch_conn_id,
            open_search_conn_class=self.opensearch_conn_class,
            log_query=self.log_query,
        )

    def execute(self, context: Context) -> Any:
        """Execute a search against a given index or a Search object on an OpenSearch Cluster."""
        result = None

        if self.query is not None:
            if not self.query.get("query"):
                raise AirflowException(
                    "Query input is missing required field Query in dictionary"
                )
            if self.index_name is None:
                raise AirflowException(
                    "Index name is required when using the query input."
                )
            try:
                result = self.hook.search(index_name=self.index_name, query=self.query)
            except OpenSearchException as e:
                raise AirflowException(e)
        elif self.search_object is not None:
            try:
                result = self.search_object.using(self.hook.client).execute()
            except OpenSearchException as e:
                raise AirflowException(e)
        else:
            raise AirflowException(
                """Input missing required input of query or search_object.
                Either query or search_object is required."""
            )
        return result


class OpenSearchCreateIndexOperator(BaseOperator):
    """
    Create a new index on an OpenSearch cluster with a given index name.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenSearchCreateIndexOperator`

    :param index_name: The name of the index to be created.
    :param index_body: A dictionary that defines index settings
    :param opensearch_conn_id: opensearch connection to use
    """

    def __init__(
        self,
        *,
        index_name: str,
        index_body: dict[str, Any],
        opensearch_conn_id: str = "opensearch_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.index_name = index_name
        self.index_body = index_body
        self.opensearch_conn_id = opensearch_conn_id

    @cached_property
    def hook(self) -> OpenSearchHook:
        """Get an instance of an OpenSearchHook."""
        return OpenSearchHook(
            open_search_conn_id=self.opensearch_conn_id, log_query=False
        )

    def execute(self, context: Context) -> Any:
        """Create an index on an OpenSearch cluster."""
        try:
            self.hook.client.indices.create(index=self.index_name, body=self.index_body)
        except OpenSearchException as e:
            raise AirflowException(e)


class OpenSearchAddDocumentOperator(BaseOperator):
    """
    Add a new document to a given Index or overwrite an existing one.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenSearchAddDocumentOperator`

    :param index_name: The name of the index to put the document.
    :param document: A dictionary representation of the document.
    :param document_id: The id for the document in the index.
    :param doc_class: A Document subclassed object using opensearch-dsl
    :param opensearch_conn_id: opensearch connection to use
    """

    def __init__(
        self,
        *,
        index_name: str | None = None,
        document: dict[str, Any] | None = None,
        doc_id: int | None = None,
        doc_class: Any | None = None,
        opensearch_conn_id: str = "opensearch_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.index_name = index_name
        self.document = document
        self.doc_id = doc_id
        self.doc_class = doc_class
        self.opensearch_conn_id = opensearch_conn_id

    @cached_property
    def hook(self) -> OpenSearchHook:
        """Get an instance of an OpenSearchHook."""
        return OpenSearchHook(
            open_search_conn_id=self.opensearch_conn_id, log_query=False
        )

    def execute(self, context: Context) -> Any:
        """Save a document to a given index on an OpenSearch cluster."""
        if self.doc_class is not None:
            try:
                doc = self.doc_class.init(using=self.hook.client)
                result = doc.save(using=self.hook.client)
            except OpenSearchException as e:
                raise AirflowException(e)
        elif (
            self.index_name is not None
            and self.document is not None
            and self.doc_id is not None
        ):
            try:
                result = self.hook.index(
                    index_name=self.index_name, document=self.document, doc_id=self.doc_id
                )
            except OpenSearchException as e:
                raise AirflowException(e)
        else:
            raise AirflowException(
                "Index name, document dictionary and doc_id or a Document subclassed object is required."
            )

        return result
