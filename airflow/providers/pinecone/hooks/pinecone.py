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

"""Hook for Pinecone."""
from __future__ import annotations

import itertools
from typing import TYPE_CHECKING, Any

import pinecone

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from pinecone.core.client.model.sparse_values import SparseValues
    from pinecone.core.client.models import DescribeIndexStatsResponse, QueryResponse, UpsertResponse


class PineconeHook(BaseHook):
    """
    Interact with Pinecone. This hook uses the Pinecone conn_id.

    :param conn_id: Optional, default connection id is `pinecone_default`. The connection id to use when
        connecting to Pinecone.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "pinecone_default"
    conn_type = "pinecone"
    hook_name = "Pinecone"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "log_level": StringField(lazy_gettext("Log Level"), widget=BS3TextFieldWidget(), default=None),
            "project_id": StringField(
                lazy_gettext("Project ID"),
                widget=BS3TextFieldWidget(),
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {"login": "Pinecone Environment", "password": "Pinecone API key"},
        }

    def __init__(self, conn_id: str = default_conn_name) -> None:
        self.conn_id = conn_id
        self.get_conn()

    def get_conn(self) -> None:
        pinecone_connection = self.get_connection(self.conn_id)
        api_key = pinecone_connection.password
        pinecone_environment = pinecone_connection.login
        pinecone_host = pinecone_connection.host
        extras = pinecone_connection.extra_dejson
        pinecone_project_id = extras.get("project_id")
        log_level = extras.get("log_level", None)
        pinecone.init(
            api_key=api_key,
            environment=pinecone_environment,
            host=pinecone_host,
            project_name=pinecone_project_id,
            log_level=log_level,
        )

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.list_indexes()
            return True, "Connection established"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def list_indexes() -> Any:
        """Retrieve a list of all indexes in your project."""
        return pinecone.list_indexes()

    @staticmethod
    def upsert(
        index_name: str,
        vectors: list[Any],
        namespace: str = "",
        batch_size: int | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> UpsertResponse:
        """
        The upsert operation writes vectors into a namespace.

        If a new value is upserted for an existing vector id, it will overwrite the previous value.

         .. seealso:: https://docs.pinecone.io/reference/upsert

        To upsert in parallel follow

        .. seealso:: https://docs.pinecone.io/docs/insert-data#sending-upserts-in-parallel

        :param index_name: The name of the index to describe.
        :param vectors: A list of vectors to upsert.
        :param namespace: The namespace to write to. If not specified, the default namespace - "" is used.
        :param batch_size: The number of vectors to upsert in each batch.
        :param show_progress: Whether to show a progress bar using tqdm. Applied only
            if batch_size is provided.
        """
        index = pinecone.Index(index_name)
        return index.upsert(
            vectors=vectors,
            namespace=namespace,
            batch_size=batch_size,
            show_progress=show_progress,
            **kwargs,
        )

    @staticmethod
    def create_index(
        index_name: str,
        dimension: int,
        index_type: str | None = "approximated",
        metric: str | None = "cosine",
        replicas: int | None = 1,
        shards: int | None = 1,
        pods: int | None = 1,
        pod_type: str | None = "p1",
        index_config: dict[str, str] | None = None,
        metadata_config: dict[str, str] | None = None,
        source_collection: str | None = "",
        timeout: int | None = None,
    ) -> None:
        """
        Create a new index.

        .. seealso:: https://docs.pinecone.io/reference/create_index/

        :param index_name: The name of the index to create.
        :param dimension: the dimension of vectors that would be inserted in the index
        :param index_type: type of index, one of {"approximated", "exact"}, defaults to "approximated".
        :param metric: type of metric used in the vector index, one of {"cosine", "dotproduct", "euclidean"}
        :param replicas: the number of replicas, defaults to 1.
        :param shards: the number of shards per index, defaults to 1.
        :param pods: Total number of pods to be used by the index. pods = shard*replicas
        :param pod_type: the pod type to be used for the index. can be one of p1 or s1.
        :param index_config: Advanced configuration options for the index
        :param metadata_config: Configuration related to the metadata index
        :param source_collection: Collection name to create the index from
        :param timeout: Timeout for wait until index gets ready.
        """
        pinecone.create_index(
            name=index_name,
            timeout=timeout,
            index_type=index_type,
            dimension=dimension,
            metric=metric,
            pods=pods,
            replicas=replicas,
            shards=shards,
            pod_type=pod_type,
            metadata_config=metadata_config,
            source_collection=source_collection,
            index_config=index_config,
        )

    @staticmethod
    def describe_index(index_name: str) -> Any:
        """
        Retrieve information about a specific index.

        :param index_name: The name of the index to describe.
        """
        return pinecone.describe_index(name=index_name)

    @staticmethod
    def delete_index(index_name: str, timeout: int | None = None) -> None:
        """
        Delete a specific index.

        :param index_name: the name of the index.
        :param timeout: Timeout for wait until index gets ready.
        """
        pinecone.delete_index(name=index_name, timeout=timeout)

    @staticmethod
    def configure_index(index_name: str, replicas: int | None = None, pod_type: str | None = "") -> None:
        """
        Changes current configuration of the index.

        :param index_name: The name of the index to configure.
        :param replicas: The new number of replicas.
        :param pod_type: the new pod_type for the index.
        """
        pinecone.configure_index(name=index_name, replicas=replicas, pod_type=pod_type)

    @staticmethod
    def create_collection(collection_name: str, index_name: str) -> None:
        """
        Create a new collection from a specified index.

        :param collection_name: The name of the collection to create.
        :param index_name: The name of the source index.
        """
        pinecone.create_collection(name=collection_name, source=index_name)

    @staticmethod
    def delete_collection(collection_name: str) -> None:
        """
        Delete a specific collection.

        :param collection_name: The name of the collection to delete.
        """
        pinecone.delete_collection(collection_name)

    @staticmethod
    def describe_collection(collection_name: str) -> Any:
        """
        Retrieve information about a specific collection.

        :param collection_name: The name of the collection to describe.
        """
        return pinecone.describe_collection(collection_name)

    @staticmethod
    def list_collections() -> Any:
        """Retrieve a list of all collections in the current project."""
        return pinecone.list_collections()

    @staticmethod
    def query_vector(
        index_name: str,
        vector: list[Any],
        query_id: str | None = None,
        top_k: int = 10,
        namespace: str | None = None,
        query_filter: dict[str, str | float | int | bool | list[Any] | dict[Any, Any]] | None = None,
        include_values: bool | None = None,
        include_metadata: bool | None = None,
        sparse_vector: SparseValues | dict[str, list[float] | list[int]] | None = None,
    ) -> QueryResponse:
        """
        The Query operation searches a namespace, using a query vector.

        It retrieves the ids of the most similar items in a namespace, along with their similarity scores.
        API reference: https://docs.pinecone.io/reference/query

        :param index_name: The name of the index to query.
        :param vector: The query vector.
        :param query_id: The unique ID of the vector to be used as a query vector.
        :param top_k: The number of results to return.
        :param namespace: The namespace to fetch vectors from. If not specified, the default namespace is used.
        :param query_filter: The filter to apply. See https://www.pinecone.io/docs/metadata-filtering/
        :param include_values: Whether to include the vector values in the result.
        :param include_metadata: Indicates whether metadata is included in the response as well as the ids.
        :param sparse_vector: sparse values of the query vector. Expected to be either a SparseValues object or a dict
         of the form: {'indices': List[int], 'values': List[float]}, where the lists each have the same length.
        """
        index = pinecone.Index(index_name)
        return index.query(
            vector=vector,
            id=query_id,
            top_k=top_k,
            namespace=namespace,
            filter=query_filter,
            include_values=include_values,
            include_metadata=include_metadata,
            sparse_vector=sparse_vector,
        )

    @staticmethod
    def _chunks(iterable: list[Any], batch_size: int = 100) -> Any:
        """Helper function to break an iterable into chunks of size batch_size."""
        it = iter(iterable)
        chunk = tuple(itertools.islice(it, batch_size))
        while chunk:
            yield chunk
            chunk = tuple(itertools.islice(it, batch_size))

    def upsert_data_async(
        self,
        index_name: str,
        data: list[tuple[Any]],
        async_req: bool = False,
        pool_threads: int | None = None,
    ) -> None | list[Any]:
        """
        Upserts (insert/update) data into the Pinecone index.

        :param index_name: Name of the index.
        :param data: List of tuples to be upserted. Each tuple is of form (id, vector, metadata).
                     Metadata is optional.
        :param async_req: If True, upsert operations will be asynchronous.
        :param pool_threads: Number of threads for parallel upserting. If async_req is True, this must be provided.
        """
        responses = []
        with pinecone.Index(index_name, pool_threads=pool_threads) as index:
            if async_req and pool_threads:
                async_results = [index.upsert(vectors=chunk, async_req=True) for chunk in self._chunks(data)]
                responses = [async_result.get() for async_result in async_results]
            else:
                for chunk in self._chunks(data):
                    response = index.upsert(vectors=chunk)
                    responses.append(response)
        return responses

    @staticmethod
    def describe_index_stats(
        index_name: str,
        stats_filter: dict[str, str | float | int | bool | list[Any] | dict[Any, Any]] | None = None,
        **kwargs: Any,
    ) -> DescribeIndexStatsResponse:
        """
        Describes the index statistics.

        Returns statistics about the index's contents. For example: The vector count per
        namespace and the number of dimensions.
        API reference: https://docs.pinecone.io/reference/describe_index_stats_post

        :param index_name: Name of the index.
        :param stats_filter: If this parameter is present, the operation only returns statistics for vectors that
         satisfy the filter. See https://www.pinecone.io/docs/metadata-filtering/
        """
        index = pinecone.Index(index_name)
        return index.describe_index_stats(filter=stats_filter, **kwargs)
