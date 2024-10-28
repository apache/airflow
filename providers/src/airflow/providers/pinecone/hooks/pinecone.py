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
import os
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pinecone import Pinecone, PodSpec, ServerlessSpec

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from pinecone import Vector
    from pinecone.core.client.model.sparse_values import SparseValues
    from pinecone.core.client.models import (
        DescribeIndexStatsResponse,
        QueryResponse,
        UpsertResponse,
    )

    from airflow.models.connection import Connection


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

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "region": StringField(
                lazy_gettext("Pinecone Region"), widget=BS3TextFieldWidget(), default=None
            ),
            "debug_curl": BooleanField(
                lazy_gettext("PINECONE_DEBUG_CURL"), default=False
            ),
            "project_id": StringField(
                lazy_gettext("Project ID"),
                widget=BS3TextFieldWidget(),
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {
                "login": "Pinecone Environment",
                "host": "Pinecone Host",
                "password": "Pinecone API key",
            },
        }

    def __init__(
        self,
        conn_id: str = default_conn_name,
        environment: str | None = None,
        region: str | None = None,
    ) -> None:
        self.conn_id = conn_id
        self._environment = environment
        self._region = region

    @property
    def api_key(self) -> str:
        key = self.conn.password
        if not key:
            raise LookupError("Pinecone API Key not found in connection")
        return key

    @cached_property
    def environment(self) -> str:
        if self._environment:
            return self._environment
        env = self.conn.login
        if not env:
            raise LookupError("Pinecone environment not found in connection")
        return env

    @cached_property
    def region(self) -> str:
        if self._region:
            return self._region
        region = self.conn.extra_dejson.get("region")
        if not region:
            raise LookupError("Pinecone region not found in connection")
        return region

    @cached_property
    def pinecone_client(self) -> Pinecone:
        """Pinecone object to interact with Pinecone."""
        pinecone_host = self.conn.host
        extras = self.conn.extra_dejson
        pinecone_project_id = extras.get("project_id")
        enable_curl_debug = extras.get("debug_curl")
        if enable_curl_debug:
            os.environ["PINECONE_DEBUG_CURL"] = "true"
        return Pinecone(
            api_key=self.api_key, host=pinecone_host, project_id=pinecone_project_id
        )

    @cached_property
    def conn(self) -> Connection:
        return self.get_connection(self.conn_id)

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.pinecone_client.list_indexes()
            return True, "Connection established"
        except Exception as e:
            return False, str(e)

    def list_indexes(self) -> Any:
        """Retrieve a list of all indexes in your project."""
        return self.pinecone_client.list_indexes()

    def upsert(
        self,
        index_name: str,
        vectors: list[Vector] | list[tuple] | list[dict],
        namespace: str = "",
        batch_size: int | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> UpsertResponse:
        """
        Write vectors into a namespace.

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
        index = self.pinecone_client.Index(index_name)
        return index.upsert(
            vectors=vectors,
            namespace=namespace,
            batch_size=batch_size,
            show_progress=show_progress,
            **kwargs,
        )

    def get_pod_spec_obj(
        self,
        *,
        replicas: int | None = None,
        shards: int | None = None,
        pods: int | None = None,
        pod_type: str | None = "p1.x1",
        metadata_config: dict | None = None,
        source_collection: str | None = None,
        environment: str | None = None,
    ) -> PodSpec:
        """
        Get a PodSpec object.

        :param replicas: The number of replicas.
        :param shards: The number of shards.
        :param pods: The number of pods.
        :param pod_type: The type of pod.
        :param metadata_config: The metadata configuration.
        :param source_collection: The source collection.
        :param environment: The environment to use when creating the index.
        """
        return PodSpec(
            environment=environment or self.environment,
            replicas=replicas,
            shards=shards,
            pods=pods,
            pod_type=pod_type,
            metadata_config=metadata_config,
            source_collection=source_collection,
        )

    def get_serverless_spec_obj(
        self, *, cloud: str, region: str | None = None
    ) -> ServerlessSpec:
        """
        Get a ServerlessSpec object.

        :param cloud: The cloud provider.
        :param region: The region to use when creating the index.
        """
        return ServerlessSpec(cloud=cloud, region=region or self.region)

    def create_index(
        self,
        index_name: str,
        dimension: int,
        spec: ServerlessSpec | PodSpec,
        metric: str | None = "cosine",
        timeout: int | None = None,
    ) -> None:
        """
        Create a new index.

        :param index_name: The name of the index.
        :param dimension: The dimension of the vectors to be indexed.
        :param spec: Pass a `ServerlessSpec` object to create a serverless index or a `PodSpec` object to create a pod index.
            ``get_serverless_spec_obj`` and ``get_pod_spec_obj`` can be used to create the Spec objects.
        :param metric: The metric to use. Defaults to cosine.
        :param timeout: The timeout to use.
        """
        self.pinecone_client.create_index(
            name=index_name,
            dimension=dimension,
            spec=spec,
            metric=metric,
            timeout=timeout,
        )

    def describe_index(self, index_name: str) -> Any:
        """
        Retrieve information about a specific index.

        :param index_name: The name of the index to describe.
        """
        return self.pinecone_client.describe_index(name=index_name)

    def delete_index(self, index_name: str, timeout: int | None = None) -> None:
        """
        Delete a specific index.

        :param index_name: the name of the index.
        :param timeout: Timeout for wait until index gets ready.
        """
        self.pinecone_client.delete_index(name=index_name, timeout=timeout)

    def configure_index(
        self, index_name: str, replicas: int | None = None, pod_type: str | None = ""
    ) -> None:
        """
        Change the current configuration of the index.

        :param index_name: The name of the index to configure.
        :param replicas: The new number of replicas.
        :param pod_type: the new pod_type for the index.
        """
        self.pinecone_client.configure_index(
            name=index_name, replicas=replicas, pod_type=pod_type
        )

    def create_collection(self, collection_name: str, index_name: str) -> None:
        """
        Create a new collection from a specified index.

        :param collection_name: The name of the collection to create.
        :param index_name: The name of the source index.
        """
        self.pinecone_client.create_collection(name=collection_name, source=index_name)

    def delete_collection(self, collection_name: str) -> None:
        """
        Delete a specific collection.

        :param collection_name: The name of the collection to delete.
        """
        self.pinecone_client.delete_collection(collection_name)

    def describe_collection(self, collection_name: str) -> Any:
        """
        Retrieve information about a specific collection.

        :param collection_name: The name of the collection to describe.
        """
        return self.pinecone_client.describe_collection(collection_name)

    def list_collections(self) -> Any:
        """Retrieve a list of all collections in the current project."""
        return self.pinecone_client.list_collections()

    def query_vector(
        self,
        index_name: str,
        vector: list[Any],
        query_id: str | None = None,
        top_k: int = 10,
        namespace: str | None = None,
        query_filter: dict[str, str | float | int | bool | list[Any] | dict[Any, Any]]
        | None = None,
        include_values: bool | None = None,
        include_metadata: bool | None = None,
        sparse_vector: SparseValues | dict[str, list[float] | list[int]] | None = None,
    ) -> QueryResponse:
        """
        Search a namespace using query vector.

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
        index = self.pinecone_client.Index(index_name)
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
        """Break an iterable into chunks of size batch_size."""
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
        with self.pinecone_client.Index(index_name, pool_threads=pool_threads) as index:
            if async_req and pool_threads:
                async_results = [
                    index.upsert(vectors=chunk, async_req=True)
                    for chunk in self._chunks(data)
                ]
                responses = [async_result.get() for async_result in async_results]
            else:
                for chunk in self._chunks(data):
                    response = index.upsert(vectors=chunk)
                    responses.append(response)
        return responses

    def describe_index_stats(
        self,
        index_name: str,
        stats_filter: dict[str, str | float | int | bool | list[Any] | dict[Any, Any]]
        | None = None,
        **kwargs: Any,
    ) -> DescribeIndexStatsResponse:
        """
        Describe the index statistics.

        Returns statistics about the index's contents. For example: The vector count per
        namespace and the number of dimensions.
        API reference: https://docs.pinecone.io/reference/describe_index_stats_post

        :param index_name: Name of the index.
        :param stats_filter: If this parameter is present, the operation only returns statistics for vectors that
         satisfy the filter. See https://www.pinecone.io/docs/metadata-filtering/
        """
        index = self.pinecone_client.Index(index_name)
        return index.describe_index_stats(filter=stats_filter, **kwargs)
