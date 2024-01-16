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
from typing import Any, Iterable, Mapping, Sequence

from grpc import RpcError
from qdrant_client import QdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse

from airflow.hooks.base import BaseHook


class QdrantHook(BaseHook):
    """
    Hook for interfacing with a Qdrant instance.

    :param conn_id: The connection id to use when connecting to Qdrant. Defaults to `qdrant_default`.
    """

    conn_name_attr = "conn_id"
    conn_type = "qdrant"
    default_conn_name = "qdrant_default"
    hook_name = "Qdrant"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, IntegerField, StringField

        return {
            "url": StringField(
                lazy_gettext("URL"),
                widget=BS3TextFieldWidget(),
                description="Optional. Qualified URL of the Qdrant instance."
                "Example: https://xyz-example.eu-central.aws.cloud.qdrant.io:6333",
            ),
            "grpc_port": IntegerField(
                lazy_gettext("GPRC Port"),
                widget=BS3TextFieldWidget(),
                description="Optional. Port of the gRPC interface.",
                default=None,
            ),
            "prefer_gprc": BooleanField(
                lazy_gettext("Prefer GRPC"),
                widget=BS3TextFieldWidget(),
                description="Optional. Whether to use gPRC interface whenever possible in custom methods.",
                default=True,
            ),
            "https": BooleanField(
                lazy_gettext("HTTPS"),
                widget=BS3TextFieldWidget(),
                description="Optional. Whether to use HTTPS(SSL) protocol.",
            ),
            "prefix": StringField(
                lazy_gettext("Prefix"),
                widget=BS3TextFieldWidget(),
                description="Optional. Prefix to the REST URL path."
                "Example: `service/v1` will result in http://localhost:6333/service/v1/{qdrant-endpoint} for REST API.",
            ),
            "timeout": IntegerField(
                lazy_gettext("Timeout"),
                widget=BS3TextFieldWidget(),
                description="Optional. Timeout for REST and gRPC API requests.",
                default=None,
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["schema", "login", "extra"],
            "relabeling": {"password": "API Key"},
        }

    def __init__(self, conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.get_conn()

    def get_conn(self) -> QdrantClient:
        """Get a Qdrant client instance."""
        connection = self.get_connection(self.conn_id)
        host = connection.host or None
        port = connection.port or 6333
        api_key = connection.password
        extra = connection.extra_dejson
        url = extra.get("url", None)
        grpc_port = extra.get("grpc_port", 6334)
        prefer_gprc = extra.get("prefer_gprc", False)
        https = extra.get("https", False)
        prefix = extra.get("prefix", "")
        timeout = extra.get("timeout", None)

        return QdrantClient(
            host=host,
            port=port,
            url=url,
            api_key=api_key,
            grpc_port=grpc_port,
            prefer_grpc=prefer_gprc,
            https=https,
            prefix=prefix,
            timeout=timeout,
        )

    @cached_property
    def conn(self) -> QdrantClient:
        """Get a Qdrant client instance."""
        return self.get_conn()

    def verify_connection(self) -> tuple[bool, str]:
        """Check the connection to the Qdrant instance."""
        try:
            self.conn.get_collections()
            return True, "Connection established!"
        except (UnexpectedResponse, RpcError, ValueError) as e:
            return False, str(e)

    def list_collections(self) -> list[str]:
        """Get a list of collections in the Qdrant instance."""
        return [collection.name for collection in self.conn.get_collections().collections]

    def upsert(
        self,
        collection_name: str,
        vectors: Iterable[models.VectorStruct],
        payload: Iterable[dict[str, Any]] | None = None,
        ids: Iterable[str | int] | None = None,
        batch_size: int = 64,
        parallel: int = 1,
        method: str | None = None,
        max_retries: int = 3,
        wait: bool = True,
    ) -> None:
        """
        Upload points to a Qdrant collection.

        :param collection_name: Name of the collection to upload points to.
        :param vectors: An iterable over vectors to upload.
        :param payload: Iterable of vectors payload, Optional. Defaults to None.
        :param ids: Iterable of custom vectors ids, Optional. Defaults to None.
        :param batch_size: Number of points to upload per-request. Defaults to 64.
        :param parallel: Number of parallel upload processes. Defaults to 1.
        :param method: Start method for parallel processes. Defaults to forkserver.
        :param max_retries: Number of retries for failed requests. Defaults to 3.
        :param wait: Await for the results to be applied on the server side. Defaults to True.
        """
        return self.conn.upload_collection(
            collection_name=collection_name,
            vectors=vectors,
            payload=payload,
            ids=ids,
            batch_size=batch_size,
            parallel=parallel,
            method=method,
            max_retries=max_retries,
            wait=wait,
        )

    def delete(
        self,
        collection_name: str,
        points_selector: models.PointsSelector,
        wait: bool = True,
        ordering: models.WriteOrdering | None = None,
        shard_key_selector: models.ShardKeySelector | None = None,
    ) -> None:
        """
        Delete points from a Qdrant collection.

        :param collection_name: Name of the collection to delete points from.
        :param points_selector: Selector for points to delete.
        :param wait: Await for the results to be applied on the server side. Defaults to True.
        :param ordering: Ordering of the write operation. Defaults to None.
        :param shard_key_selector: Selector for the shard key. Defaults to None.
        """
        self.conn.delete(
            collection_name=collection_name,
            points_selector=points_selector,
            wait=wait,
            ordering=ordering,
            shard_key_selector=shard_key_selector,
        )

    def search(
        self,
        collection_name: str,
        query_vector: Sequence[float]
        | tuple[str, list[float]]
        | models.NamedVector
        | models.NamedSparseVector,
        query_filter: models.Filter | None = None,
        search_params: models.SearchParams | None = None,
        limit: int = 10,
        offset: int | None = None,
        with_payload: bool | Sequence[str] | models.PayloadSelector = True,
        with_vectors: bool | Sequence[str] = False,
        score_threshold: float | None = None,
        consistency: models.ReadConsistency | None = None,
        shard_key_selector: models.ShardKeySelector | None = None,
        timeout: int | None = None,
    ):
        """
        Search for the closest points in a Qdrant collection.

        :param collection_name: Name of the collection to upload points to.
        :param quey_vector: Query vector to search for.
        :param query_filter: Filter for the query. Defaults to None.
        :param search_params: Additional search parameters. Defaults to None.
        :param limit: Number of results to return. Defaults to 10.
        :param offset: Offset of the first results to return. Defaults to None.
        :param with_payload: To specify which stored payload should be attached to the result. Defaults to True.
        :param with_vectors: To specify whether vectors should be attached to the result. Defaults to False.
        :param score_threshold: To specify the minimum score threshold of the results. Defaults to None.
        :param consistency: Defines how many replicas should be queried before returning the result. Defaults to None.
        :param shard_key_selector: To specify which shards should be queried.. Defaults to None.
        :param wait: Await for the results to be applied on the server side. Defaults to True.
        """
        return self.conn.search(
            collection_name=collection_name,
            query_vector=query_vector,
            query_filter=query_filter,
            search_params=search_params,
            limit=limit,
            offset=offset,
            with_payload=with_payload,
            with_vectors=with_vectors,
            score_threshold=score_threshold,
            consistency=consistency,
            shard_key_selector=shard_key_selector,
            timeout=timeout,
        )

    def create_collection(
        self,
        collection_name: str,
        vectors_config: models.VectorParams | Mapping[str, models.VectorParams],
        sparse_vectors_config: Mapping[str, models.SparseVectorParams] | None = None,
        shard_number: int | None = None,
        sharding_method: models.ShardingMethod | None = None,
        replication_factor: int | None = None,
        write_consistency_factor: int | None = None,
        on_disk_payload: bool | None = None,
        hnsw_config: models.HnswConfigDiff | None = None,
        optimizers_config: models.OptimizersConfigDiff | None = None,
        wal_config: models.WalConfigDiff | None = None,
        quantization_config: models.QuantizationConfig | None = None,
        init_from: models.InitFrom | None = None,
        timeout: int | None = None,
    ) -> bool:
        """
        Create a new Qdrant collection.

        :param collection_name: Name of the collection to upload points to.
        :param vectors_config: Configuration of the vector storage contains size and distance for the vectors.
        :param sparse_vectors_config: Configuration of the sparse vector storage. Defaults to None.
        :param shard_number: Number of shards in collection. Default is 1, minimum is 1.
        :param sharding_method: Defines strategy for shard creation. Defaults to auto.
        :param replication_factor: Replication factor for collection. Default is 1, minimum is 1.
        :param write_consistency_factor: Write consistency factor for collection. Default is 1, minimum is 1.
        :param on_disk_payload: If true - point`s payload will not be stored in memory.
        :param hnsw_config: Parameters for HNSW index.
        :param optimizers_config: Parameters for optimizer.
        :param wal_config: Parameters for Write-Ahead-Log.
        :param quantization_config: Parameters for quantization, if None - quantization will be disabled.
        :param init_from: Whether to use data stored in another collection to initialize this collection.
        :param timeout: Timeout for the request. Defaults to None.
        """
        return self.conn.create_collection(
            collection_name=collection_name,
            vectors_config=vectors_config,
            sparse_vectors_config=sparse_vectors_config,
            shard_number=shard_number,
            sharding_method=sharding_method,
            replication_factor=replication_factor,
            write_consistency_factor=write_consistency_factor,
            on_disk_payload=on_disk_payload,
            hnsw_config=hnsw_config,
            optimizers_config=optimizers_config,
            wal_config=wal_config,
            quantization_config=quantization_config,
            init_from=init_from,
            timeout=timeout,
        )

    def get_collection(self, collection_name: str) -> models.CollectionInfo:
        """
        Get information about a Qdrant collection.

        :param collection_name: Name of the collection to get information about.
        """
        return self.conn.get_collection(collection_name=collection_name)

    def delete_collection(self, collection_name: str, timeout: int | None) -> bool:
        """
        Delete a Qdrant collection.

        :param collection_name: Name of the collection to delete.
        """
        return self.conn.delete_collection(collection_name=collection_name, timeout=timeout)
