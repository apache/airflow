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
from airflow.providers.pinecone.hooks.pinecone import PineconeHook

if TYPE_CHECKING:
    from pinecone import Vector

    try:
        from airflow.sdk.definitions.context import Context
    except ImportError:
        from airflow.utils.context import Context


class PineconeIngestOperator(BaseOperator):
    """
    Ingest vector embeddings into Pinecone.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PineconeIngestOperator`

    :param conn_id: The connection id to use when connecting to Pinecone.
    :param index_name: Name of the Pinecone index.
    :param input_vectors: Data to be ingested, in the form of a list of vectors, list of tuples,
        or list of dictionaries.
    :param namespace: The namespace to write to. If not specified, the default namespace is used.
    :param batch_size: The number of vectors to upsert in each batch.
    :param upsert_kwargs: .. seealso:: https://docs.pinecone.io/reference/upsert
    """

    template_fields: Sequence[str] = ("index_name", "input_vectors", "namespace")

    def __init__(
        self,
        *,
        conn_id: str = PineconeHook.default_conn_name,
        index_name: str,
        input_vectors: list[Vector] | list[tuple] | list[dict],
        namespace: str = "",
        batch_size: int | None = None,
        upsert_kwargs: dict | None = None,
        **kwargs: Any,
    ) -> None:
        self.upsert_kwargs = upsert_kwargs or {}
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.index_name = index_name
        self.namespace = namespace
        self.batch_size = batch_size
        self.input_vectors = input_vectors

    @cached_property
    def hook(self) -> PineconeHook:
        """Return an instance of the PineconeHook."""
        return PineconeHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> None:
        """Ingest data into Pinecone using the PineconeHook."""
        self.hook.upsert(
            index_name=self.index_name,
            vectors=self.input_vectors,
            namespace=self.namespace,
            batch_size=self.batch_size,
            **self.upsert_kwargs,
        )

        self.log.info("Successfully ingested data into Pinecone index %s.", self.index_name)


class CreatePodIndexOperator(BaseOperator):
    """
    Create a pod based index in Pinecone.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CreatePodIndexOperator`

    :param conn_id: The connection id to use when connecting to Pinecone.
    :param index_name: Name of the Pinecone index.
    :param dimension: The dimension of the vectors to be indexed.
    :param environment: The environment to use when creating the index.
    :param replicas: The number of replicas to use.
    :param shards: The number of shards to use.
    :param pods: The number of pods to use.
    :param pod_type: The type of pod to use. Defaults to p1.x1
    :param metadata_config: The metadata configuration to use.
    :param source_collection: The source collection to use.
    :param metric: The metric to use. Defaults to cosine.
    :param timeout: The timeout to use.
    """

    def __init__(
        self,
        *,
        conn_id: str = PineconeHook.default_conn_name,
        index_name: str,
        dimension: int,
        environment: str | None = None,
        replicas: int | None = None,
        shards: int | None = None,
        pods: int | None = None,
        pod_type: str = "p1.x1",
        metadata_config: dict | None = None,
        source_collection: str | None = None,
        metric: str = "cosine",
        timeout: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.index_name = index_name
        self.dimension = dimension
        self.environment = environment
        self.replicas = replicas
        self.shards = shards
        self.pods = pods
        self.pod_type = pod_type
        self.metadata_config = metadata_config
        self.source_collection = source_collection
        self.metric = metric
        self.timeout = timeout

    @cached_property
    def hook(self) -> PineconeHook:
        return PineconeHook(conn_id=self.conn_id, environment=self.environment)

    def execute(self, context: Context) -> None:
        pod_spec_obj = self.hook.get_pod_spec_obj(
            replicas=self.replicas,
            shards=self.shards,
            pods=self.pods,
            pod_type=self.pod_type,
            metadata_config=self.metadata_config,
            source_collection=self.source_collection,
            environment=self.environment,
        )
        self.hook.create_index(
            index_name=self.index_name,
            dimension=self.dimension,
            spec=pod_spec_obj,
            metric=self.metric,
            timeout=self.timeout,
        )


class CreateServerlessIndexOperator(BaseOperator):
    """
    Create a serverless index in Pinecone.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CreateServerlessIndexOperator`

    :param conn_id: The connection id to use when connecting to Pinecone.
    :param index_name: Name of the Pinecone index.
    :param dimension: The dimension of the vectors to be indexed.
    :param cloud: The cloud to use when creating the index.
    :param region: The region to use when creating the index.
    :param metric: The metric to use.
    :param timeout: The timeout to use.
    """

    def __init__(
        self,
        *,
        conn_id: str = PineconeHook.default_conn_name,
        index_name: str,
        dimension: int,
        cloud: str,
        region: str | None = None,
        metric: str | None = None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.index_name = index_name
        self.dimension = dimension
        self.cloud = cloud
        self.region = region
        self.metric = metric
        self.timeout = timeout

    @cached_property
    def hook(self) -> PineconeHook:
        return PineconeHook(conn_id=self.conn_id, region=self.region)

    def execute(self, context: Context) -> None:
        serverless_spec_obj = self.hook.get_serverless_spec_obj(cloud=self.cloud, region=self.region)
        self.hook.create_index(
            index_name=self.index_name,
            dimension=self.dimension,
            spec=serverless_spec_obj,
            metric=self.metric,
            timeout=self.timeout,
        )
