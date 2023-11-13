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

from airflow.models import BaseOperator
from airflow.providers.pinecone.hooks.pinecone import PineconeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PineconeIngestOperator(BaseOperator):
    """
    Ingest vector embeddings into Pinecone.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:PineconeIngestOperator`

    :param conn_id: The connection id to use when connecting to Pinecone.
    :param index_name: Name of the Pinecone index.
    :param input_vectors: Data to be ingested, in the form of a list of tuples where each tuple
        contains (id, vector_embedding, metadata).
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
        input_vectors: list[tuple],
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
