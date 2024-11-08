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
from typing import TYPE_CHECKING, Any, Iterable, Sequence

from airflow.models import BaseOperator
from airflow.providers.qdrant.hooks.qdrant import QdrantHook

if TYPE_CHECKING:
    from qdrant_client.models import VectorStruct

    from airflow.utils.context import Context


class QdrantIngestOperator(BaseOperator):
    """
    Upload points to a Qdrant collection.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:QdrantIngestOperator`

    :param conn_id: The connection id to connect to a Qdrant instance.
    :param collection_name: The name of the collection to ingest data into.
    :param vectors: An iterable over vectors to upload.
    :param payload: Iterable of vector payloads, Optional. Defaults to None.
    :param ids: Iterable of custom vector ids, Optional. Defaults to None.
    :param batch_size: Number of points to upload per-request. Defaults to 64.
    :param parallel: Number of parallel upload processes. Defaults to 1.
    :param method: Start method for parallel processes. Defaults to 'forkserver'.
    :param max_retries: Number of retries for failed requests. Defaults to 3.
    :param wait: Await for the results to be applied on the server side. Defaults to True.
    :param kwargs: Additional keyword arguments passed to the BaseOperator constructor.
    """

    template_fields: Sequence[str] = (
        "collection_name",
        "vectors",
        "payload",
        "ids",
        "batch_size",
        "parallel",
        "method",
        "max_retries",
        "wait",
    )

    def __init__(
        self,
        *,
        conn_id: str = QdrantHook.default_conn_name,
        collection_name: str,
        vectors: Iterable[VectorStruct],
        payload: Iterable[dict[str, Any]] | None = None,
        ids: Iterable[int | str] | None = None,
        batch_size: int = 64,
        parallel: int = 1,
        method: str | None = None,
        max_retries: int = 3,
        wait: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.collection_name = collection_name
        self.vectors = vectors
        self.payload = payload
        self.ids = ids
        self.batch_size = batch_size
        self.parallel = parallel
        self.method = method
        self.max_retries = max_retries
        self.wait = wait

    @cached_property
    def hook(self) -> QdrantHook:
        """Return an instance of QdrantHook."""
        return QdrantHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> None:
        """Upload points to a Qdrant collection."""
        self.hook.conn.upload_collection(
            collection_name=self.collection_name,
            vectors=self.vectors,
            payload=self.payload,
            ids=self.ids,
            batch_size=self.batch_size,
            parallel=self.parallel,
            method=self.method,
            max_retries=self.max_retries,
            wait=self.wait,
        )
