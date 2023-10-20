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
from typing import TYPE_CHECKING, Any, Callable, Collection, Mapping, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.pinecone.hooks.pinecone_hook import PineconeHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PineconeIngestOperator(BaseOperator):
    """
    Airflow Operator to ingest vector embeddings into Pinecone.

    :param conn_id: pinecone_conn_id: The connection id to use when connecting to Pinecone.
    :param index_name: Name of the Pinecone index.
    :param vectors: Data to be ingested, in the form of a list of tuples where each tuple
        contains (id, vector_embedding, metadata).
    :param namespace: The namespace to write to. If not specified, the default namespace is used.
    :param batch_size: The number of vectors to upsert in each batch.
    """

    template_fields: Sequence[str] = ("input_vectors",)

    def __init__(
        self,
        *,
        conn_id: str = PineconeHook.default_conn_name,
        index_name: str,
        input_vectors: list[Any] | None = None,
        input_callable: Callable[[Any], list[Any]] | None = None,
        input_callable_args: Collection[Any] | None = None,
        input_callable_kwargs: Mapping[str, Any] | None = None,
        namespace: str = "",
        batch_size: int | None = None,
        **kwargs: Any,
    ) -> None:
        self.upsert_kwargs = kwargs.pop("upsert_kwargs", {})
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.index_name = index_name
        self.namespace = namespace
        self.batch_size = batch_size
        self.input_vectors = input_vectors
        self.input_callable = input_callable
        self.input_callable_args = input_callable_args or ()
        self.input_callable_kwargs = input_callable_kwargs or {}

    @cached_property
    def hook(self) -> PineconeHook:
        """Return an instance of the PineconeHook."""
        return PineconeHook(
            conn_id=self.conn_id,
        )

    def _get_vector(self) -> list[Any]:
        if self.input_vectors and self.input_callable:
            raise RuntimeError("Only one of 'input_text' and 'input_callable' is allowed")
        if self.input_callable:
            if not callable(self.input_callable):
                raise AirflowException("`input_callable` param must be callable")
            input_vectors = self.input_callable(*self.input_callable_args, **self.input_callable_kwargs)
        elif self.input_vectors:
            input_vectors = self.input_vectors
        else:
            raise RuntimeError("Either one of 'input_json' and 'input_callable' must be provided")

        return input_vectors

    def execute(self, context: Context) -> None:
        """Ingest data into Pinecone using the PineconeHook."""
        self.hook.upsert(
            index_name=self.index_name,
            vectors=self._get_vector(),
            namespace=self.namespace,
            batch_size=self.batch_size,
            **self.upsert_kwargs,
        )

        self.log.info(f"Successfully ingested data into Pinecone index {self.index_name}.")
