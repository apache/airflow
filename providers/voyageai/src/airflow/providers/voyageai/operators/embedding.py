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

# This will be our Operator. It will use the Hook to perform a specific action.

from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.voyageai.hooks.voyage import ContextualizedEmbeddingsResult, VoyageAIHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class VoyageEmbeddingOperator(BaseOperator):
    """
    Generate embeddings for a list of texts using the Voyage AI API.

    This operator uses the :class:`~airflow.providers.voyageai.hooks.voyage.VoyageAIHook`
    to send a list of texts to the Voyage AI embedding endpoint. The resulting list
    of embedding vectors is pushed to XCom for use by downstream tasks.

    :param conn_id: The Airflow connection ID for the Voyage AI service.
    :param texts: A list of strings to be embedded. This field is templated.
    :param model: The name of the Voyage AI model to use for embedding (e.g., 'voyage-2').
    """

    template_fields: Collection[str] = ("texts",)

    def __init__(
        self,
        *,
        conn_id: str,
        texts: list[str],
        model: str,
        output_dimension: int = 1024,
        output_dtype: str = "float",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.texts = texts
        self.model = model
        self.output_dimension = output_dimension
        self.output_dtype = output_dtype

    def execute(self, context: Context) -> dict:
        """Instantiate the hook, call the embed method, and return the result."""
        self.log.info("Executing VoyageEmbeddingOperator for %d texts.", len(self.texts))
        hook = VoyageAIHook(conn_id=self.conn_id)

        embeddings = hook.embed(
            texts=self.texts,
            model=self.model,
            output_dimension=self.output_dimension,
            output_dtype=self.output_dtype,
        )

        self.log.info("Successfully retrieved %d embedding vectors.", len(embeddings))

        return embeddings


class VoyageContextEmbeddingOperator(BaseOperator):
    """
    Airflow operator to generate contextualized embeddings for batches of texts using the VoyageAI service.

    This operator uses the VoyageAIHook to connect to the VoyageAI API and obtain embeddings
    for a list of text inputs. It supports specifying the embedding model, output dimension,
    output data type, and an optional chunking function to process texts in chunks.

    Attributes:
        conn_id (str): Airflow connection ID for the VoyageAI service.
        texts (list[list[str]]): A list of lists of strings representing the texts to embed.
        model (str): The embedding model to use.
        output_dimension (int): The dimension of the output embedding vectors. Default is 1024.
        output_dtype (str): The data type of the output embeddings. Default is "float".
        chunk_fn (callable, optional): Optional function to chunk texts before embedding.
    """

    template_fields: Collection[str] = ("texts",)

    def __init__(
        self,
        *,
        conn_id: str,
        texts: list[list[str]],
        model: str,
        output_dimension: int = 1024,
        output_dtype: str = "float",
        chunk_fn=None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.texts = texts
        self.model = model
        self.output_dimension = output_dimension
        self.output_dtype = output_dtype
        self.chunk_fn = chunk_fn

    def execute(self, context: Context) -> ContextualizedEmbeddingsResult:
        """
        Execute the operator.

        Instantiates the VoyageAIHook, calls the contextualized_embed method with the provided parameters,
        and returns the embeddings result.

        Args:
            context (Context): Airflow execution context.

        Returns:
            ContextualizedEmbeddingsResult: The embeddings result containing contextualized embeddings.
        """
        self.log.info(
            "Executing VoyageContextEmbeddingOperator for %d texts.",
            len(self.texts),
        )
        hook = VoyageAIHook(conn_id=self.conn_id)

        embeddings = hook.contextualized_embed(
            texts=self.texts,
            model=self.model,
            output_dimension=self.output_dimension,
            output_dtype=self.output_dtype,
            chunk_fn=self.chunk_fn,
        )

        self.log.info("Successfully retrieved %d embedding vectors.", len(embeddings.results))

        return embeddings
