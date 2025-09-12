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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.models.baseoperator import BaseOperator
from airflow.providers.voyageai.hooks.voyage import VoyageAIHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class VoyageEmbeddingOperator(BaseOperator):
    """
    Airflow Operator to generate vector embeddings for a list of input texts using the Voyage AI API.

    This operator leverages the VoyageAIHook to interact with the Voyage AI service, sending a batch of texts
    and retrieving their corresponding embedding vectors. The embeddings can be used for downstream tasks such
    as semantic search, clustering, or machine learning pipelines.

    Attributes:
        conn_id (str): The Airflow connection ID to authenticate with the Voyage AI API.
        input_texts (list[str]): A list of text strings to be embedded.
        model (str): The identifier of the embedding model to use.

    Template Fields:
        input_texts: This field supports templating, allowing dynamic generation of input texts at runtime.

    Usage example:
        voyage_embedding = VoyageEmbeddingOperator(
            task_id='generate_embeddings',
            conn_id='voyage_ai_default',
            input_texts=['text1', 'text2', 'text3'],
            model='voyage-embedding-model-v1',
        )
    """

    template_fields: Sequence[str] = ("input_texts",)

    def __init__(
        self,
        *,
        conn_id: str,
        input_texts: list[str],
        model: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_texts = input_texts
        self.model = model

    def execute(self, context: Context) -> list[list[float]]:
        """
        Execute the operator by calling the Voyage AI API to generate embeddings.

        This method instantiates the VoyageAIHook with the provided connection ID, sends the input texts
        to the API using the specified model, and returns the resulting list of embedding vectors.

        Args:
            context (Context): The Airflow execution context.

        Returns:
            list[list[float]]: A list of embedding vectors, each corresponding to an input text.
        """
        self.log.info(f"Executing VoyageEmbeddingOperator for {len(self.input_texts)} texts.")
        hook = VoyageAIHook(conn_id=self.conn_id)

        embeddings = hook.embed(
            texts=self.input_texts,
            model=self.model,
        )

        self.log.info(f"Successfully retrieved {len(embeddings)} embedding vectors.")

        return embeddings
