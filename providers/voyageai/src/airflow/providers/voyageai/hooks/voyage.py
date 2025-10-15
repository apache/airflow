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

from dataclasses import dataclass, field
from typing import Any

from voyageai import Client

try:
    from airflow.sdk import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook as BaseHook  # type: ignore


@dataclass
class EmbeddingsResult:
    """
    Data class representing the result of an embedding operation.

    Attributes:
        embeddings (list[Any]): List of embedding vectors.
        total_tokens (int): Total number of tokens processed to generate embeddings.
    """

    embeddings: list[Any] = field(default_factory=list)
    total_tokens: int = 0


@dataclass
class ContextualizedEmbeddings:
    """
    Data class representing the embedding result for a single input document.

    Attributes:
        embeddings (list[list[Any]]): List of embedding vectors for chunks of the document.
        index (int): Index of the document in the batch.
        chunk_texts (list[str] | None): Optional list of text chunks corresponding to the embeddings.
    """

    embeddings: list[list[Any]] = field(default_factory=list)
    index: int = 0
    chunk_texts: list[str] | None = None


@dataclass
class ContextualizedEmbeddingsResult:
    """
    Data class representing the entire result from the contextualized_embed method.

    Attributes:
        results (list[ContextualizedEmbeddings]): List of contextualized embeddings for each input document.
        total_tokens (int): Total number of tokens processed across all documents.
    """

    results: list[ContextualizedEmbeddings]
    total_tokens: int = 0


class VoyageAIHook(BaseHook):
    """
    Interact with the Voyage AI API to generate text embeddings.

    This hook manages the connection and authentication with the Voyage AI service,
    providing a convenient interface to the ``voyageai`` client library. The API
    key must be stored in the password field of the configured Airflow connection.

    :param conn_id: The Airflow connection ID to use for connecting to Voyage AI.
    """

    conn_name_attr = "voyage_conn_id"
    default_conn_name = "voyage_default"
    conn_type = "voyageai"
    hook_name = "Voyage AI"

    def __init__(self, conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.client: Client | None = None

    def get_conn(self) -> Client:
        """
        Return an authenticated ``voyageai.Client`` instance.

        This method retrieves the API key from the password field of the Airflow
        connection and uses it to instantiate the client.

        :return: An initialized ``voyageai.Client`` object.
        :raises ValueError: If the API key is not found in the connection.
        """
        if self.client:
            return self.client

        conn = self.get_connection(self.conn_id)
        api_key = conn.password

        if not api_key:
            raise ValueError("API Key not found in connection '%s' (password field).", self.conn_id)

        self.log.info("Authenticating and creating Voyage AI client.")
        self.client = Client(api_key=api_key)
        return self.client

    def contextualized_embed(
        self,
        texts: list[list[str]],
        model: str,
        output_dimension: int = 1024,
        output_dtype: str = "float",
        chunk_fn=None,
    ) -> ContextualizedEmbeddingsResult:
        """
        Generate embeddings for a list of documents and returns a JSON-serializable result.

        :param texts: A list of strings to embed.
        :param model: The name of the model to use (e.g., 'voyage-2').
        :return: A serializable ContextualizedEmbeddingsResult dataclass instance.
        """
        client = self.get_conn()
        self.log.info("Generating embeddings for %d documents with model '%s'.", len(texts), model)

        # This is the raw, unserializable result from the voyageai library
        api_result = client.contextualized_embed(
            inputs=texts,
            model=model,
            input_type="document",
            output_dimension=output_dimension,
            output_dtype=output_dtype,
            chunk_fn=chunk_fn,
        )

        serializable_results = [
            ContextualizedEmbeddings(
                embeddings=res.embeddings,
                index=idx,
                chunk_texts=getattr(res, "chunk_texts", None),  # Safely get chunk_texts if it exists
            )
            for idx, res in enumerate(api_result.results)
        ]

        # Return your custom, serializable object instead of the raw one
        return ContextualizedEmbeddingsResult(
            results=serializable_results, total_tokens=api_result.total_tokens
        )

    def embed(
        self,
        texts: list[str],
        model: str,
        output_dimension: int = 1024,
        output_dtype: str = "float",
    ) -> dict:
        """
        Generate embeddings for a list of documents.

        :param texts: A list of strings to embed.
        :param model: The name of the model to use (e.g., 'voyage-2').
        :return: A list of embedding vectors, where each vector is a list of floats.
        """
        client = self.get_conn()
        self.log.info("Generating embeddings for %d documents with model '%s'.", len(texts), model)
        result = client.embed(
            texts,
            model=model,
            input_type="document",
            truncation=False,
            output_dimension=output_dimension,
            output_dtype=output_dtype,
        )
        return {
            "embeddings": result.embeddings,
            "total_tokens": result.total_tokens,
        }
