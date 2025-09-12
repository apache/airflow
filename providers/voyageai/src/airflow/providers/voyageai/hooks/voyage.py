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

import voyageai
from voyageai import Client

from airflow.hooks.base import BaseHook


class VoyageAIHook(BaseHook):
    """
    Airflow Hook to interact with the Voyage AI API.

    This hook manages authentication using an API key stored in an Airflow connection's password field.
    It provides a convenient interface to create and reuse an authenticated `voyageai.Client` instance
    for making API calls to Voyage AI services.

    Attributes:
        conn_name_attr (str): The connection ID attribute name.
        default_conn_name (str): The default connection ID.
        conn_type (str): The connection type for Airflow.
        hook_name (str): The human-readable name of the hook.

    Usage:
        hook = VoyageAIHook(conn_id='my_voyage_connection')
        client = hook.get_conn()
        embeddings = hook.embed(["text1", "text2"], model="voyage-2")

    Raises:
        ValueError: If the API key is not found in the connection's password field.
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
        Return an authenticated `voyageai.Client` instance.

        This method retrieves the API key from the Airflow connection specified by `conn_id`,
        authenticates with the Voyage AI API, and returns a client instance for making API calls.

        Returns:
            Client: An authenticated instance of `voyageai.Client`.

        Raises:
            ValueError: If the API key is not found in the connection's password field.
        """
        if self.client:
            return self.client

        conn = self.get_connection(self.conn_id)
        api_key = conn.password

        if not api_key:
            raise ValueError(f"API Key not found in connection '{self.conn_id}' (password field).")

        self.log.info("Authenticating and creating Voyage AI client.")
        self.client = voyageai.Client(api_key=api_key)
        return self.client

    def embed(self, texts: list[str], model: str) -> list[list[float]]:
        """
        Generate embeddings for a list of documents.

        :param texts: A list of strings to embed.
        :param model: The name of the model to use (e.g., 'voyage-2').
        """
        client = self.get_conn()
        self.log.info(f"Generating embeddings for {len(texts)} documents with model '{model}'.")
        result = client.embed(texts, model=model, input_type="document")
        return result.embeddings
