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

import logging
import warnings
from functools import cached_property
from typing import Any

import cohere

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook

logger = logging.getLogger(__name__)


class CohereHook(BaseHook):
    """
    Use Cohere Python SDK to interact with Cohere platform using API v2.

    .. seealso:: https://docs.cohere.com/docs

    :param conn_id: :ref:`Cohere connection id <howto/connection:cohere>`
    :param timeout: Request timeout in seconds. Optional.
    :param max_retries: Maximal number of retries for requests. Deprecated, use request_options instead. Optional.
    :param request_options: Dictionary for function-specific request configuration. Optional.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "cohere_default"
    conn_type = "cohere"
    hook_name = "Cohere"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        timeout: int | None = None,
        max_retries: int | None = None,
        request_options: dict | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_options = request_options

        if self.max_retries is not None:
            warnings.warn(
                "The 'max_retries' parameter is deprecated. Use 'request_options' with {'max_retries': value} instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            if self.request_options is None:
                self.request_options = {"max_retries": self.max_retries}
            else:
                self.request_options["max_retries"] = self.max_retries

    @cached_property
    def get_conn(self) -> cohere.ClientV2:
        ":return: Cohere V2 client for API interaction."
        conn = self.get_connection(self.conn_id)
        return cohere.ClientV2(
            api_key=conn.password,
            timeout=self.timeout,
            base_url=conn.host or None,
            request_options=self.request_options,
        )

    def create_embeddings(
        self, texts: list[str], model: str = "embed-multilingual-v3.0"
    ) -> list[list[float]]:
        """
        Create embeddings for the given texts using the specified model.

        :param texts: List of texts to create embeddings for.
        :param model: The model to use for creating embeddings. Default is 'embed-multilingual-v3.0'.
        :return: List of embedding vectors, where each inner list represents an embedding.
        """
        logger.info("Creating embeddings with model: embed-multilingual-v3.0")
        response = self.get_conn.embed(texts=texts, model=model, request_options=self.request_options)
        embeddings = response.embeddings
        return embeddings

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        ":return: Dictionary defining the UI behavior for connection configuration in Airflow."
        return {
            "hidden_fields": ["schema", "login", "port", "extra"],
            "relabeling": {
                "password": "API Key",
            },
        }

    def test_connection(self) -> tuple[bool, str]:
        """
        Test the connection to Cohere by attempting a chat request.

        :return: Tuple containing a boolean indicating success and a message.
        """
        try:
            self.get_conn.chat("Test")
            return True, "Connection successfully established."
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
