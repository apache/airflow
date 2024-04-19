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

import warnings
from functools import cached_property
from typing import Any

import cohere

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook


class CohereHook(BaseHook):
    """
    Use Cohere Python SDK to interact with Cohere platform.

    .. seealso:: https://docs.cohere.com/docs

    :param conn_id: :ref:`Cohere connection id <howto/connection:cohere>`
    :param timeout: Request timeout in seconds.
    :param max_retries: Maximal number of retries for requests.
    :param request_options: Request-specific configuration.
        Fields:
        - timeout_in_seconds: int. The number of seconds to await an API call before timing out.

        - max_retries: int. The max number of retries to attempt if the API call fails.

        - additional_headers: typing.Dict[str, typing.Any]. A dictionary containing additional parameters to spread into the request's header dict

        - additional_query_parameters: typing.Dict[str, typing.Any]. A dictionary containing additional parameters to spread into the request's query parameters dict

        - additional_body_parameters: typing.Dict[str, typing.Any]. A dictionary containing additional parameters to spread into the request's body parameters dict
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
        if self.max_retries:
            warnings.warn(
                "Argument `max_retries` is deprecated. Please use `request_options` dict for function-specific request configuration instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            self.request_options = (
                {"max_retries": self.max_retries}
                if self.request_options is None
                else self.request_options.update({"max_retries": self.max_retries})
            )

    @cached_property
    def get_conn(self) -> cohere.Client:  # type: ignore[override]
        conn = self.get_connection(self.conn_id)
        return cohere.Client(api_key=conn.password, timeout=self.timeout, base_url=conn.host)

    def create_embeddings(
        self, texts: list[str], model: str = "embed-multilingual-v2.0"
    ) -> list[list[float]] | cohere.EmbedByTypeResponseEmbeddings:
        response = self.get_conn.embed(texts=texts, model=model, request_options=self.request_options)
        embeddings = response.embeddings
        return embeddings

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema", "login", "port", "extra"],
            "relabeling": {
                "password": "API Key",
            },
        }

    def test_connection(self) -> tuple[bool, str]:
        if self.max_retries:
            self.request_options.update(max_retries=self.max_retries)
        try:
            self.get_conn.generate(prompt="Test", max_tokens=10, request_options=self.request_options)
            return True, "Connection established"
        except Exception as e:
            return False, str(e)
