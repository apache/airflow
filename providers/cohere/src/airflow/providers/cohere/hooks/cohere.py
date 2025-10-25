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
from typing import TYPE_CHECKING, Any

import cohere
from cohere.types import UserChatMessageV2

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from cohere.core.request_options import RequestOptions
    from cohere.types import ChatMessages


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
        request_options: RequestOptions | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_options = request_options
        self._client: cohere.ClientV2 | None = None

        if self.max_retries:
            warnings.warn(
                "Argument `max_retries` is deprecated. Use `request_options` dict for function-specific request configuration.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            if self.request_options is None:
                self.request_options = {"max_retries": self.max_retries}
            else:
                self.request_options.update({"max_retries": self.max_retries})

    def get_conn(self) -> cohere.ClientV2:
        """Return a new or cached Cohere client instance."""
        if self._client is None:
            # create a new client instance if there is no existing client
            conn = self.get_connection(self.conn_id)
            self._client = cohere.ClientV2(
                api_key=conn.password,
                timeout=self.timeout,
                base_url=conn.host or None,
            )
        return self._client

    def create_embeddings(
        self, texts: list[str], model: str = "embed-multilingual-v3.0"
    ) -> list[list[float]]:
        logger.info("Creating embeddings with model: embed-multilingual-v3.0")
        response = self.get_conn().embed(
            texts=texts,
            model=model,
            input_type="search_document",
            embedding_types=["float"],
            request_options=self.request_options,
        )
        # NOTE: Return type `EmbedByTypeResponseEmbeddings` was removed temporarily due to limitations
        # in XCom serialization/deserialization of complex types like Cohere embeddings and Pydantic models.
        #
        # Tracking issue: https://github.com/apache/airflow/issues/50867
        # Once that issue is resolved, XCom (de)serialization of such types will be supported and
        # we can safely restore the `EmbedByTypeResponseEmbeddings` return type here.
        if response.embeddings.float_ is None:
            raise ValueError("Embeddings response is missing float_ field")
        return response.embeddings.float_

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema", "login", "port", "extra"],
            "relabeling": {
                "password": "API Key",
            },
        }

    def test_connection(
        self,
        model: str = "command-r-plus-08-2024",
        messages: ChatMessages | None = None,
    ) -> tuple[bool, str]:
        try:
            if messages is None:
                messages = [UserChatMessageV2(role="user", content="hello world!")]
            self.get_conn().chat(model=model, messages=messages)
            return True, "Connection successfully established."
        except Exception as e:
            return False, f"Unexpected error: {str(e)}"
