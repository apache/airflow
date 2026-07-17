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

from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.cohere.hooks.cohere import CohereHook
from airflow.providers.common.compat.sdk import BaseOperator

if TYPE_CHECKING:
    from cohere.core.request_options import RequestOptions

    from airflow.providers.common.compat.sdk import Context


class CohereEmbeddingOperator(BaseOperator):
    """
    Creates the embedding base by interacting with cohere hosted services.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CohereEmbeddingOperator`

    :param input_text: single string text or list of text items that need to be embedded.
    :param conn_id: Optional. The name of the Airflow connection to get connection
        information for Cohere. Defaults to "cohere_default".
    :param timeout: Timeout in seconds for Cohere API.
    :param max_retries: Number of times to retry before failing.
    :param request_options: Request-specific configuration.
        Fields:
        - timeout_in_seconds: int. The number of seconds to await an API call before timing out.

        - max_retries: int. The max number of retries to attempt if the API call fails.

        - additional_headers: typing.Dict[str, typing.Any]. A dictionary containing additional parameters to spread into the request's header dict

        - additional_query_parameters: typing.Dict[str, typing.Any]. A dictionary containing additional parameters to spread into the request's query parameters dict

        - additional_body_parameters: typing.Dict[str, typing.Any]. A dictionary containing additional parameters to spread into the request's body parameters dict
    """

    template_fields: Sequence[str] = ("input_text",)

    def __init__(
        self,
        input_text: list[str] | str,
        conn_id: str = CohereHook.default_conn_name,
        timeout: int | None = None,
        max_retries: int | None = None,
        request_options: RequestOptions | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        if isinstance(input_text, str):
            input_text = [input_text]
        self.conn_id = conn_id
        self.input_text = input_text
        self.timeout = timeout
        self.max_retries = max_retries
        self.request_options = request_options

    @cached_property
    def hook(self) -> CohereHook:
        """Return an instance of the CohereHook."""
        return CohereHook(
            conn_id=self.conn_id,
            timeout=self.timeout,
            max_retries=self.max_retries,
            request_options=self.request_options,
        )

    def execute(self, context: Context) -> list[list[float]]:
        """Embed texts using Cohere embed services."""
        embedding_response = self.hook.create_embeddings(self.input_text)
        # NOTE: Return type `EmbedByTypeResponseEmbeddings` was removed temporarily due to limitations
        # in XCom serialization/deserialization of complex types like Cohere embeddings and Pydantic models.
        #
        # Tracking issue: https://github.com/apache/airflow/issues/50867
        # Once that issue is resolved, XCom (de)serialization of such types will be supported, and
        # we can safely restore the `EmbedByTypeResponseEmbeddings` return type here.
        return embedding_response
