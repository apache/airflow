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
from typing import TYPE_CHECKING, Any, Sequence

from airflow.models import BaseOperator
from airflow.providers.cohere.hooks.cohere import CohereHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


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
    """

    template_fields: Sequence[str] = ("input_text",)

    def __init__(
        self,
        input_text: list[str] | str,
        conn_id: str = CohereHook.default_conn_name,
        timeout: int | None = None,
        max_retries: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        if isinstance(input_text, str):
            input_text = [input_text]
        self.conn_id = conn_id
        self.input_text = input_text
        self.timeout = timeout
        self.max_retries = max_retries

    @cached_property
    def hook(self) -> CohereHook:
        """Return an instance of the CohereHook."""
        return CohereHook(
            conn_id=self.conn_id, timeout=self.timeout, max_retries=self.max_retries
        )

    def execute(self, context: Context) -> list[list[float]]:
        """Embed texts using Cohere embed services."""
        return self.hook.create_embeddings(self.input_text)
