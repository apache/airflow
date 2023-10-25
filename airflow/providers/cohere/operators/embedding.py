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
from typing import TYPE_CHECKING, Any, Callable, Collection, Mapping

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.cohere.hooks.cohere import CohereHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class CohereEmbeddingOperator(BaseOperator):
    """Creates the embedding base by interacting with cohere hosted services.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:CohereEmbeddingOperator`

    :param conn_id: Optional. The name of the Airflow connection to get connection
        information for Cohere. Defaults to "cohere_default".
    :param input_text: list of text items that need to be embedded. Only one of input_text or input_callable
        should be provided.
    :param input_callable: The callable that provides the input texts to generate embeddings for.
        Only one of input_text or input_callable should be provided.
    :param input_callable_args: The list of arguments to be passed to ``input_callable``.
    :param input_callable_kwargs: The kwargs to be passed to ``input_callable``.
    :param timeout: Timeout in seconds for Cohere API.
    :param max_retries: No. of times to retry before failing.
    """

    def __init__(
        self,
        input_text: list[str] | None = None,
        input_callable: Callable[[Any], list[str]] | None = None,
        input_callable_args: Collection[Any] | None = None,
        input_callable_kwargs: Mapping[str, Any] | None = None,
        conn_id: str = CohereHook.default_conn_name,
        timeout: int | None = None,
        max_retries: int | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_text = input_text
        self.input_callable = input_callable
        self.input_callable_args = input_callable_args or ()
        self.input_callable_kwargs = input_callable_kwargs or {}
        self.timeout = timeout
        self.max_retries = max_retries
        self.text_to_embed = self._get_text_to_embed()

    @cached_property
    def hook(self) -> CohereHook:
        """Return an instance of the CohereHook."""
        return CohereHook(conn_id=self.conn_id, timeout=self.timeout, max_retries=self.max_retries)

    def _get_text_to_embed(self) -> list[str]:
        """Get the text to embed by evaluating ``input_text`` and ``input_callable``."""
        if self.input_text and self.input_callable:
            raise RuntimeError("Only one of 'input_text' and 'input_callable' is allowed")
        if self.input_callable:
            if not callable(self.input_callable):
                raise AirflowException("`input_callable` param must be callable")
            return self.input_callable(*self.input_callable_args, **self.input_callable_kwargs)
        elif self.input_text:
            return self.input_text
        else:
            raise RuntimeError("Either one of 'input_text' and 'input_callable' must be provided")

    def execute(self, context: Context) -> list[list[float]]:
        """Embed texts using Cohere embed services."""
        return self.hook.create_embeddings(self.text_to_embed)
