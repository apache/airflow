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
from typing import TYPE_CHECKING, Any, Callable, Collection, Mapping, Sequence

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.openai.hooks.openai import OpenAIHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OpenAIEmbeddingOperator(BaseOperator):
    """
    Operator that accepts input text to generate OpenAI embeddings using the specified model.

    :param conn_id: The OpenAI connection.
    :param input_text: The text to generate OpenAI embeddings on. Either input_text or input_callable
        should be provided.
    :param input_callable: The callable that provides the input text to generate OpenAI embeddings.
        Either input_text or input_callable should be provided.
    :param input_callable_args: The list of arguments to be passed to ``input_callable``
    :param input_callable_kwargs: The kwargs to be passed to ``input_callable``
    :param model: The OpenAI model to be used for generating the embeddings.
    """

    template_fields: Sequence[str] = ("input_text",)

    def __init__(
        self,
        conn_id: str,
        input_text: str | list[Any] | None = None,
        input_callable: Callable[[Any], str | list[Any]] | None = None,
        input_callable_args: Collection[Any] | None = None,
        input_callable_kwargs: Mapping[str, Any] | None = None,
        model: str = "text-embedding-ada-002",
        **kwargs: Any,
    ):
        self.embedding_params = kwargs.pop("embedding_params", {})
        self.hook_params = kwargs.pop("hook_params", {})
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_text = input_text
        self.input_callable = input_callable
        self.input_callable_args = input_callable_args or ()
        self.input_callable_kwargs = input_callable_kwargs or {}
        self.model = model

    @cached_property
    def hook(self) -> OpenAIHook:
        """Return an instance of the OpenAIHook."""
        return OpenAIHook(conn_id=self.conn_id, **self.hook_params)

    def execute(self, context: Context) -> list[float]:
        if self.input_text and self.input_callable:
            raise RuntimeError("Only one of 'input_text' and 'input_callable' is allowed")
        if self.input_callable:
            if not callable(self.input_callable):
                raise AirflowException("`input_callable` param must be callable")
            input_text = self.input_callable(*self.input_callable_args, **self.input_callable_kwargs)
        elif self.input_text:
            input_text = self.input_text
        else:
            raise RuntimeError("Either one of 'input_text' and 'input_callable' must be provided")
        self.log.info("Input text: %s", input_text)
        embeddings = self.hook.create_embeddings(input_text, model=self.model, **self.embedding_params)
        self.log.info("Embeddings: %s", embeddings)
        return embeddings
