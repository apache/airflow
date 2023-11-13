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
from airflow.providers.openai.hooks.openai import OpenAIHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OpenAIEmbeddingOperator(BaseOperator):
    """
    Operator that accepts input text to generate OpenAI embeddings using the specified model.

    :param conn_id: The OpenAI connection ID to use.
    :param input_text: The text to generate OpenAI embeddings for. This can be a string, a list of strings,
                    a list of integers, or a list of lists of integers.
    :param model: The OpenAI model to be used for generating the embeddings.
    :param embedding_kwargs: Additional keyword arguments to pass to the OpenAI `create_embeddings` method.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:OpenAIEmbeddingOperator`
        For possible options for `embedding_kwargs`, see:
        https://platform.openai.com/docs/api-reference/embeddings/create
    """

    template_fields: Sequence[str] = ("input_text",)

    def __init__(
        self,
        conn_id: str,
        input_text: str | list[str] | list[int] | list[list[int]],
        model: str = "text-embedding-ada-002",
        embedding_kwargs: dict | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_text = input_text
        self.model = model
        self.embedding_kwargs = embedding_kwargs or {}

    @cached_property
    def hook(self) -> OpenAIHook:
        """Return an instance of the OpenAIHook."""
        return OpenAIHook(conn_id=self.conn_id)

    def execute(self, context: Context) -> list[float]:
        if not self.input_text or not isinstance(self.input_text, (str, list)):
            raise ValueError(
                "The 'input_text' must be a non-empty string, list of strings, list of integers, or list of lists of integers."
            )
        self.log.info("Generating embeddings for the input text of length: %d", len(self.input_text))
        embeddings = self.hook.create_embeddings(self.input_text, model=self.model, **self.embedding_kwargs)
        self.log.info("Generated embeddings for %d items", len(embeddings))
        return embeddings
