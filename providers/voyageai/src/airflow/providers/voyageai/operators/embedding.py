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

# This will be our Operator. It will use the Hook to perform a specific action.

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.voyageai.hooks.voyage import VoyageAIHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class VoyageEmbeddingOperator(BaseOperator):
    """
    Generate embeddings for a list of texts using the Voyage AI API.

    This operator uses the :class:`~airflow.providers.voyageai.hooks.voyage.VoyageAIHook`
    to send a list of texts to the Voyage AI embedding endpoint. The resulting list
    of embedding vectors is pushed to XCom for use by downstream tasks.

    :param conn_id: The Airflow connection ID for the Voyage AI service.
    :param input_texts: A list of strings to be embedded. This field is templated.
    :param model: The name of the Voyage AI model to use for embedding (e.g., 'voyage-2').
    """

    template_fields: Sequence[str] = ("input_texts",)

    def __init__(
        self,
        *,
        conn_id: str,
        input_texts: list[str],
        model: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id
        self.input_texts = input_texts
        self.model = model

    def execute(self, context: Context) -> list[list[float]] | list[list[int]]:
        """Instantiate the hook, call the embed method, and return the result."""
        self.log.info("Executing VoyageEmbeddingOperator for %d texts.", len(self.input_texts))
        hook = VoyageAIHook(conn_id=self.conn_id)

        embeddings = hook.embed(
            texts=self.input_texts,
            model=self.model,
        )

        self.log.info("Successfully retrieved %d embedding vectors.", len(embeddings))

        return embeddings
