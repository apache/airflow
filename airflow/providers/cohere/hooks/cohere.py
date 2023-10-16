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

from typing import Any

import cohere

from airflow.hooks.base import BaseHook


class CohereHook(BaseHook):
    """
    Use Cohere Python SDK to interact with Cohere platform.

    .. seealso:: https://docs.cohere.com/docs

    :param conn_id: :ref:`Cohere connection id <howto/connection:cohere>`
    """

    conn_name_attr = "conn_id"
    default_conn_name = "cohere_default"
    conn_type = "cohere"
    hook_name = "Cohere"

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = conn_id
        self.client = None

    def get_conn(self) -> None:
        raise NotImplementedError()

    def _get_api_key(self) -> str:
        return str(self.get_connection(self.conn_id).password)

    def get_client(self) -> cohere.Client:
        if self.client is None:
            self.client = cohere.Client(self._get_api_key())
        return self.client

    def embed_text(self, texts: list[str], model: str = "embed-multilingual-v2.0") -> list[list[float]]:
        response = self.get_client().embed(texts=texts, model=model)
        embeddings = response.embeddings
        return embeddings  # type:ignore[no-any-return]

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        return {
            "hidden_fields": ["host", "schema", "login", "port", "extra"],
            "relabeling": {},
        }

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.get_client().list_custom_models()
            return True, "Connection established"
        except Exception as e:
            return False, str(e)
