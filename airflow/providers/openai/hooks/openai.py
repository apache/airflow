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

import openai

from airflow.hooks.base import BaseHook


class OpenAIHook(BaseHook):
    """
    Use OpenAI SDK to interact with OpenAI APIs.

    .. seealso:: https://platform.openai.com/docs/introduction/overview

    :param conn_id: :ref:`OpenAI connection id <howto/connection:openai>`
    """

    conn_name_attr = "conn_id"
    default_conn_name = "openai_default"
    conn_type = "openai"
    hook_name = "OpenAI"

    def __init__(self, conn_id: str = default_conn_name, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        openai.api_key = self._get_api_key()
        api_base = self._get_api_base()
        if api_base:
            openai.api_base = api_base

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "login", "extra"],
            "relabeling": {"password": "API Key"},
            "placeholders": {},
        }

    def test_connection(self) -> tuple[bool, str]:
        try:
            openai.Model.list()
            return True, "Connection established!"
        except Exception as e:
            return False, str(e)

    def _get_api_key(self) -> str:
        """Get the OpenAI API key from the connection."""
        conn = self.get_connection(self.conn_id)
        if not conn.password:
            raise ValueError("OpenAI API key not found in connection")
        return str(conn.password)

    def _get_api_base(self) -> None | str:
        conn = self.get_connection(self.conn_id)
        return conn.host

    def create_embeddings(
        self,
        text: str | list[str] | list[int] | list[list[int]],
        model: str = "text-embedding-ada-002",
        **kwargs: Any,
    ) -> list[float]:
        """Generate embeddings for the given text using the given model.

        :param text: The text to generate embeddings for.
        :param model: The model to use for generating embeddings.
        """
        response = openai.Embedding.create(model=model, input=text, **kwargs)
        embeddings: list[float] = response["data"][0]["embedding"]
        return embeddings
