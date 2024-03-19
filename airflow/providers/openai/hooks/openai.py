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
from typing import Any

from openai import OpenAI

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

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "login"],
            "relabeling": {"password": "API Key"},
            "placeholders": {},
        }

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.conn.models.list()
            return True, "Connection established!"
        except Exception as e:
            return False, str(e)

    @cached_property
    def conn(self) -> OpenAI:
        """Return an OpenAI connection object."""
        return self.get_conn()

    def get_conn(self) -> OpenAI:
        """Return an OpenAI connection object."""
        conn = self.get_connection(self.conn_id)
        extras = conn.extra_dejson
        openai_client_kwargs = extras.get("openai_client_kwargs", {})
        api_key = openai_client_kwargs.pop("api_key", None) or conn.password
        base_url = openai_client_kwargs.pop("base_url", None) or conn.host or None
        return OpenAI(
            api_key=api_key,
            base_url=base_url,
            **openai_client_kwargs,
        )

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
        response = self.conn.embeddings.create(model=model, input=text, **kwargs)
        embeddings: list[float] = response.data[0].embedding
        return embeddings
