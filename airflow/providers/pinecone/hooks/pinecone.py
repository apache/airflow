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

"""Hook for Pinecone."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import pinecone

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from pinecone.core.client.models import UpsertResponse


class PineconeHook(BaseHook):
    """
    Interact with Pinecone. This hook uses the Pinecone conn_id.

    :param conn_id: Optional, default connection id is `pinecone_default`. The connection id to use when
        connecting to Pinecone.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "pinecone_default"
    conn_type = "pinecone"
    hook_name = "Pinecone"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "log_level": StringField(lazy_gettext("Log Level"), widget=BS3TextFieldWidget(), default=None),
            "project_name": StringField(
                lazy_gettext("Project Name"),
                widget=BS3TextFieldWidget(),
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {"login": "Pinecone Environment", "password": "Pinecone API key"},
        }

    def __init__(self, conn_id: str = default_conn_name) -> None:
        self.conn_id = conn_id
        self.get_conn()

    def get_conn(self) -> None:
        pinecone_connection = self.get_connection(self.conn_id)
        api_key = pinecone_connection.password
        pinecone_environment = pinecone_connection.login
        pinecone_host = pinecone_connection.host
        extras = pinecone_connection.extra_dejson
        pinecone_project_name = extras.get("project_name")
        log_level = extras.get("log_level", None)
        pinecone.init(
            api_key=api_key,
            environment=pinecone_environment,
            host=pinecone_host,
            project_name=pinecone_project_name,
            log_level=log_level,
        )

    def test_connection(self) -> tuple[bool, str]:
        try:
            pinecone.list_indexes()
            return True, "Connection established"
        except Exception as e:
            return False, str(e)

    @staticmethod
    def upsert(
        index_name: str,
        vectors: list[Any],
        namespace: str = "",
        batch_size: int | None = None,
        show_progress: bool = True,
        **kwargs: Any,
    ) -> UpsertResponse:
        """
        The upsert operation writes vectors into a namespace.

        If a new value is upserted for an existing vector id, it will overwrite the previous value.

         .. seealso:: https://docs.pinecone.io/reference/upsert

        To upsert in parallel follow

        .. seealso:: https://docs.pinecone.io/docs/insert-data#sending-upserts-in-parallel

        :param index_name: The name of the index to describe.
        :param vectors: A list of vectors to upsert.
        :param namespace: The namespace to write to. If not specified, the default namespace - "" is used.
        :param batch_size: The number of vectors to upsert in each batch.
        :param show_progress: Whether to show a progress bar using tqdm. Applied only
            if batch_size is provided.
        """
        index = pinecone.Index(index_name)
        return index.upsert(
            vectors=vectors,
            namespace=namespace,
            batch_size=batch_size,
            show_progress=show_progress,
            **kwargs,
        )
