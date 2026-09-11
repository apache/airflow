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
from typing import TYPE_CHECKING, Any

from grpc import RpcError
from qdrant_client import QdrantClient
from qdrant_client.http.exceptions import UnexpectedResponse

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from qdrant_client import models


class QdrantHook(BaseHook):
    """
    Hook for interfacing with a Qdrant instance.

    :param conn_id: The connection id to use when connecting to Qdrant. Defaults to `qdrant_default`.
    """

    conn_name_attr = "conn_id"
    conn_type = "qdrant"
    default_conn_name = "qdrant_default"
    hook_name = "Qdrant"

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Return connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, IntegerField, StringField

        return {
            "url": StringField(
                lazy_gettext("URL"),
                widget=BS3TextFieldWidget(),
                description="Optional. Qualified URL of the Qdrant instance."
                "Example: https://xyz-example.eu-central.aws.cloud.qdrant.io:6333",
            ),
            "grpc_port": IntegerField(
                lazy_gettext("GPRC Port"),
                widget=BS3TextFieldWidget(),
                description="Optional. Port of the gRPC interface.",
                default=6334,
            ),
            "prefer_gprc": BooleanField(
                lazy_gettext("Prefer GRPC"),
                widget=BS3TextFieldWidget(),
                description="Optional. Whether to use gPRC interface whenever possible in custom methods.",
                default=False,
            ),
            "https": BooleanField(
                lazy_gettext("HTTPS"),
                widget=BS3TextFieldWidget(),
                description="Optional. Whether to use HTTPS(SSL) protocol.",
            ),
            "prefix": StringField(
                lazy_gettext("Prefix"),
                widget=BS3TextFieldWidget(),
                description="Optional. Prefix to the REST URL path."
                "Example: `service/v1` will result in http://localhost:6333/service/v1/{qdrant-endpoint} for REST API.",
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom field behaviour."""
        return {
            "hidden_fields": ["schema", "login", "extra"],
            "relabeling": {"password": "API Key"},
        }

    def __init__(self, conn_id: str = default_conn_name, **kwargs) -> None:
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def get_conn(self) -> QdrantClient:
        """Get a Qdrant client instance for interfacing with the database."""
        connection = self.get_connection(self.conn_id)
        host = connection.host or None
        port = connection.port or 6333
        api_key = connection.password
        extra = connection.extra_dejson
        url = extra.get("url", None)
        grpc_port = extra.get("grpc_port", 6334)
        prefer_gprc = extra.get("prefer_gprc", False)
        https = extra.get("https", None)
        prefix = extra.get("prefix", None)

        return QdrantClient(
            host=host,
            port=port,
            url=url,
            api_key=api_key,
            grpc_port=grpc_port,
            prefer_grpc=prefer_gprc,
            https=https,
            prefix=prefix,
        )

    @cached_property
    def conn(self) -> QdrantClient:
        """Get a Qdrant client instance for interfacing with the database."""
        return self.get_conn()

    def verify_connection(self) -> tuple[bool, str]:
        """Check the connection to the Qdrant instance."""
        try:
            self.conn.get_collections()
            return True, "Connection established!"
        except (UnexpectedResponse, RpcError, ValueError) as e:
            return False, str(e)

    def test_connection(self) -> tuple[bool, str]:
        """Test the connection to the Qdrant instance."""
        return self.verify_connection()

    def search(
        self,
        collection_name: str,
        query: Any,
        *,
        query_filter: models.Filter | None = None,
        search_params: models.SearchParams | None = None,
        limit: int = 10,
        offset: int | None = None,
        with_payload: bool | list[str] = True,
        with_vectors: bool | list[str] = False,
        score_threshold: float | None = None,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        """
        Run a similarity search against a Qdrant collection and return the matches.

        Wraps ``QdrantClient.query_points`` and returns plain, XCom-serializable
        dictionaries (via ``ScoredPoint.model_dump``) instead of pydantic objects.

        :param collection_name: Name of the collection to search.
        :param query: The query. Commonly a dense vector (``list[float]``); it may
            also be a point id, a named/sparse vector, or a ``qdrant_client.models``
            query object. See the Qdrant ``query_points`` docs for all supported forms.
        :param query_filter: Optional filter to restrict which points are considered.
        :param search_params: Optional search-tuning parameters (e.g. ``hnsw_ef``).
        :param limit: Maximum number of results to return (top-k). Defaults to 10.
        :param offset: Number of results to skip, for pagination. Optional.
        :param with_payload: Whether (or which payload fields) to include. Defaults to True.
        :param with_vectors: Whether (or which vectors) to include. Defaults to False.
        :param score_threshold: Minimal similarity score for a result to be returned.
        :param kwargs: Additional keyword arguments forwarded to ``query_points``.
        :return: A list of scored points as dictionaries, ordered by descending score.
        """
        response = self.conn.query_points(
            collection_name=collection_name,
            query=query,
            query_filter=query_filter,
            search_params=search_params,
            limit=limit,
            offset=offset,
            with_payload=with_payload,
            with_vectors=with_vectors,
            score_threshold=score_threshold,
            **kwargs,
        )
        return [point.model_dump() for point in response.points]
