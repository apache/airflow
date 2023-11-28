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

from weaviate import Client as WeaviateClient
from weaviate.auth import AuthApiKey, AuthBearerToken, AuthClientCredentials, AuthClientPassword

from airflow.hooks.base import BaseHook


class WeaviateHook(BaseHook):
    """
    Interact with Weaviate database to store vectors. This hook uses the `conn_id`.

    :param conn_id: The connection id to use when connecting to Weaviate. <howto/connection:weaviate>
    """

    conn_name_attr = "conn_id"
    default_conn_name = "weaviate_default"
    conn_type = "weaviate"
    hook_name = "Weaviate"

    def __init__(self, conn_id: str = default_conn_name, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    @classmethod
    def get_connection_form_widgets(cls) -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import PasswordField

        return {
            "token": PasswordField(lazy_gettext("Weaviate API Key"), widget=BS3PasswordFieldWidget()),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {
                "login": "OIDC Username",
                "password": "OIDC Password",
            },
        }

    def get_conn(self) -> WeaviateClient:
        conn = self.get_connection(self.conn_id)
        url = conn.host
        username = conn.login or ""
        password = conn.password or ""
        extras = conn.extra_dejson
        access_token = extras.get("access_token", None)
        refresh_token = extras.get("refresh_token", None)
        expires_in = extras.get("expires_in", 60)
        # previously token was used as api_key(backwards compatibility)
        api_key = extras.get("api_key", None) or extras.get("token", None)
        client_secret = extras.get("client_secret", None)
        additional_headers = extras.pop("additional_headers", {})
        scope = extras.get("scope", None) or extras.get("oidc_scope", "offline_access")
        if api_key:
            auth_client_secret = AuthApiKey(api_key)
        elif access_token:
            auth_client_secret = AuthBearerToken(
                access_token, expires_in=expires_in, refresh_token=refresh_token
            )
        elif client_secret:
            auth_client_secret = AuthClientCredentials(client_secret=client_secret, scope=scope)
        else:
            auth_client_secret = AuthClientPassword(username=username, password=password, scope=scope)

        return WeaviateClient(
            url=url, auth_client_secret=auth_client_secret, additional_headers=additional_headers
        )

    def get_client(self) -> WeaviateClient:
        # Keeping this for backwards compatibility
        return self.get_conn()

    def test_connection(self) -> tuple[bool, str]:
        try:
            client = self.get_client()
            client.schema.get()
            return True, "Connection established!"
        except Exception as e:
            self.log.error("Error testing Weaviate connection: %s", e)
            return False, str(e)

    def create_class(self, class_json: dict[str, Any]) -> None:
        """Create a new class."""
        client = self.get_client()
        client.schema.create_class(class_json)

    def create_schema(self, schema_json: dict[str, Any]) -> None:
        """
        Create a new Schema.

        Instead of adding classes one by one , you can upload a full schema in JSON format at once.

        :param schema_json: The schema to create
        """
        client = self.get_client()
        client.schema.create(schema_json)

    def batch_data(
        self, class_name: str, data: list[dict[str, Any]], batch_config_params: dict[str, Any] | None = None
    ) -> None:
        client = self.get_client()
        if not batch_config_params:
            batch_config_params = {}
        client.batch.configure(**batch_config_params)
        with client.batch as batch:
            # Batch import all data
            for index, data_obj in enumerate(data):
                self.log.debug("importing data: %s", index + 1)
                vector = data_obj.pop("Vector", None)
                if vector is not None:
                    batch.add_data_object(data_obj, class_name, vector=vector)
                else:
                    batch.add_data_object(data_obj, class_name)

    def delete_class(self, class_name: str) -> None:
        """Delete an existing class."""
        client = self.get_client()
        client.schema.delete_class(class_name)

    def query_with_vector(
        self,
        embeddings: list[float],
        class_name: str,
        *properties: list[str],
        certainty: float = 0.7,
        limit: int = 1,
    ) -> dict[str, dict[Any, Any]]:
        """
        Query weaviate database with near vectors.

        This method uses a vector search using a Get query. we are using a with_near_vector to provide
        weaviate with a query with vector itself. This is needed for query a  Weaviate class with a custom,
        external vectorizer. Weaviate then converts this into a vector through the inference API
        (OpenAI in this particular example) and uses that vector as the basis for a vector search.
        """
        client = self.get_client()
        results: dict[str, dict[Any, Any]] = (
            client.query.get(class_name, properties[0])
            .with_near_vector({"vector": embeddings, "certainty": certainty})
            .with_limit(limit)
            .do()
        )
        return results

    def query_without_vector(
        self, search_text: str, class_name: str, *properties: list[str], limit: int = 1
    ) -> dict[str, dict[Any, Any]]:
        """
        Query using near text.

        This method uses a vector search using a Get query. we are using a nearText operator to provide
        weaviate with a query search_text. Weaviate then converts this into a vector through the inference
        API (OpenAI in this particular example) and uses that vector as the basis for a vector search.
        """
        client = self.get_client()
        results: dict[str, dict[Any, Any]] = (
            client.query.get(class_name, properties[0])
            .with_near_text({"concepts": [search_text]})
            .with_limit(limit)
            .do()
        )
        return results
