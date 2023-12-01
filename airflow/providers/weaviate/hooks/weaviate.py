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

import warnings
from functools import cached_property
from typing import TYPE_CHECKING

from weaviate import Client as WeaviateClient
from weaviate.auth import AuthApiKey, AuthBearerToken, AuthClientCredentials, AuthClientPassword
from weaviate.exceptions import ObjectAlreadyExistsException
from weaviate.util import generate_uuid5

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from typing import Any

    import pandas as pd
    from weaviate.types import UUID


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
        scope = extras.get("scope", None) or extras.get("oidc_scope", None)
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

    @cached_property
    def conn(self) -> WeaviateClient:
        """Returns a Weaviate client."""
        return self.get_conn()

    def get_client(self) -> WeaviateClient:
        """Returns a Weaviate client."""
        # Keeping this for backwards compatibility
        warnings.warn(
            "The `get_client` method has been renamed to `get_conn`",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        return self.conn

    def test_connection(self) -> tuple[bool, str]:
        try:
            client = self.conn
            client.schema.get()
            return True, "Connection established!"
        except Exception as e:
            self.log.error("Error testing Weaviate connection: %s", e)
            return False, str(e)

    def create_class(self, class_json: dict[str, Any]) -> None:
        """Create a new class."""
        client = self.conn
        client.schema.create_class(class_json)

    def create_schema(self, schema_json: dict[str, Any]) -> None:
        """
        Create a new Schema.

        Instead of adding classes one by one , you can upload a full schema in JSON format at once.

        :param schema_json: The schema to create
        """
        client = self.conn
        client.schema.create(schema_json)

    def batch_data(
        self, class_name: str, data: list[dict[str, Any]], batch_config_params: dict[str, Any] | None = None
    ) -> None:
        client = self.conn
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
        client = self.conn
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
        client = self.conn
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
        client = self.conn
        results: dict[str, dict[Any, Any]] = (
            client.query.get(class_name, properties[0])
            .with_near_text({"concepts": [search_text]})
            .with_limit(limit)
            .do()
        )
        return results

    def create_object(
        self, data_object: dict | str, class_name: str, **kwargs
    ) -> str | dict[str, Any] | None:
        """Create a new object.

        :param data_object: Object to be added. If type is str it should be either a URL or a file.
        :param class_name: Class name associated with the object given.
        :param kwargs: Additional parameters to be passed to weaviate_client.data_object.create()
        """
        client = self.conn
        # generate deterministic uuid if not provided
        uuid = kwargs.pop("uuid", generate_uuid5(data_object))
        try:
            return client.data_object.create(data_object, class_name, uuid=uuid, **kwargs)
        except ObjectAlreadyExistsException:
            self.log.warning("Object with the UUID %s already exists", uuid)
            return None

    def get_or_create_object(
        self, data_object: dict | str | None = None, class_name: str | None = None, **kwargs
    ) -> str | dict[str, Any] | None:
        """Get or Create a new object.

        Returns the object if already exists

        :param data_object: Object to be added. If type is str it should be either a URL or a file. This is required
            to create a new object.
        :param class_name: Class name associated with the object given. This is required to create a new object.
        :param kwargs: Additional parameters to be passed to weaviate_client.data_object.create() and
            weaviate_client.data_object.get()
        """
        vector = kwargs.pop("vector", None)
        obj = self.get_object(class_name=class_name, **kwargs)
        if not obj:
            if not (data_object and class_name):
                raise ValueError("data_object and class_name are required to create a new object")
            uuid = kwargs.pop("uuid", generate_uuid5(data_object))
            consistency_level = kwargs.pop("consistency_level", None)
            tenant = kwargs.pop("tenant", None)
            return self.create_object(
                data_object,
                class_name,
                vector=vector,
                uuid=uuid,
                consistency_level=consistency_level,
                tenant=tenant,
            )
        return obj

    def get_object(self, **kwargs) -> dict[str, Any] | None:
        """Get objects or an object from weaviate.

        :param kwargs: parameters to be passed to weaviate_client.data_object.get() or
            weaviate_client.data_object.get_by_id()
        """
        client = self.conn
        return client.data_object.get(**kwargs)

    def get_all_objects(
        self, after: str | UUID | None = None, as_dataframe: bool = False, **kwargs
    ) -> list[dict[str, Any]] | pd.DataFrame:
        """Get all objects from weaviate.

        if after is provided, it will be used as the starting point for the listing.

        :param after: uuid of the object to start listing from
        :param as_dataframe: if True, returns a pandas dataframe
        :param kwargs: parameters to be passed to weaviate_client.data_object.get()
        """
        all_objects = []
        after = kwargs.pop("after", after)
        while True:
            results = self.get_object(after=after, **kwargs) or {}
            if not results.get("objects"):
                break
            all_objects.extend(results["objects"])
            after = results["objects"][-1]["id"]
        if as_dataframe:
            import pandas

            return pandas.DataFrame(all_objects)
        return all_objects

    def delete_object(self, uuid: UUID | str, **kwargs) -> None:
        """Delete an object from weaviate.

        :param uuid: uuid of the object to be deleted
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.delete()
        """
        client = self.conn
        client.data_object.delete(uuid, **kwargs)

    def update_object(self, data_object: dict | str, class_name: str, uuid: UUID | str, **kwargs) -> None:
        """Update an object in weaviate.

        :param data_object: The object states the fields that should be updated. Fields not specified in the
            'data_object' remain unchanged. Fields that are None will not be changed.
            If type is str it should be either an URL or a file.
        :param class_name: Class name associated with the object given.
        :param uuid: uuid of the object to be updated
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.update()
        """
        client = self.conn
        client.data_object.update(data_object, class_name, uuid, **kwargs)

    def replace_object(self, data_object: dict | str, class_name: str, uuid: UUID | str, **kwargs) -> None:
        """Replace an object in weaviate.

        :param data_object: The object states the fields that should be updated. Fields not specified in the
            'data_object' will be set to None. If type is str it should be either an URL or a file.
        :param class_name: Class name associated with the object given.
        :param uuid: uuid of the object to be replaced
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.replace()
        """
        client = self.conn
        client.data_object.replace(data_object, class_name, uuid, **kwargs)

    def validate_object(self, data_object: dict | str, class_name: str, **kwargs):
        """Validate an object in weaviate.

        :param data_object: The object to be validated. If type is str it should be either an URL or a file.
        :param class_name: Class name associated with the object given.
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.validate()
        """
        client = self.conn
        client.data_object.validate(data_object, class_name, **kwargs)

    def object_exists(self, uuid: str | UUID, **kwargs) -> bool:
        """Check if an object exists in weaviate.

        :param uuid: The UUID of the object that may or may not exist within Weaviate.
        :param kwargs: Optional parameters to be passed to weaviate_client.data_object.exists()
        """
        client = self.conn
        return client.data_object.exists(uuid, **kwargs)
