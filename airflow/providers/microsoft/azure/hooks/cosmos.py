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
"""
This module contains integration with Azure CosmosDB.

AzureCosmosDBHook communicates via the Azure Cosmos library. Make sure that a
Airflow connection of type `azure_cosmos` exists. Authorization can be done by supplying a
login (=Endpoint uri), password (=secret key) and extra fields database_name and collection_name to specify
the default database and collection to use (see connection `azure_cosmos_default` for an example).
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from azure.cosmos.cosmos_client import CosmosClient
from azure.cosmos.exceptions import CosmosHttpResponseError

from airflow.exceptions import AirflowBadRequest
from airflow.hooks.base import BaseHook


class AzureCosmosDBHook(BaseHook):
    """
    Interacts with Azure CosmosDB.

    login should be the endpoint uri, password should be the master key
    optionally, you can use the following extras to default these values
    {"database_name": "<DATABASE_NAME>", "collection_name": "COLLECTION_NAME"}.

    :param azure_cosmos_conn_id: Reference to the
        :ref:`Azure CosmosDB connection<howto/connection:azure_cosmos>`.
    """

    conn_name_attr = "azure_cosmos_conn_id"
    default_conn_name = "azure_cosmos_default"
    conn_type = "azure_cosmos"
    hook_name = "Azure CosmosDB"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__azure_cosmos__database_name": StringField(
                lazy_gettext("Cosmos Database Name (optional)"), widget=BS3TextFieldWidget()
            ),
            "extra__azure_cosmos__collection_name": StringField(
                lazy_gettext("Cosmos Collection Name (optional)"), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "port", "host", "extra"],
            "relabeling": {
                "login": "Cosmos Endpoint URI",
                "password": "Cosmos Master Key Token",
            },
            "placeholders": {
                "login": "endpoint uri",
                "password": "master key",
                "extra__azure_cosmos__database_name": "database name",
                "extra__azure_cosmos__collection_name": "collection name",
            },
        }

    def __init__(self, azure_cosmos_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = azure_cosmos_conn_id
        self._conn: CosmosClient | None = None

        self.default_database_name = None
        self.default_collection_name = None

    def get_conn(self) -> CosmosClient:
        """Return a cosmos db client."""
        if not self._conn:
            conn = self.get_connection(self.conn_id)
            extras = conn.extra_dejson
            endpoint_uri = conn.login
            master_key = conn.password

            self.default_database_name = extras.get("database_name") or extras.get(
                "extra__azure_cosmos__database_name"
            )
            self.default_collection_name = extras.get("collection_name") or extras.get(
                "extra__azure_cosmos__collection_name"
            )

            # Initialize the Python Azure Cosmos DB client
            self._conn = CosmosClient(endpoint_uri, {"masterKey": master_key})
        return self._conn

    def __get_database_name(self, database_name: str | None = None) -> str:
        self.get_conn()
        db_name = database_name
        if db_name is None:
            db_name = self.default_database_name

        if db_name is None:
            raise AirflowBadRequest("Database name must be specified")

        return db_name

    def __get_collection_name(self, collection_name: str | None = None) -> str:
        self.get_conn()
        coll_name = collection_name
        if coll_name is None:
            coll_name = self.default_collection_name

        if coll_name is None:
            raise AirflowBadRequest("Collection name must be specified")

        return coll_name

    def does_collection_exist(self, collection_name: str, database_name: str) -> bool:
        """Checks if a collection exists in CosmosDB."""
        if collection_name is None:
            raise AirflowBadRequest("Collection name cannot be None.")

        existing_container = list(
            self.get_conn()
            .get_database_client(self.__get_database_name(database_name))
            .query_containers(
                "SELECT * FROM r WHERE r.id=@id",
                parameters=[json.dumps({"name": "@id", "value": collection_name})],
            )
        )
        if len(existing_container) == 0:
            return False

        return True

    def create_collection(
        self,
        collection_name: str,
        database_name: str | None = None,
        partition_key: str | None = None,
    ) -> None:
        """Creates a new collection in the CosmosDB database."""
        if collection_name is None:
            raise AirflowBadRequest("Collection name cannot be None.")

        # We need to check to see if this container already exists so we don't try
        # to create it twice
        existing_container = list(
            self.get_conn()
            .get_database_client(self.__get_database_name(database_name))
            .query_containers(
                "SELECT * FROM r WHERE r.id=@id",
                parameters=[json.dumps({"name": "@id", "value": collection_name})],
            )
        )

        # Only create if we did not find it already existing
        if len(existing_container) == 0:
            self.get_conn().get_database_client(self.__get_database_name(database_name)).create_container(
                collection_name, partition_key=partition_key
            )

    def does_database_exist(self, database_name: str) -> bool:
        """Checks if a database exists in CosmosDB."""
        if database_name is None:
            raise AirflowBadRequest("Database name cannot be None.")

        existing_database = list(
            self.get_conn().query_databases(
                "SELECT * FROM r WHERE r.id=@id",
                parameters=[json.dumps({"name": "@id", "value": database_name})],
            )
        )
        if len(existing_database) == 0:
            return False

        return True

    def create_database(self, database_name: str) -> None:
        """Creates a new database in CosmosDB."""
        if database_name is None:
            raise AirflowBadRequest("Database name cannot be None.")

        # We need to check to see if this database already exists so we don't try
        # to create it twice
        existing_database = list(
            self.get_conn().query_databases(
                "SELECT * FROM r WHERE r.id=@id",
                parameters=[json.dumps({"name": "@id", "value": database_name})],
            )
        )

        # Only create if we did not find it already existing
        if len(existing_database) == 0:
            self.get_conn().create_database(database_name)

    def delete_database(self, database_name: str) -> None:
        """Deletes an existing database in CosmosDB."""
        if database_name is None:
            raise AirflowBadRequest("Database name cannot be None.")

        self.get_conn().delete_database(database_name)

    def delete_collection(self, collection_name: str, database_name: str | None = None) -> None:
        """Deletes an existing collection in the CosmosDB database."""
        if collection_name is None:
            raise AirflowBadRequest("Collection name cannot be None.")

        self.get_conn().get_database_client(self.__get_database_name(database_name)).delete_container(
            collection_name
        )

    def upsert_document(self, document, database_name=None, collection_name=None, document_id=None):
        """
        Inserts a new document (or updates an existing one) into an existing
        collection in the CosmosDB database.
        """
        # Assign unique ID if one isn't provided
        if document_id is None:
            document_id = str(uuid.uuid4())

        if document is None:
            raise AirflowBadRequest("You cannot insert a None document")

        # Add document id if isn't found
        if "id" in document:
            if document["id"] is None:
                document["id"] = document_id
        else:
            document["id"] = document_id

        created_document = (
            self.get_conn()
            .get_database_client(self.__get_database_name(database_name))
            .get_container_client(self.__get_collection_name(collection_name))
            .upsert_item(document)
        )

        return created_document

    def insert_documents(
        self, documents, database_name: str | None = None, collection_name: str | None = None
    ) -> list:
        """Insert a list of new documents into an existing collection in the CosmosDB database."""
        if documents is None:
            raise AirflowBadRequest("You cannot insert empty documents")

        created_documents = []
        for single_document in documents:
            created_documents.append(
                self.get_conn()
                .get_database_client(self.__get_database_name(database_name))
                .get_container_client(self.__get_collection_name(collection_name))
                .create_item(single_document)
            )

        return created_documents

    def delete_document(
        self,
        document_id: str,
        database_name: str | None = None,
        collection_name: str | None = None,
        partition_key: str | None = None,
    ) -> None:
        """Delete an existing document out of a collection in the CosmosDB database."""
        if document_id is None:
            raise AirflowBadRequest("Cannot delete a document without an id")
        (
            self.get_conn()
            .get_database_client(self.__get_database_name(database_name))
            .get_container_client(self.__get_collection_name(collection_name))
            .delete_item(document_id, partition_key=partition_key)
        )

    def get_document(
        self,
        document_id: str,
        database_name: str | None = None,
        collection_name: str | None = None,
        partition_key: str | None = None,
    ):
        """Get a document from an existing collection in the CosmosDB database."""
        if document_id is None:
            raise AirflowBadRequest("Cannot get a document without an id")

        try:
            return (
                self.get_conn()
                .get_database_client(self.__get_database_name(database_name))
                .get_container_client(self.__get_collection_name(collection_name))
                .read_item(document_id, partition_key=partition_key)
            )
        except CosmosHttpResponseError:
            return None

    def get_documents(
        self,
        sql_string: str,
        database_name: str | None = None,
        collection_name: str | None = None,
        partition_key: str | None = None,
    ) -> list | None:
        """Get a list of documents from an existing collection in the CosmosDB database via SQL query."""
        if sql_string is None:
            raise AirflowBadRequest("SQL query string cannot be None")

        try:
            result_iterable = (
                self.get_conn()
                .get_database_client(self.__get_database_name(database_name))
                .get_container_client(self.__get_collection_name(collection_name))
                .query_items(sql_string, partition_key=partition_key)
            )
            return list(result_iterable)
        except CosmosHttpResponseError:
            return None

    def test_connection(self):
        """Test a configured Azure Cosmos connection."""
        try:
            # Attempt to list existing databases under the configured subscription and retrieve the first in
            # the returned iterator. The Azure Cosmos API does allow for creation of a
            # CosmosClient with incorrect values but then will fail properly once items are
            # retrieved using the client. We need to _actually_ try to retrieve an object to properly test the
            # connection.
            next(iter(self.get_conn().list_databases()), None)
        except Exception as e:
            return False, str(e)
        return True, "Successfully connected to Azure Cosmos."


def get_database_link(database_id: str) -> str:
    """Get Azure CosmosDB database link"""
    return "dbs/" + database_id


def get_collection_link(database_id: str, collection_id: str) -> str:
    """Get Azure CosmosDB collection link"""
    return get_database_link(database_id) + "/colls/" + collection_id


def get_document_link(database_id: str, collection_id: str, document_id: str) -> str:
    """Get Azure CosmosDB document link"""
    return get_collection_link(database_id, collection_id) + "/docs/" + document_id
