# -*- coding: utf-8 -*-
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
import uuid

import azure.cosmos.cosmos_client as cosmos_client
from azure.cosmos.errors import HTTPFailure

from airflow.exceptions import AirflowBadRequest
from airflow.hooks.base_hook import BaseHook


class AzureCosmosDBHook(BaseHook):
    """
    Interacts with Azure CosmosDB.

    login should be the endpoint uri, password should be the master key
    optionally, you can use the following extras to default these values
    {"database_name": "<DATABASE_NAME>", "collection_name": "COLLECTION_NAME"}.

    :param azure_cosmos_conn_id: Reference to the Azure CosmosDB connection.
    :type azure_cosmos_conn_id: str
    """

    def __init__(self, azure_cosmos_conn_id='azure_cosmos_default'):
        self.conn_id = azure_cosmos_conn_id
        self.connection = self.get_connection(self.conn_id)
        self.extras = self.connection.extra_dejson

        self.endpoint_uri = self.connection.login
        self.master_key = self.connection.password
        self.default_database_name = self.extras.get('database_name')
        self.default_collection_name = self.extras.get('collection_name')
        self.cosmos_client = None

    def get_conn(self):
        """
        Return a cosmos db client.
        """
        if self.cosmos_client is not None:
            return self.cosmos_client

        # Initialize the Python Azure Cosmos DB client
        self.cosmos_client = cosmos_client.CosmosClient(self.endpoint_uri, {'masterKey': self.master_key})

        return self.cosmos_client

    def __get_database_name(self, database_name=None):
        db_name = database_name
        if db_name is None:
            db_name = self.default_database_name

        if db_name is None:
            raise AirflowBadRequest("Database name must be specified")

        return db_name

    def __get_collection_name(self, collection_name=None):
        coll_name = collection_name
        if coll_name is None:
            coll_name = self.default_collection_name

        if coll_name is None:
            raise AirflowBadRequest("Collection name must be specified")

        return coll_name

    def does_collection_exist(self, collection_name, database_name=None):
        """
        Checks if a collection exists in CosmosDB.
        """
        if collection_name is None:
            raise AirflowBadRequest("Collection name cannot be None.")

        existing_container = list(self.get_conn().QueryContainers(
            get_database_link(self.__get_database_name(database_name)), {
                "query": "SELECT * FROM r WHERE r.id=@id",
                "parameters": [
                    {"name": "@id", "value": collection_name}
                ]
            }))
        if len(existing_container) == 0:
            return False

        return True

    def create_collection(self, collection_name, database_name=None):
        """
        Creates a new collection in the CosmosDB database.
        """
        if collection_name is None:
            raise AirflowBadRequest("Collection name cannot be None.")

        # We need to check to see if this container already exists so we don't try
        # to create it twice
        existing_container = list(self.get_conn().QueryContainers(
            get_database_link(self.__get_database_name(database_name)), {
                "query": "SELECT * FROM r WHERE r.id=@id",
                "parameters": [
                    {"name": "@id", "value": collection_name}
                ]
            }))

        # Only create if we did not find it already existing
        if len(existing_container) == 0:
            self.get_conn().CreateContainer(
                get_database_link(self.__get_database_name(database_name)),
                {"id": collection_name})

    def does_database_exist(self, database_name):
        """
        Checks if a database exists in CosmosDB.
        """
        if database_name is None:
            raise AirflowBadRequest("Database name cannot be None.")

        existing_database = list(self.get_conn().QueryDatabases({
            "query": "SELECT * FROM r WHERE r.id=@id",
            "parameters": [
                {"name": "@id", "value": database_name}
            ]
        }))
        if len(existing_database) == 0:
            return False

        return True

    def create_database(self, database_name):
        """
        Creates a new database in CosmosDB.
        """
        if database_name is None:
            raise AirflowBadRequest("Database name cannot be None.")

        # We need to check to see if this database already exists so we don't try
        # to create it twice
        existing_database = list(self.get_conn().QueryDatabases({
            "query": "SELECT * FROM r WHERE r.id=@id",
            "parameters": [
                {"name": "@id", "value": database_name}
            ]
        }))

        # Only create if we did not find it already existing
        if len(existing_database) == 0:
            self.get_conn().CreateDatabase({"id": database_name})

    def delete_database(self, database_name):
        """
        Deletes an existing database in CosmosDB.
        """
        if database_name is None:
            raise AirflowBadRequest("Database name cannot be None.")

        self.get_conn().DeleteDatabase(get_database_link(database_name))

    def delete_collection(self, collection_name, database_name=None):
        """
        Deletes an existing collection in the CosmosDB database.
        """
        if collection_name is None:
            raise AirflowBadRequest("Collection name cannot be None.")

        self.get_conn().DeleteContainer(
            get_collection_link(self.__get_database_name(database_name), collection_name))

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
        if 'id' in document:
            if document['id'] is None:
                document['id'] = document_id
        else:
            document['id'] = document_id

        created_document = self.get_conn().CreateItem(
            get_collection_link(
                self.__get_database_name(database_name),
                self.__get_collection_name(collection_name)),
            document)

        return created_document

    def insert_documents(self, documents, database_name=None, collection_name=None):
        """
        Insert a list of new documents into an existing collection in the CosmosDB database.
        """
        if documents is None:
            raise AirflowBadRequest("You cannot insert empty documents")

        created_documents = []
        for single_document in documents:
            created_documents.append(
                self.get_conn().CreateItem(
                    get_collection_link(
                        self.__get_database_name(database_name),
                        self.__get_collection_name(collection_name)),
                    single_document))

        return created_documents

    def delete_document(self, document_id, database_name=None, collection_name=None):
        """
        Delete an existing document out of a collection in the CosmosDB database.
        """
        if document_id is None:
            raise AirflowBadRequest("Cannot delete a document without an id")

        self.get_conn().DeleteItem(
            get_document_link(
                self.__get_database_name(database_name),
                self.__get_collection_name(collection_name),
                document_id))

    def get_document(self, document_id, database_name=None, collection_name=None):
        """
        Get a document from an existing collection in the CosmosDB database.
        """
        if document_id is None:
            raise AirflowBadRequest("Cannot get a document without an id")

        try:
            return self.get_conn().ReadItem(
                get_document_link(
                    self.__get_database_name(database_name),
                    self.__get_collection_name(collection_name),
                    document_id))
        except HTTPFailure:
            return None

    def get_documents(self, sql_string, database_name=None, collection_name=None, partition_key=None):
        """
        Get a list of documents from an existing collection in the CosmosDB database via SQL query.
        """
        if sql_string is None:
            raise AirflowBadRequest("SQL query string cannot be None")

        # Query them in SQL
        query = {'query': sql_string}

        try:
            result_iterable = self.get_conn().QueryItems(
                get_collection_link(
                    self.__get_database_name(database_name),
                    self.__get_collection_name(collection_name)),
                query,
                partition_key)

            return list(result_iterable)
        except HTTPFailure:
            return None


def get_database_link(database_id):
    return "dbs/" + database_id


def get_collection_link(database_id, collection_id):
    return get_database_link(database_id) + "/colls/" + collection_id


def get_document_link(database_id, collection_id, document_id):
    return get_collection_link(database_id, collection_id) + "/docs/" + document_id
