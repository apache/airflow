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
"""Hook for Mongo DB"""
from ssl import CERT_NONE
from types import TracebackType
from typing import List, Optional, Type

import pymongo
from pymongo import MongoClient, ReplaceOne

from airflow.hooks.base_hook import BaseHook


class MongoHook(BaseHook):
    """
    PyMongo Wrapper to Interact With Mongo Database
    Mongo Connection Documentation
    https://docs.mongodb.com/manual/reference/connection-string/index.html
    You can specify connection string options in extra field of your connection
    https://docs.mongodb.com/manual/reference/connection-string/index.html#connection-string-options

    If you want use DNS seedlist, set `srv` to True.

    ex.
        {"srv": true, "replicaSet": "test", "ssl": true, "connectTimeoutMS": 30000}
    """

    conn_name_attr = 'conn_id'
    default_conn_name = 'mongo_default'
    conn_type = 'mongo'
    hook_name = 'MongoDB'

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:

        super().__init__()
        self.mongo_conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson.copy()
        self.client = None

        srv = self.extras.pop('srv', False)
        scheme = 'mongodb+srv' if srv else 'mongodb'

        self.uri = '{scheme}://{creds}{host}{port}/{database}'.format(
            scheme=scheme,
            creds=f'{self.connection.login}:{self.connection.password}@' if self.connection.login else '',
            host=self.connection.host,
            port='' if self.connection.port is None else f':{self.connection.port}',
            database=self.connection.schema,
        )

    def __enter__(self):
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        if self.client is not None:
            self.close_conn()

    def get_conn(self) -> MongoClient:
        """Fetches PyMongo Client"""
        if self.client is not None:
            return self.client

        # Mongo Connection Options dict that is unpacked when passed to MongoClient
        options = self.extras

        # If we are using SSL disable requiring certs from specific hostname
        if options.get('ssl', False):
            options.update({'ssl_cert_reqs': CERT_NONE})

        self.client = MongoClient(self.uri, **options)

        return self.client

    def close_conn(self) -> None:
        """Closes connection"""
        client = self.client
        if client is not None:
            client.close()
            self.client = None

    def get_collection(
        self, mongo_collection: str, mongo_db: Optional[str] = None
    ) -> pymongo.collection.Collection:
        """
        Fetches a mongo collection object for querying.

        Uses connection schema as DB unless specified.
        """
        mongo_db = mongo_db if mongo_db is not None else self.connection.schema
        mongo_conn: MongoClient = self.get_conn()

        return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)

    def aggregate(
        self, mongo_collection: str, aggregate_query: list, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.command_cursor.CommandCursor:
        """
        Runs an aggregation pipeline and returns the results
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
        https://api.mongodb.com/python/current/examples/aggregation.html
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.aggregate(aggregate_query, **kwargs)

    def find(
        self,
        mongo_collection: str,
        query: dict,
        find_one: bool = False,
        mongo_db: Optional[str] = None,
        **kwargs,
    ) -> pymongo.cursor.Cursor:
        """
        Runs a mongo find query and returns the results
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if find_one:
            return collection.find_one(query, **kwargs)
        else:
            return collection.find(query, **kwargs)

    def insert_one(
        self, mongo_collection: str, doc: dict, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.results.InsertOneResult:
        """
        Inserts a single document into a mongo collection
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_one(doc, **kwargs)

    def insert_many(
        self, mongo_collection: str, docs: dict, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.results.InsertManyResult:
        """
        Inserts many docs into a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_many
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_many(docs, **kwargs)

    def update_one(
        self,
        mongo_collection: str,
        filter_doc: dict,
        update_doc: dict,
        mongo_db: Optional[str] = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Updates a single document in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_one

        :param mongo_collection: The name of the collection to update.
        :type mongo_collection: str
        :param filter_doc: A query that matches the documents to update.
        :type filter_doc: dict
        :param update_doc: The modifications to apply.
        :type update_doc: dict
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        :type mongo_db: str

        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.update_one(filter_doc, update_doc, **kwargs)

    def update_many(
        self,
        mongo_collection: str,
        filter_doc: dict,
        update_doc: dict,
        mongo_db: Optional[str] = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Updates one or more documents in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.update_many

        :param mongo_collection: The name of the collection to update.
        :type mongo_collection: str
        :param filter_doc: A query that matches the documents to update.
        :type filter_doc: dict
        :param update_doc: The modifications to apply.
        :type update_doc: dict
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        :type mongo_db: str

        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.update_many(filter_doc, update_doc, **kwargs)

    def replace_one(
        self,
        mongo_collection: str,
        doc: dict,
        filter_doc: Optional[dict] = None,
        mongo_db: Optional[str] = None,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        """
        Replaces a single document in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.replace_one

        .. note::
            If no ``filter_doc`` is given, it is assumed that the replacement
            document contain the ``_id`` field which is then used as filters.

        :param mongo_collection: The name of the collection to update.
        :type mongo_collection: str
        :param doc: The new document.
        :type doc: dict
        :param filter_doc: A query that matches the documents to replace.
            Can be omitted; then the _id field from doc will be used.
        :type filter_doc: dict
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        :type mongo_db: str
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if not filter_doc:
            filter_doc = {'_id': doc['_id']}

        return collection.replace_one(filter_doc, doc, **kwargs)

    def replace_many(
        self,
        mongo_collection: str,
        docs: List[dict],
        filter_docs: Optional[List[dict]] = None,
        mongo_db: Optional[str] = None,
        upsert: bool = False,
        collation: Optional[pymongo.collation.Collation] = None,
        **kwargs,
    ) -> pymongo.results.BulkWriteResult:
        """
        Replaces many documents in a mongo collection.

        Uses bulk_write with multiple ReplaceOne operations
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.bulk_write

        .. note::
            If no ``filter_docs``are given, it is assumed that all
            replacement documents contain the ``_id`` field which are then
            used as filters.

        :param mongo_collection: The name of the collection to update.
        :type mongo_collection: str
        :param docs: The new documents.
        :type docs: list[dict]
        :param filter_docs: A list of queries that match the documents to replace.
            Can be omitted; then the _id fields from docs will be used.
        :type filter_docs: list[dict]
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        :type mongo_db: str
        :param upsert: If ``True``, perform an insert if no documents
            match the filters for the replace operation.
        :type upsert: bool
        :param collation: An instance of
            :class:`~pymongo.collation.Collation`. This option is only
            supported on MongoDB 3.4 and above.
        :type collation: pymongo.collation.Collation

        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if not filter_docs:
            filter_docs = [{'_id': doc['_id']} for doc in docs]

        requests = [
            ReplaceOne(filter_docs[i], docs[i], upsert=upsert, collation=collation) for i in range(len(docs))
        ]

        return collection.bulk_write(requests, **kwargs)

    def delete_one(
        self, mongo_collection: str, filter_doc: dict, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.results.DeleteResult:
        """
        Deletes a single document in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.delete_one

        :param mongo_collection: The name of the collection to delete from.
        :type mongo_collection: str
        :param filter_doc: A query that matches the document to delete.
        :type filter_doc: dict
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        :type mongo_db: str

        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.delete_one(filter_doc, **kwargs)

    def delete_many(
        self, mongo_collection: str, filter_doc: dict, mongo_db: Optional[str] = None, **kwargs
    ) -> pymongo.results.DeleteResult:
        """
        Deletes one or more documents in a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.delete_many

        :param mongo_collection: The name of the collection to delete from.
        :type mongo_collection: str
        :param filter_doc: A query that matches the documents to delete.
        :type filter_doc: dict
        :param mongo_db: The name of the database to use.
            Can be omitted; then the database from the connection string is used.
        :type mongo_db: str

        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.delete_many(filter_doc, **kwargs)
