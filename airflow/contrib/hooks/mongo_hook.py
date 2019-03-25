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
from ssl import CERT_NONE

from airflow.hooks.base_hook import BaseHook
from pymongo import MongoClient


class MongoHook(BaseHook):
    """
    PyMongo Wrapper to Interact With Mongo Database
    Mongo Connection Documentation
    https://docs.mongodb.com/manual/reference/connection-string/index.html
    You can specify connection string options in extra field of your connection
    https://docs.mongodb.com/manual/reference/connection-string/index.html#connection-string-options
    ex. ``{replicaSet: test, ssl: True, connectTimeoutMS: 30000}``
    """
    conn_type = 'mongo'

    def __init__(self, conn_id='mongo_default', *args, **kwargs):
        super(MongoHook, self).__init__(source='mongo')

        self.mongo_conn_id = conn_id
        self.connection = self.get_connection(conn_id)
        self.extras = self.connection.extra_dejson
        self.client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client is not None:
            self.close_conn()

    def get_conn(self):
        """
        Fetches PyMongo Client
        """
        if self.client is not None:
            return self.client

        conn = self.connection

        uri = 'mongodb://{creds}{host}{port}/{database}'.format(
            creds='{}:{}@'.format(
                conn.login, conn.password
            ) if conn.login else '',

            host=conn.host,
            port='' if conn.port is None else ':{}'.format(conn.port),
            database=conn.schema
        )

        # Mongo Connection Options dict that is unpacked when passed to MongoClient
        options = self.extras

        # If we are using SSL disable requiring certs from specific hostname
        if options.get('ssl', False):
            options.update({'ssl_cert_reqs': CERT_NONE})

        self.client = MongoClient(uri, **options)

        return self.client

    def close_conn(self):
        client = self.client
        if client is not None:
            client.close()
            self.client = None

    def get_collection(self, mongo_collection, mongo_db=None):
        """
        Fetches a mongo collection object for querying.

        Uses connection schema as DB unless specified.
        """
        mongo_db = mongo_db if mongo_db is not None else self.connection.schema
        mongo_conn = self.get_conn()

        return mongo_conn.get_database(mongo_db).get_collection(mongo_collection)

    def aggregate(self, mongo_collection, aggregate_query, mongo_db=None, **kwargs):
        """
        Runs an aggregation pipeline and returns the results
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.aggregate
        https://api.mongodb.com/python/current/examples/aggregation.html
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.aggregate(aggregate_query, **kwargs)

    def find(self, mongo_collection, query, find_one=False, mongo_db=None, **kwargs):
        """
        Runs a mongo find query and returns the results
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.find
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        if find_one:
            return collection.find_one(query, **kwargs)
        else:
            return collection.find(query, **kwargs)

    def insert_one(self, mongo_collection, doc, mongo_db=None, **kwargs):
        """
        Inserts a single document into a mongo collection
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_one
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_one(doc, **kwargs)

    def insert_many(self, mongo_collection, docs, mongo_db=None, **kwargs):
        """
        Inserts many docs into a mongo collection.
        https://api.mongodb.com/python/current/api/pymongo/collection.html#pymongo.collection.Collection.insert_many
        """
        collection = self.get_collection(mongo_collection, mongo_db=mongo_db)

        return collection.insert_many(docs, **kwargs)
