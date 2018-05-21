# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin


class MongoHook(BaseHook, LoggingMixin):
    """
    Hook for interacting with a MongoDB node or cluster using Pymongo.
    The associated connection is rewritten as the following MongoDB URI,
    where empty sections are omitted:

    mongodb://{login}:{password}@{host}:{port}/{schema}?{extra}

    The hook will not re-write the {extra} section, i.e. it must be
    properly query-encoded.

    Pymongo API documentation can be found at:
    https://api.mongodb.com/python/current/api/index.html
    """

    def __init__(self,
                 mongo_conn_id='mongo_default',
                 reuse_connection=True):
        self.mongo_conn_id = mongo_conn_id
        self.reuse_connection = reuse_connection
        self.mongo_client = None

        conn = self.get_connection(self.mongo_conn_id)

        self.mongo_uri = self.build_uri(conn)
        self.shadow_mongo_uri = self.build_uri(conn, shadow=True)

    @staticmethod
    def build_uri(conn, shadow=False):
        """
        Given an Airflow connection, construct the MongoDB connection URI.

        https://docs.mongodb.com/manual/reference/connection-string/
        """

        uri = 'mongodb://'

        if conn.login:
            uri += conn.login

            if conn.password:
                if shadow:
                    uri += ':***'
                else:
                    uri += ':' + conn.password

            uri += '@'

        uri += conn.host or 'localhost'

        if conn.port:
            uri += ':' + str(int(conn.port))

        if conn.schema:
            uri += '/' + conn.schema

        if conn.extra:
            uri += '?' + conn.extra

        return uri

    def get_conn(self, check_connection=True):
        """
        Returns an initialised pymongo MongoClient.
        """
        if self.reuse_connection and self.mongo_client:
            return self.mongo_client

        client = MongoClient(self.mongo_uri)

        self.log.debug(
            'Opening Mongo connection for: {}'.format(
                self.shadow_mongo_uri))

        if check_connection:
            try:
                client.admin.command('ismaster')
            except ConnectionFailure:
                self.log.error(
                    'Mongo connection {} failed: is the '
                    'database reachable?'.format(
                        self.shadow_mongo_uri))
                raise

        self.mongo_client = client
        return client

    def get_database(self, *args, **kwargs):
        """
        Return database `name`. If not provided, use the default database
        provided in the connection string (or the "schema" field of the
        Airflow connection).

        The full set of supported arguments can be found at:

        https://api.mongodb.com/python/current/api/pymongo/mongo_client.html#pymongo.mongo_client.MongoClient.get_database
        """
        return self.get_conn().get_database(*args, **kwargs)
