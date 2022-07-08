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
import warnings
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch
from es.elastic.api import Connection as ESConnection, connect

from airflow.compat.functools import cached_property
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection as AirflowConnection
from airflow.providers.common.sql.hooks.sql import DbApiHook


class ElasticsearchSQLHook(DbApiHook):
    """
    Interact with Elasticsearch through the elasticsearch-dbapi.

    This hook uses the Elasticsearch conn_id.

    :param elasticsearch_conn_id: The :ref:`ElasticSearch connection id <howto/connection:elasticsearch>`
        used for Elasticsearch credentials.
    """

    conn_name_attr = 'elasticsearch_conn_id'
    default_conn_name = 'elasticsearch_default'
    conn_type = 'elasticsearch'
    hook_name = 'Elasticsearch'

    def __init__(self, schema: str = "http", connection: Optional[AirflowConnection] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = schema
        self.connection = connection

    def get_conn(self) -> ESConnection:
        """Returns a elasticsearch connection object"""
        conn_id = getattr(self, self.conn_name_attr)
        conn = self.connection or self.get_connection(conn_id)

        conn_args = dict(
            host=conn.host,
            port=conn.port,
            user=conn.login or None,
            password=conn.password or None,
            scheme=conn.schema or "http",
        )

        if conn.extra_dejson.get('http_compress', False):
            conn_args["http_compress"] = bool(["http_compress"])

        if conn.extra_dejson.get('timeout', False):
            conn_args["timeout"] = conn.extra_dejson["timeout"]

        conn = connect(**conn_args)

        return conn

    def get_uri(self) -> str:
        conn_id = getattr(self, self.conn_name_attr)
        conn = self.connection or self.get_connection(conn_id)

        login = ''
        if conn.login:
            login = '{conn.login}:{conn.password}@'.format(conn=conn)
        host = conn.host
        if conn.port is not None:
            host += f':{conn.port}'
        uri = '{conn.conn_type}+{conn.schema}://{login}{host}/'.format(conn=conn, login=login, host=host)

        extras_length = len(conn.extra_dejson)
        if not extras_length:
            return uri

        uri += '?'

        for arg_key, arg_value in conn.extra_dejson.items():
            extras_length -= 1
            uri += f"{arg_key}={arg_value}"

            if extras_length:
                uri += '&'

        return uri


class ElasticsearchHook(ElasticsearchSQLHook):
    """
    This class is deprecated and was renamed to ElasticsearchSQLHook.
    Please use `airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchSQLHook`.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated.
            Please use `airflow.providers.elasticsearch.hooks.elasticsearch.ElasticsearchSQLHook`.""",
            DeprecationWarning,
            stacklevel=3,
        )
        super().__init__(*args, **kwargs)


class ElasticsearchPythonHook(BaseHook):
    """
    Interacts with Elasticsearch. This hook uses the official Elasticsearch Python Client.

    :param hosts: list: A list of a single or many Elasticsearch instances. Example: ["http://localhost:9200"]
    :param es_conn_args: dict: Additional arguments you might need to enter to connect to Elasticsearch.
                                Example: {"ca_cert":"/path/to/cert", "basic_auth": "(user, pass)"}
    """

    def __init__(self, hosts: List[Any], es_conn_args: Optional[dict] = None):
        super().__init__()
        self.hosts = hosts
        self.es_conn_args = es_conn_args if es_conn_args else {}

    def _get_elastic_connection(self):
        """Returns the Elasticsearch client"""
        client = Elasticsearch(self.hosts, **self.es_conn_args)

        return client

    @cached_property
    def get_conn(self):
        """Returns the Elasticsearch client (cached)"""
        return self._get_elastic_connection()

    def search(self, query: Dict[Any, Any], index: str = "_all") -> dict:
        """
        Returns results matching a query using Elasticsearch DSL

        :param index: str: The index you want to query
        :param query: dict: The query you want to run

        :returns: dict: The response 'hits' object from Elasticsearch
        """
        es_client = self.get_conn
        result = es_client.search(index=index, body=query)
        return result['hits']
