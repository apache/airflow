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

from typing import Optional

from es.elastic.api import Connection as ESConnection, connect

from airflow.hooks.dbapi import DbApiHook
from airflow.models.connection import Connection as AirflowConnection


class ElasticsearchHook(DbApiHook):
    """Interact with Elasticsearch through the elasticsearch-dbapi."""

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
