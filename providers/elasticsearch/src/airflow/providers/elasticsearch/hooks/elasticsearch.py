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
from __future__ import annotations

from collections.abc import Iterable, Mapping
from copy import deepcopy
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast
from urllib import parse

from elasticsearch import Elasticsearch

from airflow.providers.common.compat.sdk import BaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from elastic_transport import ObjectApiResponse

    from airflow.models.connection import Connection as AirflowConnection


def connect(
    host: str = "localhost",
    port: int = 9200,
    user: str | None = None,
    password: str | None = None,
    scheme: str = "http",
    **kwargs: Any,
) -> ESConnection:
    return ESConnection(host, port, user, password, scheme, **kwargs)


class ElasticsearchSQLCursor:
    """A PEP 249-like Cursor class for Elasticsearch SQL API."""

    def __init__(self, es: Elasticsearch, **kwargs):
        self.es = es
        self.body = {
            "fetch_size": kwargs.get("fetch_size", 1000),
            "field_multi_value_leniency": kwargs.get("field_multi_value_leniency", False),
        }
        self._response: ObjectApiResponse | None = None

    @property
    def response(self) -> ObjectApiResponse:
        return self._response or {}  # type: ignore

    @response.setter
    def response(self, value):
        self._response = value

    @property
    def cursor(self):
        return self.response.get("cursor")

    @property
    def rows(self):
        return self.response.get("rows", [])

    @property
    def rowcount(self) -> int:
        return len(self.rows)

    @property
    def description(self) -> list[tuple]:
        return [(column["name"], column["type"]) for column in self.response.get("columns", [])]

    def execute(
        self, statement: str, params: Iterable | Mapping[str, Any] | None = None
    ) -> ObjectApiResponse:
        self.body["query"] = statement
        if params:
            self.body["params"] = params
        self.response = self.es.sql.query(body=self.body)
        if self.cursor:
            self.body["cursor"] = self.cursor
        else:
            self.body.pop("cursor", None)
        return self.response

    def fetchone(self):
        if self.rows:
            return self.rows[0]
        return None

    def fetchmany(self, size: int | None = None):
        raise NotImplementedError()

    def fetchall(self):
        results = self.rows
        while self.cursor:
            self.execute(statement=self.body["query"])
            results.extend(self.rows)
        return results

    def close(self):
        self._response = None


class ESConnection:
    """wrapper class for elasticsearch.Elasticsearch."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 9200,
        user: str | None = None,
        password: str | None = None,
        scheme: str = "http",
        **kwargs: Any,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.scheme = scheme
        self.kwargs = deepcopy(kwargs)
        kwargs.pop("fetch_size", None)
        kwargs.pop("field_multi_value_leniency", None)
        netloc = f"{host}:{port}"
        self.url = parse.urlunparse((scheme, netloc, "/", None, None, None))
        if user and password:
            self.es = Elasticsearch(self.url, http_auth=(user, password), **kwargs)
        else:
            self.es = Elasticsearch(self.url, **kwargs)

    def cursor(self) -> ElasticsearchSQLCursor:
        return ElasticsearchSQLCursor(self.es, **self.kwargs)

    def close(self):
        self.es.close()

    def commit(self):
        pass

    def execute_sql(
        self, query: str, params: Iterable | Mapping[str, Any] | None = None
    ) -> ObjectApiResponse:
        return self.cursor().execute(query, params)


class ElasticsearchSQLHook(DbApiHook):
    """
    Interact with Elasticsearch through the elasticsearch-dbapi.

    This hook uses the Elasticsearch conn_id.

    :param elasticsearch_conn_id: The :ref:`ElasticSearch connection id <howto/connection:elasticsearch>`
        used for Elasticsearch credentials.
    """

    conn_name_attr = "elasticsearch_conn_id"
    default_conn_name = "elasticsearch_default"
    connector = ESConnection  # type: ignore[assignment]
    conn_type = "elasticsearch"
    hook_name = "Elasticsearch"

    def __init__(self, schema: str = "http", connection: AirflowConnection | None = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.schema = schema

    def get_conn(self) -> ESConnection:
        """Return an elasticsearch connection object."""
        conn = self.connection

        conn_args = {
            "host": cast("str", conn.host),
            "port": cast("int", conn.port),
            "user": conn.login or None,
            "password": conn.password or None,
            "scheme": conn.schema or "http",
        }

        conn_args.update(conn.extra_dejson)

        if conn_args.get("http_compress", False):
            conn_args["http_compress"] = bool(conn_args["http_compress"])

        return connect(**conn_args)  # type: ignore[arg-type]

    def get_uri(self) -> str:
        conn = self.connection

        login = ""
        if conn.login:
            login = f"{conn.login}:{conn.password}@"
        host = conn.host or ""
        if conn.port is not None:
            host += f":{conn.port}"
        uri = f"{conn.conn_type}+{conn.schema}://{login}{host}/"

        extras_length = len(conn.extra_dejson)
        if not extras_length:
            return uri

        uri += "?"

        for arg_key, arg_value in conn.extra_dejson.items():
            extras_length -= 1
            uri += f"{arg_key}={arg_value}"

            if extras_length:
                uri += "&"

        return uri

    def _get_polars_df(
        self,
        sql,
        parameters: list | tuple | Mapping[str, Any] | None = None,
        **kwargs,
    ):
        # TODO: Custom ElasticsearchSQLCursor is incompatible with polars.read_database.
        # To support: either adapt cursor to polars._executor interface or create custom polars reader.
        # https://github.com/apache/airflow/pull/50454
        raise NotImplementedError("Polars is not supported for Elasticsearch")


class ElasticsearchPythonHook(BaseHook):
    """
    Interacts with Elasticsearch. This hook uses the official Elasticsearch Python Client.

    :param hosts: list: A list of a single or many Elasticsearch instances. Example: ["http://localhost:9200"]
    :param es_conn_args: dict: Additional arguments you might need to enter to connect to Elasticsearch.
                                Example: {"ca_cert":"/path/to/cert", "basic_auth": "(user, pass)"}
    """

    def __init__(self, hosts: list[Any], es_conn_args: dict | None = None):
        super().__init__()
        self.hosts = hosts
        self.es_conn_args = es_conn_args or {}

    def _get_elastic_connection(self):
        """Return the Elasticsearch client."""
        client = Elasticsearch(self.hosts, **self.es_conn_args)

        return client

    @cached_property
    def get_conn(self):
        """Return the Elasticsearch client (cached)."""
        return self._get_elastic_connection()

    def search(self, query: dict[Any, Any], index: str = "_all") -> dict:
        """
        Return results matching a query using Elasticsearch DSL.

        :param index: str: The index you want to query
        :param query: dict: The query you want to run

        :returns: dict: The response 'hits' object from Elasticsearch
        """
        es_client = self.get_conn
        result = es_client.search(index=index, body=query)
        return result["hits"]
