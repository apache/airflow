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
"""This module allows to connect to a Neo4j database."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from neo4j import Driver, GraphDatabase

try:
    from airflow.sdk.bases.hook import BaseHook
except ImportError:
    from airflow.hooks.base import BaseHook  # type: ignore[attr-defined,no-redef]

if TYPE_CHECKING:
    from airflow.models import Connection


class Neo4jHook(BaseHook):
    """
    Interact with Neo4j.

    Performs a connection to Neo4j and runs the query.

    :param neo4j_conn_id: Reference to :ref:`Neo4j connection id <howto/connection:neo4j>`.
    """

    conn_name_attr = "neo4j_conn_id"
    default_conn_name = "neo4j_default"
    conn_type = "neo4j"
    hook_name = "Neo4j"
    DEFAULT_PORT = 7687

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.neo4j_conn_id = conn_id
        self.connection = kwargs.pop("connection", None)
        self.client: Driver | None = None

    def get_conn(self) -> Driver:
        """Initiate a new Neo4j connection with username, password and database schema."""
        if self.client is not None:
            return self.client

        self.connection = self.get_connection(self.neo4j_conn_id)
        uri = self.get_uri(self.connection)
        self.log.info("URI: %s", uri)
        self.client = self._create_driver(uri)
        return self.client

    def _create_driver(self, uri: str) -> Driver:
        """Create and return Neo4j Driver instance."""
        encrypted = self.connection.extra_dejson.get("encrypted", False)
        parsed_uri = urlsplit(uri)
        kwargs = {"encrypted": encrypted} if parsed_uri.scheme in ["bolt", "neo4j"] else {}
        return GraphDatabase.driver(uri, auth=(self.connection.login, self.connection.password), **kwargs)

    def get_uri(self, conn: Connection) -> str:
        """
        Build the uri based on extras.

        - Default: bolt://
        - neo4j_scheme: neo4j://
        - certs_self_signed: neo4j+ssc://
        - certs_trusted_ca: neo4j+s://

        :param conn: Connection object.
        :return: URI string
        """
        scheme = "neo4j" if conn.extra_dejson.get("neo4j_scheme", False) else "bolt"

        if conn.extra_dejson.get("certs_self_signed", False):
            scheme += "+ssc"
        elif conn.extra_dejson.get("certs_trusted_ca", False):
            scheme += "+s"

        port = conn.port or self.DEFAULT_PORT
        return f"{scheme}://{conn.host}:{port}"

    def run(self, query: str, parameters: dict[str, Any] | None = None) -> list[Any]:
        """
        Create a neo4j session and execute the query.

        :param query: Neo4j query
        :param parameters: Optional query parameters
        :return: Query results
        """
        driver = self.get_conn()
        session_params = {"database": self.connection.schema} if self.connection.schema else {}

        with driver.session(**session_params) as session:
            result = session.run(query, parameters)
            return result.data()
