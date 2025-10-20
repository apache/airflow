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

# Default Neo4j port
DEFAULT_NEO4J_PORT = 7687


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
        encrypted = self.connection.extra_dejson.get("encrypted", False)
        self.client = self._create_driver(self.connection, encrypted, uri)
        return self.client

    def _create_driver(self, conn: Connection, encrypted: bool, uri: str) -> Driver:
        """
        Create a Neo4j driver instance.

        :param conn: Connection object.
        :param encrypted: Boolean indicating if encrypted connection is required.
        :param uri: URI string for connection.
        :return: Neo4j Driver instance.
        """
        parsed_uri = urlsplit(uri)
        kwargs: dict[str, Any] = {}
        if parsed_uri.scheme in ["bolt", "neo4j"]:
            kwargs["encrypted"] = encrypted
        auth = None
        if conn.login is not None and conn.password is not None:
            auth = (conn.login, conn.password)
        return GraphDatabase.driver(uri, auth=auth, **kwargs)

    def get_uri(self, conn: Connection) -> str:
        """
        Build the URI based on connection extras.

        - Default scheme: bolt
        - Neo4j scheme: neo4j (if enabled)
        - Encryption schemes:
            - certs_self_signed: +ssc
            - certs_trusted_ca: +s

        :param conn: Connection object.
        :return: Constructed URI string.
        """
        scheme = "neo4j" if conn.extra_dejson.get("neo4j_scheme", False) else "bolt"

        # Determine encryption scheme
        encryption_scheme = ""
        if conn.extra_dejson.get("certs_self_signed", False):
            encryption_scheme = "+ssc"
        elif conn.extra_dejson.get("certs_trusted_ca", False):
            encryption_scheme = "+s"

        port = conn.port or DEFAULT_NEO4J_PORT
        return f"{scheme}{encryption_scheme}://{conn.host}:{port}"

    def run(self, query: str, parameters: dict[str, Any] | None = None) -> list[Any]:
        """
        Execute a Neo4j query within a session.

        :param query: Neo4j query string.
        :param parameters: Optional parameters for the query.
        :return: List of result records.
        """
        driver = self.get_conn()
        session_params = {"database": self.connection.schema} if self.connection.schema else {}

        with driver.session(**session_params) as session:
            result = session.run(query, parameters) if parameters else session.run(query)
            return result.data()
