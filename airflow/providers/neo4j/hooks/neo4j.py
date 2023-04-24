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

from typing import Any
from urllib.parse import urlsplit

from neo4j import Driver, GraphDatabase

from airflow.hooks.base import BaseHook
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

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.neo4j_conn_id = conn_id
        self.connection = kwargs.pop("connection", None)
        self.client: Driver | None = None

    def get_conn(self) -> Driver:
        """
        Function that initiates a new Neo4j connection
        with username, password and database schema.
        """
        if self.client is not None:
            return self.client

        self.connection = self.get_connection(self.neo4j_conn_id)

        uri = self.get_uri(self.connection)
        self.log.info("URI: %s", uri)

        is_encrypted = self.connection.extra_dejson.get("encrypted", False)

        self.client = self.get_client(self.connection, is_encrypted, uri)

        return self.client

    def get_client(self, conn: Connection, encrypted: bool, uri: str) -> Driver:
        """
        Function to determine that relevant driver based on extras.
        :param conn: Connection object.
        :param encrypted: boolean if encrypted connection or not.
        :param uri: uri string for connection.
        :return: Driver
        """
        parsed_uri = urlsplit(uri)
        kwargs: dict[str, Any] = {}
        if parsed_uri.scheme in ["bolt", "neo4j"]:
            kwargs["encrypted"] = encrypted
        return GraphDatabase.driver(uri, auth=(conn.login, conn.password), **kwargs)

    def get_uri(self, conn: Connection) -> str:
        """
        Build the uri based on extras
        - Default - uses bolt scheme(bolt://)
        - neo4j_scheme - neo4j://
        - certs_self_signed - neo4j+ssc://
        - certs_trusted_ca - neo4j+s://

        :param conn: connection object.
        :return: uri
        """
        use_neo4j_scheme = conn.extra_dejson.get("neo4j_scheme", False)
        scheme = "neo4j" if use_neo4j_scheme else "bolt"

        # Self signed certificates
        ssc = conn.extra_dejson.get("certs_self_signed", False)

        # Only certificates signed by CA.
        trusted_ca = conn.extra_dejson.get("certs_trusted_ca", False)
        encryption_scheme = ""

        if ssc:
            encryption_scheme = "+ssc"
        elif trusted_ca:
            encryption_scheme = "+s"

        return f"{scheme}{encryption_scheme}://{conn.host}:{7687 if conn.port is None else conn.port}"

    def run(self, query) -> list[Any]:
        """
        Function to create a neo4j session
        and execute the query in the session.

        :param query: Neo4j query
        :return: Result
        """
        driver = self.get_conn()
        if not self.connection.schema:
            with driver.session() as session:
                result = session.run(query)
                return result.data()
        else:
            with driver.session(database=self.connection.schema) as session:
                result = session.run(query)
                return result.data()
