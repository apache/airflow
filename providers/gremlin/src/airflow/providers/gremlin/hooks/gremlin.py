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
This module allows connecting to an Graph DB using the Gremlin API.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import nest_asyncio
from gremlin_python.driver.client import Client
from gremlin_python.driver.serializer import GraphSONSerializersV2d0

from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from airflow.models import Connection

# Apply nest_asyncio to allow nested event loops.
nest_asyncio.apply()

logger = logging.getLogger(__name__)


class GremlinHook(BaseHook):
    """
    Interact with Graph DB using the Gremlin API.

    This hook creates a connection to Graph DB and allows you to run Gremlin queries.`

    :param gremlin_conn_id: Reference to the connection ID configured in Airflow.
    """

    conn_name_attr = "gremlin_conn_id"
    default_conn_name = "gremlin_default"
    conn_type = "gremlin"
    hook_name = "Gremlin"
    default_port = 443
    traversal_source = "g"

    def __init__(self, conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.gremlin_conn_id = conn_id
        self.connection = kwargs.pop("connection", None)
        self.client: Client | None = None

    def get_conn(self) -> Client:
        """
        Establish a connection to Graph DB with the Gremlin API.
        """
        if self.client is not None:
            return self.client

        self.connection = self.get_connection(self.gremlin_conn_id)

        uri = self.get_uri(self.connection)
        self.log.info("Connecting to URI: %s", uri)

        self.client = self.get_client(self.connection, self.traversal_source, uri)

        return self.client

    def get_uri(self, conn: Connection) -> str:
        """
        Build the URI from the connection object and extra parameters.

        :param conn: Airflow Connection object.
        :return: URI string.
        """
        # Check for extra parameter "development-graphdb". Adjust the scheme if needed.
        use_development = conn.extra_dejson.get("development-graphdb", False)
        # For Graph DB using Gremlin, the secure WebSocket scheme is typically "wss"
        scheme = "wss" if use_development or not use_development else "ws"
        host = conn.host
        port = conn.port if conn.port is not None else self.default_port
        return f"{scheme}://{host}:{port}/"

    def get_client(self, conn: Connection, traversal_source: str, uri: str) -> Client:
        """
        Create and return a new Gremlin client.

        :param conn: Airflow Connection object.
        :param traversal_source: Traversal source for the Gremlin client.
        :param uri: URI string for connecting to Graph DB.
        :return: An instance of the Gremlin Client.
        """
        # Build the username. This example uses the connection's schema and login.
        username = f"/dbs/{conn.schema}/colls/{conn.login}" if conn.schema and conn.login else ""
        password = conn.password or ""

        # Remove the redundant addition of traversal_source to kwargs.
        return Client(
            url=uri,
            traversal_source=traversal_source,
            username=username,
            password=password,
            message_serializer=GraphSONSerializersV2d0(),
        )

    def run(self, query: str) -> list[Any]:
        """
        Execute a Gremlin query and return the results.

        :param query: Gremlin query string.
        :return: List containing the query results.
        """
        client = self.get_conn()

        try:
            results_list = client.submit(query).all().result()
        except Exception as e:
            logger.error("An error occurred while running the query: %s", str(e))
            raise e

        return results_list
