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
"""This module allows connecting to an Graph DB using the Gremlin Client."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from gremlin_python.driver.client import Client

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from airflow.models import Connection


logger = logging.getLogger(__name__)


class GremlinHook(BaseHook):
    """
    Interact with Graph DB using the Gremlin Client.

    This hook creates a connection to Graph DB and allows you to run Gremlin queries.`

    :param gremlin_conn_id: Reference to the connection ID configured in Airflow.
    """

    conn_name_attr = "gremlin__conn_id"
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

    def get_conn(self, serializer=None) -> Client:
        """
        Establish a connection to Graph DB with the Gremlin Client.

        :param serializer: Message serializer to use for the client.

        :return: An instance of the Gremlin Client.
        """
        if self.client is not None:
            return self.client

        self.connection = self.get_connection(self.gremlin_conn_id)

        uri = self.get_uri(self.connection)
        self.log.info("Connecting to URI: %s", uri)

        self.client = self.get_client(
            self.connection,
            self.traversal_source,
            uri,
            message_serializer=serializer,
        )
        return self.client

    def get_uri(self, conn: Connection) -> str:
        """
        Build the URI from the connection object and extra parameters.

        :param conn: Airflow Connection object.

        :return: URI string.
        """
        # For Graph DB using Gremlin, the secure WebSocket scheme is typically "wss"
        scheme = "wss" if conn.conn_type == "gremlin" else "ws"
        host = conn.host
        port = conn.port if conn.port is not None else self.default_port
        schema = "" if conn.conn_type == "gremlin" else "gremlin"
        return f"{scheme}://{host}:{port}/{schema}"

    def get_client(
        self, conn: Connection, traversal_source: str, uri: str, message_serializer=None
    ) -> Client:
        """
        Create and return a new Gremlin client.

        :param conn: Airflow Connection object.
        :param traversal_source: Traversal source for the Gremlin client.
        :param uri: URI string for connecting to Graph DB.
        :param message_serializer: Message serializer to use for the client.

        :return: An instance of the Gremlin Client.
        """
        # Build the username. This example uses the connection's schema and login.
        login = conn.login if conn.login not in ["mylogin", None] else ""
        schema = conn.schema if conn.schema not in ["gremlin", None] else ""
        password = conn.password if conn.password not in ["mysecret", None] else ""
        username = f"/dbs/{login}/colls/{schema}" if login and schema else ""
        # Build the kwargs for the Client.
        client_kwargs = {
            "url": uri,
            "traversal_source": traversal_source,
            "username": username,
            "password": password,
        }

        # If a serializer is provided, check if it's a type and instantiate it.
        if message_serializer is not None:
            if isinstance(message_serializer, type):
                message_serializer = message_serializer()
            client_kwargs["message_serializer"] = message_serializer

        return Client(**client_kwargs)

    def run(self, query: str, serializer=None, bindings=None, request_options=None) -> list[Any]:
        """
        Execute a Gremlin query and return the results.

        :param query: Gremlin query string.
        :param serializer: Message serializer to use for the query.
        :param bindings: Bindings to use for the query.
        :param request_options: Request options to use for the query.

        :return: List containing the query results.
        """
        client = self.get_conn(serializer)

        try:
            results_list = (
                client.submit(message=query, bindings=bindings, request_options=request_options)
                .all()
                .result()
            )
        except Exception as e:
            logger.error("An error occurred while running the query: %s", str(e))
            raise e
        finally:
            if client is not None:
                client.close()
                self.client = None

        return results_list
