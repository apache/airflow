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
"""This module allows connecting to a ArangoDB."""
from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from arango import AQLQueryExecuteError, ArangoClient as ArangoDBClient

from airflow import AirflowException
from airflow.hooks.base import BaseHook

if TYPE_CHECKING:
    from arango.cursor import Cursor
    from arango.database import StandardDatabase

    from airflow.models import Connection


class ArangoDBHook(BaseHook):
    """
    Interact with ArangoDB.

    Performs a connection to ArangoDB and retrieves client.

    :param arangodb_conn_id: Reference to :ref:`ArangoDB connection id <howto/connection:arangodb>`.
    """

    conn_name_attr = "arangodb_conn_id"
    default_conn_name = "arangodb_default"
    conn_type = "arangodb"
    hook_name = "ArangoDB"

    def __init__(self, arangodb_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.arangodb_conn_id = arangodb_conn_id

    @cached_property
    def client(self) -> ArangoDBClient:
        """Initiates a new ArangoDB connection (cached)."""
        return ArangoDBClient(hosts=self.hosts)

    @cached_property
    def db_conn(self) -> StandardDatabase:
        """Connect to an ArangoDB database and return the database API wrapper."""
        return self.client.db(name=self.database, username=self.username, password=self.password)

    @cached_property
    def _conn(self) -> Connection:
        return self.get_connection(self.arangodb_conn_id)

    @property
    def hosts(self) -> list[str]:
        if not self._conn.host:
            raise AirflowException(f"No ArangoDB Host(s) provided in connection: {self.arangodb_conn_id!r}.")
        return self._conn.host.split(",")

    @property
    def database(self) -> str:
        if not self._conn.schema:
            raise AirflowException(f"No ArangoDB Database provided in connection: {self.arangodb_conn_id!r}.")
        return self._conn.schema

    @property
    def username(self) -> str:
        if not self._conn.login:
            raise AirflowException(f"No ArangoDB Username provided in connection: {self.arangodb_conn_id!r}.")
        return self._conn.login

    @property
    def password(self) -> str:
        return self._conn.password or ""

    def get_conn(self) -> ArangoDBClient:
        """Function that initiates a new ArangoDB connection (cached)."""
        return self.client

    def query(self, query, **kwargs) -> Cursor:
        """
        Function to create an ArangoDB session
        and execute the AQL query in the session.

        :param query: AQL query
        """
        try:
            if self.db_conn:
                result = self.db_conn.aql.execute(query, **kwargs)
                if TYPE_CHECKING:
                    assert isinstance(result, Cursor)
                return result
            else:
                raise AirflowException(
                    f"Failed to execute AQLQuery, error connecting to database: {self.database}"
                )
        except AQLQueryExecuteError as error:
            raise AirflowException(f"Failed to execute AQLQuery, error: {str(error)}")

    def create_collection(self, name):
        if not self.db_conn.has_collection(name):
            self.db_conn.create_collection(name)
            return True
        else:
            self.log.info("Collection already exists: %s", name)
            return False

    def create_database(self, name):
        if not self.db_conn.has_database(name):
            self.db_conn.create_database(name)
            return True
        else:
            self.log.info("Database already exists: %s", name)
            return False

    def create_graph(self, name):
        if not self.db_conn.has_graph(name):
            self.db_conn.create_graph(name)
            return True
        else:
            self.log.info("Graph already exists: %s", name)
            return False

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        return {
            "hidden_fields": ["port", "extra"],
            "relabeling": {
                "host": "ArangoDB Host URL or  comma separated list of URLs (coordinators in a cluster)",
                "schema": "ArangoDB Database",
                "login": "ArangoDB Username",
                "password": "ArangoDB Password",
            },
            "placeholders": {
                "host": 'eg."http://127.0.0.1:8529" or "http://127.0.0.1:8529,http://127.0.0.1:8530"'
                " (coordinators in a cluster)",
                "schema": "_system",
                "login": "root",
                "password": "password",
            },
        }
