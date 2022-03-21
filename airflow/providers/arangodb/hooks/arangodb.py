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
from typing import Dict, Optional, Any

from arango import ArangoClient as ArangoDBClient, AQLQueryExecuteError
from arango.cursor import Cursor
from arango.database import StandardDatabase
from arango.result import Result

from airflow import AirflowException
from airflow.hooks.base import BaseHook


class ArangoDBHook(BaseHook):
    """
    Interact with ArangoDB.

    Performs a connection to ArangoDB and retrieves client.

    :param arangodb_conn_id: Reference to :ref:`ArangoDB connection id <howto/connection:arangodb>`.
    """

    conn_name_attr = 'arangodb_conn_id'
    default_conn_name = 'arangodb_default'
    conn_type = 'arangodb'
    hook_name = 'ArangoDB'

    def __init__(self, arangodb_conn_id: str = default_conn_name, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.hosts = None
        self.database = None
        self.username = None
        self.password = None
        self.db_conn = None
        self.arangodb_conn_id = arangodb_conn_id
        self.client: Optional[ArangoDBClient] = None
        self.get_conn()

    def get_conn(self) -> ArangoDBClient:
        """
        Function that initiates a new ArangoDB connection
        """
        if self.client is not None:
            return self.client

        conn = self.get_connection(self.arangodb_conn_id)
        self.hosts = conn.host.split(",")
        self.database = conn.schema
        self.username = conn.login
        self.password = conn.password

        self.client = ArangoDBClient(hosts=self.hosts)
        self.db_conn = self.client.db(name=self.database, username=self.username, password=self.password)

        return self.client

    def query(self, query) -> Result:
        """
        Function to create a arangodb session
        and execute the AQL query in the session.

        :param query: AQL query
        :return: Result
        """
        try:
            result = self.db_conn.aql.execute(query)
            return result
        except AQLQueryExecuteError as error:
            raise AirflowException(f"Failed to execute AQLQuery, error: {str(error)}")



    @staticmethod
    def get_ui_field_behaviour() -> Dict[str, Any]:
        return {
            "hidden_fields": ['schema', 'port', 'extra'],
            "relabeling": {
                'host': 'ArangoDB Host URL or  comma separated list of URLs (coordinators in a cluster)',
                'schema': 'ArangoDB Database',
                'login': 'ArangoDB Username',
                'password': 'ArangoDB Password',
            },
            "placeholders": {
                'host': 'eg."http://127.0.0.1:8529" or "http://127.0.0.1:8529,http://127.0.0.1:8530" (coordinators in '
                        'a cluster)',
                'schema': '_system',
                'login': 'root',
                'password': 'passwd',
            },
        }
