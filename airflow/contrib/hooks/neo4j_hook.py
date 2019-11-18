# -*- coding: utf-8 -*-
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

from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from neo4j import GraphDatabase


class Neo4JHook(BaseHook):
    """
    Interact with Neo4J.
    This class is a thin wrapper around the neo4j python library.
    """
    _config = None
    _driver = None
    _session = None
    _n4j_conn_id = None

    def __init__(self, n4j_conn_id='n4j_default', *args, **kwargs):
        # super().__init__(*args, **kwargs) TypeError: __init__() missing 1 required positional argument: 'source'
        super().__init__()
        self._n4j_conn_id = n4j_conn_id

    def _get_config(self):
        """
        Obtain the Username + Password from the Airflow connection definition
        Store them in _config dictionary as:
         credentials = a tuple of username/password eg. ("username", "password")
         host = String for Neo4J URI eg. "bolt://1.1.1.1:7687"
        :return: None
        """
        if self._n4j_conn_id:
            # Initialize with empty dictionary
            if self._config is None:
                self._config = {}

            connection_object = self.get_connection(self._n4j_conn_id)
            if connection_object.login and connection_object.host:
                self._config['credentials'] = connection_object.login, connection_object.password
                host_string = "bolt://{0}:{1}".format(connection_object.host, connection_object.port)
                self._config['host'] = host_string
        else:
            raise AirflowException("No Neo4J connection: {}".format(self._n4j_conn_id))

    def _get_driver(self):
        """
        Establish a TCP connection to the server
        """
        # We need the configuration information before we can establish a connection with the driver
        if self._config is None:
            self._get_config()

        # Check if we already have a driver we can re-use before creating a new one
        if self._driver is None:
            self._driver = GraphDatabase.driver(
                uri=self._config['host'],
                auth=self._config['credentials']
            )

    def _get_session(self):
        """
        Get a neo4j.session from the driver.
        """
        # We need a driver to be established, so check this is done, if not, set it up
        if self._driver is None:
            self._get_driver()

        # Check if we already have a session we can re-use before creating a new one
        if self._session is None:
            self._session = self._driver.session()

        return self._session

    def run_query(self, cypher_query, parameters=None):
        """
        Uses a session to execute submit a query for execution
        :param cypher_query: Cypher query eg. MATCH (a) RETURN (a)
        :param parameters: Optional list of parameters to use with the query
        :return: neo4j.BoltStatementResult see https://neo4j.com/docs/api/python-driver/current/results.html
        """
        with self._get_session() as session:
            self.log.info("Executing query: {}".format(cypher_query))
            result = session.read_transaction(
                lambda tx, inputs: tx.run(cypher_query, inputs),
                parameters
            )
        return result
