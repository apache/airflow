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
    _n4j_conn_id = None

    def __init__(self, n4j_conn_id='n4j_default', *args, **kwargs):
        super().__init__()
        self._n4j_conn_id = n4j_conn_id

    @staticmethod
    def get_config(n4j_conn_id):
        """
        Obtain the Username + Password from the Airflow connection definition
        Store them in _config dictionary as:
         credentials = a tuple of username/password eg. ("username", "password")
         host = String for Neo4J URI eg. "bolt://1.1.1.1:7687"
        :return: dictionary with configuration values
        """
        config = {}
        if n4j_conn_id:
            # Initialize with empty dictionary
            connection_object = Neo4JHook.get_connection(n4j_conn_id)
            if connection_object.login and connection_object.host:
                config['credentials'] = connection_object.login, connection_object.password
                config['host'] = "bolt://{0}:{1}".format(connection_object.host, connection_object.port)
        else:
            raise AirflowException("No Neo4J connection: {}".format(n4j_conn_id))

        return config

    @staticmethod
    def get_driver(config):
        """
        Establish a TCP connection to the server
        """
        # Check if we already have a driver we can re-use before creating a new one
        return GraphDatabase.driver(
            uri=config['host'],
            auth=config['credentials']
        )

    @staticmethod
    def get_session(driver):
        """
        Get a neo4j.session from the driver.
        """
        # Check if we already have a session we can re-use before creating a new one
        return driver.session()

    def run_query(self, cypher_query, parameters=None):
        """
        Uses a session to execute submit a query for execution
        :param cypher_query: Cypher query eg. MATCH (a) RETURN (a)
        :param parameters: Optional list of parameters to use with the query
        :return: neo4j.BoltStatementResult see https://neo4j.com/docs/api/python-driver/current/results.html
        """
        a = Neo4JHook.get_config(self._n4j_conn_id)
        b = Neo4JHook.get_driver(a)
        c = Neo4JHook.get_session(b)

        with c as session:
            self.log.info("Executing query: {}".format(cypher_query))
            result = session.read_transaction(
                lambda tx, inputs: tx.run(cypher_query, inputs),
                parameters
            )
        return result
