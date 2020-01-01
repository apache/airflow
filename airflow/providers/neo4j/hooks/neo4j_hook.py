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
"""
This hook provides minimal thin wrapper around the neo4j python library to provide query execution
"""
import csv

from neo4j import BoltStatementResult, Driver, GraphDatabase, Session

from airflow.hooks.base_hook import BaseHook


class Neo4JHook(BaseHook):
    """
    This class enables the neo4j operator to execute queries against a configured neo4j server.
    It requires the configuration name as set in Airflow -> Admin -> Connections

    :param n4j_conn_id: Name of connection configured in Airflow
    :type n4j_conn_id: str
    """
    n4j_conn_id: str

    def __init__(self, n4j_conn_id: str = 'n4j_default', *args, **kwargs):
        self.n4j_conn_id = n4j_conn_id

    @staticmethod
    def get_config(n4j_conn_id: str) -> dict:
        """
        Obtain the Username + Password from the Airflow connection definition
        Store them in _config dictionary as:
        *credentials* -- a tuple of username/password eg. ("username", "password")
        *host* -- String for Neo4J URI eg. "bolt://1.1.1.1:7687"

        :param n4j_conn_id: Name of connection configured in Airflow
        :type n4j_conn_id: str
        :return: dictionary with configuration values
        :rtype: dict
        """
        config: dict = {}
        connection_object = Neo4JHook.get_connection(n4j_conn_id)
        if connection_object.login and connection_object.host:
            config['credentials'] = connection_object.login, connection_object.password
            config['host'] = "bolt://{0}:{1}".format(connection_object.host, connection_object.port)

        return config

    @staticmethod
    def get_driver(config: dict) -> Driver:
        """
        Establish a TCP connection to the server

        :param config: Dictionary containing the host and credentials needed to connect
        :type config: dict containing 'host' and 'credentials' keys
        :return: Driver connection
        :rtype: neo4j.Driver
        """
        return GraphDatabase.driver(
            uri=config['host'],
            auth=config['credentials']
        )

    @staticmethod
    def get_session(driver: Driver) -> Session:
        """
        Get a neo4j.session from the driver.

        :param driver Neo4J Driver (established connection to a server)
        :type driver: neo4j.Driver
        :return: Neo4J session which may contain many transactions
        :rtype: neo4j.Session
        """
        return driver.session()

    def run_query(self, cypher_query: str, parameters=None) -> BoltStatementResult:
        """
        Uses a session to execute submit a query for execution

        :param cypher_query: Cypher query eg. MATCH (a) RETURN (a)
        :type cypher_query: str
        :param parameters: Optional list of parameters to use with the query
        :type parameters: list[str]
        :return: Result of query execution (nodes & relationships)
        :rtype: neo4j.BoltStatementResult see https://neo4j.com/docs/api/python-driver/current/results.html
        """
        neo4j_config: dict = Neo4JHook.get_config(self.n4j_conn_id)
        neo4j_driver: Driver = Neo4JHook.get_driver(neo4j_config)
        neo4j_session: Session = Neo4JHook.get_session(neo4j_driver)

        with neo4j_session as session:
            self.log.info("Executing query: {}".format(cypher_query))
            result: BoltStatementResult = session.read_transaction(
                lambda tx, inputs: tx.run(cypher_query, inputs),
                parameters
            )
        return result

    def to_csv(self, result: BoltStatementResult, output_filename: str):
        """
        Local utility method to write out the results of query execution
        to a CSV. Better options could be added in the future

        :param result: Result of query execution
        :type result: neo4j.BoltStatementResult
        :param output_filename: Name of file to write
        :type output_filename: str
        :return: Count of rows written
        :rtype: int
        """
        total_row_count = 0

        with open(output_filename, 'w', newline='') as output_file:
            output_writer = csv.DictWriter(output_file, fieldnames=result.keys())
            output_writer.writeheader()

            for total_row_count, row in enumerate(result, start=1):
                output_writer.writerow(row.data())

        self.log.info("Saved %s with %s rows", output_filename, total_row_count)

        return total_row_count
