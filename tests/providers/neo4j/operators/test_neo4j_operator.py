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
Test the functioning of the Neo4J Operator for Apache Airflow
"""
import unittest
from unittest.mock import patch

from airflow.providers.neo4j.operators import neo4j_operator


class TestNeo4JOperator(unittest.TestCase):
    """
    This class tests the minimum functionality of the Neo4J operator
    """

    def test_init(self):
        """
        Simple test to validate we can instantiate the class
        """
        operator = neo4j_operator.Neo4JOperator(task_id="test_task",
                                                cypher_query="QUERY",
                                                output_filename="filename.txt",
                                                n4j_conn_id="mock_connection",
                                                soft_fail=True)
        assert operator is not None

    @patch('airflow.providers.neo4j.hooks.neo4j_hook.Neo4JHook.run_query')
    @patch('airflow.providers.neo4j.hooks.neo4j_hook.Neo4JHook.__init__', return_value=None)
    def test_execute(self, mock_hook_init, mock_hook_run_query):
        """
        Test that the execute() method will make the expected calls to the hook
        """
        operator = neo4j_operator.Neo4JOperator(task_id="test_task",
                                                cypher_query="QUERY",
                                                output_filename="filename.txt",
                                                n4j_conn_id="mock_connection",
                                                soft_fail=True)
        operator.execute(context=None)

        mock_hook_init.assert_called_once_with(n4j_conn_id='mock_connection')
        mock_hook_run_query.assert_called_once_with(cypher_query='QUERY')
