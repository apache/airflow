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
"""Test the functioning of the Neo4J Operator for Apache Airflow"""
import unittest
from unittest.mock import MagicMock, patch
from neo4j import BoltStatementResult
from airflow.contrib.operators import neo4j_operator


class TestNeo4JOperator(unittest.TestCase):
    """This class tests the minimum functionality of the Neo4J operator"""

    def test_init(self):
        """Simple test to validate we can instantiate the class"""
        operator = neo4j_operator.Neo4JOperator(task_id="test_task",
                                                cypher_query="QUERY",
                                                output_filename="filename.txt",
                                                n4j_conn_id="mock_connection",
                                                fail_on_no_results=True)
        assert operator is not None

    @patch('airflow.contrib.hooks.neo4j_hook.Neo4JHook.run_query')
    @patch('airflow.contrib.hooks.neo4j_hook.Neo4JHook.__init__', return_value=None)
    def test_execute(self, mock_hook_init, mock_hook_run_query):
        """Test that the execute() method will make the expected calls to the hook"""
        operator = neo4j_operator.Neo4JOperator(task_id="test_task",
                                                cypher_query="QUERY",
                                                output_filename="filename.txt",
                                                n4j_conn_id="mock_connection",
                                                fail_on_no_results=True)
        operator.execute(context=None)

        mock_hook_init.assert_called_once_with(n4j_conn_id='mock_connection')
        mock_hook_run_query.assert_called_once_with(cypher_query='QUERY')

    def test_make_csv(self):
        """Test that make_csv will use the results from query execution to produce
        the desired output file"""
        operator = neo4j_operator.Neo4JOperator(task_id="test_task",
                                                cypher_query="QUERY",
                                                output_filename="filename.txt",
                                                n4j_conn_id="mock_connection",
                                                fail_on_no_results=True)

        # Given a result object ...
        data_mock = MagicMock()
        data_mock.data = MagicMock(return_value={'field1': 'value1', 'field2': 'value2'})

        # Pack the data in result object
        result_mock = MagicMock(BoltStatementResult)
        result_mock.keys = MagicMock(return_value=['field1', 'field2'])
        result_mock.__iter__ = MagicMock(return_value=iter([data_mock]))

        # When it is passed into make a CSV file...
        row_count = operator._make_csv(result=result_mock)

        # Then it should....
        assert row_count == 1
        result_mock.keys.assert_called_once()
        result_mock.__iter__.assert_called_once()
        data_mock.data.assert_called_once()


if __name__ == '__main__':
    unittest.main()
