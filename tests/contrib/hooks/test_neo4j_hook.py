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
import unittest
from unittest.mock import Mock, patch
from airflow.contrib.hooks import neo4j_hook
from airflow.models import Connection
from airflow.utils import db


class TestNeo4JHook(unittest.TestCase):

    _conn_id = 'neo4j_conn_id_test'
    _hook = None

    def setUp(self):
        self._hook = neo4j_hook.Neo4JHook(self._conn_id)

    @patch('airflow.contrib.hooks.neo4j_hook.Neo4JHook.get_config')
    @patch('airflow.contrib.hooks.neo4j_hook.Neo4JHook.get_driver')
    @patch('airflow.contrib.hooks.neo4j_hook.Neo4JHook.get_session')
    def test_run_query_with_session(self, mock_get_config, mock_get_driver, mock_get_session):
        result = self._hook.run_query(cypher_query="QUERY")
        assert mock_get_config.called
        assert mock_get_driver.called
        assert mock_get_session.called
        # assert result.assert_called_once_with(lambda tx, inputs: tx.run("QUERY", inputs), None)

    @patch('neo4j.GraphDatabase.driver')
    def test_get_driver(self, mock_driver):
        config = {
            "host": "host",
            "credentials": ("username", "password")
        }
        result = self._hook.get_driver(config)
        assert mock_driver.called_with(uri="host", auth=("username", "password"))

    def test_get_config(self):
        db.merge_conn(
            Connection(
                conn_id='mock_config',
                conn_type='jdbc',
                host='localhost',
                port=1234,
                login='your_name_here',
                password='your_token_here'
            ))
        result = self._hook.get_config('mock_config')
        assert result['host'] == 'bolt://localhost:1234'
        assert result['credentials'] == ('your_name_here', 'your_token_here')

    def test_get_session(self):
        driver = Mock()
        result = self._hook.get_session(driver)
        assert driver.session.called


if __name__ == '__main__':
    unittest.main()
