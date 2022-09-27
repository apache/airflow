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
from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from airflow.models import Connection
from airflow.providers.arangodb.hooks.arangodb import ArangoDBHook
from airflow.utils import db

arangodb_client_mock = Mock(name="arangodb_client_for_test")


class TestArangoDBHook(unittest.TestCase):
    def setUp(self):
        super().setUp()
        db.merge_conn(
            Connection(
                conn_id='arangodb_default',
                conn_type='arangodb',
                host='http://127.0.0.1:8529',
                login='root',
                password='password',
                schema='_system',
            )
        )

    @patch(
        "airflow.providers.arangodb.hooks.arangodb.ArangoDBClient",
        autospec=True,
        return_value=arangodb_client_mock,
    )
    def test_get_conn(self, arango_mock):
        arangodb_hook = ArangoDBHook()

        assert arangodb_hook.hosts == ['http://127.0.0.1:8529']
        assert arangodb_hook.username == 'root'
        assert arangodb_hook.password == 'password'
        assert arangodb_hook.database == '_system'
        assert arangodb_hook.client is not None
        assert arango_mock.called
        assert isinstance(arangodb_hook.client, Mock)

    @patch(
        "airflow.providers.arangodb.hooks.arangodb.ArangoDBClient",
        autospec=True,
        return_value=arangodb_client_mock,
    )
    def test_query(self, arango_mock):
        arangodb_hook = ArangoDBHook()
        arangodb_hook.db_conn = Mock(name="arangodb_database_for_test")

        arangodb_query = "FOR doc IN students RETURN doc"
        arangodb_hook.query(arangodb_query)

        assert arango_mock.called
        assert isinstance(arangodb_hook.client, Mock)
        assert arango_mock.return_value.db.called

    @patch(
        "airflow.providers.arangodb.hooks.arangodb.ArangoDBClient",
        autospec=True,
        return_value=arangodb_client_mock,
    )
    def test_create_database(self, arango_mock):
        arangodb_hook = ArangoDBHook()
        arangodb_hook.create_database(name="_system")

        arango_mock.return_value.db.return_value.has_database.return_value = False

        assert arango_mock.called
        assert isinstance(arangodb_hook.client, Mock)
        assert arango_mock.return_value.db.called
        assert arango_mock.return_value.db.return_value.has_database.called

    @patch(
        "airflow.providers.arangodb.hooks.arangodb.ArangoDBClient",
        autospec=True,
        return_value=arangodb_client_mock,
    )
    def test_create_collection(self, arango_mock):
        arangodb_hook = ArangoDBHook()
        arangodb_hook.create_collection(name="student")

        arango_mock.return_value.db.return_value.has_collection.return_value = False

        assert arango_mock.called
        assert isinstance(arangodb_hook.client, Mock)
        assert arango_mock.return_value.db.called
        assert arango_mock.return_value.db.return_value.has_collection.called

    @patch(
        "airflow.providers.arangodb.hooks.arangodb.ArangoDBClient",
        autospec=True,
        return_value=arangodb_client_mock,
    )
    def test_create_graph(self, arango_mock):
        arangodb_hook = ArangoDBHook()
        arangodb_hook.create_graph(name="student_network")

        arango_mock.return_value.db.return_value.has_graph.return_value = False

        assert arango_mock.called
        assert isinstance(arangodb_hook.client, Mock)
        assert arango_mock.return_value.db.called
        assert arango_mock.return_value.db.return_value.has_graph.called
