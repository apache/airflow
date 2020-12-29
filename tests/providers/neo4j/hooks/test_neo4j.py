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
#
import json
import unittest
from unittest import mock

from airflow.models import Connection
from airflow.providers.neo4j.hooks.neo4j import Neo4jHook


class TestNeo4jHookConn(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.neo4j_hook = Neo4jHook()
        self.connection = Connection(
            conn_type='neo4j', login='login', password='password', host='host', schema='schema'
        )

    def test_get_uri_neo4j_scheme(self):

        self.neo4j_hook.get_connection = mock.Mock()
        self.neo4j_hook.get_connection.return_value = self.connection
        uri = self.neo4j_hook.get_uri(self.connection)

        self.assertEqual(uri, "bolt://host:7687")

    def test_get_uri_bolt_scheme(self):

        self.connection.extra = json.dumps({"bolt_scheme": True})
        self.neo4j_hook.get_connection = mock.Mock()
        self.neo4j_hook.get_connection.return_value = self.connection
        uri = self.neo4j_hook.get_uri(self.connection)

        self.assertEqual(uri, "bolt://host:7687")

    def test_get_uri_bolt_ssc_scheme(self):
        self.connection.extra = json.dumps({"certs_self_signed": True, "bolt_scheme": True})
        self.neo4j_hook.get_connection = mock.Mock()
        self.neo4j_hook.get_connection.return_value = self.connection
        uri = self.neo4j_hook.get_uri(self.connection)

        self.assertEqual(uri, "bolt+ssc://host:7687")

    def test_get_uri_bolt_trusted_ca_scheme(self):
        self.connection.extra = json.dumps({"certs_trusted_ca": True, "bolt_scheme": True})
        self.neo4j_hook.get_connection = mock.Mock()
        self.neo4j_hook.get_connection.return_value = self.connection
        uri = self.neo4j_hook.get_uri(self.connection)

        self.assertEqual(uri, "bolt+s://host:7687")
