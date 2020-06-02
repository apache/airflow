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

from airflow.models import Connection
from airflow.utils.session import create_session, provide_session
from airflow.www import app
from tests.test_utils.db import clear_db_connections


class TestConnectionEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        # we want only the connection created here for this test
        with create_session() as session:
            session.query(Connection).delete()

    def tearDown(self) -> None:
        clear_db_connections()


class TestDeleteConnection(TestConnectionEndpoint):
    @unittest.skip("Not implemented yet")
    def test_should_response_200(self):
        response = self.client.delete("/api/v1/connections/1")
        assert response.status_code == 200


class TestGetConnection(TestConnectionEndpoint):

    @provide_session
    def test_should_response_200(self, session):
        connection_model = Connection(conn_id='test-connection-id',
                                      conn_type='mysql',
                                      host='mysql',
                                      login='login',
                                      schema='testschema',
                                      port=80
                                      )
        session.add(connection_model)
        session.commit()
        result = session.query(Connection).all()
        assert len(result) == 1
        response = self.client.get("/api/v1/connections/test-connection-id")
        assert response.status_code == 200
        self.assertEqual(
            response.json,
            {
                "connection_id": "test-connection-id",
                "conn_type": 'mysql',
                "host": 'mysql',
                "login": 'login',
                'schema': 'testschema',
                'port': 80
            },
        )


class TestGetConnections(TestConnectionEndpoint):

    @provide_session
    def test_should_response_200(self, session):
        connection_model_1 = Connection(conn_id='test-connection-id-1',
                                        conn_type='mysql',
                                        host='mysql',
                                        login='login',
                                        schema='testschema',
                                        port=80
                                        )
        connection_model_2 = Connection(conn_id='test-connection-id-2',
                                        conn_type='mysql',
                                        host='mysql',
                                        login='login',
                                        schema='testschema',
                                        port=80
                                        )
        connections = [connection_model_1, connection_model_2]
        session.add_all(connections)
        session.commit()
        result = session.query(Connection).all()
        assert len(result) == 2
        response = self.client.get("/api/v1/connections")
        assert response.status_code == 200
        self.assertEqual(
            response.json,
            {
                'connections': [
                    {
                        "connection_id": "test-connection-id-1",
                        "conn_type": 'mysql',
                        "host": 'mysql',
                        "login": 'login',
                        'schema': 'testschema',
                        'port': 80
                    },
                    {
                        "connection_id": "test-connection-id-2",
                        "conn_type": 'mysql',
                        "host": 'mysql',
                        "login": 'login',
                        'schema': 'testschema',
                        'port': 80
                    }
                ],
                'total_entries': 2
            }
        )


class TestPatchConnection(TestConnectionEndpoint):
    @unittest.skip("Not implemented yet")
    def test_should_response_200(self):
        response = self.client.patch("/api/v1/connections/1")
        assert response.status_code == 200


class TestPostConnection(TestConnectionEndpoint):
    @unittest.skip("Not implemented yet")
    def test_should_response_200(self):
        response = self.client.post("/api/v1/connections/")
        assert response.status_code == 200
