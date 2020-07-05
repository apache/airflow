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

from parameterized import parameterized

from airflow.models import Connection
from airflow.utils.session import provide_session
from airflow.www import app
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_connections


class TestConnectionEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        # we want only the connection created here for this test
        clear_db_connections()

    def tearDown(self) -> None:
        clear_db_connections()

    def _create_connection(self, session):
        connection_model = Connection(conn_id='test-connection-id',
                                      conn_type='test_type')
        session.add(connection_model)
        session.commit()


class TestDeleteConnection(TestConnectionEndpoint):

    @provide_session
    def test_delete_should_response_204(self, session):
        connection_model = Connection(conn_id='test-connection',
                                      conn_type='test_type')

        session.add(connection_model)
        session.commit()
        conn = session.query(Connection).all()
        assert len(conn) == 1
        response = self.client.delete("/api/v1/connections/test-connection")
        assert response.status_code == 204
        connection = session.query(Connection).all()
        assert len(connection) == 0

    def test_delete_should_response_404(self):
        response = self.client.delete("/api/v1/connections/test-connection")
        assert response.status_code == 404
        self.assertEqual(
            response.json,
            {
                'detail': None,
                'status': 404,
                'title': 'Connection not found',
                'type': 'about:blank'
            }
        )


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

    def test_should_response_404(self):
        response = self.client.get("/api/v1/connections/invalid-connection")
        assert response.status_code == 404
        self.assertEqual(
            {
                'detail': None,
                'status': 404,
                'title': 'Connection not found',
                'type': 'about:blank'
            },
            response.json
        )


class TestGetConnections(TestConnectionEndpoint):

    @provide_session
    def test_should_response_200(self, session):
        connection_model_1 = Connection(conn_id='test-connection-id-1',
                                        conn_type='test_type')
        connection_model_2 = Connection(conn_id='test-connection-id-2',
                                        conn_type='test_type')
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
                        "conn_type": 'test_type',
                        "host": None,
                        "login": None,
                        'schema': None,
                        'port': None
                    },
                    {
                        "connection_id": "test-connection-id-2",
                        "conn_type": 'test_type',
                        "host": None,
                        "login": None,
                        'schema': None,
                        'port': None
                    }
                ],
                'total_entries': 2
            }
        )


class TestGetConnectionsPagination(TestConnectionEndpoint):

    @parameterized.expand(
        [
            ("/api/v1/connections?limit=1", ['TEST_CONN_ID1']),
            ("/api/v1/connections?limit=2", ['TEST_CONN_ID1', "TEST_CONN_ID2"]),
            (
                "/api/v1/connections?offset=5",
                [
                    "TEST_CONN_ID6",
                    "TEST_CONN_ID7",
                    "TEST_CONN_ID8",
                    "TEST_CONN_ID9",
                    "TEST_CONN_ID10",
                ],
            ),
            (
                "/api/v1/connections?offset=0",
                [
                    "TEST_CONN_ID1",
                    "TEST_CONN_ID2",
                    "TEST_CONN_ID3",
                    "TEST_CONN_ID4",
                    "TEST_CONN_ID5",
                    "TEST_CONN_ID6",
                    "TEST_CONN_ID7",
                    "TEST_CONN_ID8",
                    "TEST_CONN_ID9",
                    "TEST_CONN_ID10",
                ],
            ),
            ("/api/v1/connections?limit=1&offset=5", ["TEST_CONN_ID6"]),
            ("/api/v1/connections?limit=1&offset=1", ["TEST_CONN_ID2"]),
            (
                "/api/v1/connections?limit=2&offset=2",
                ["TEST_CONN_ID3", "TEST_CONN_ID4"],
            ),
        ]
    )
    @provide_session
    def test_handle_limit_offset(self, url, expected_conn_ids, session):
        connections = self._create_connections(10)
        session.add_all(connections)
        session.commit()
        response = self.client.get(url)
        assert response.status_code == 200
        self.assertEqual(response.json["total_entries"], 10)
        conn_ids = [conn["connection_id"] for conn in response.json["connections"] if conn]
        self.assertEqual(conn_ids, expected_conn_ids)

    @provide_session
    def test_should_respect_page_size_limit_default(self, session):
        connection_models = self._create_connections(200)
        session.add_all(connection_models)
        session.commit()

        response = self.client.get("/api/v1/connections")
        assert response.status_code == 200

        self.assertEqual(response.json["total_entries"], 200)
        self.assertEqual(len(response.json["connections"]), 100)

    @provide_session
    def test_limit_of_zero_should_return_default(self, session):
        connection_models = self._create_connections(200)
        session.add_all(connection_models)
        session.commit()

        response = self.client.get("/api/v1/connections?limit=0")
        assert response.status_code == 200

        self.assertEqual(response.json["total_entries"], 200)
        self.assertEqual(len(response.json["connections"]), 100)

    @provide_session
    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self, session):
        connection_models = self._create_connections(200)
        session.add_all(connection_models)
        session.commit()

        response = self.client.get("/api/v1/connections?limit=180")
        assert response.status_code == 200
        self.assertEqual(len(response.json['connections']), 150)

    def _create_connections(self, count):
        return [Connection(
            conn_id='TEST_CONN_ID' + str(i),
            conn_type='TEST_CONN_TYPE' + str(i)
        ) for i in range(1, count + 1)]


class TestPatchConnection(TestConnectionEndpoint):

    @parameterized.expand(
        [
            (
                {
                    "connection_id": "test-connection-id",
                    "conn_type": 'test_type',
                    "extra": "{'key': 'var'}"
                },
            ),
            (
                {
                    "extra": "{'key': 'var'}"
                },
            )
        ]
    )
    @provide_session
    def test_patch_should_response_200(self, payload, session):
        self._create_connection(session)

        response = self.client.patch("/api/v1/connections/test-connection-id",
                                     json=payload)
        assert response.status_code == 200

    @provide_session
    def test_patch_should_response_200_with_update_mask(self, session):
        self._create_connection(session)
        test_connection = "test-connection-id"
        payload = {
            "connection_id": test_connection,
            "conn_type": 'test_type_2',
            "extra": "{'key': 'var'}",
            'login': "login",
            "port": 80,
        }
        response = self.client.patch(
            "/api/v1/connections/test-connection-id?update_mask=port,login",
            json=payload
        )
        assert response.status_code == 200
        connection = session.query(Connection).filter_by(conn_id=test_connection).first()
        self.assertEqual(connection.password, None)
        self.assertEqual(
            response.json,
            {
                "connection_id": test_connection,  # not updated
                "conn_type": 'test_type',  # Not updated
                "extra": None,  # Not updated
                'login': "login",  # updated
                "port": 80,  # updated
                "schema": None,
                "host": None

            }
        )

    @parameterized.expand(
        [
            (
                {
                    "connection_id": 'test-connection-id',
                    "conn_type": 'test_type_2',
                    "extra": "{'key': 'var'}",
                    'login': "login",
                    "port": 80,
                },
                'update_mask=ports, login',  # posts is unknown
                "'ports' is unknown or cannot be updated."

            ),
            (
                {
                    "connection_id": 'test-connection-id',
                    "conn_type": 'test_type_2',
                    "extra": "{'key': 'var'}",
                    'login': "login",
                    "port": 80,
                },
                'update_mask=port, login, conn_id',  # conn_id is unknown
                "'conn_id' is unknown or cannot be updated."

            ),
            (
                {
                    "connection_id": 'test-connection-id',
                    "conn_type": 'test_type_2',
                    "extra": "{'key': 'var'}",
                    'login': "login",
                    "port": 80,
                },
                'update_mask=port, login, connection_id',  # connection_id cannot be updated
                "'connection_id' is unknown or cannot be updated."

            ),
            (
                {
                    "connection_id": "test-connection",  # trying to change connection_id
                    "conn_type": "test-type",
                    "login": "login",
                },
                '',  # not necessary
                "The connection_id cannot be updated."
            ),
        ]
    )
    @provide_session
    def test_patch_should_response_400_for_invalid_fields_in_update_mask(
        self, payload, update_mask, error_message, session
    ):
        self._create_connection(session)
        response = self.client.patch(
            f"/api/v1/connections/test-connection-id?{update_mask}",
            json=payload
        )
        assert response.status_code == 400
        self.assertEqual(response.json['title'], error_message)

    @parameterized.expand(
        [
            (
                {
                    "connection_id": "test-connection-id",
                    "conn_type": "test-type",
                    "extra": 0,  # expected string
                }, "0 is not of type 'string' - 'extra'"
            ),
            (
                {
                    "connection_id": "test-connection-id",
                    "conn_type": "test-type",
                    "extras": "{}",  # extras not a known field e.g typo
                }, "Extra arguments passed: ['extras']"
            ),
            (
                {
                    "connection_id": "test-connection-id",
                    "conn_type": "test-type",
                    "invalid_field": "invalid field",  # unknown field
                    "_password": "{}",  # _password not a known field
                }, "Extra arguments passed:"
            ),
        ]
    )
    @provide_session
    def test_patch_should_response_400_for_invalid_update(
        self, payload, error_message, session
    ):
        self._create_connection(session)
        response = self.client.patch(
            "/api/v1/connections/test-connection-id",
            json=payload
        )
        assert response.status_code == 400
        self.assertIn(error_message, response.json['detail'])

    def test_patch_should_response_404_not_found(self):
        payload = {
            "connection_id": "test-connection-id",
            "conn_type": "test-type",
            "port": 90
        }
        response = self.client.patch(
            "/api/v1/connections/test-connection-id",
            json=payload
        )
        assert response.status_code == 404
        self.assertEqual(
            {
                'detail': None,
                'status': 404,
                'title': 'Connection not found',
                'type': 'about:blank'
            },
            response.json
        )


class TestPostConnection(TestConnectionEndpoint):

    @provide_session
    def test_post_should_response_200(self, session):
        payload = {
            "connection_id": "test-connection-id",
            "conn_type": 'test_type'
        }
        response = self.client.post("/api/v1/connections", json=payload)
        assert response.status_code == 200
        connection = session.query(Connection).all()
        assert len(connection) == 1
        self.assertEqual(connection[0].conn_id, 'test-connection-id')

    def test_post_should_response_400_for_invalid_payload(self):
        payload = {
            "connection_id": "test-connection-id",
        }  # conn_type missing
        response = self.client.post("/api/v1/connections", json=payload)
        assert response.status_code == 400
        self.assertEqual(response.json,
                         {'detail': "{'conn_type': ['Missing data for required field.']}",
                          'status': 400,
                          'title': 'Bad request',
                          'type': 'about:blank'}
                         )

    def test_post_should_response_409_already_exist(self):
        payload = {
            "connection_id": "test-connection-id",
            "conn_type": 'test_type'
        }
        response = self.client.post("/api/v1/connections", json=payload)
        assert response.status_code == 200
        # Another request
        response = self.client.post("/api/v1/connections", json=payload)
        assert response.status_code == 409
        self.assertEqual(
            response.json,
            {
                'detail': None,
                'status': 409,
                'title': 'Connection already exist. ID: test-connection-id',
                'type': 'about:blank'
            }
        )
