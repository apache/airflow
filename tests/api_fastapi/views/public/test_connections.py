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

import pytest

from airflow.models import Connection
from airflow.utils.session import provide_session
from tests_common.test_utils.db import clear_db_connections

pytestmark = pytest.mark.db_test

TEST_CONN_ID = "test_connection_id"
TEST_CONN_ID_2 = "test_connection_id_2"
TEST_CONN_TYPE = "test_type"
TEST_CONN_TYPE_2 = "test_type_2"


@provide_session
def _create_connection(session) -> None:
    connection_model = Connection(conn_id=TEST_CONN_ID, conn_type=TEST_CONN_TYPE)
    session.add(connection_model)


@provide_session
def _create_connections(session) -> None:
    connection_model_1 = Connection(conn_id=TEST_CONN_ID, conn_type=TEST_CONN_TYPE)
    connection_model_2 = Connection(conn_id=TEST_CONN_ID_2, conn_type=TEST_CONN_TYPE_2)
    connections = [connection_model_1, connection_model_2]
    session.add_all(connections)


class TestConnectionEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_db_connections(False)

    def teardown_method(self) -> None:
        clear_db_connections()

    def create_connection(self):
        _create_connection()

    def create_connections(self):
        _create_connections()


class TestDeleteConnection(TestConnectionEndpoint):
    def test_delete_should_respond_204(self, test_client, session):
        self.create_connection()
        conns = session.query(Connection).all()
        assert len(conns) == 1
        response = test_client.delete(f"/public/connections/{TEST_CONN_ID}")
        assert response.status_code == 204
        connection = session.query(Connection).all()
        assert len(connection) == 0

    def test_delete_should_respond_404(self, test_client):
        response = test_client.delete(f"/public/connections/{TEST_CONN_ID}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Connection with connection_id: `{TEST_CONN_ID}` was not found" == body["detail"]


class TestGetConnection(TestConnectionEndpoint):
    def test_get_should_respond_200(self, test_client, session):
        self.create_connection()
        response = test_client.get(f"/public/connections/{TEST_CONN_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["connection_id"] == TEST_CONN_ID
        assert body["conn_type"] == TEST_CONN_TYPE

    def test_get_should_respond_404(self, test_client):
        response = test_client.get(f"/public/connections/{TEST_CONN_ID}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Connection with connection_id: `{TEST_CONN_ID}` was not found" == body["detail"]

    def test_get_should_respond_200_with_extra(self, test_client, session):
        self.create_connection()
        connection = session.query(Connection).first()
        connection.extra = '{"extra_key": "extra_value"}'
        session.commit()
        response = test_client.get(f"/public/connections/{TEST_CONN_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["connection_id"] == TEST_CONN_ID
        assert body["conn_type"] == TEST_CONN_TYPE
        assert body["extra"] == '{"extra_key": "extra_value"}'

    @pytest.mark.enable_redact
    def test_get_should_respond_200_with_extra_redacted(self, test_client, session):
        self.create_connection()
        connection = session.query(Connection).first()
        connection.extra = '{"password": "test-password"}'
        session.commit()
        response = test_client.get(f"/public/connections/{TEST_CONN_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["connection_id"] == TEST_CONN_ID
        assert body["conn_type"] == TEST_CONN_TYPE
        assert body["extra"] == '{"password": "***"}'


class TestGetConnections(TestConnectionEndpoint):
    def test_should_respond_200(self, test_client, session):
        self.create_connections()
        result = session.query(Connection).all()
        assert len(result) == 2
        response = test_client.get("/public/connections/")
        assert response.status_code == 200
        assert response.json() == {
            "connections": [
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "description": None,
                    "extra": None,
                    "host": None,
                    "login": None,
                    "schema": None,
                    "port": None,
                },
                {
                    "connection_id": TEST_CONN_ID_2,
                    "conn_type": TEST_CONN_TYPE_2,
                    "description": None,
                    "extra": None,
                    "host": None,
                    "login": None,
                    "schema": None,
                    "port": None,
                },
            ],
            "total_entries": 2,
        }

    def test_should_respond_200_with_order_by(self, test_client, session):
        self.create_connections()
        result = session.query(Connection).all()
        assert len(result) == 2
        response = test_client.get("/public/connections/?order_by=-connection_id")
        assert response.status_code == 200
        # Using - means descending
        assert response.json() == {
            "connections": [
                {
                    "connection_id": TEST_CONN_ID_2,
                    "conn_type": TEST_CONN_TYPE_2,
                    "description": None,
                    "extra": None,
                    "host": None,
                    "login": None,
                    "schema": None,
                    "port": None,
                },
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "description": None,
                    "extra": None,
                    "host": None,
                    "login": None,
                    "schema": None,
                    "port": None,
                },
            ],
            "total_entries": 2,
        }
