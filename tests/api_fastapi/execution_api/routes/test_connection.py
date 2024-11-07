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

from unittest import mock

import pytest

from airflow.models.connection import Connection

pytestmark = pytest.mark.db_test


class TestGetConnection:
    def test_connection_get_from_db(self, client, session):
        connection = Connection(
            conn_id="test_conn",
            conn_type="http",
            description="description",
            host="localhost",
            login="root",
            password="admin",
            schema="http",
            port=8080,
            extra='{"x_secret": "testsecret", "y_secret": "test"}',
        )

        session.add(connection)
        session.commit()

        response = client.get("/execution/connection/test_conn")

        assert response.status_code == 200
        assert response.json() == {
            "conn_id": "test_conn",
            "conn_type": "http",
            "host": "localhost",
            "login": "root",
            "password": "admin",
            "schema": "http",
            "port": 8080,
            "extra": '{"x_secret": "testsecret", "y_secret": "test"}',
        }

        # Remove connection
        session.delete(connection)
        session.commit()

    @mock.patch.dict(
        "os.environ",
        {"AIRFLOW_CONN_TEST_CONN2": '{"uri": "http://root:admin@localhost:8080/https?headers=header"}'},
    )
    def test_connection_get_from_env_var(self, client, session):
        response = client.get("/execution/connection/test_conn2")

        assert response.status_code == 200
        assert response.json() == {
            "conn_id": "test_conn2",
            "conn_type": "http",
            "host": "localhost",
            "login": "root",
            "password": "admin",
            "schema": "https",
            "port": 8080,
            "extra": '{"headers": "header"}',
        }

    def test_connection_get_not_found(self, client):
        response = client.get("/execution/connection/non_existent_test_conn")

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Connection with ID non_existent_test_conn not found",
                "reason": "not_found",
            }
        }

    def test_connection_get_access_denied(self, client):
        with mock.patch(
            "airflow.api_fastapi.execution_api.routes.connections.has_connection_access", return_value=False
        ):
            response = client.get("/execution/connection/test_conn")

        # Assert response status code and detail for access denied
        assert response.status_code == 403
        assert response.json() == {
            "detail": {
                "reason": "access_denied",
                "message": "Task does not have access to connection test_conn",
            }
        }
