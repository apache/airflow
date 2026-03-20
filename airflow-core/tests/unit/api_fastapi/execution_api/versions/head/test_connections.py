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
from fastapi import FastAPI, HTTPException, status

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

        response = client.get("/execution/connections/test_conn")

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
        response = client.get("/execution/connections/test_conn2")

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
        response = client.get("/execution/connections/non_existent_test_conn")

        assert response.status_code == 404
        assert response.json() == {
            "detail": {
                "message": "Connection with ID non_existent_test_conn not found",
                "reason": "not_found",
            }
        }

    @mock.patch("airflow.settings.ENABLE_EXECUTION_API_AUTHZ", True)
    def test_connection_get_access_denied(self, client):
        response = client.get("/execution/connections/test_conn")

        # Assert response status code and detail for access denied
        assert response.status_code == 403
        assert response.json() == {
            "detail": {
                "reason": "access_denied",
                "message": "Task does not have access to connection test_conn",
            }
        }

    @mock.patch("airflow.settings.ENABLE_EXECUTION_API_AUTHZ", True)
    def test_connection_get_access_allowed(self, client, session, create_task_instance):
        connection = Connection(
            conn_id="test_conn_auth",
            conn_type="http",
            description="description",
            host="localhost",
        )
        session.add(connection)

        ti = create_task_instance()
        session.commit()

        from fastapi import Request

        from airflow.api_fastapi.execution_api.datamodels.token import TIToken
        from airflow.api_fastapi.execution_api.security import _jwt_bearer

        async def mock_jwt_bearer(request: Request):
            return TIToken(id=ti.id, claims={"sub": str(ti.id), "scope": "execution"})

        last_route = client.app.routes[-1]
        exec_app = last_route.app
        exec_app.dependency_overrides[_jwt_bearer] = mock_jwt_bearer

        response = client.get("/execution/connections/test_conn_auth")

        assert response.status_code == 200
        assert response.json()["conn_id"] == "test_conn_auth"

        exec_app.dependency_overrides.pop(_jwt_bearer)
        session.delete(connection)
        session.commit()
