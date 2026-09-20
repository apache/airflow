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

import asyncio
import threading
from unittest import mock

import pytest
from fastapi import FastAPI, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from airflow.api_fastapi.execution_api.datamodels.connection import ConnectionResponse
from airflow.api_fastapi.execution_api.routes.connections import get_connection
from airflow.models.connection import Connection

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


@pytest.fixture
def access_denied(client):
    from airflow.api_fastapi.execution_api.routes.connections import has_connection_access

    last_route = client.app.routes[-1]
    assert isinstance(last_route.app, FastAPI)
    exec_app = last_route.app

    async def _(connection_id: str):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={
                "reason": "access_denied",
                "message": f"Task does not have access to connection {connection_id}",
            },
        )

    exec_app.dependency_overrides[has_connection_access] = _

    yield

    exec_app.dependency_overrides = {}


class TestGetConnection:
    @mock.patch(
        "airflow.api_fastapi.execution_api.routes.connections.resolve_connection",
        new_callable=mock.AsyncMock,
    )
    def test_connection_get_awaits_resolver_with_team_and_session(self, resolve_connection, client):
        from airflow.api_fastapi.execution_api.security import get_team_name_dep

        exec_app = client.app.routes[-1].app
        assert isinstance(exec_app, FastAPI)
        exec_app.dependency_overrides[get_team_name_dep] = lambda: "analytics"
        resolve_connection.return_value = Connection(conn_id="test_conn", conn_type="http")

        try:
            with conf_vars({("core", "multi_team"): "True"}):
                response = client.get("/execution/connections/test_conn")
        finally:
            exec_app.dependency_overrides.pop(get_team_name_dep)

        assert response.status_code == 200
        resolve_connection.assert_awaited_once()
        assert resolve_connection.call_args.args == ("test_conn",)
        assert resolve_connection.call_args.kwargs["team_name"] == "analytics"
        assert isinstance(resolve_connection.call_args.kwargs["session"], AsyncSession)

    @mock.patch(
        "airflow.api_fastapi.execution_api.routes.connections.resolve_connection",
        new_callable=mock.AsyncMock,
    )
    @mock.patch("airflow.api_fastapi.execution_api.routes.connections.ConnectionResponse.model_validate")
    @pytest.mark.asyncio
    async def test_response_materialization_yields_event_loop(self, model_validate, resolve_connection):
        loop = asyncio.get_running_loop()
        started = threading.Event()
        progressed = threading.Event()
        release = threading.Event()
        observed: list[bool] = []
        async_started = asyncio.Event()
        connection = Connection(conn_id="test_conn", conn_type="http")
        expected = ConnectionResponse.model_construct(conn_id="test_conn", conn_type="http")
        resolve_connection.return_value = connection

        def validate_response(resolved_connection):
            assert resolved_connection is connection
            started.set()
            loop.call_soon_threadsafe(async_started.set)
            release.wait(timeout=1)
            return expected

        def observe_progress():
            started.wait(timeout=1)
            observed.append(progressed.wait(timeout=0.5))
            release.set()

        model_validate.side_effect = validate_response
        helper = threading.Thread(target=observe_progress)
        helper.start()
        task = asyncio.create_task(
            get_connection("test_conn", session=mock.AsyncMock(spec=AsyncSession), team_name=None)
        )
        await asyncio.wait_for(async_started.wait(), timeout=1)
        progressed.set()

        assert await task is expected
        helper.join(timeout=1)
        assert observed == [True]

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

    @pytest.mark.usefixtures("access_denied")
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
