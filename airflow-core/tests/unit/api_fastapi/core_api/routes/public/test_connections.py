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

import json
import os
from importlib.metadata import PackageNotFoundError, metadata
from unittest import mock

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from airflow.api_fastapi.auth.managers.base_auth_manager import BaseAuthManager
from airflow.api_fastapi.core_api.datamodels.common import BulkActionResponse, BulkBody
from airflow.api_fastapi.core_api.datamodels.connections import ConnectionBody
from airflow.api_fastapi.core_api.services.public.connections import BulkConnectionService
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models import Connection
from airflow.models.connection_test import ConnectionTestRequest, ConnectionTestState
from airflow.secrets.environment_variables import CONN_ENV_PREFIX
from airflow.utils.session import NEW_SESSION, provide_session

from tests_common.test_utils.api_fastapi import _check_last_log
from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_connection_tests,
    clear_db_connections,
    clear_db_logs,
    clear_test_connections,
)
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

pytestmark = pytest.mark.db_test

TEST_CONN_ID = "test_connection_id"
TEST_CONN_TYPE = "test_type"
TEST_CONN_DESCRIPTION = "some_description_a"
TEST_CONN_HOST = "some_host_a"
TEST_CONN_PORT = 8080
TEST_CONN_LOGIN = "some_login"
TEST_CONN_SCHEMA = "https"
TEST_CONN_EXTRA = '{"extra_key": "extra_value"}'


TEST_CONN_ID_2 = "test_connection_id_2"
TEST_CONN_TYPE_2 = "test_type_2"
TEST_CONN_DESCRIPTION_2 = "some_description_b"
TEST_CONN_HOST_2 = "some_host_b"
TEST_CONN_PORT_2 = 8081
TEST_CONN_LOGIN_2 = "some_login_b"


TEST_CONN_ID_3 = "test_connection_id_3"
TEST_CONN_TYPE_3 = "test_type_3"


@provide_session
def _create_connection(team_name: str | None = None, *, session: Session = NEW_SESSION) -> None:
    connection_model = Connection(
        conn_id=TEST_CONN_ID,
        conn_type=TEST_CONN_TYPE,
        description=TEST_CONN_DESCRIPTION,
        host=TEST_CONN_HOST,
        port=TEST_CONN_PORT,
        login=TEST_CONN_LOGIN,
        team_name=team_name,
    )
    session.add(connection_model)


@provide_session
def _create_connections(*, session: Session = NEW_SESSION) -> None:
    _create_connection(session=session)
    connection_model_2 = Connection(
        conn_id=TEST_CONN_ID_2,
        conn_type=TEST_CONN_TYPE_2,
        description=TEST_CONN_DESCRIPTION_2,
        host=TEST_CONN_HOST_2,
        port=TEST_CONN_PORT_2,
        login=TEST_CONN_LOGIN_2,
    )
    session.add(connection_model_2)


class TestConnectionEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        clear_test_connections(False)
        clear_db_connections(False)
        clear_db_connection_tests()
        clear_db_logs()

    def teardown_method(self) -> None:
        clear_db_connections()
        clear_db_connection_tests()

    def create_connection(self, team_name: str | None = None):
        _create_connection(team_name=team_name)

    def create_connections(self):
        _create_connections()


class TestDeleteConnection(TestConnectionEndpoint):
    def test_delete_should_respond_204(self, test_client, session):
        self.create_connection()
        conns = session.scalars(select(Connection)).all()
        assert len(conns) == 1
        response = test_client.delete(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 204
        connection = session.scalars(select(Connection)).all()
        assert len(connection) == 0
        _check_last_log(session, dag_id=None, event="delete_connection", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 403

    def test_delete_should_respond_404(self, test_client):
        response = test_client.delete(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Connection with connection_id: `{TEST_CONN_ID}` was not found" == body["detail"]


class TestGetConnection(TestConnectionEndpoint):
    def test_get_should_respond_200(self, test_client, testing_team, session):
        self.create_connection(team_name=testing_team.name)
        response = test_client.get(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["connection_id"] == TEST_CONN_ID
        assert body["conn_type"] == TEST_CONN_TYPE
        assert body["team_name"] == testing_team.name

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 403

    def test_get_should_respond_404(self, test_client):
        response = test_client.get(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 404
        body = response.json()
        assert f"The Connection with connection_id: `{TEST_CONN_ID}` was not found" == body["detail"]

    def test_get_should_respond_200_with_extra(self, test_client, session):
        self.create_connection()
        connection = session.scalars(select(Connection)).first()
        connection.extra = '{"extra_key": "extra_value"}'
        session.commit()
        response = test_client.get(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["connection_id"] == TEST_CONN_ID
        assert body["conn_type"] == TEST_CONN_TYPE
        assert body["extra"] == '{"extra_key": "extra_value"}'

    @pytest.mark.parametrize("extra_value", [None, ""])
    def test_get_should_respond_200_with_empty_extra(self, test_client, session, extra_value):
        """Empty string or NULL extra should not break serialization (regression test for #64950)."""
        self.create_connection()
        connection = session.scalars(select(Connection)).first()
        connection.extra = extra_value
        session.commit()
        response = test_client.get(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["connection_id"] == TEST_CONN_ID
        assert body["extra"] == extra_value

    @pytest.mark.enable_redact
    def test_get_should_respond_200_with_extra_redacted(self, test_client, session):
        self.create_connection()
        connection = session.scalars(select(Connection)).first()
        connection.extra = '{"password": "test-password"}'
        session.commit()
        response = test_client.get(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["connection_id"] == TEST_CONN_ID
        assert body["conn_type"] == TEST_CONN_TYPE
        assert body["extra"] == '{"password": "***"}'

    @pytest.mark.enable_redact
    def test_get_should_not_overmask_short_password_value_in_extra(self, test_client, session):
        connection = Connection(
            conn_id=TEST_CONN_ID, conn_type="generic", login="a", password="a", extra='{"key": "value"}'
        )
        session.add(connection)
        session.commit()

        response = test_client.get(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 200
        body = response.json()
        assert body["connection_id"] == TEST_CONN_ID
        assert body["conn_type"] == "generic"
        assert body["login"] == "a"
        assert body["extra"] == '{"key": "value"}'


class TestGetConnections(TestConnectionEndpoint):
    @pytest.mark.parametrize(
        ("query_params", "expected_total_entries", "expected_ids"),
        [
            # Filters
            ({}, 2, [TEST_CONN_ID, TEST_CONN_ID_2]),
            ({"limit": 1}, 2, [TEST_CONN_ID]),
            ({"limit": 1, "offset": 1}, 2, [TEST_CONN_ID_2]),
            # Sort
            ({"order_by": "-connection_id"}, 2, [TEST_CONN_ID_2, TEST_CONN_ID]),
            ({"order_by": "conn_type"}, 2, [TEST_CONN_ID, TEST_CONN_ID_2]),
            ({"order_by": "-conn_type"}, 2, [TEST_CONN_ID_2, TEST_CONN_ID]),
            ({"order_by": "description"}, 2, [TEST_CONN_ID, TEST_CONN_ID_2]),
            ({"order_by": "-description"}, 2, [TEST_CONN_ID_2, TEST_CONN_ID]),
            ({"order_by": "host"}, 2, [TEST_CONN_ID, TEST_CONN_ID_2]),
            ({"order_by": "-host"}, 2, [TEST_CONN_ID_2, TEST_CONN_ID]),
            ({"order_by": "port"}, 2, [TEST_CONN_ID, TEST_CONN_ID_2]),
            ({"order_by": "-port"}, 2, [TEST_CONN_ID_2, TEST_CONN_ID]),
            ({"order_by": "id"}, 2, [TEST_CONN_ID, TEST_CONN_ID_2]),
            ({"order_by": "-id"}, 2, [TEST_CONN_ID_2, TEST_CONN_ID]),
            # Search
            ({"connection_id_pattern": "n_id_2"}, 1, [TEST_CONN_ID_2]),
            ({"connection_id_prefix_pattern": "test_connection_id_2"}, 1, [TEST_CONN_ID_2]),
        ],
    )
    def test_should_respond_200(
        self, test_client, session, query_params, expected_total_entries, expected_ids
    ):
        self.create_connections()

        with assert_queries_count(3):
            response = test_client.get("/connections", params=query_params)

        assert response.status_code == 200

        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [connection["connection_id"] for connection in body["connections"]] == expected_ids

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/connections", params={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/connections", params={})
        assert response.status_code == 403

    @mock.patch(
        "airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_connections"
    )
    def test_should_call_get_authorized_connections(self, mock_get_authorized_connections, test_client):
        self.create_connections()
        mock_get_authorized_connections.return_value = {TEST_CONN_ID}
        response = test_client.get("/connections")
        mock_get_authorized_connections.assert_called_once_with(user=mock.ANY, method="GET")
        assert response.status_code == 200
        body = response.json()

        assert body["total_entries"] == 1
        assert [connection["connection_id"] for connection in body["connections"]] == [TEST_CONN_ID]


class TestPostConnection(TestConnectionEndpoint):
    @pytest.mark.parametrize(
        "body",
        [
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE},
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "extra": None},
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "extra": "{}"},
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "extra": '{"key": "value"}'},
            {
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "description": "test_description",
                "host": "test_host",
                "login": "test_login",
                "schema": "test_schema",
                "port": 8080,
                "extra": '{"key": "value"}',
            },
        ],
    )
    def test_post_should_respond_201(self, test_client, session, body):
        response = test_client.post("/connections", json=body)
        assert response.status_code == 201
        connection = session.scalars(select(Connection)).all()
        assert len(connection) == 1
        _check_last_log(session, dag_id=None, event="post_connection", logical_date=None)

    @conf_vars({("core", "multi_team"): "True"})
    def test_post_should_respond_201_with_team(self, test_client, session, testing_team):
        response = test_client.post(
            "/connections",
            json={
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "team_name": testing_team.name,
            },
        )
        assert response.status_code == 201
        assert response.json() == {
            "connection_id": TEST_CONN_ID,
            "conn_type": TEST_CONN_TYPE,
            "description": None,
            "extra": None,
            "host": None,
            "login": None,
            "password": None,
            "port": None,
            "schema": None,
            "team_name": testing_team.name,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/connections", json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/connections", json={})
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "body",
        [
            {"connection_id": "****", "conn_type": TEST_CONN_TYPE},
            {"connection_id": "test()", "conn_type": TEST_CONN_TYPE},
            {"connection_id": "this_^$#is_invalid", "conn_type": TEST_CONN_TYPE},
            {"connection_id": "iam_not@#$_connection_id", "conn_type": TEST_CONN_TYPE},
        ],
    )
    def test_post_should_respond_422_for_invalid_conn_id(self, test_client, body):
        response = test_client.post("/connections", json=body)
        assert response.status_code == 422
        # This regex is used for validation in ConnectionBody
        assert response.json() == {
            "detail": [
                {
                    "ctx": {"pattern": r"^[\w.-]+$"},
                    "input": f"{body['connection_id']}",
                    "loc": ["body", "connection_id"],
                    "msg": "String should match pattern '^[\\w.-]+$'",
                    "type": "string_pattern_mismatch",
                }
            ]
        }

    @conf_vars({("core", "multi_team"): "False"})
    def test_post_rejects_team_name_when_multi_team_disabled(self, test_client):
        response = test_client.post(
            "/connections",
            json={
                "connection_id": TEST_CONN_ID_2,
                "conn_type": TEST_CONN_TYPE_2,
                "team_name": "test_team",
            },
        )
        assert response.status_code == 422
        assert (
            "team_name cannot be set when multi_team mode is disabled. Please contact your administrator."
            in response.json()["detail"][0]["msg"]
        )

    @pytest.mark.parametrize(
        "body",
        [
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE},
        ],
    )
    def test_post_should_respond_already_exist(self, test_client, body):
        response = test_client.post("/connections", json=body)
        assert response.status_code == 201
        # Another request
        response = test_client.post("/connections", json=body)
        assert response.status_code == 409
        response_json = response.json()
        assert "detail" in response_json
        assert list(response_json["detail"].keys()) == ["reason", "statement", "orig_error", "message"]

    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        ("body", "expected_response"),
        [
            (
                {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "password": "test-password"},
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "description": None,
                    "extra": None,
                    "host": None,
                    "login": None,
                    "password": "***",
                    "port": None,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "password": "?>@#+!_%()#"},
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "description": None,
                    "extra": None,
                    "host": None,
                    "login": None,
                    "password": "***",
                    "port": None,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "password": "A!rF|0wi$aw3s0m3",
                    "extra": '{"password": "test-password"}',
                },
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "description": None,
                    "extra": '{"password": "***"}',
                    "host": None,
                    "login": None,
                    "password": "***",
                    "port": None,
                    "schema": None,
                    "team_name": None,
                },
            ),
        ],
    )
    def test_post_should_response_201_redacted_password(self, test_client, body, expected_response, session):
        response = test_client.post("/connections", json=body)
        assert response.status_code == 201
        assert response.json() == expected_response
        _check_last_log(session, dag_id=None, event="post_connection", logical_date=None, check_masked=True)


class TestPatchConnection(TestConnectionEndpoint):
    @pytest.mark.parametrize(
        ("body", "expected_result"),
        [
            (
                {"connection_id": TEST_CONN_ID, "conn_type": "new_type", "extra": '{"key": "var"}'},
                {
                    "conn_type": "new_type",
                    "connection_id": TEST_CONN_ID,
                    "description": TEST_CONN_DESCRIPTION,
                    "extra": '{"key": "var"}',
                    "host": TEST_CONN_HOST,
                    "login": TEST_CONN_LOGIN,
                    "password": None,
                    "port": TEST_CONN_PORT,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                {"connection_id": TEST_CONN_ID, "conn_type": "type_patch", "host": "test_host_patch"},
                {
                    "conn_type": "type_patch",
                    "connection_id": TEST_CONN_ID,
                    "description": TEST_CONN_DESCRIPTION,
                    "extra": None,
                    "host": "test_host_patch",
                    "login": TEST_CONN_LOGIN,
                    "password": None,
                    "port": TEST_CONN_PORT,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": "surprise",
                    "host": "test_host_patch",
                    "port": 80,
                },
                {
                    "conn_type": "surprise",
                    "connection_id": TEST_CONN_ID,
                    "description": TEST_CONN_DESCRIPTION,
                    "extra": None,
                    "host": "test_host_patch",
                    "login": TEST_CONN_LOGIN,
                    "password": None,
                    "port": 80,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                {"connection_id": TEST_CONN_ID, "conn_type": "really_new_type", "login": "test_login_patch"},
                {
                    "conn_type": "really_new_type",
                    "connection_id": TEST_CONN_ID,
                    "description": TEST_CONN_DESCRIPTION,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": "test_login_patch",
                    "password": None,
                    "port": TEST_CONN_PORT,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "port": 80},
                {
                    "conn_type": TEST_CONN_TYPE,
                    "connection_id": TEST_CONN_ID,
                    "description": TEST_CONN_DESCRIPTION,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": TEST_CONN_LOGIN,
                    "password": None,
                    "port": 80,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "port": 80,
                    "login": "test_login_patch",
                    "password": "test_password_patch",
                },
                {
                    "conn_type": TEST_CONN_TYPE,
                    "connection_id": TEST_CONN_ID,
                    "description": TEST_CONN_DESCRIPTION,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": "test_login_patch",
                    "password": "test_password_patch",
                    "port": 80,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                # Sensitive "***" should be ignored.
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "port": 80,
                    "login": "test_login_patch",
                    "password": "***",
                },
                {
                    "conn_type": TEST_CONN_TYPE,
                    "connection_id": TEST_CONN_ID,
                    "description": TEST_CONN_DESCRIPTION,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": "test_login_patch",
                    "password": None,
                    "port": 80,
                    "schema": None,
                    "team_name": None,
                },
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "schema": "http_patch",
                    "extra": '{"extra_key_patch": "extra_value_patch"}',
                },
                {
                    "conn_type": TEST_CONN_TYPE,
                    "connection_id": TEST_CONN_ID,
                    "description": TEST_CONN_DESCRIPTION,
                    "extra": '{"extra_key_patch": "extra_value_patch"}',
                    "host": TEST_CONN_HOST,
                    "login": TEST_CONN_LOGIN,
                    "password": None,
                    "port": TEST_CONN_PORT,
                    "schema": "http_patch",
                    "team_name": None,
                },
            ),
            (
                {  # Explicitly test that None is applied compared to if not provided
                    "conn_type": TEST_CONN_TYPE,
                    "connection_id": TEST_CONN_ID,
                    "description": None,
                    "extra": None,
                    "host": None,
                    "login": None,
                    "password": None,
                    "port": None,
                    "schema": None,
                },
                {
                    "conn_type": TEST_CONN_TYPE,
                    "connection_id": TEST_CONN_ID,
                    "description": None,
                    "extra": None,
                    "host": None,
                    "login": None,
                    "password": None,
                    "port": None,
                    "schema": None,
                    "team_name": None,
                },
            ),
        ],
    )
    def test_patch_should_respond_200(
        self, test_client, body: dict[str, str], expected_result: dict[str, str], session
    ):
        self.create_connection()

        response = test_client.patch(f"/connections/{TEST_CONN_ID}", json=body)
        assert response.status_code == 200
        _check_last_log(session, dag_id=None, event="patch_connection", logical_date=None)

        assert response.json() == expected_result

    @conf_vars({("core", "multi_team"): "True"})
    def test_patch_with_team_should_respond_200(self, test_client, testing_team, session):
        self.create_connection()

        response = test_client.patch(
            f"/connections/{TEST_CONN_ID}",
            json={"connection_id": TEST_CONN_ID, "conn_type": "new_type", "team_name": testing_team.name},
        )
        assert response.status_code == 200
        _check_last_log(session, dag_id=None, event="patch_connection", logical_date=None)

        assert response.json() == {
            "conn_type": "new_type",
            "connection_id": TEST_CONN_ID,
            "description": TEST_CONN_DESCRIPTION,
            "extra": None,
            "host": TEST_CONN_HOST,
            "login": TEST_CONN_LOGIN,
            "password": None,
            "port": TEST_CONN_PORT,
            "schema": None,
            "team_name": testing_team.name,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(f"/connections/{TEST_CONN_ID}", json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(f"/connections/{TEST_CONN_ID}", json={})
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("body", "updated_connection", "update_mask"),
        [
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": '{"key": "var"}',
                    "login": TEST_CONN_LOGIN,
                    "port": TEST_CONN_PORT,
                },
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": TEST_CONN_LOGIN,
                    "port": TEST_CONN_PORT,
                    "schema": None,
                    "password": None,
                    "description": TEST_CONN_DESCRIPTION,
                    "team_name": None,
                },
                {"update_mask": ["login", "port"]},
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": '{"key": "var"}',
                    "login": None,
                    "port": None,
                },
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": None,
                    "port": None,
                    "schema": None,
                    "password": None,
                    "description": TEST_CONN_DESCRIPTION,
                    "team_name": None,
                },
                {"update_mask": ["login", "port"]},
            ),
            (
                {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "host": "test_host_patch"},
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": None,
                    "host": "test_host_patch",
                    "login": TEST_CONN_LOGIN,
                    "port": TEST_CONN_PORT,
                    "schema": None,
                    "password": None,
                    "description": TEST_CONN_DESCRIPTION,
                    "team_name": None,
                },
                {"update_mask": ["host"]},
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "host": "test_host_patch",
                    "port": 80,
                },
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": None,
                    "host": "test_host_patch",
                    "login": TEST_CONN_LOGIN,
                    "port": 80,
                    "schema": None,
                    "password": None,
                    "description": TEST_CONN_DESCRIPTION,
                    "team_name": None,
                },
                {"update_mask": ["host", "port"]},
            ),
            (
                {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "login": "test_login_patch"},
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": "test_login_patch",
                    "port": TEST_CONN_PORT,
                    "schema": None,
                    "password": None,
                    "description": TEST_CONN_DESCRIPTION,
                    "team_name": None,
                },
                {"update_mask": ["login"]},
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "host": TEST_CONN_HOST,
                    "port": 80,
                },
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": TEST_CONN_LOGIN,
                    "port": TEST_CONN_PORT,
                    "password": None,
                    "schema": None,
                    "description": TEST_CONN_DESCRIPTION,
                    "team_name": None,
                },
                {"update_mask": ["host"]},
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": '{"new_extra_key": "new_extra_value"}',
                    "host": TEST_CONN_HOST,
                    "schema": "new_schema",
                    "port": 80,
                },
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": '{"new_extra_key": "new_extra_value"}',
                    "host": TEST_CONN_HOST,
                    "login": TEST_CONN_LOGIN,
                    "port": TEST_CONN_PORT,
                    "password": None,
                    "schema": "new_schema",
                    "description": TEST_CONN_DESCRIPTION,
                    "team_name": None,
                },
                {"update_mask": ["schema", "extra"]},
            ),
        ],
    )
    def test_patch_should_respond_200_with_update_mask(
        self, test_client, session, body, updated_connection, update_mask
    ):
        self.create_connection()
        response = test_client.patch(f"/connections/{TEST_CONN_ID}", json=body, params=update_mask)
        assert response.status_code == 200
        connection = session.scalars(select(Connection).where(Connection.conn_id == TEST_CONN_ID)).first()
        assert connection.password is None
        assert response.json() == updated_connection

    @pytest.mark.parametrize(
        "body",
        [
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "extra": '{"key": "var"}',
            },
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "host": "test_host_patch",
            },
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "host": "test_host_patch",
                "port": 80,
            },
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "login": "test_login_patch",
            },
            {"connection_id": "i_am_not_a_connection", "conn_type": TEST_CONN_TYPE, "port": 80},
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "port": 80,
                "login": "test_login_patch",
            },
        ],
    )
    def test_patch_should_respond_400(self, test_client, body):
        self.create_connection()
        response = test_client.patch(f"/connections/{TEST_CONN_ID}", json=body)
        assert response.status_code == 400
        assert response.json() == {
            "detail": "The connection_id in the request body does not match the URL parameter",
        }

    @pytest.mark.parametrize(
        "body",
        [
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "extra": '{"key": "var"}',
            },
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "host": "test_host_patch",
            },
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "host": "test_host_patch",
                "port": 80,
            },
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "login": "test_login_patch",
            },
            {"connection_id": "i_am_not_a_connection", "conn_type": TEST_CONN_TYPE, "port": 80},
            {
                "connection_id": "i_am_not_a_connection",
                "conn_type": TEST_CONN_TYPE,
                "port": 80,
                "login": "test_login_patch",
            },
        ],
    )
    def test_patch_should_respond_404(self, test_client, body):
        response = test_client.patch(f"/connections/{body['connection_id']}", json=body)
        assert response.status_code == 404
        assert response.json() == {
            "detail": f"The Connection with connection_id: `{body['connection_id']}` was not found",
        }

    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        ("body", "expected_response", "update_mask"),
        [
            (
                {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "password": "test-password"},
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "description": "some_description_a",
                    "extra": None,
                    "host": "some_host_a",
                    "login": "some_login",
                    "password": "***",
                    "port": 8080,
                    "schema": None,
                    "team_name": None,
                },
                {"update_mask": ["password"]},
            ),
            (
                {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "password": "?>@#+!_%()#"},
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "description": "some_description_a",
                    "extra": None,
                    "host": "some_host_a",
                    "login": "some_login",
                    "password": "***",
                    "port": 8080,
                    "schema": None,
                    "team_name": None,
                },
                {"update_mask": ["password"]},
            ),
            (
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "password": "A!rF|0wi$aw3s0m3",
                    "extra": '{"password": "test-password"}',
                },
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "description": "some_description_a",
                    "extra": '{"password": "***"}',
                    "host": "some_host_a",
                    "login": "some_login",
                    "password": "***",
                    "port": 8080,
                    "schema": None,
                    "team_name": None,
                },
                {"update_mask": ["password", "extra"]},
            ),
        ],
    )
    def test_patch_should_response_200_redacted_password(
        self, test_client, session, body, expected_response, update_mask
    ):
        self.create_connections()
        response = test_client.patch(f"/connections/{TEST_CONN_ID}", json=body, params=update_mask)
        assert response.status_code == 200
        assert response.json() == expected_response
        _check_last_log(session, dag_id=None, event="patch_connection", logical_date=None, check_masked=True)

    def test_patch_with_update_mask_validates_extra_as_json(self, test_client):
        """When update_mask includes 'extra', the extra field validator should still reject invalid JSON."""
        self.create_connection()
        response = test_client.patch(
            f"/connections/{TEST_CONN_ID}",
            json={
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "extra": "not valid json",
            },
            params={"update_mask": ["extra"]},
        )
        assert response.status_code == 422

    def test_patch_with_update_mask_rejects_extra_fields(self, test_client):
        """Partial model should still forbid unknown fields."""
        self.create_connection()
        response = test_client.patch(
            f"/connections/{TEST_CONN_ID}",
            json={
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "unknown_field": "value",
            },
            params={"update_mask": ["host"]},
        )
        assert response.status_code == 422

    @conf_vars({("core", "multi_team"): "False"})
    def test_patch_rejects_team_name_when_multi_team_disabled(self, test_client):
        self.create_connection()
        response = test_client.patch(
            f"/connections/{TEST_CONN_ID_2}",
            json={
                "connection_id": TEST_CONN_ID_2,
                "conn_type": "new_type",
                "team_name": "test_team",
            },
        )
        assert response.status_code == 422
        assert (
            "team_name cannot be set when multi_team mode is disabled. Please contact your administrator."
            in response.json()["detail"][0]["msg"]
        )


class TestConnection(TestConnectionEndpoint):
    def setup_method(self):
        try:
            metadata("apache-airflow-providers-sqlite")
        except PackageNotFoundError:
            pytest.skip("The SQlite distribution package is not installed.")

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    @pytest.mark.parametrize(
        ("body", "message"),
        [
            ({"connection_id": TEST_CONN_ID, "conn_type": "sqlite"}, "Connection successfully tested"),
            (
                {"connection_id": TEST_CONN_ID, "conn_type": "fs", "extra": '{"path": "/"}'},
                "Path / is existing.",
            ),
        ],
    )
    def test_should_respond_200(self, test_client, body, message):
        response = test_client.post("/connections/test", json=body)
        assert response.status_code == 200
        assert response.json() == {
            "status": True,
            "message": message,
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            "/connections/test", json={"connection_id": TEST_CONN_ID, "conn_type": "sqlite"}
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            "/connections/test", json={"connection_id": TEST_CONN_ID, "conn_type": "sqlite"}
        )
        assert response.status_code == 403

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_unreadable_existing_connection_indistinguishable_from_missing(self, test_client):
        """Route-level POST authorization is not enough on its own — when the
        request references an existing connection_id, the caller must also be
        authorized to read that specific connection before its hidden fields
        are merged into the test object. A caller lacking read access must
        get the same response shape as for a non-existent connection_id, so
        the route cannot be used to enumerate protected connection ids."""
        self.create_connection()

        from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager

        real_method = SimpleAuthManager.is_authorized_connection

        def gated_authz(self, *, method, details=None, user=None):
            if method == "GET":
                return False
            return real_method(self, method=method, details=details, user=user)

        with mock.patch.object(SimpleAuthManager, "is_authorized_connection", gated_authz):
            existing_response = test_client.post(
                "/connections/test",
                json={"connection_id": TEST_CONN_ID, "conn_type": "sqlite"},
            )
            missing_response = test_client.post(
                "/connections/test",
                json={"connection_id": "this_connection_does_not_exist", "conn_type": "sqlite"},
            )

        # Both calls reach the body-only test path (the unreadable existing
        # connection is treated as if it did not exist), so status + body
        # shape must be identical — no existence oracle for protected ids.
        assert existing_response.status_code == missing_response.status_code
        assert set(existing_response.json().keys()) == set(missing_response.json().keys())

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_unreadable_existing_connection_does_not_trigger_secrets_lookup(self, test_client):
        """The route must gate the secrets-backend lookup on the GET
        authorization check, not perform the lookup and then suppress the
        result. Otherwise an unauthorized caller can still force
        ``Connection.get_connection_from_secrets`` to query every configured
        secrets backend for arbitrary connection ids — leaking timing /
        existence signals, generating access-log entries in audited
        backends, and imposing backend load."""
        self.create_connection()

        from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager

        real_method = SimpleAuthManager.is_authorized_connection

        def gated_authz(self, *, method, details=None, user=None):
            if method == "GET":
                return False
            return real_method(self, method=method, details=details, user=user)

        with (
            mock.patch.object(SimpleAuthManager, "is_authorized_connection", gated_authz),
            mock.patch.object(
                Connection,
                "get_connection_from_secrets",
                wraps=Connection.get_connection_from_secrets,
            ) as spy_secrets,
        ):
            response = test_client.post(
                "/connections/test",
                json={"connection_id": TEST_CONN_ID, "conn_type": "sqlite"},
            )

        assert response.status_code == 200
        spy_secrets.assert_not_called()

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    @conf_vars({("core", "multi_team"): "true"})
    def test_existing_connection_lookup_preserves_team_scope(self, test_client, testing_team, session):
        """The secrets-backend lookup must propagate the authorized
        ``team_name`` to ``Connection.get_connection_from_secrets``.
        Otherwise the call falls back to global / wrong-team paths in
        team-aware backends (Vault, Akeyless, …) and can return a
        cross-scope secret with the same ``conn_id`` — exactly what the
        team-scoped authorization check above is supposed to prevent."""
        self.create_connection(team_name=testing_team.name)
        session.commit()

        with mock.patch.object(
            Connection,
            "get_connection_from_secrets",
            wraps=Connection.get_connection_from_secrets,
        ) as spy_secrets:
            response = test_client.post(
                "/connections/test",
                json={"connection_id": TEST_CONN_ID, "conn_type": "sqlite"},
            )

        assert response.status_code == 200
        spy_secrets.assert_called_once_with(TEST_CONN_ID, team_name=testing_team.name)

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    @conf_vars({("core", "multi_team"): "true"})
    def test_secrets_only_team_connection_uses_body_team_scope(self, test_client, testing_team):
        """When the connection exists only in a team-aware secrets backend
        (no metadata-DB row), ``Connection.get_team_name`` returns None.
        The route must then fall back to the body's validated ``team_name``
        so the GET authorization and the subsequent secrets lookup both
        run in the right team scope — otherwise a team-scoped existing
        connection would be authorized and looked up as global, losing
        the multi-team isolation guarantee in deployments that keep
        connections in Vault / Kubernetes / Akeyless rather than the DB."""
        # No ``self.create_connection()`` — TEST_CONN_ID lives only in a
        # secrets backend in this scenario.

        with mock.patch.object(
            Connection,
            "get_connection_from_secrets",
            wraps=Connection.get_connection_from_secrets,
        ) as spy_secrets:
            response = test_client.post(
                "/connections/test",
                json={
                    "connection_id": TEST_CONN_ID,
                    "conn_type": "sqlite",
                    "team_name": testing_team.name,
                },
            )

        assert response.status_code == 200
        spy_secrets.assert_called_once_with(TEST_CONN_ID, team_name=testing_team.name)

    @skip_if_force_lowest_dependencies_marker
    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    @pytest.mark.parametrize(
        "body",
        [
            {"connection_id": TEST_CONN_ID, "conn_type": "sqlite"},
            {"connection_id": TEST_CONN_ID, "conn_type": "ftp"},
        ],
    )
    def test_connection_env_is_cleaned_after_run(self, test_client, body):
        test_client.post("/connections/test", json=body)
        assert not any([key.startswith(CONN_ENV_PREFIX) for key in os.environ.keys()])

    @pytest.mark.parametrize(
        "body",
        [
            {"connection_id": TEST_CONN_ID, "conn_type": "sqlite"},
            {"connection_id": TEST_CONN_ID, "conn_type": "ftp"},
        ],
    )
    def test_should_respond_403_by_default(self, test_client, body):
        response = test_client.post("/connections/test", json=body)
        assert response.status_code == 403
        assert response.json() == {
            "detail": "Testing connections is disabled in Airflow configuration. "
            "Contact your deployment admin to enable it."
        }

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_should_merge_password_with_existing_connection(self, test_client, session):
        connection = Connection(
            conn_id=TEST_CONN_ID,
            conn_type="sqlite",
            password="existing_password",
        )
        session.add(connection)
        session.commit()
        initial_count = session.scalar(select(func.count()).select_from(Connection))

        captured_value = {}

        def mock_test_connection(self):
            captured_value["password"] = self.password
            captured_value["conn_type"] = self.conn_type
            return True, "mocked"

        body = {
            "connection_id": TEST_CONN_ID,
            "conn_type": "new_sqlite",
            "password": "***",
        }

        with mock.patch.object(Connection, "test_connection", mock_test_connection):
            response = test_client.post("/connections/test", json=body)

        assert response.status_code == 200
        assert response.json()["status"] is True
        # Verify that the existing password was used, not "***"
        assert captured_value["password"] == "existing_password"
        # Verify that payload info were used for other fields
        assert captured_value["conn_type"] == "new_sqlite"

        # Verify DB was not mutated
        session.expire_all()
        db_conn = session.scalar(select(Connection).filter_by(conn_id=TEST_CONN_ID))
        assert db_conn.password == "existing_password"
        assert session.scalar(select(func.count()).select_from(Connection)) == initial_count

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_should_merge_extra_with_existing_connection(self, test_client, session):
        connection = Connection(
            conn_id=TEST_CONN_ID,
            conn_type="fs",
            extra='{"path": "/", "existing_key": "existing_value"}',
        )
        session.add(connection)
        session.commit()
        initial_count = session.scalar(select(func.count()).select_from(Connection))

        captured_extra = {}

        def mock_test_connection(self):
            captured_extra["value"] = self.extra
            return True, "mocked"

        body = {
            "connection_id": TEST_CONN_ID,
            "conn_type": "fs",
            "extra": '{"path": "/", "new_key": "new_value"}',
        }

        with mock.patch.object(Connection, "test_connection", mock_test_connection):
            response = test_client.post("/connections/test", json=body)

        assert response.status_code == 200
        assert response.json()["status"] is True
        # Verify that new_key is reflected in the merged extra
        merged_extra = json.loads(captured_extra["value"])
        assert merged_extra["new_key"] == "new_value"
        assert merged_extra["path"] == "/"

        # Verify DB was not mutated
        session.expire_all()
        db_conn = session.scalar(select(Connection).filter_by(conn_id=TEST_CONN_ID))
        assert json.loads(db_conn.extra) == {"path": "/", "existing_key": "existing_value"}
        assert session.scalar(select(func.count()).select_from(Connection)) == initial_count

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_should_merge_both_password_and_extra(self, test_client, session):
        connection = Connection(
            conn_id=TEST_CONN_ID,
            conn_type="fs",
            password="existing_password",
            extra='{"path": "/", "existing_key": "existing_value"}',
        )
        session.add(connection)
        session.commit()
        initial_count = session.scalar(select(func.count()).select_from(Connection))

        captured_values = {}

        def mock_test_connection(self):
            captured_values["password"] = self.password
            captured_values["extra"] = self.extra
            return True, "mocked"

        body = {
            "connection_id": TEST_CONN_ID,
            "conn_type": "fs",
            "password": "***",
            "extra": '{"path": "/", "new_key": "new_value"}',
        }

        with mock.patch.object(Connection, "test_connection", mock_test_connection):
            response = test_client.post("/connections/test", json=body)

        assert response.status_code == 200
        assert response.json()["status"] is True
        # Verify that the existing password was used, not "***"
        assert captured_values["password"] == "existing_password"
        # Verify that new_key is reflected in the merged extra
        merged_extra = json.loads(captured_values["extra"])
        assert merged_extra["new_key"] == "new_value"
        assert merged_extra["path"] == "/"

        # Verify DB was not mutated
        session.expire_all()
        db_conn = session.scalar(select(Connection).filter_by(conn_id=TEST_CONN_ID))
        assert db_conn.password == "existing_password"
        assert json.loads(db_conn.extra) == {"path": "/", "existing_key": "existing_value"}
        assert session.scalar(select(func.count()).select_from(Connection)) == initial_count

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    @pytest.mark.parametrize(
        "override",
        [
            pytest.param({"host": "other_host"}, id="host-changed"),
            pytest.param({"host": "stored_host", "port": 9999}, id="port-changed"),
        ],
    )
    def test_should_reject_test_when_target_overridden_without_credentials(
        self, test_client, session, override
    ):
        session.add(
            Connection(
                conn_id=TEST_CONN_ID,
                conn_type="sqlite",
                host="stored_host",
                port=1234,
                password="existing_password",
            )
        )
        session.commit()

        body = {"connection_id": TEST_CONN_ID, "conn_type": "sqlite", **override}
        response = test_client.post("/connections/test", json=body)

        assert response.status_code == 400

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    @pytest.mark.parametrize(
        ("override", "expected_password"),
        [
            pytest.param(
                {"host": "stored_host", "port": 1234}, "existing_password", id="same-target-reuses-stored"
            ),
            pytest.param(
                {"host": "other_host", "password": "supplied_password"},
                "supplied_password",
                id="overridden-target-uses-supplied-creds",
            ),
        ],
    )
    def test_stored_secret_reused_only_for_same_target(
        self, test_client, session, override, expected_password
    ):
        session.add(
            Connection(
                conn_id=TEST_CONN_ID,
                conn_type="sqlite",
                host="stored_host",
                port=1234,
                password="existing_password",
            )
        )
        session.commit()

        body = {"connection_id": TEST_CONN_ID, "conn_type": "sqlite", **override}

        with mock.patch.object(Connection, "test_connection", autospec=True) as mock_test:
            mock_test.return_value = (True, "mocked")
            response = test_client.post("/connections/test", json=body)

        assert response.status_code == 200
        tested_connection = mock_test.call_args.args[0]
        assert tested_connection.password == expected_password

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_should_test_new_connection_without_existing(self, test_client):
        body = {
            "connection_id": "non_existent_conn",
            "conn_type": "sqlite",
        }
        response = test_client.post("/connections/test", json=body)
        assert response.status_code == 200
        assert response.json()["status"] is True


class TestAsyncConnectionTest(TestConnectionEndpoint):
    """Tests for the async connection test endpoints (POST + GET polling)."""

    TEST_REQUEST_BODY = {
        "connection_id": TEST_CONN_ID,
        "conn_type": TEST_CONN_TYPE,
        "host": TEST_CONN_HOST,
    }

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_should_respond_202(self, test_client, session):
        """POST /connections/enqueue-test returns 202 + token."""
        response = test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        assert response.status_code == 202
        body = response.json()
        assert "token" in body
        assert body["connection_id"] == TEST_CONN_ID
        assert body["state"] == "pending"
        assert len(body["token"]) > 0

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        assert response.status_code == 403

    def test_should_respond_403_by_default(self, test_client):
        """Connection testing is disabled by default."""
        response = test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        assert response.status_code == 403

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_creates_connection_test_request_row(self, test_client, session):
        """POST creates a ConnectionTestRequest row in PENDING state with connection fields."""
        response = test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        assert response.status_code == 202
        token = response.json()["token"]

        ct = session.scalar(select(ConnectionTestRequest).filter_by(token=token))
        assert ct is not None
        assert ct.connection_id == TEST_CONN_ID
        assert ct.conn_type == TEST_CONN_TYPE
        assert ct.host == TEST_CONN_HOST
        assert ct.state == "pending"

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_passes_queue_parameter(self, test_client, session):
        """POST /connections/enqueue-test passes the queue parameter."""
        body = {**self.TEST_REQUEST_BODY, "queue": "gpu_workers"}
        response = test_client.post("/connections/enqueue-test", json=body)
        assert response.status_code == 202
        token = response.json()["token"]

        ct = session.scalar(select(ConnectionTestRequest).filter_by(token=token))
        assert ct is not None
        assert ct.queue == "gpu_workers"

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_stores_commit_on_success(self, test_client, session):
        """POST /connections/enqueue-test stores the commit_on_success flag."""
        body = {**self.TEST_REQUEST_BODY, "commit_on_success": True}
        response = test_client.post("/connections/enqueue-test", json=body)
        assert response.status_code == 202
        token = response.json()["token"]

        ct = session.scalar(select(ConnectionTestRequest).filter_by(token=token))
        assert ct is not None
        assert ct.commit_on_success is True

    @mock.patch("airflow.api_fastapi.core_api.routes.public.connections.get_auth_manager", autospec=True)
    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_commit_on_success_existing_connection_requires_edit_permission(
        self, mock_get_auth_manager, test_client, session
    ):
        """POST /connections/enqueue-test requires edit permission when it can update a connection."""
        self.create_connection()
        mock_auth_manager = mock.create_autospec(BaseAuthManager, instance=True)
        mock_get_auth_manager.return_value = mock_auth_manager
        mock_auth_manager.is_authorized_connection.side_effect = lambda *, method, details, user: (
            method == "POST"
        )
        body = {**self.TEST_REQUEST_BODY, "commit_on_success": True}

        response = test_client.post("/connections/enqueue-test", json=body)

        assert response.status_code == 403
        mock_auth_manager.is_authorized_connection.assert_called_once()
        assert mock_auth_manager.is_authorized_connection.call_args.kwargs["method"] == "PUT"
        assert session.scalar(select(ConnectionTestRequest)) is None

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_returns_409_for_duplicate_active_test(self, test_client, session):
        """POST returns 409 when there's already an active test for the same connection_id."""
        response = test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        assert response.status_code == 202

        response = test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        assert response.status_code == 409
        assert "active connection test already exists" in response.json()["detail"].lower()

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_rejects_unknown_executor_with_422(self, test_client, session):
        """POST returns 422 when the requested executor is not configured."""
        body = {**self.TEST_REQUEST_BODY, "executor": "no_such_executor"}
        response = test_client.post("/connections/enqueue-test", json=body)
        assert response.status_code == 422
        assert "no_such_executor" in response.text

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_accepts_configured_executor(self, test_client, session):
        """POST accepts an executor name that matches a configured executor."""
        configured = ExecutorLoader.get_executor_names(validate_teams=False)
        executor_name = configured[0].alias or configured[0].module_path
        body = {**self.TEST_REQUEST_BODY, "executor": executor_name}
        response = test_client.post("/connections/enqueue-test", json=body)
        assert response.status_code == 202

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_rejects_team_name_mismatch_with_existing_connection(
        self, test_client, session, testing_team
    ):
        """A test claiming a different team than the connection's owner is rejected (no cross-team write)."""
        self.create_connection(team_name=testing_team.name)
        body = {**self.TEST_REQUEST_BODY, "team_name": "some_other_team", "commit_on_success": True}

        response = test_client.post("/connections/enqueue-test", json=body)
        assert response.status_code == 403
        assert "does not match the team" in response.json()["detail"]
        assert session.scalar(select(ConnectionTestRequest)) is None

    @conf_vars({("core", "multi_team"): "True"})
    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_post_accepts_matching_team_for_existing_connection(self, test_client, session, testing_team):
        """A test for an existing connection is authorized against that connection's team."""
        self.create_connection(team_name=testing_team.name)
        body = {**self.TEST_REQUEST_BODY, "team_name": testing_team.name}

        response = test_client.post("/connections/enqueue-test", json=body)
        assert response.status_code == 202

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_get_status_returns_pending(self, test_client, session):
        """GET /connections/enqueue-test/{token} returns current status."""
        post_response = test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        token = post_response.json()["token"]

        response = test_client.get(
            "/connections/enqueue-test", headers={"Airflow-Connection-Test-Token": token}
        )
        assert response.status_code == 200
        body = response.json()
        assert body["token"] == token
        assert body["connection_id"] == TEST_CONN_ID
        assert body["state"] == "pending"
        assert body["result_message"] is None
        assert "created_at" in body
        assert "reverted" not in body

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_get_status_returns_completed_result(self, test_client, session):
        """GET returns result after the worker has updated the test."""
        post_response = test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        token = post_response.json()["token"]

        ct = session.scalar(select(ConnectionTestRequest).filter_by(token=token))
        ct.state = ConnectionTestState.SUCCESS
        ct.result_message = "Connection successfully tested"
        session.commit()

        response = test_client.get(
            "/connections/enqueue-test", headers={"Airflow-Connection-Test-Token": token}
        )
        assert response.status_code == 200
        body = response.json()
        assert body["state"] == "success"
        assert body["result_message"] == "Connection successfully tested"

    def test_get_status_returns_404_for_invalid_token(self, test_client):
        """GET with an unknown token returns 404."""
        response = test_client.get(
            "/connections/enqueue-test", headers={"Airflow-Connection-Test-Token": "nonexistent-token"}
        )
        assert response.status_code == 404

    def test_get_status_requires_token_header(self, test_client):
        """GET without the token header is rejected (422), so the token is never in the URL."""
        response = test_client.get("/connections/enqueue-test")
        assert response.status_code == 422

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_get_status_unauthorized_user_does_not_leak_row(
        self, test_client, unauthorized_test_client, session
    ):
        """A user without rights on the conn_id never sees the row payload via GET-by-token."""
        post_response = test_client.post("/connections/enqueue-test", json=self.TEST_REQUEST_BODY)
        assert post_response.status_code == 202
        token = post_response.json()["token"]

        response = unauthorized_test_client.get(
            "/connections/enqueue-test", headers={"Airflow-Connection-Test-Token": token}
        )
        assert response.status_code in (401, 403, 404)
        body = (
            response.json() if response.headers.get("content-type", "").startswith("application/json") else {}
        )
        assert "result_message" not in body
        assert "connection_id" not in body


class TestEditDeleteWithActiveAsyncTest(TestConnectionEndpoint):
    """PATCH/DELETE on a connection are not blocked by an in-flight async test."""

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_patch_succeeds_with_active_test(self, test_client, session):
        self.create_connection()
        test_client.post(
            "/connections/enqueue-test",
            json={
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "host": TEST_CONN_HOST,
            },
        )

        response = test_client.patch(
            f"/connections/{TEST_CONN_ID}",
            json={
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "host": "updated-host.example.com",
            },
        )
        assert response.status_code == 200

    @mock.patch.dict(os.environ, {"AIRFLOW__CORE__TEST_CONNECTION": "Enabled"})
    def test_delete_succeeds_with_active_test(self, test_client, session):
        self.create_connection()
        test_client.post(
            "/connections/enqueue-test",
            json={
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "host": TEST_CONN_HOST,
            },
        )

        response = test_client.delete(f"/connections/{TEST_CONN_ID}")
        assert response.status_code == 204


class TestCreateDefaultConnections(TestConnectionEndpoint):
    def test_should_respond_204(self, test_client, session):
        response = test_client.post("/connections/defaults")
        assert response.status_code == 204
        assert response.content == b""
        _check_last_log(session, dag_id=None, event="create_default_connections", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post("/connections/defaults")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post("/connections/defaults")
        assert response.status_code == 403

    @mock.patch("airflow.api_fastapi.core_api.routes.public.connections.db_create_default_connections")
    def test_should_call_db_create_default_connections(self, mock_db_create_default_connections, test_client):
        response = test_client.post("/connections/defaults")
        assert response.status_code == 204
        mock_db_create_default_connections.assert_called_once()


class TestBulkConnections(TestConnectionEndpoint):
    @pytest.mark.parametrize(
        ("actions", "expected_results"),
        [
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "connection_id": "NOT_EXISTING_CONN_ID",
                                    "conn_type": "NOT_EXISTING_CONN_TYPE",
                                }
                            ],
                            "action_on_existence": "skip",
                        }
                    ]
                },
                {
                    "create": {
                        "success": ["NOT_EXISTING_CONN_ID"],
                        "errors": [],
                    }
                },
                id="test_successful_create",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "connection_id": TEST_CONN_ID,
                                    "conn_type": TEST_CONN_TYPE,
                                },
                                {
                                    "connection_id": "NOT_EXISTING_CONN_ID",
                                    "conn_type": "NOT_EXISTING_CONN_TYPE",
                                },
                            ],
                            "action_on_existence": "skip",
                        }
                    ]
                },
                {
                    "create": {
                        "success": ["NOT_EXISTING_CONN_ID"],
                        "errors": [],
                    }
                },
                id="test_successful_create_with_skip",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "connection_id": TEST_CONN_ID,
                                    "conn_type": TEST_CONN_TYPE,
                                    "description": "new_description",
                                }
                            ],
                            "action_on_existence": "overwrite",
                        }
                    ]
                },
                {
                    "create": {
                        "success": [TEST_CONN_ID],
                        "errors": [],
                    }
                },
                id="test_create_with_overwrite",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "connection_id": TEST_CONN_ID,
                                    "conn_type": TEST_CONN_TYPE,
                                    "description": TEST_CONN_DESCRIPTION,
                                    "host": TEST_CONN_HOST,
                                    "port": TEST_CONN_PORT,
                                    "login": TEST_CONN_LOGIN,
                                },
                            ],
                            "action_on_existence": "fail",
                        }
                    ]
                },
                {
                    "create": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The connections with these connection_ids: {'test_connection_id'} already exist.",
                                "status_code": 409,
                            },
                        ],
                    }
                },
                id="test_create_conflict",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "connection_id": TEST_CONN_ID,
                                    "conn_type": TEST_CONN_TYPE,
                                    "description": "new_description",
                                }
                            ],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {
                    "update": {
                        "success": [TEST_CONN_ID],
                        "errors": [],
                    }
                },
                id="test_successful_update",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "connection_id": "NOT_EXISTING_CONN_ID",
                                    "conn_type": "NOT_EXISTING_CONN_TYPE",
                                }
                            ],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {
                    "update": {
                        "success": [],
                        "errors": [],
                    }
                },
                id="test_update_with_skip",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "connection_id": "NOT_EXISTING_CONN_ID",
                                    "conn_type": "NOT_EXISTING_CONN_TYPE",
                                }
                            ],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {
                    "update": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The connections with these connection_ids: {'NOT_EXISTING_CONN_ID'} were not found.",
                                "status_code": 404,
                            }
                        ],
                    }
                },
                id="test_update_with_fail",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "connection_id": TEST_CONN_ID,
                                    "conn_type": TEST_CONN_TYPE,
                                    "description": "updated_description",
                                }
                            ],
                            "update_mask": ["description"],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {"update": {"success": [TEST_CONN_ID], "errors": []}},
                id="test_connection_update_with_valid_update_mask",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": [TEST_CONN_ID],
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [TEST_CONN_ID],
                        "errors": [],
                    }
                },
                id="test_successful_delete",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": ["NOT_EXISTING_CONN_ID"],
                            "action_on_non_existence": "skip",
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [],
                        "errors": [],
                    }
                },
                id="test_delete_with_skip",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "delete",
                            "entities": ["NOT_EXISTING_CONN_ID"],
                            "action_on_non_existence": "fail",
                        }
                    ]
                },
                {
                    "delete": {
                        "success": [],
                        "errors": [
                            {
                                "error": "The connections with these connection_ids: {'NOT_EXISTING_CONN_ID'} were not found.",
                                "status_code": 404,
                            }
                        ],
                    }
                },
                id="test_delete_not_found",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "create",
                            "entities": [
                                {
                                    "connection_id": "NOT_EXISTING_CONN_ID",
                                    "conn_type": "NOT_EXISTING_CONN_TYPE",
                                }
                            ],
                            "action_on_existence": "skip",
                        },
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "connection_id": TEST_CONN_ID,
                                    "conn_type": TEST_CONN_TYPE,
                                    "description": "new_description",
                                }
                            ],
                            "action_on_non_existence": "skip",
                        },
                        {
                            "action": "delete",
                            "entities": [TEST_CONN_ID],
                            "action_on_non_existence": "skip",
                        },
                    ]
                },
                {
                    "create": {
                        "success": ["NOT_EXISTING_CONN_ID"],
                        "errors": [],
                    },
                    "update": {
                        "success": [TEST_CONN_ID],
                        "errors": [],
                    },
                    "delete": {
                        "success": [TEST_CONN_ID],
                        "errors": [],
                    },
                },
                id="test_create_update_delete",
            ),
            pytest.param(
                {
                    "actions": [
                        {
                            "action": "update",
                            "entities": [
                                {
                                    "connection_id": TEST_CONN_ID,
                                    "conn_type": TEST_CONN_TYPE,
                                    "description": "updated_description",
                                }
                            ],
                            "update_mask": ["description"],
                            "action_on_non_existence": "fail",
                        },
                        {
                            "action": "delete",
                            "entities": [TEST_CONN_ID],
                            "action_on_non_existence": "fail",
                        },
                    ]
                },
                {
                    "update": {"success": [TEST_CONN_ID], "errors": []},
                    "delete": {"success": [TEST_CONN_ID], "errors": []},
                },
                id="test_connection_create_update_delete_with_update_mask",
            ),
        ],
    )
    def test_bulk_connections(self, test_client, actions, expected_results, session):
        self.create_connections()
        response = test_client.patch("/connections", json=actions)
        response_data = response.json()
        for connection_id, value in expected_results.items():
            assert response_data[connection_id] == value
        _check_last_log(session, dag_id=None, event="bulk_connections", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch("/connections", json={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(
            "/connections",
            json={
                "actions": [
                    {
                        "action": "create",
                        "entities": [
                            {"connection_id": "test1", "conn_type": "test1"},
                        ],
                    },
                ]
            },
        )
        assert response.status_code == 403

    def test_bulk_update_avoids_n_plus_one_queries(self, session):
        self.create_connections()
        session.expire_all()

        request = BulkBody[ConnectionBody].model_validate(
            {
                "actions": [
                    {
                        "action": "update",
                        "entities": [
                            {
                                "connection_id": TEST_CONN_ID,
                                "conn_type": TEST_CONN_TYPE,
                                "description": "updated_description",
                            },
                            {
                                "connection_id": TEST_CONN_ID_2,
                                "conn_type": TEST_CONN_TYPE_2,
                                "description": "updated_description_2",
                            },
                        ],
                        "update_mask": ["description"],
                        "action_on_non_existence": "fail",
                    }
                ]
            }
        )
        service = BulkConnectionService(session=session, request=request)
        results = BulkActionResponse()

        with assert_queries_count(1, session=session):
            service.handle_bulk_update(request.actions[0], results)

        assert sorted(results.success) == [TEST_CONN_ID, TEST_CONN_ID_2]

    def test_bulk_delete_avoids_n_plus_one_queries(self, session):
        self.create_connections()
        session.expire_all()

        request = BulkBody[ConnectionBody].model_validate(
            {
                "actions": [
                    {
                        "action": "delete",
                        "entities": [TEST_CONN_ID, TEST_CONN_ID_2],
                        "action_on_non_existence": "fail",
                    }
                ]
            }
        )
        service = BulkConnectionService(session=session, request=request)
        results = BulkActionResponse()

        with assert_queries_count(1, session=session):
            service.handle_bulk_delete(request.actions[0], results)

        assert sorted(results.success) == [TEST_CONN_ID, TEST_CONN_ID_2]

    @conf_vars({("core", "multi_team"): "False"})
    def test_bulk_rejects_team_name_when_multi_team_is_disabled(self, test_client):
        actions = {
            "actions": [
                {
                    "action": "create",
                    "entities": [
                        {
                            "connection_id": "test_conn_id_1",
                            "conn_type": TEST_CONN_TYPE,
                            "description": "description",
                        },
                        {
                            "connection_id": "test_conn_id_2",
                            "conn_type": TEST_CONN_TYPE_2,
                            "description": "description_2",
                            "team_name": "test_team",
                        },
                    ],
                },
                {
                    "action": "update",
                    "entities": [
                        {
                            "connection_id": "test_conn_id_3",
                            "conn_type": TEST_CONN_TYPE,
                            "description": "updated_description",
                            "team_name": "test_team",
                        },
                        {
                            "connection_id": "test_conn_id_4",
                            "conn_type": TEST_CONN_TYPE_2,
                            "description": "updated_description_2",
                        },
                    ],
                },
            ]
        }
        response = test_client.patch("/connections", json=actions)
        assert response.status_code == 422
        detail = response.json()["detail"]

        assert all(
            "team_name cannot be set when multi_team mode is disabled. Please contact your administrator."
            in err["msg"]
            for err in detail
        ), f"Unexpected errors in detail: {detail}"

        expected_error_conn_ids = {err["input"]["connection_id"] for err in detail}
        assert sorted(expected_error_conn_ids) == ["test_conn_id_2", "test_conn_id_3"]

    @conf_vars({("core", "multi_team"): "True"})
    def test_bulk_create_overwrite_preserves_unset_team_name(self, test_client, testing_team, session):
        """A bulk create+overwrite that omits ``team_name`` must NOT reset an existing connection's
        ``team_name`` to ``None`` (parity with the pools fix). Overwriting with only ``conn_type``
        previously clobbered every unset field via ``model_dump(by_alias=True)`` — silently nulling
        the connection's multi-team ownership. ``exclude_unset=True`` preserves omitted fields.
        """
        self.create_connection(team_name=testing_team.name)
        before = session.scalar(select(Connection).where(Connection.conn_id == TEST_CONN_ID))
        assert before.team_name == testing_team.name

        response = test_client.patch(
            "/connections",
            json={
                "actions": [
                    {
                        "action": "create",
                        "action_on_existence": "overwrite",
                        "entities": [{"connection_id": TEST_CONN_ID, "conn_type": "new_type"}],
                    }
                ]
            },
        )
        assert response.status_code == 200
        assert response.json()["create"]["success"] == [TEST_CONN_ID]

        session.expire_all()
        after = session.scalar(select(Connection).where(Connection.conn_id == TEST_CONN_ID))
        assert after.conn_type == "new_type"  # provided field is applied
        assert after.team_name == testing_team.name, (
            "bulk overwrite that omitted team_name must preserve existing ownership, "
            f"got team_name={after.team_name!r}"
        )


class TestPostConnectionExtraBackwardCompatibility(TestConnectionEndpoint):
    def test_post_should_accept_empty_string_as_extra(self, test_client, session):
        body = {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "extra": ""}

        response = test_client.post("/connections", json=body)
        assert response.status_code == 201

        connection = session.scalars(select(Connection).where(Connection.conn_id == TEST_CONN_ID)).first()
        assert connection is not None
        assert connection.extra == "{}"  # Backward compatibility: treat "" as empty JSON object

    @pytest.mark.parametrize(
        ("extra", "expected_error_message"),
        [
            ("[1,2,3]", "Expected JSON object in `extra` field, got non-dict JSON"),
            ("some_string", "Encountered non-JSON in `extra` field"),
        ],
    )
    def test_post_should_fail_with_non_json_object_as_extra(
        self, test_client, extra, expected_error_message, session
    ):
        """JSON primitives are a valid JSON and should raise 422 validation error."""
        body = {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "extra": extra}

        response = test_client.post("/connections", json=body)
        assert response.status_code == 422
        assert (
            "Value error, The `extra` field must be a valid JSON object (e.g., {'key': 'value'})"
            in response.json()["detail"][0]["msg"]
        )

        _check_last_log(
            session,
            dag_id=None,
            event="post_connection",
            logical_date=None,
            expected_extra={
                "connection_id": "test_connection_id",
                "conn_type": "test_type",
                "extra": expected_error_message,
                "method": "POST",
            },
        )
