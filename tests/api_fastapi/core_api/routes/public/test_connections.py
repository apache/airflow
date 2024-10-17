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
TEST_CONN_TYPE = "test_type"
TEST_CONN_DESCRIPTION = "some_description_a"
TEST_CONN_HOST = "some_host_a"
TEST_CONN_PORT = 8080
TEST_CONN_LOGIN = "some_login"


TEST_CONN_ID_2 = "test_connection_id_2"
TEST_CONN_TYPE_2 = "test_type_2"
TEST_CONN_DESCRIPTION_2 = "some_description_b"
TEST_CONN_HOST_2 = "some_host_b"
TEST_CONN_PORT_2 = 8081
TEST_CONN_LOGIN_2 = "some_login_b"


@provide_session
def _create_connection(session) -> None:
    connection_model = Connection(
        conn_id=TEST_CONN_ID,
        conn_type=TEST_CONN_TYPE,
        description=TEST_CONN_DESCRIPTION,
        host=TEST_CONN_HOST,
        port=TEST_CONN_PORT,
        login=TEST_CONN_LOGIN,
    )
    session.add(connection_model)


@provide_session
def _create_connections(session) -> None:
    _create_connection(session)
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
    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_ids",
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
        ],
    )
    def test_should_respond_200(
        self, test_client, session, query_params, expected_total_entries, expected_ids
    ):
        self.create_connections()
        response = test_client.get("/public/connections/", params=query_params)
        assert response.status_code == 200

        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert [connection["connection_id"] for connection in body["connections"]] == expected_ids


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
    def test_post_should_respond_200(self, test_client, session, body):
        response = test_client.post("/public/connections/", json=body)
        assert response.status_code == 201
        connection = session.query(Connection).all()
        assert len(connection) == 1

    @pytest.mark.parametrize(
        "body",
        [
            {"connection_id": "****", "conn_type": TEST_CONN_TYPE},
            {"connection_id": "test()", "conn_type": TEST_CONN_TYPE},
            {"connection_id": "this_^$#is_invalid", "conn_type": TEST_CONN_TYPE},
            {"connection_id": "iam_not@#$_connection_id", "conn_type": TEST_CONN_TYPE},
        ],
    )
    def test_post_should_respond_400_for_invalid_conn_id(self, test_client, body):
        response = test_client.post("/public/connections/", json=body)
        assert response.status_code == 400
        connection_id = body["connection_id"]
        assert response.json() == {
            "detail": f"The key '{connection_id}' has to be made of "
            "alphanumeric characters, dashes, dots and underscores exclusively",
        }

    @pytest.mark.parametrize(
        "body",
        [
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE},
        ],
    )
    def test_post_should_respond_already_exist(self, test_client, body):
        response = test_client.post("/public/connections/", json=body)
        assert response.status_code == 201
        # Another request
        response = test_client.post("/public/connections/", json=body)
        assert response.status_code == 409
        assert response.json() == {
            "detail": f"Connection with connection_id: `{TEST_CONN_ID}` already exists",
        }

    @pytest.mark.enable_redact
    @pytest.mark.parametrize(
        "body, expected_response",
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
                },
            ),
        ],
    )
    def test_post_should_response_201_redacted_password(self, test_client, body, expected_response):
        response = test_client.post("/public/connections/", json=body)
        assert response.status_code == 201
        assert response.json() == expected_response


class TestPatchConnection(TestConnectionEndpoint):
    @pytest.mark.parametrize(
        "payload",
        [
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "extra": '{"key": "var"}'},
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "host": "test_host_patch"},
            {
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "host": "test_host_patch",
                "port": 80,
            },
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "login": "test_login_patch"},
            {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "port": 80},
            {
                "connection_id": TEST_CONN_ID,
                "conn_type": TEST_CONN_TYPE,
                "port": 80,
                "login": "test_login_patch",
            },
        ],
    )
    @provide_session
    def test_patch_should_respond_200(self, test_client, payload, session):
        self.create_connection()

        response = test_client.patch(f"/public/connections/{TEST_CONN_ID}", json=payload)
        assert response.status_code == 200

    @pytest.mark.parametrize(
        "payload, updated_connection, update_mask",
        [
            (
                {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "extra": '{"key": "var"}'},
                {
                    "connection_id": TEST_CONN_ID,
                    "conn_type": TEST_CONN_TYPE,
                    "extra": None,
                    "host": TEST_CONN_HOST,
                    "login": TEST_CONN_LOGIN,
                    "port": TEST_CONN_PORT,
                    "schema": None,
                    "description": TEST_CONN_DESCRIPTION,
                },
                {"update_mask": ["login, port"]},
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
                    "description": TEST_CONN_DESCRIPTION,
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
                    "description": TEST_CONN_DESCRIPTION,
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
                    "description": TEST_CONN_DESCRIPTION,
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
                    "schema": None,
                    "description": TEST_CONN_DESCRIPTION,
                },
                {"update_mask": ["host"]},
            ),
        ],
    )
    def test_patch_should_respond_200_with_update_mask(
        self, test_client, session, payload, updated_connection, update_mask
    ):
        self.create_connection()
        test_connection = TEST_CONN_ID
        response = test_client.patch(f"/public/connections/{TEST_CONN_ID}", json=payload, params=update_mask)
        assert response.status_code == 200
        connection = session.query(Connection).filter_by(conn_id=test_connection).first()
        assert connection.password is None
        assert response.json() == updated_connection

    @pytest.mark.parametrize(
        "payload",
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
    def test_patch_should_respond_400(self, test_client, payload):
        self.create_connection()
        response = test_client.patch(f"/public/connections/{TEST_CONN_ID}", json=payload)
        assert response.status_code == 400
        print(response.json())
        assert {
            "detail": "The connection_id in the request body does not match the URL parameter",
        } == response.json()

    @pytest.mark.parametrize(
        "payload",
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
    def test_patch_should_respond_404(self, test_client, payload):
        payload = {"connection_id": TEST_CONN_ID, "conn_type": TEST_CONN_TYPE, "port": 90}
        response = test_client.patch(f"/public/connections/{TEST_CONN_ID}", json=payload)
        assert response.status_code == 404
        print(response.json())
        assert {
            "detail": f"The Connection with connection_id: `{TEST_CONN_ID}` was not found",
        } == response.json()
