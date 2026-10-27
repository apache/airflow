#
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

from unittest.mock import patch

import pytest

from tests_common.test_utils.config import conf_vars


class TestLogin:
    @patch("airflow.api_fastapi.auth.managers.simple.routes.login.SimpleAuthManagerLogin")
    def test_create_token(
        self,
        mock_simple_auth_manager_login,
        test_client,
        auth_manager,
    ):
        mock_simple_auth_manager_login.create_token.return_value = "DUMMY_TOKEN"

        response = test_client.post(
            "/auth/token",
            json={"username": "test1", "password": "DUMMY_PASS"},
        )
        assert response.status_code == 201
        assert "access_token" in response.json()

    @patch("airflow.api_fastapi.auth.managers.simple.routes.login.SimpleAuthManagerLogin")
    def test_create_token_with_form_data(
        self,
        mock_simple_auth_manager_login,
        test_client,
        auth_manager,
        test_user,
    ):
        mock_simple_auth_manager_login.create_token.return_value = "DUMMY_TOKEN"

        response = test_client.post(
            "/auth/token",
            data={
                "username": "test1",
                "password": "DUMMY_PASS",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        assert response.status_code == 201
        assert "access_token" in response.json()

    def test_create_token_invalid_user_password(self, test_client):
        response = test_client.post(
            "/auth/token",
            json={"username": "INVALID_USER", "password": "INVALID_PASS"},
        )
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid credentials"

    def test_create_token_all_admins(self, test_client):
        with conf_vars({("core", "simple_auth_manager_all_admins"): "true"}):
            response = test_client.get("/auth/token")
            assert response.status_code == 201

    def test_create_token_all_admins_config_disabled(self, test_client):
        response = test_client.get("/auth/token")
        assert response.status_code == 403

    def test_login_all_admins(self, test_client):
        with conf_vars({("core", "simple_auth_manager_all_admins"): "true", ("api", "ssl_cert"): "false"}):
            response = test_client.get("/auth/token/login", follow_redirects=False)
            assert response.status_code == 307
            assert "location" in response.headers
            assert response.cookies.get("_token") is not None
            assert "samesite=lax" in response.headers["set-cookie"].lower()

    def test_login_all_admins_redirects_to_next_url(self, test_client):
        with conf_vars({("core", "simple_auth_manager_all_admins"): "true", ("api", "ssl_cert"): "false"}):
            response = test_client.get(
                "/auth/token/login?next=/dags/example_dag/runs/manual__2026-05-20/tasks/example_task",
                follow_redirects=False,
            )
            assert response.status_code == 307
            assert (
                response.headers["location"] == "/dags/example_dag/runs/manual__2026-05-20/tasks/example_task"
            )
            assert response.cookies.get("_token") is not None

    def test_login_all_admins_ignores_unsafe_next_url(self, test_client):
        with conf_vars({("core", "simple_auth_manager_all_admins"): "true", ("api", "ssl_cert"): "false"}):
            response = test_client.get(
                "/auth/token/login?next=https://example.com/malicious",
                follow_redirects=False,
            )
            assert response.status_code == 307
            assert response.headers["location"] == "/"
            assert response.cookies.get("_token") is not None

    def test_login_all_admins_config_disabled(self, test_client):
        response = test_client.get("/auth/token/login", follow_redirects=False)
        assert response.status_code == 403

    @patch("airflow.api_fastapi.auth.managers.simple.routes.login.SimpleAuthManagerLogin")
    def test_create_token_cli(self, mock_simple_auth_manager_login, test_client, auth_manager):
        mock_simple_auth_manager_login.create_token.return_value = "DUMMY_TOKEN"

        response = test_client.post(
            "/auth/token/cli",
            json={"username": "test1", "password": "DUMMY_PASS"},
        )
        assert response.status_code == 201
        assert response.json()["access_token"]

    def test_create_token_invalid_user_password_cli(self, test_client):
        response = test_client.post(
            "/auth/token/cli",
            json={"username": "INVALID_USER", "password": "INVALID_PASS"},
        )
        assert response.status_code == 401
        assert response.json()["detail"] == "Invalid credentials"


# Bodies that reach ``parse_login_body`` without being valid credentials: the first five make
# ``json.loads`` itself raise, the last three parse to something that is not a JSON object.
UNPARSABLE_JSON_BODIES = [
    pytest.param(b"{", id="malformed"),
    pytest.param(b"", id="empty"),
    pytest.param(b'{"username": "\xff\xfe"}', id="invalid_utf8"),
    pytest.param(b'{"username": ' + b"1" * 4301 + b"}", id="oversized_int"),
    pytest.param(b"[" * 20_000, id="deeply_nested"),
    pytest.param(b'["test1", "DUMMY_PASS"]', id="json_array"),
    pytest.param(b"5", id="json_scalar"),
    pytest.param(b"null", id="json_null"),
]


class TestCreateTokenUnparsableBody:
    """
    ``/auth/token`` must answer a body it cannot read rather than raise out of the dependency.

    Unlike ``/auth/token/cli``, which declares ``body: LoginBody`` and so gets FastAPI's own
    validation, this route takes its body through ``Depends(parse_login_body)``. FastAPI parses
    no body for it, leaving that dependency the only parse. The route is unauthenticated, so
    anything it raises is reachable without credentials.
    """

    @pytest.mark.parametrize("payload", UNPARSABLE_JSON_BODIES)
    def test_answers_like_the_natively_parsed_route(self, payload, test_client):
        """
        Both routes take the same body and must answer it the same way.

        Asserting the pair agree rather than naming status codes keeps this pinned to whatever
        FastAPI does with a body it cannot read, instead of to numbers copied out of it.
        """
        headers = {"Content-Type": "application/json"}
        declared = test_client.post("/auth/token/cli", content=payload, headers=headers)
        via_dependency = test_client.post("/auth/token", content=payload, headers=headers)

        assert declared.status_code < 500
        assert via_dependency.status_code == declared.status_code

    def test_form_parser_error_keeps_its_own_detail(self, test_client):
        """
        A form body the parser rejects must keep starlette's message, not the generic one.

        The catch-all that turns an unreadable body into a 400 also sees the 400 starlette
        raises from its own form parser, so it has to let that one through untouched.
        """
        response = test_client.post(
            "/auth/token",
            content=b"&".join(b"a=1" for _ in range(2000)),
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

        assert response.status_code == 400
        assert "Too many fields" in response.json()["detail"]
