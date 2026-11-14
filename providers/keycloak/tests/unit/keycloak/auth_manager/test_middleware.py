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

import threading
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from jwt import ExpiredSignatureError, InvalidTokenError

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN
from airflow.api_fastapi.core_api import security as core_api_security
from airflow.providers.keycloak.auth_manager import middleware
from airflow.providers.keycloak.auth_manager.constants import (
    COOKIE_NAME_ACCESS_TOKEN,
    COOKIE_NAME_REFRESH_TOKEN,
)
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers.keycloak.auth_manager.middleware import KeycloakJWTMiddleware
from airflow.providers.keycloak.auth_manager.user import KeycloakAuthManagerUser

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import (
    AIRFLOW_V_3_1_7_PLUS,
    AIRFLOW_V_3_3_PLUS,
    AIRFLOW_V_3_5_PLUS,
)

if AIRFLOW_V_3_1_7_PLUS:
    from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
else:
    AuthManagerRefreshTokenExpiredException = None  # type: ignore[assignment,misc]

from unit.keycloak.test_utils import keycloak_token, token_with_payload

MODULE = "airflow.providers.keycloak.auth_manager.middleware"

_default_cookies = [
    "_token=issued_token; HttpOnly; Path=/; SameSite=lax",
    "_access_token=issued_access_token; HttpOnly; Path=/; SameSite=lax",
    "_refresh_token=issued_refresh_token; HttpOnly; Path=/; SameSite=lax",
]


def _build_app(static_dir=None, root_path: str = "") -> tuple[FastAPI, dict]:
    """Return an app wrapped by the middleware and a dict capturing what the endpoint saw."""
    seen: dict = {}
    app = FastAPI(root_path=root_path)

    @app.get("/api/v2/dags")
    def dags(request: Request):
        seen["user"] = getattr(request.state, "user", None)
        seen["authenticated_via"] = getattr(request.state, "user_authenticated_via", None)
        return PlainTextResponse("ok")

    @app.get("/api/v2/monitor/health")
    def health():
        return PlainTextResponse("healthy")

    @app.get("/api/v2/version")
    def version():
        return PlainTextResponse("3.x")

    @app.get("/auth/login_callback")
    def login_callback():
        response = PlainTextResponse("logged in")
        response.set_cookie(
            COOKIE_NAME_JWT_TOKEN, "issued_token", path=middleware.get_cookie_path(), httponly=True
        )
        response.set_cookie(
            COOKIE_NAME_ACCESS_TOKEN,
            "issued_access_token",
            path=middleware.get_cookie_path(),
            httponly=True,
        )
        response.set_cookie(
            COOKIE_NAME_REFRESH_TOKEN,
            "issued_refresh_token",
            path=middleware.get_cookie_path(),
            httponly=True,
        )
        return response

    @app.get("/api/v2/auth/logout")
    def logout():
        response = PlainTextResponse("logged out")
        response.delete_cookie(COOKIE_NAME_JWT_TOKEN, path=middleware.get_cookie_path(), httponly=True)
        return response

    @app.get("/api/v2/stream")
    def stream():
        return StreamingResponse(iter([b"chunk1-", b"chunk2-", b"chunk3"]), media_type="text/plain")

    if static_dir is not None:
        app.mount("/static", StaticFiles(directory=static_dir), name="webapp_static_folder")

    app.add_middleware(KeycloakJWTMiddleware, skip_path_prefixes=("/static/",))
    return app, seen


def _build_user(user_id: str, name: str, access_token: str, refresh_token: str):
    user = KeycloakAuthManagerUser(user_id=user_id, name=name, access_token="", refresh_token=None)
    if not AIRFLOW_V_3_5_PLUS:
        user.access_token = access_token
        user.refresh_token = refresh_token
    return user


@pytest.fixture
def mock_auth_manager():
    with patch(f"{MODULE}.get_auth_manager", autospec=True) as mock_get_auth_manager:
        mock_auth_manager = MagicMock(spec=KeycloakAuthManager)
        mock_auth_manager.refresh_user.return_value = None
        mock_user = _build_user(
            user_id="user_id",
            name="name",
            access_token=keycloak_token("user_id"),
            refresh_token="refresh_token",
        )
        mock_auth_manager.get_user_from_token = AsyncMock(return_value=mock_user)
        mock_auth_manager.generate_jwt.return_value = "new_token"
        mock_auth_manager._token_subject = KeycloakAuthManager._token_subject
        mock_get_auth_manager.return_value = mock_auth_manager
        yield mock_auth_manager


@pytest.fixture
def mock_generate_jwt(mock_auth_manager):
    return mock_auth_manager.generate_jwt


@pytest.fixture(autouse=True)
def no_ssl_cert():
    with conf_vars({("api", "ssl_cert"): ""}):
        yield


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS, reason="KeycloakJWTMiddleware relies on custom middleware interface."
)
@pytest.mark.asyncio
class TestKeycloakJWTMiddleware:
    def test_no_token(self, mock_auth_manager):
        app, _ = _build_app()

        response = TestClient(app, cookies={}).get("/api/v2/dags")

        assert response.status_code == 200
        assert "set-cookie" not in response.headers
        mock_auth_manager.get_user_from_token.assert_not_called()

    def test_invalid_token_clears_cookie(self, mock_auth_manager):
        mock_auth_manager.get_user_from_token.side_effect = HTTPException(
            status_code=403, detail="Invalid JWT token"
        )
        app, seen = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "invalid_token"}).get("/api/v2/dags")

        assert response.status_code == 200
        assert seen["user"] is None
        assert '_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax' in response.headers["set-cookie"]
        mock_auth_manager.refresh_user.assert_not_called()

    def test_no_refreshed_user(self, mock_auth_manager, mock_generate_jwt):
        access_token = keycloak_token("user_id")
        mock_auth_manager.refresh_user.return_value = None
        app, seen = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        assert "set-cookie" not in response.headers
        assert seen["user"] is mock_auth_manager.get_user_from_token.return_value
        assert seen["authenticated_via"] is trusted_marker
        if AIRFLOW_V_3_5_PLUS:
            mock_auth_manager.get_user_from_token.assert_called_once_with("valid_token")
        else:
            mock_auth_manager.get_user_from_token.assert_called_once_with(
                "valid_token", access_token, "refresh_token"
            )
        mock_generate_jwt.assert_not_called()

    def test_refreshed_user(self, mock_auth_manager):
        access_token = keycloak_token("user_id")
        current_user = _build_user(
            user_id="user_id", name="name", access_token=access_token, refresh_token="refresh_token"
        )
        refreshed_user = KeycloakAuthManagerUser(
            user_id="user_id", name="name", access_token=access_token, refresh_token="new_refresh_token"
        )
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_auth_manager.get_user_from_token.side_effect = [current_user, ExpiredSignatureError]
        app, seen = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )

        assert seen["user"] is refreshed_user
        assert seen["authenticated_via"] is trusted_marker
        mock_auth_manager.refresh_user.assert_called_once_with(user=current_user)
        mock_auth_manager.generate_jwt.assert_called_once_with(refreshed_user)
        assert (
            response.headers["set-cookie"]
            == f"_token=new_token; HttpOnly; Path=/; SameSite=lax, _access_token={access_token}; HttpOnly; Path=/; SameSite=lax, _refresh_token=new_refresh_token; HttpOnly; Path=/; SameSite=lax"
        )

    def test_invalid_token_does_not_clear_token_set_by_route(self, mock_auth_manager):
        mock_auth_manager.get_user_from_token.side_effect = HTTPException(
            status_code=403, detail="Invalid JWT token"
        )
        app, _ = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "stale_token"}).get("/auth/login_callback")

        assert response.headers.get_list("set-cookie") == _default_cookies

    def test_refreshed_token_does_not_override_logout(self, mock_generate_jwt, mock_auth_manager):
        access_token = keycloak_token("user_id")
        refreshed_user = KeycloakAuthManagerUser(
            user_id="user_id", name="name", access_token=access_token, refresh_token="new_refresh_token"
        )
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_generate_jwt.return_value = "new_token"
        app, _ = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/auth/logout")

        set_cookie_headers = response.headers.get_list("set-cookie")
        assert len(set_cookie_headers) == 3
        print(f"Headers: {set_cookie_headers}")
        assert "new_token" not in set_cookie_headers[0]
        assert access_token not in set_cookie_headers[1]
        assert "new_refresh_token" not in set_cookie_headers[2]

    @patch(f"{MODULE}.get_cookie_path", autospec=True)
    def test_root_cookie_cleared_when_route_sets_token(self, mock_cookie_path, mock_auth_manager):
        mock_cookie_path.return_value = "/team-a/"
        mock_auth_manager.get_user_from_token.side_effect = HTTPException(
            status_code=403, detail="Invalid JWT token"
        )
        app, _ = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "stale_token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/auth/login_callback")

        set_cookie_headers = response.headers.get_list("set-cookie")
        assert len(set_cookie_headers) == 4
        assert any("Path=/team-a/" in h and "_token=issued_token" in h for h in set_cookie_headers)
        assert any(
            "Path=/team-a/" in h and "_access_token=issued_access_token" in h for h in set_cookie_headers
        )
        assert any(
            "Path=/team-a/" in h and "_refresh_token=issued_refresh_token" in h for h in set_cookie_headers
        )
        assert any(
            "Path=/" in h and "Path=/team-a/" not in h and "Max-Age=0" in h for h in set_cookie_headers
        )

    def test_refresh_user_runs_off_the_event_loop(self, mock_auth_manager):
        """``refresh_user`` is synchronous and may do blocking I/O, so it must not block the loop."""
        refresh_threads = []
        mock_auth_manager.refresh_user.side_effect = lambda user: refresh_threads.append(
            threading.get_ident()
        )
        loop_threads = []
        if AIRFLOW_V_3_5_PLUS:
            get_user = lambda token: loop_threads.append(threading.get_ident())
        else:
            get_user = lambda token, access_token, refresh_token: loop_threads.append(threading.get_ident())
        mock_auth_manager.get_user_from_token.side_effect = get_user
        app, _ = _build_app()

        TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        assert len(refresh_threads) == 1
        assert refresh_threads != loop_threads

    def test_streaming_response_passthrough(self, mock_generate_jwt, mock_auth_manager):
        mock_auth_manager.refresh_user.return_value = KeycloakAuthManagerUser(
            user_id="user_id",
            name="name",
            access_token=keycloak_token("user_id"),
            refresh_token="refresh_token",
        )
        mock_generate_jwt.return_value = "new_token"
        app, _ = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: keycloak_token("user_id"),
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/stream")

        assert response.content == b"chunk1-chunk2-chunk3"
        assert response.headers["content-type"].startswith("text/plain")
        assert "_token=new_token" in response.headers["set-cookie"]

    @patch(f"{MODULE}.get_auth_manager")
    def test_http_exception_from_inner_app_returned_as_response(self, mock_get_auth_manager):
        mock_get_auth_manager.return_value = KeycloakAuthManager()

        async def raising_app(scope, receive, send):
            raise HTTPException(status_code=418, detail="teapot")

        app = KeycloakJWTMiddleware(raising_app)

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        assert response.status_code == 418
        assert response.json() == {"detail": "teapot"}

    @pytest.mark.parametrize("root_path", ["", "/team-a"])
    @pytest.mark.parametrize("path", ["/api/v2/monitor/health", "/api/v2/version", "/static/main.js"])
    def test_skipped_paths(self, mock_auth_manager, tmp_path, path, root_path):
        """Static assets and unauthenticated endpoints never decode the JWT or hit the DB."""
        (tmp_path / "main.js").write_text("console.log('hi');")
        app, _ = _build_app(static_dir=tmp_path, root_path=root_path)
        client = TestClient(
            app,
            root_path=root_path,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        )

        response = client.get(f"{root_path}{path}")

        assert response.status_code == 200
        assert "set-cookie" not in response.headers
        mock_auth_manager.get_user_from_token.assert_not_called()
        mock_auth_manager.refresh_user.assert_not_called()

    @pytest.mark.parametrize(
        ("headers", "expected_status", "expected_body"),
        [
            pytest.param({}, 200, b"0123456789", id="full"),
            pytest.param({"range": "bytes=2-4"}, 206, b"234", id="range"),
        ],
    )
    def test_static_files_served_unchanged(self, tmp_path, headers, expected_status, expected_body):
        (tmp_path / "main.js").write_bytes(b"0123456789")
        app, _ = _build_app(static_dir=tmp_path)
        client = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        )

        response = client.get("/static/main.js", headers=headers)

        assert response.status_code == expected_status
        assert response.content == expected_body
        assert "etag" in response.headers
        assert "last-modified" in response.headers
        revalidated = client.get("/static/main.js", headers={"if-none-match": response.headers["etag"]})
        assert revalidated.status_code == 304

    @pytest.mark.parametrize(
        ("scheme", "ssl_cert", "expected_secure"),
        [
            pytest.param("https", "", True, id="https-no-local-ssl-cert"),
            pytest.param("http", "/etc/ssl/cert.pem", True, id="http-with-local-ssl-cert"),
            pytest.param("https", "/etc/ssl/cert.pem", True, id="https-with-local-ssl-cert"),
            pytest.param("http", "", False, id="http-no-local-ssl-cert"),
        ],
    )
    def test_cookie_secure_flag(
        self, mock_generate_jwt, mock_auth_manager, scheme, ssl_cert, expected_secure
    ):
        """The cookie Secure flag must follow the request scheme as well as the local ssl_cert."""
        mock_auth_manager.refresh_user.return_value = KeycloakAuthManagerUser(
            user_id="user_id",
            name="name",
            access_token=keycloak_token("user_id"),
            refresh_token="new_refresh_token",
        )
        mock_generate_jwt.return_value = "new_token"
        with conf_vars({("api", "ssl_cert"): ssl_cert}):
            app, _ = _build_app()
            client = TestClient(
                app,
                base_url=f"{scheme}://testserver",
                cookies={
                    COOKIE_NAME_JWT_TOKEN: "valid_token",
                    COOKIE_NAME_ACCESS_TOKEN: "access_token",
                    COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
                },
            )

            response = client.get("/api/v2/dags")

        assert ("Secure" in response.headers["set-cookie"]) is expected_secure

    @pytest.mark.skipif(not AIRFLOW_V_3_1_7_PLUS, reason="Relies on AuthManagerRefreshTokenExpiredException")
    @pytest.mark.asyncio
    async def test_dispatch_expired_keycloak_token(self, mock_auth_manager):
        mock_user = _build_user(
            user_id="user_id", name="name", access_token="expired_token", refresh_token="expired_token"
        )
        mock_auth_manager.get_user_from_token.return_value = mock_user
        mock_auth_manager._token_subject = Mock(return_value="user_id")
        mock_auth_manager.refresh_user.side_effect = AuthManagerRefreshTokenExpiredException()

        app, _ = _build_app()
        client = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: "expired_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        )

        response = client.get("/api/v2/dags")
        if AIRFLOW_V_3_5_PLUS:
            mock_auth_manager.get_user_from_token.assert_called_once_with("valid_token")
        else:
            mock_auth_manager.get_user_from_token.assert_called_once_with(
                "valid_token", "expired_token", "refresh_token"
            )
        mock_auth_manager.refresh_user.assert_called_once_with(user=mock_user)

        if AIRFLOW_V_3_1_7_PLUS:
            assert '_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax' in response.headers["set-cookie"]
        mock_auth_manager.generate_jwt.assert_not_called()

    @pytest.mark.parametrize(
        "refresh_outcome",
        [
            pytest.param("refreshed", id="refreshed-token"),
            pytest.param("invalid", id="invalid-token"),
        ],
    )
    @patch(f"{MODULE}.get_cookie_path", autospec=True)
    def test_cookie_uses_subpath_and_clears_root_cookie(
        self, mock_cookie_path, mock_generate_jwt, mock_auth_manager, refresh_outcome
    ):
        """With a subpath, the cookie is scoped to it and a stale root-path cookie is cleared."""
        mock_cookie_path.return_value = "/team-a/"
        if refresh_outcome == "refreshed":
            mock_auth_manager.refresh_user.return_value = KeycloakAuthManagerUser(
                user_id="user_id",
                name="name",
                access_token=keycloak_token("user_id"),
                refresh_token="new_refresh_token",
            )
            mock_generate_jwt.return_value = "new_token"
        else:
            mock_auth_manager.get_user_from_token.side_effect = HTTPException(
                status_code=403, detail="Invalid JWT token"
            )
        app, _ = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "token",
                COOKIE_NAME_ACCESS_TOKEN: keycloak_token("user_id"),
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        set_cookie_headers = response.headers.get_list("set-cookie")
        if refresh_outcome == "refreshed":
            assert len(set_cookie_headers) == 4
        else:
            assert len(set_cookie_headers) == 2
        assert any("Path=/team-a/" in h for h in set_cookie_headers)
        assert any(
            "Path=/" in h and "Path=/team-a/" not in h and "Max-Age=0" in h for h in set_cookie_headers
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("side_effect", "new_user", "expected"),
        [
            pytest.param(
                KeycloakAuthManagerUser(
                    user_id="user_id",
                    name="name",
                    access_token=keycloak_token("user_id"),
                    refresh_token="refresh_token",
                ),
                KeycloakAuthManagerUser(
                    user_id="user_id",
                    name="new_user",
                    access_token=keycloak_token("user_id"),
                    refresh_token="new_refresh_token",
                ),
                "new_token",
                id="new_user",
            ),
            pytest.param(
                ExpiredSignatureError,
                KeycloakAuthManagerUser(
                    user_id="user_id",
                    name="new_user",
                    access_token=keycloak_token("user_id"),
                    refresh_token=None,
                ),
                "new_token",
                id="expired_token",
            ),
            pytest.param(
                InvalidTokenError,
                None,
                "",
                id="invalid_token",
            ),
            pytest.param(
                KeycloakAuthManagerUser(
                    user_id="user_id",
                    name="name",
                    access_token=keycloak_token("user_id"),
                    refresh_token="refresh_token",
                ),
                None,
                "",
                id="no_new_user",
            ),
        ],
    )
    @patch(f"{MODULE}.KeycloakJWTMiddleware._get_user_from_request")
    async def test_generate_new_user_token(
        self,
        mock_get_user_from_request,
        mock_generate_jwt,
        mock_auth_manager,
        side_effect,
        new_user,
        expected,
    ):
        mock_generate_jwt.return_value = "new_token"
        mock_auth_manager.refresh_user.return_value = new_user
        if isinstance(side_effect, Exception):
            mock_get_user_from_request.side_effect = side_effect
        else:
            mock_get_user_from_request.return_value = side_effect

        app, seen = _build_app()
        mock_request = TestClient(app).build_request(
            method="GET",
            url="/api/v2/dags",
        )
        mock_request.cookies = {
            COOKIE_NAME_JWT_TOKEN: "token",
            COOKIE_NAME_ACCESS_TOKEN: keycloak_token("user_id"),
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        }
        new_token = await KeycloakJWTMiddleware.generate_new_user_token(mock_request, new_user)
        assert new_token == expected

    def test_refresh_user_sets_keycloak_cookies(self, mock_generate_jwt, mock_auth_manager):
        access_token = keycloak_token("user_id")
        refreshed_user = MagicMock(spec=KeycloakAuthManagerUser)
        refreshed_user.user_id = "user_id"
        refreshed_user.access_token = access_token
        refreshed_user.refresh_token = "new_refresh_token"
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_generate_jwt.return_value = "new_token"
        app, seen = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        # JWTRefreshMiddleware must stamp the trust sentinel so `get_user()` accepts
        # the cached user. Without the marker, the auth dependency falls through
        # to a fresh JWT validation, defeating the refresh.
        assert seen["user"] is refreshed_user
        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )
        assert seen["authenticated_via"] is trusted_marker
        mock_auth_manager.refresh_user.assert_called_once_with(
            user=mock_auth_manager.get_user_from_token.return_value
        )
        mock_generate_jwt.assert_called_once_with(refreshed_user)
        assert (
            response.headers["set-cookie"]
            == f"_token=new_token; HttpOnly; Path=/; SameSite=lax, _access_token={access_token}; HttpOnly; Path=/; SameSite=lax, _refresh_token=new_refresh_token; HttpOnly; Path=/; SameSite=lax"
        )

    def test_refresh_user_clears_refresh_token_cookie(self, mock_generate_jwt, mock_auth_manager):
        access_token = keycloak_token("user_id")
        refreshed_user = KeycloakAuthManagerUser(
            user_id="user_id",
            name="name",
            access_token=access_token,
            refresh_token=None,
        )
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_generate_jwt.return_value = "new_token"
        app, seen = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        # JWTRefreshMiddleware must stamp the trust sentinel so `get_user()` accepts
        # the cached user. Without the marker, the auth dependency falls through
        # to a fresh JWT validation, defeating the refresh.
        assert seen["user"] is refreshed_user
        trusted_marker = getattr(
            core_api_security,
            "USER_INJECTED_BY_TRUSTED_MIDDLEWARE",
            None,
        )
        assert seen["authenticated_via"] is trusted_marker
        mock_auth_manager.refresh_user.assert_called_once_with(
            user=mock_auth_manager.get_user_from_token.return_value
        )
        mock_generate_jwt.assert_called_once_with(refreshed_user)
        assert (
            response.headers["set-cookie"]
            == f'_token=new_token; HttpOnly; Path=/; SameSite=lax, _access_token={refreshed_user.access_token}; HttpOnly; Path=/; SameSite=lax, _refresh_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax'
        )

    @patch(f"{MODULE}.KeycloakJWTMiddleware.get_extra_cookies")
    def test_invalid_token_does_not_clear_extra_cookie_set_by_route(
        self, mock_get_extra_cookies: MagicMock, mock_auth_manager
    ):
        mock_get_extra_cookies.return_value = {"_access_token": "", "_refresh_token": ""}
        mock_auth_manager.get_user_from_token.side_effect = HTTPException(
            status_code=403, detail="Invalid JWT token"
        )
        app, _ = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "stale_token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/auth/login_callback")

        assert response.headers.get_list("set-cookie") == [
            '_access_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax',
            '_refresh_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax',
            "_token=issued_token; HttpOnly; Path=/; SameSite=lax",
            "_access_token=issued_access_token; HttpOnly; Path=/; SameSite=lax",
            "_refresh_token=issued_refresh_token; HttpOnly; Path=/; SameSite=lax",
        ]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "jwt_token",
        [
            pytest.param("not-a-jwt", id="no-payload-segment"),
            pytest.param(token_with_payload("not json"), id="payload-not-json"),
            pytest.param(token_with_payload("1"), id="payload-not-an-object"),
            pytest.param(token_with_payload('{"other": "user_id"}'), id="payload-without-sub"),
            pytest.param(token_with_payload('{"sub": null}'), id="payload-with-null-sub"),
        ],
    )
    @patch(f"{MODULE}.get_auth_manager")
    async def test_refresh_user_rejects_unparsable_token(self, mock_get_auth_manager, jwt_token):
        """A token whose subject cannot be read matches no user and is refused."""
        mock_get_auth_manager.return_value = KeycloakAuthManager()

        app, _ = _build_app()
        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: jwt_token,
                COOKIE_NAME_ACCESS_TOKEN: keycloak_token("user_id"),
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")
        assert '_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax' in response.headers["set-cookie"]

    @pytest.mark.asyncio
    async def test_get_user_from_token_rejects_another_subjects_token(self, mock_auth_manager):
        """A Keycloak token naming a different subject must not attach to this session."""
        access_token = keycloak_token("someone_else")
        mock_user = _build_user("user_id", "name", access_token, "refresh_token")
        mock_auth_manager.get_user_from_token = AsyncMock(return_value=mock_user)
        # mock_auth_manager.get_user_from_token.return_value = mock_user

        app, _ = _build_app()
        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")
        assert '_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax' in response.headers["set-cookie"]
