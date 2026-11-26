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

import re
import threading
from http.cookies import SimpleCookie
from typing import TYPE_CHECKING
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, call, create_autospec, patch

import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient
from jwt import InvalidTokenError
from keycloak import KeycloakPostError

from airflow.api_fastapi import app as api_fastapi_app
from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN, BaseAuthManager
from airflow.api_fastapi.auth.tokens import JWTValidator
from airflow.api_fastapi.core_api import security as core_api_security
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLIENT_ID_KEY,
    CONF_CLIENT_SECRET_KEY,
    CONF_REALM_KEY,
    CONF_SECTION_NAME,
    CONF_SERVER_URL_KEY,
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
    AIRFLOW_V_3_4_PLUS,
)
from unit.keycloak.utils import keycloak_token, token_with_payload

if AIRFLOW_V_3_1_7_PLUS:
    from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException
else:
    AuthManagerRefreshTokenExpiredException = None  # type: ignore[assignment,misc]

if TYPE_CHECKING:
    from httpx2 import Response

MODULE = "airflow.providers.keycloak.auth_manager.middleware"

_default_cookies = [
    "_token=issued_token; HttpOnly; Path=/; SameSite=lax",
    "_access_token=issued_access_token; HttpOnly; Path=/; SameSite=lax",
    "_refresh_token=issued_refresh_token; HttpOnly; Path=/; SameSite=lax",
]


def _get_browser_cookies(response: Response) -> dict[tuple[str, str], str | None]:
    """Apply the ``Set-Cookie`` headers in order like a browser, keyed on name and path; ``None`` is cleared."""
    cookies: dict[tuple[str, str], str | None] = {}
    for header in response.headers.get_list("set-cookie"):
        for name, morsel in SimpleCookie(header).items():
            cookies[(name, morsel["path"])] = None if morsel["max-age"] == "0" else morsel.value
    return cookies


def _build_app(static_dir: str | None = None) -> tuple[FastAPI, dict]:
    """Return an app wrapped by the middleware and a dict capturing what the endpoint saw."""
    from airflow.api_fastapi.app import get_cookie_path, init_middlewares
    from airflow.api_fastapi.core_api.routes.public.auth import auth_router
    from airflow.api_fastapi.core_api.security import get_user

    seen: dict = {}
    app = FastAPI()
    cookie_path = get_cookie_path()

    @app.get("/user")
    def read_user(user=Depends(get_user)):
        return {"name": user.get_name()}

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
            COOKIE_NAME_JWT_TOKEN,
            "issued_token",
            path=cookie_path,
            httponly=True,
            samesite="lax",
        )
        response.set_cookie(
            COOKIE_NAME_ACCESS_TOKEN,
            "issued_access_token",
            path=cookie_path,
            httponly=True,
            samesite="lax",
        )
        response.set_cookie(
            COOKIE_NAME_REFRESH_TOKEN,
            "issued_refresh_token",
            path=cookie_path,
            httponly=True,
            samesite="lax",
        )
        return response

    @app.get("/api/v2/stream")
    def stream():
        return StreamingResponse(iter([b"chunk1-", b"chunk2-", b"chunk3"]), media_type="text/plain")

    if static_dir is not None:
        app.mount("/static", StaticFiles(directory=static_dir), name="webapp_static_folder")

    app.include_router(auth_router, prefix="/api/v2")
    init_middlewares(app)
    return app, seen


def _build_user(user_id: str, name: str, access_token: str, refresh_token: str):
    user = KeycloakAuthManagerUser(user_id=user_id, name=name, access_token="", refresh_token=None)
    if not KeycloakAuthManager._middleware_handles_cookies():
        user.access_token = access_token
        user.refresh_token = refresh_token
    return user


def _get_user_from_token(mock_user: KeycloakAuthManagerUser):
    def _get_user(token, *, should_return_user=False):
        return mock_user if should_return_user else None

    return _get_user


def _refresh_user(mock_user: KeycloakAuthManagerUser | None):
    def _refresh(user: KeycloakAuthManagerUser | None):
        if user and user.access_token:
            return mock_user
        return None

    return _refresh


@pytest.fixture(autouse=True)
def keycloak_config():
    with conf_vars(
        {
            ("api", "ssl_cert"): "",
            (CONF_SECTION_NAME, CONF_CLIENT_ID_KEY): "client_id",
            (CONF_SECTION_NAME, CONF_CLIENT_SECRET_KEY): "client_secret",
            (CONF_SECTION_NAME, CONF_REALM_KEY): "realm",
            (CONF_SECTION_NAME, CONF_SERVER_URL_KEY): "server_url",
        }
    ):
        yield


@pytest.fixture
def token_validator():
    token_validator = Mock(spec=JWTValidator)
    token_validator.avalidated_claims.return_value = {"user_id": "user_id", "name": "name"}
    token_validator.revoke_token.return_value = None
    return token_validator


@pytest.fixture(autouse=True)
def mock_get_auth_manager():
    """Patch get_auth_manager() to always return a KeycloakAuthManager instance."""
    auth_manager = KeycloakAuthManager()
    get_auth_manager = create_autospec(api_fastapi_app.get_auth_manager, return_value=auth_manager)
    with (
        patch("airflow.api_fastapi.app.get_auth_manager", get_auth_manager),
        patch("airflow.api_fastapi.auth.middlewares.refresh_token.get_auth_manager", get_auth_manager),
        patch("airflow.api_fastapi.core_api.security.get_auth_manager", get_auth_manager),
        patch(f"{MODULE}.get_auth_manager", get_auth_manager),
    ):
        yield get_auth_manager


@pytest.fixture
def mock_user():
    return _build_user(
        user_id="user_id",
        name="name",
        access_token=keycloak_token("user_id"),
        refresh_token="refresh_token",
    )


@pytest.fixture
def mock_auth_manager(mock_get_auth_manager, mock_user, token_validator):
    """Replaces the KeycloakAuthManager from get_auth_manager() with a mock."""

    mock_auth_manager = MagicMock(spec=KeycloakAuthManager)
    mock_auth_manager.refresh_user.return_value = None
    mock_auth_manager.get_user_from_token.side_effect = _get_user_from_token(mock_user)
    mock_auth_manager.generate_jwt.return_value = "new_token"
    mock_auth_manager._token_subject = KeycloakAuthManager._token_subject
    mock_auth_manager._token_expired = KeycloakAuthManager._token_expired
    mock_auth_manager._get_token_validator.return_value = token_validator
    mock_auth_manager.get_fastapi_middlewares.return_value = KeycloakAuthManager().get_fastapi_middlewares()
    jwt_middleware = (
        KeycloakAuthManager().get_jwt_refresh_middleware()
        if hasattr(BaseAuthManager, "get_jwt_refresh_middleware")
        else []
    )
    mock_auth_manager.get_jwt_refresh_middleware.return_value = jwt_middleware
    mock_get_auth_manager.return_value = mock_auth_manager

    return mock_auth_manager


@pytest.fixture
def mock_get_cookie_path():
    cookie_path = "/team-a/"
    with (
        patch(f"{MODULE}.get_cookie_path", return_value=cookie_path),
        patch("airflow.api_fastapi.auth.middlewares.refresh_token.get_cookie_path", return_value=cookie_path),
        patch("airflow.api_fastapi.app.get_cookie_path", return_value=cookie_path),
    ):
        yield


@pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS, reason="KeycloakJWTMiddleware relies on custom middleware interface."
)
class TestKeycloakJWTMiddleware:
    def test_no_token(self, mock_auth_manager):
        app, _ = _build_app()

        response = TestClient(app, cookies={}).get("/api/v2/dags")

        assert response.status_code == 200
        assert "set-cookie" not in response.headers
        mock_auth_manager.get_user_from_token.assert_not_called()

    @pytest.mark.parametrize(
        ("token", "access_token", "refresh_token"),
        [
            ("invalid_token", None, None),
            ("invalid_token", "invalid_token", None),
            ("invalid_token", keycloak_token("user_id"), None),
            ("invalid_token", "invalid_token", "valid_token"),
        ],
    )
    def test_invalid_token_clears_cookie(self, mock_auth_manager, token, access_token, refresh_token):
        mock_auth_manager.get_user_from_token.side_effect = HTTPException(
            status_code=403, detail="Invalid JWT token"
        )
        app, seen = _build_app()

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: token,
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: refresh_token,
            },
        ).get("/api/v2/dags")

        assert response.status_code == 200
        assert seen["user"] is None
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): None,
            (COOKIE_NAME_ACCESS_TOKEN, "/"): None,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): None,
        }
        mock_auth_manager.refresh_user.assert_not_called()

    def test_no_refreshed_user(self, mock_auth_manager, mock_user):
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
        assert seen["user"] is mock_user
        assert seen["authenticated_via"] is trusted_marker
        mock_auth_manager.get_user_from_token.assert_any_call("valid_token", should_return_user=True)
        mock_auth_manager.generate_jwt.assert_not_called()

    def test_refreshed_user(self, mock_auth_manager, mock_user):
        access_token = keycloak_token("user_id")
        refreshed_user = KeycloakAuthManagerUser(
            user_id="user_id", name="name", access_token=access_token, refresh_token="new_refresh_token"
        )
        mock_auth_manager.refresh_user.side_effect = _refresh_user(refreshed_user)
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
        mock_auth_manager.refresh_user.assert_any_call(
            user=KeycloakAuthManager._attach_keycloak_tokens(mock_user, access_token, "refresh_token")
        )
        mock_auth_manager.generate_jwt.assert_any_call(refreshed_user)
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): "new_token",
            (COOKIE_NAME_ACCESS_TOKEN, "/"): access_token,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): "new_refresh_token",
        }

    def test_invalid_token_does_not_clear_token_set_by_route(self, mock_auth_manager):
        mock_auth_manager.get_user_from_token.side_effect = HTTPException(
            status_code=403, detail="Invalid JWT token"
        )
        app, _ = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "stale_token"}).get("/auth/login_callback")

        assert response.headers.get_list("set-cookie") == [
            '_access_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax',
            '_refresh_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax',
            *_default_cookies,
        ]
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): "issued_token",
            (COOKIE_NAME_ACCESS_TOKEN, "/"): "issued_access_token",
            (COOKIE_NAME_REFRESH_TOKEN, "/"): "issued_refresh_token",
        }

    def test_refreshed_token_does_not_override_logout(self, mock_auth_manager):
        access_token = keycloak_token("user_id")
        refreshed_user = KeycloakAuthManagerUser(
            user_id="user_id",
            name="name",
            access_token=access_token,
            refresh_token="new_refresh_token",
        )
        mock_auth_manager.refresh_user.side_effect = _refresh_user(refreshed_user)
        mock_auth_manager.generate_jwt.return_value = "new_token"
        app, _ = _build_app()
        app.state.auth_manager = mock_auth_manager

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/auth/logout", follow_redirects=False)

        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): None,
            (COOKIE_NAME_ACCESS_TOKEN, "/"): None,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): None,
        }
        # Clients that don't apply Set-Cookie headers in order must not pick up a token minted during logout
        assert all(
            not any(refreshed_token in header for header in response.headers.get_list("set-cookie"))
            for refreshed_token in ["new_token", "new_refresh_token", access_token]
        )

    @pytest.mark.parametrize(
        ("refresh_token", "expected_calls"),
        [
            pytest.param("refresh_token", [call("refresh_token")], id="with_refresh_token"),
            pytest.param(None, [], id="without_refresh_token"),
        ],
    )
    @patch(f"{MODULE}.KeycloakAuthManager.get_keycloak_client", autospec=True)
    def test_logout_ends_keycloak_session(
        self, mock_get_keycloak_client, mock_auth_manager, refresh_token, expected_calls
    ):
        cookies = {COOKIE_NAME_JWT_TOKEN: "valid_token", COOKIE_NAME_ACCESS_TOKEN: keycloak_token("user_id")}
        if refresh_token:
            cookies[COOKIE_NAME_REFRESH_TOKEN] = refresh_token
        app, _ = _build_app()
        app.state.auth_manager = mock_auth_manager

        TestClient(app, cookies=cookies).get("/api/v2/auth/logout", follow_redirects=False)

        assert mock_get_keycloak_client.return_value.logout.call_args_list == expected_calls
        if expected_calls:
            assert mock_get_keycloak_client.return_value.connection.timeout == 5

    @patch(f"{MODULE}.KeycloakAuthManager.get_keycloak_client", autospec=True)
    def test_logout_completes_when_keycloak_cannot_end_the_session(
        self, mock_get_keycloak_client, mock_auth_manager
    ):
        mock_get_keycloak_client.return_value.logout.side_effect = KeycloakPostError(
            error_message="invalid_grant", response_code=400
        )
        app, _ = _build_app()
        app.state.auth_manager = mock_auth_manager

        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: keycloak_token("user_id"),
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): None,
            (COOKIE_NAME_ACCESS_TOKEN, "/"): None,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): None,
        }

    def test_root_cookie_cleared_when_route_sets_token(self, mock_get_cookie_path, mock_auth_manager):
        mock_auth_manager.get_user_from_token.side_effect = InvalidTokenError
        app, _ = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "stale_token"}).get("/auth/login_callback")

        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): None,
            (COOKIE_NAME_JWT_TOKEN, "/team-a/"): "issued_token",
            (COOKIE_NAME_ACCESS_TOKEN, "/team-a/"): "issued_access_token",
            (COOKIE_NAME_REFRESH_TOKEN, "/team-a/"): "issued_refresh_token",
        }

    @patch(f"{MODULE}.KeycloakAuthManager._attach_keycloak_tokens")
    def test_refresh_user_runs_off_the_event_loop(self, _mock_attach_tokens, mock_auth_manager):
        """``refresh_user`` is synchronous and may do blocking I/O, so it must not block the loop."""

        def get_user_from_token_mock(token, *, should_return_user=False):
            loop_threads.append(threading.get_ident())

        refresh_threads = []
        mock_auth_manager.refresh_user.side_effect = lambda user: refresh_threads.append(
            threading.get_ident()
        )
        loop_threads = []
        mock_auth_manager.get_user_from_token.side_effect = get_user_from_token_mock
        app, _ = _build_app()

        TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: "access_token",
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        # JWTRefreshMiddleware will also call refresh_user if it is not overridden.
        expected_threads = 1 if KeycloakAuthManager._middleware_handles_cookies() else 2
        assert len(refresh_threads) == expected_threads
        assert refresh_threads != loop_threads

    def test_streaming_response_passthrough(self, mock_auth_manager):
        mock_auth_manager.refresh_user.side_effect = _refresh_user(
            KeycloakAuthManagerUser(
                user_id="user_id",
                name="name",
                access_token=keycloak_token("user_id"),
                refresh_token="refresh_token",
            )
        )
        mock_auth_manager.generate_jwt.return_value = "new_token"
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
        assert "set-cookie" not in response.headers

    @pytest.mark.parametrize("root_path", ["", "/team-a"])
    @pytest.mark.parametrize("path", ["/api/v2/monitor/health", "/api/v2/version", "/static/main.js"])
    def test_skipped_paths(self, mock_auth_manager, tmp_path, path, root_path):
        """Static assets and unauthenticated endpoints never decode the JWT or hit the DB."""
        (tmp_path / "main.js").write_text("console.log('hi');")
        app, _ = _build_app(static_dir=tmp_path)
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
        # JWTRefreshMiddleware would previously not skip paths
        expected_calls = 0 if AIRFLOW_V_3_4_PLUS else 1
        assert mock_auth_manager.get_user_from_token.call_count == expected_calls
        assert mock_auth_manager.refresh_user.call_count == expected_calls

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
    def test_cookie_secure_flag(self, mock_auth_manager, scheme, ssl_cert, expected_secure):
        """The cookie Secure flag must follow the request scheme as well as the local ssl_cert."""
        mock_auth_manager.refresh_user.side_effect = _refresh_user(
            KeycloakAuthManagerUser(
                user_id="user_id",
                name="name",
                access_token=keycloak_token("user_id"),
                refresh_token="new_refresh_token",
            )
        )
        mock_auth_manager.generate_jwt.return_value = "new_token"
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

    @pytest.mark.asyncio
    async def test_dispatch_expired_keycloak_token(
        self,
        mock_auth_manager,
    ):
        access_token = keycloak_token("user_id")
        mock_user = KeycloakAuthManagerUser(
            user_id="user_id", name="name", access_token="", refresh_token=None
        )
        mock_auth_manager.get_user_from_token = AsyncMock(return_value=mock_user)
        mock_auth_manager.refresh_user.side_effect = AuthManagerRefreshTokenExpiredException

        with patch.object(KeycloakAuthManager, "_token_expired", return_value=True):
            app, _ = _build_app()

            response = TestClient(
                app,
                cookies={
                    COOKIE_NAME_JWT_TOKEN: "token",
                    COOKIE_NAME_ACCESS_TOKEN: access_token,
                    COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
                },
            ).get("/api/v2/dags")

        mock_auth_manager.get_user_from_token.assert_called_once_with("token", should_return_user=True)
        mock_auth_manager.refresh_user.assert_called_once_with(
            user=KeycloakAuthManager._attach_keycloak_tokens(mock_user, access_token, "refresh_token")
        )

        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): None,
            (COOKIE_NAME_ACCESS_TOKEN, "/"): None,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): None,
        }
        mock_auth_manager.generate_jwt.assert_not_called()

    @pytest.mark.parametrize(
        "refresh_outcome",
        [
            pytest.param("refreshed", id="refreshed-token"),
            pytest.param("invalid", id="invalid-token"),
        ],
    )
    def test_cookie_uses_subpath_and_clears_root_cookie(
        self, mock_get_cookie_path, mock_auth_manager, refresh_outcome
    ):
        """With a subpath, the cookie is scoped to it and a stale root-path cookie is cleared."""
        access_token = keycloak_token("user_id")

        if refresh_outcome == "refreshed":
            mock_auth_manager.refresh_user.side_effect = _refresh_user(
                KeycloakAuthManagerUser(
                    user_id="user_id",
                    name="name",
                    access_token=access_token,
                    refresh_token="new_refresh_token",
                )
            )
            mock_auth_manager.generate_jwt.return_value = "new_token"
        else:
            mock_auth_manager.get_user_from_token.side_effect = InvalidTokenError
        app, _ = _build_app()

        response = TestClient(
            app,
            root_path="/team-a/",
            cookies={
                COOKIE_NAME_JWT_TOKEN: "token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")

        cookies = _get_browser_cookies(response)
        if refresh_outcome == "refreshed":
            assert cookies == {
                (COOKIE_NAME_JWT_TOKEN, "/"): None,
                (COOKIE_NAME_JWT_TOKEN, "/team-a/"): "new_token",
                (COOKIE_NAME_ACCESS_TOKEN, "/team-a/"): access_token,
                (COOKIE_NAME_REFRESH_TOKEN, "/team-a/"): "new_refresh_token",
            }
        else:
            assert cookies == {
                (COOKIE_NAME_JWT_TOKEN, "/"): None,
                (COOKIE_NAME_JWT_TOKEN, "/team-a/"): None,
                (COOKIE_NAME_ACCESS_TOKEN, "/team-a/"): None,
                (COOKIE_NAME_REFRESH_TOKEN, "/team-a/"): None,
            }

    def test_refresh_user_sets_keycloak_cookies(self, mock_auth_manager, mock_user):
        access_token = keycloak_token("user_id")
        refreshed_user = MagicMock(spec=KeycloakAuthManagerUser)
        refreshed_user.user_id = "user_id"
        refreshed_user.access_token = access_token
        refreshed_user.refresh_token = "new_refresh_token"
        mock_auth_manager.refresh_user.side_effect = _refresh_user(refreshed_user)
        mock_auth_manager.generate_jwt.return_value = "new_token"
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
        mock_auth_manager.refresh_user.assert_any_call(user=mock_user)
        mock_auth_manager.generate_jwt.assert_any_call(refreshed_user)
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): "new_token",
            (COOKIE_NAME_ACCESS_TOKEN, "/"): access_token,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): "new_refresh_token",
        }

    def test_refresh_user_clears_refresh_token_cookie(self, mock_auth_manager, mock_user):
        access_token = keycloak_token("user_id")
        refreshed_user = KeycloakAuthManagerUser(
            user_id="user_id",
            name="name",
            access_token=access_token,
            refresh_token=None,
        )
        mock_auth_manager.refresh_user.side_effect = _refresh_user(refreshed_user)
        mock_auth_manager.generate_jwt.return_value = "new_token"
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
        mock_auth_manager.refresh_user.assert_any_call(
            user=KeycloakAuthManager._attach_keycloak_tokens(mock_user, access_token, "refresh_token")
        )
        assert all(c == call(refreshed_user) for c in mock_auth_manager.generate_jwt.mock_calls)
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): "new_token",
            (COOKIE_NAME_ACCESS_TOKEN, "/"): refreshed_user.access_token,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): refreshed_user.refresh_token,
        }

    def test_invalid_token_does_not_clear_extra_cookie_set_by_route(self, mock_auth_manager):
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
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): "issued_token",
            (COOKIE_NAME_ACCESS_TOKEN, "/"): "issued_access_token",
            (COOKIE_NAME_REFRESH_TOKEN, "/"): "issued_refresh_token",
        }

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
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): None,
            (COOKIE_NAME_ACCESS_TOKEN, "/"): None,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): None,
        }

    @pytest.mark.asyncio
    async def test_get_user_from_token_rejects_another_subjects_token(self, mock_auth_manager):
        """A Keycloak token naming a different subject must not attach to this session."""
        access_token = keycloak_token("someone_else")
        mock_user = _build_user("user_id", "name", access_token, "refresh_token")
        mock_auth_manager.get_user_from_token = AsyncMock(return_value=mock_user)

        app, _ = _build_app()
        response = TestClient(
            app,
            cookies={
                COOKIE_NAME_JWT_TOKEN: "valid_token",
                COOKIE_NAME_ACCESS_TOKEN: access_token,
                COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
            },
        ).get("/api/v2/dags")
        assert _get_browser_cookies(response) == {
            (COOKIE_NAME_JWT_TOKEN, "/"): None,
            (COOKIE_NAME_ACCESS_TOKEN, "/"): None,
            (COOKIE_NAME_REFRESH_TOKEN, "/"): None,
        }


ENDED_KEYCLOAK_SESSIONS = [
    pytest.param(
        {
            COOKIE_NAME_ACCESS_TOKEN: keycloak_token(subject="user_id", expires_in=-300),
            COOKIE_NAME_REFRESH_TOKEN: "refresh_token",
        },
        id="expired_refresh_token",
    ),
    pytest.param({}, id="no_keycloak_cookies"),
]


@pytest.mark.skipif(not AIRFLOW_V_3_3_PLUS, reason="The Keycloak JWTs travel in their own cookies since 3.3")
class TestKeycloakJWTMiddlewareInTheApiServer:
    """The middlewares as the API server installs them, in front of a route that needs the user."""

    @pytest.fixture
    def revoke_token(self):
        with patch.object(KeycloakAuthManager, "revoke_token", autospec=True) as revoke_token:
            yield revoke_token

    @pytest.fixture
    def client(self, token_validator, revoke_token, mock_get_auth_manager):
        from airflow.api_fastapi.auth.managers.exceptions import AuthManagerRefreshTokenExpiredException

        with conf_vars(
            {
                (CONF_SECTION_NAME, CONF_CLIENT_ID_KEY): "client_id",
                (CONF_SECTION_NAME, CONF_CLIENT_SECRET_KEY): "client_secret",
                (CONF_SECTION_NAME, CONF_REALM_KEY): "realm",
                (CONF_SECTION_NAME, CONF_SERVER_URL_KEY): "server_url",
            }
        ):
            auth_manager = KeycloakAuthManager()

        mock_get_auth_manager.return_value = auth_manager
        app, _ = _build_app()
        app.state.auth_manager = auth_manager

        with (
            patch("airflow.api_fastapi.app.get_auth_manager", mock_get_auth_manager),
            patch(
                "airflow.api_fastapi.auth.middlewares.refresh_token.get_auth_manager", mock_get_auth_manager
            ),
            patch("airflow.api_fastapi.core_api.security.get_auth_manager", mock_get_auth_manager),
            patch(
                "airflow.providers.keycloak.auth_manager.middleware.get_auth_manager", mock_get_auth_manager
            ),
            patch.object(KeycloakAuthManager, "_get_token_validator", return_value=token_validator),
            patch.object(KeycloakAuthManager, "get_keycloak_client", autospec=True),
            patch.object(
                KeycloakAuthManager,
                "refresh_tokens",
                autospec=True,
                side_effect=AuthManagerRefreshTokenExpiredException,
            ),
        ):
            yield TestClient(app)

    @staticmethod
    def cleared_jwt_cookies(response) -> list[str]:
        return [
            cookie
            for cookie in response.headers.get_list("set-cookie")
            if (
                cookie.startswith(f"{COOKIE_NAME_JWT_TOKEN}=")
                or cookie.startswith(f"{COOKIE_NAME_ACCESS_TOKEN}=")
                or cookie.startswith(f"{COOKIE_NAME_REFRESH_TOKEN}=")
            )
            and "Max-Age=0" in cookie
        ]

    def test_session_with_keycloak_tokens_keeps_its_cookie(self, client, revoke_token):
        client.cookies.set(COOKIE_NAME_JWT_TOKEN, "token")
        client.cookies.set(COOKIE_NAME_ACCESS_TOKEN, keycloak_token(subject="user_id", expires_in=300))
        client.cookies.set(COOKIE_NAME_REFRESH_TOKEN, "refresh_token")

        response = client.get("/user")

        assert response.status_code == 200
        assert response.json() == {"name": "name"}
        assert self.cleared_jwt_cookies(response) == []
        revoke_token.assert_not_called()

    @pytest.mark.parametrize("keycloak_cookies", ENDED_KEYCLOAK_SESSIONS)
    def test_ended_keycloak_session_answers_401_and_clears_the_cookie(
        self, client, revoke_token, keycloak_cookies
    ):
        client.cookies.set(COOKIE_NAME_JWT_TOKEN, "token")
        for name, value in keycloak_cookies.items():
            client.cookies.set(name, value)

        response = client.get("/user")

        assert response.status_code == 401
        assert all(
            any(
                re.search(rf'{cookie_name}=""; (expires=.*; )?HttpOnly; Max-Age=0; Path=/; SameSite=lax', c)
                for c in self.cleared_jwt_cookies(response)
            )
            for cookie_name in [
                COOKIE_NAME_JWT_TOKEN,
                COOKIE_NAME_ACCESS_TOKEN,
                COOKIE_NAME_REFRESH_TOKEN,
            ]
        )
        revoke_token.assert_not_called()

    @pytest.mark.parametrize("keycloak_cookies", ENDED_KEYCLOAK_SESSIONS)
    def test_logout_of_an_ended_keycloak_session_revokes_the_token(
        self, client, revoke_token, keycloak_cookies
    ):
        client.cookies.set(COOKIE_NAME_JWT_TOKEN, "token")
        for name, value in keycloak_cookies.items():
            client.cookies.set(name, value)

        response = client.get("/api/v2/auth/logout", follow_redirects=False)

        assert response.status_code == 307
        assert all(
            any(
                re.search(rf'{cookie_name}=""; (expires=.*; )?HttpOnly; Max-Age=0; Path=/; SameSite=lax', c)
                for c in self.cleared_jwt_cookies(response)
            )
            for cookie_name in [
                COOKIE_NAME_JWT_TOKEN,
                COOKIE_NAME_ACCESS_TOKEN,
                COOKIE_NAME_REFRESH_TOKEN,
            ]
        )
        revoke_token.assert_called_once_with(ANY, "token")

    def test_rejected_jwt_answers_401_and_clears_the_cookie_without_revoking(
        self, client, token_validator, revoke_token
    ):
        token_validator.avalidated_claims.side_effect = InvalidTokenError("Token has been revoked")
        client.cookies.set(COOKIE_NAME_JWT_TOKEN, "token")
        client.cookies.set(COOKIE_NAME_ACCESS_TOKEN, keycloak_token(subject="user_id", expires_in=300))
        client.cookies.set(COOKIE_NAME_REFRESH_TOKEN, "refresh_token")

        response = client.get("/user")

        assert response.status_code == 401
        assert all(
            any(
                re.search(rf'{cookie_name}=""; (expires=.*; )?HttpOnly; Max-Age=0; Path=/; SameSite=lax', c)
                for c in self.cleared_jwt_cookies(response)
            )
            for cookie_name in [
                COOKIE_NAME_JWT_TOKEN,
                COOKIE_NAME_ACCESS_TOKEN,
                COOKIE_NAME_REFRESH_TOKEN,
            ]
        )
        revoke_token.assert_not_called()
