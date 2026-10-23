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

import threading
from unittest.mock import MagicMock, patch

import pytest
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.testclient import TestClient

from airflow.api_fastapi.auth.managers.base_auth_manager import COOKIE_NAME_JWT_TOKEN, BaseAuthManager
from airflow.api_fastapi.auth.managers.models.base_user import BaseUser
from airflow.api_fastapi.auth.middlewares import refresh_token
from airflow.api_fastapi.auth.middlewares.refresh_token import JWTRefreshMiddleware
from airflow.api_fastapi.core_api.security import USER_INJECTED_BY_TRUSTED_MIDDLEWARE

from tests_common.test_utils.config import conf_vars

MODULE = "airflow.api_fastapi.auth.middlewares.refresh_token"


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
            COOKIE_NAME_JWT_TOKEN, "issued_token", path=refresh_token.get_cookie_path(), httponly=True
        )
        return response

    @app.get("/api/v2/auth/logout")
    def logout():
        response = PlainTextResponse("logged out")
        response.delete_cookie(COOKIE_NAME_JWT_TOKEN, path=refresh_token.get_cookie_path(), httponly=True)
        return response

    @app.get("/api/v2/stream")
    def stream():
        return StreamingResponse(iter([b"chunk1-", b"chunk2-", b"chunk3"]), media_type="text/plain")

    if static_dir is not None:
        app.mount("/static", StaticFiles(directory=static_dir), name="webapp_static_folder")

    app.add_middleware(JWTRefreshMiddleware, skip_path_prefixes=("/static/",))
    return app, seen


@pytest.fixture
def mock_auth_manager():
    with patch(f"{MODULE}.get_auth_manager", autospec=True) as mock_get_auth_manager:
        auth_manager = MagicMock(spec=BaseAuthManager)
        auth_manager.refresh_user.return_value = None
        mock_get_auth_manager.return_value = auth_manager
        yield auth_manager


@pytest.fixture
def mock_resolve_user():
    with patch(f"{MODULE}.resolve_user_from_token", autospec=True) as mock_resolve:
        mock_resolve.return_value = MagicMock(spec=BaseUser)
        yield mock_resolve


@pytest.fixture(autouse=True)
def no_ssl_cert():
    with conf_vars({("api", "ssl_cert"): ""}):
        yield


class TestJWTRefreshMiddleware:
    def test_no_token(self, mock_resolve_user, mock_auth_manager):
        app, _ = _build_app()

        response = TestClient(app).get("/api/v2/dags")

        assert response.status_code == 200
        assert "set-cookie" not in response.headers
        mock_resolve_user.assert_not_called()

    def test_invalid_token_clears_cookie(self, mock_resolve_user, mock_auth_manager):
        mock_resolve_user.side_effect = HTTPException(status_code=403, detail="Invalid JWT token")
        app, seen = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "invalid_token"}).get("/api/v2/dags")

        assert response.status_code == 200
        assert seen["user"] is None
        assert '_token=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax' in response.headers["set-cookie"]
        mock_auth_manager.refresh_user.assert_not_called()

    def test_no_refreshed_user(self, mock_resolve_user, mock_auth_manager):
        app, seen = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}).get("/api/v2/dags")

        assert "set-cookie" not in response.headers
        assert seen["user"] is mock_resolve_user.return_value
        assert seen["authenticated_via"] is USER_INJECTED_BY_TRUSTED_MIDDLEWARE
        mock_resolve_user.assert_called_once_with("valid_token")
        mock_auth_manager.generate_jwt.assert_not_called()

    def test_refreshed_user(self, mock_resolve_user, mock_auth_manager):
        refreshed_user = MagicMock(spec=BaseUser)
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_auth_manager.generate_jwt.return_value = "new_token"
        app, seen = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}).get("/api/v2/dags")

        # JWTRefreshMiddleware must stamp the trust sentinel so `get_user()` accepts
        # the cached user. Without the marker, the auth dependency falls through
        # to a fresh JWT validation, defeating the refresh.
        assert seen["user"] is refreshed_user
        assert seen["authenticated_via"] is USER_INJECTED_BY_TRUSTED_MIDDLEWARE
        mock_auth_manager.refresh_user.assert_called_once_with(user=mock_resolve_user.return_value)
        mock_auth_manager.generate_jwt.assert_called_once_with(refreshed_user)
        assert response.headers["set-cookie"] == "_token=new_token; HttpOnly; Path=/; SameSite=lax"

    def test_invalid_token_does_not_clear_token_set_by_route(self, mock_resolve_user, mock_auth_manager):
        mock_resolve_user.side_effect = HTTPException(status_code=403, detail="Invalid JWT token")
        app, _ = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "stale_token"}).get("/auth/login_callback")

        assert response.headers.get_list("set-cookie") == [
            "_token=issued_token; HttpOnly; Path=/; SameSite=lax"
        ]

    def test_refreshed_token_does_not_override_logout(self, mock_resolve_user, mock_auth_manager):
        mock_auth_manager.refresh_user.return_value = MagicMock(spec=BaseUser)
        mock_auth_manager.generate_jwt.return_value = "new_token"
        app, _ = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}).get("/api/v2/auth/logout")

        set_cookie_headers = response.headers.get_list("set-cookie")
        assert len(set_cookie_headers) == 1
        assert "Max-Age=0" in set_cookie_headers[0]
        assert "new_token" not in set_cookie_headers[0]

    @patch(f"{MODULE}.get_cookie_path", autospec=True, return_value="/team-a/")
    def test_root_cookie_cleared_when_route_sets_token(
        self, mock_cookie_path, mock_resolve_user, mock_auth_manager
    ):
        mock_resolve_user.side_effect = HTTPException(status_code=403, detail="Invalid JWT token")
        app, _ = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "stale_token"}).get("/auth/login_callback")

        set_cookie_headers = response.headers.get_list("set-cookie")
        assert len(set_cookie_headers) == 2
        assert set_cookie_headers[0] == "_token=issued_token; HttpOnly; Path=/team-a/; SameSite=lax"
        assert "Max-Age=0; Path=/;" in set_cookie_headers[1]

    def test_refresh_user_runs_off_the_event_loop(self, mock_resolve_user, mock_auth_manager):
        """``refresh_user`` is synchronous and may do blocking I/O, so it must not block the loop."""
        refresh_threads = []
        mock_auth_manager.refresh_user.side_effect = lambda user: refresh_threads.append(
            threading.get_ident()
        )
        loop_threads = []
        mock_resolve_user.side_effect = lambda token: loop_threads.append(threading.get_ident())
        app, _ = _build_app()

        TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}).get("/api/v2/dags")

        assert len(refresh_threads) == 1
        assert refresh_threads != loop_threads

    def test_streaming_response_passthrough(self, mock_resolve_user, mock_auth_manager):
        mock_auth_manager.refresh_user.return_value = MagicMock(spec=BaseUser)
        mock_auth_manager.generate_jwt.return_value = "new_token"
        app, _ = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}).get("/api/v2/stream")

        assert response.content == b"chunk1-chunk2-chunk3"
        assert response.headers["content-type"].startswith("text/plain")
        assert "_token=new_token" in response.headers["set-cookie"]

    def test_http_exception_from_inner_app_returned_as_response(self, mock_resolve_user, mock_auth_manager):
        async def raising_app(scope, receive, send):
            raise HTTPException(status_code=418, detail="teapot")

        app = JWTRefreshMiddleware(raising_app)

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}).get("/api/v2/dags")

        assert response.status_code == 418
        assert response.json() == {"detail": "teapot"}

    @pytest.mark.parametrize("root_path", ["", "/team-a"])
    @pytest.mark.parametrize("path", ["/api/v2/monitor/health", "/api/v2/version", "/static/main.js"])
    def test_skipped_paths(self, mock_resolve_user, mock_auth_manager, tmp_path, path, root_path):
        """Static assets and unauthenticated endpoints never decode the JWT or hit the DB."""
        (tmp_path / "main.js").write_text("console.log('hi');")
        app, _ = _build_app(static_dir=tmp_path, root_path=root_path)
        client = TestClient(app, root_path=root_path, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"})

        response = client.get(f"{root_path}{path}")

        assert response.status_code == 200
        assert "set-cookie" not in response.headers
        mock_resolve_user.assert_not_called()
        mock_auth_manager.refresh_user.assert_not_called()

    @pytest.mark.parametrize(
        ("headers", "expected_status", "expected_body"),
        [
            pytest.param({}, 200, b"0123456789", id="full"),
            pytest.param({"range": "bytes=2-4"}, 206, b"234", id="range"),
        ],
    )
    def test_static_files_served_unchanged(
        self, mock_resolve_user, tmp_path, headers, expected_status, expected_body
    ):
        (tmp_path / "main.js").write_bytes(b"0123456789")
        app, _ = _build_app(static_dir=tmp_path)
        client = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"})

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
        self, mock_resolve_user, mock_auth_manager, scheme, ssl_cert, expected_secure
    ):
        """The cookie Secure flag must follow the request scheme as well as the local ssl_cert."""
        mock_auth_manager.refresh_user.return_value = MagicMock(spec=BaseUser)
        mock_auth_manager.generate_jwt.return_value = "new_token"
        with conf_vars({("api", "ssl_cert"): ssl_cert}):
            app, _ = _build_app()
            client = TestClient(
                app, base_url=f"{scheme}://testserver", cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}
            )

            response = client.get("/api/v2/dags")

        assert ("Secure" in response.headers["set-cookie"]) is expected_secure

    @pytest.mark.parametrize(
        "refresh_outcome",
        [
            pytest.param("refreshed", id="refreshed-token"),
            pytest.param("invalid", id="invalid-token"),
        ],
    )
    @patch(f"{MODULE}.get_cookie_path", autospec=True, return_value="/team-a/")
    def test_cookie_uses_subpath_and_clears_root_cookie(
        self, mock_cookie_path, mock_resolve_user, mock_auth_manager, refresh_outcome
    ):
        """With a subpath, the cookie is scoped to it and a stale root-path cookie is cleared."""
        if refresh_outcome == "refreshed":
            mock_auth_manager.refresh_user.return_value = MagicMock(spec=BaseUser)
            mock_auth_manager.generate_jwt.return_value = "new_token"
        else:
            mock_resolve_user.side_effect = HTTPException(status_code=403, detail="Invalid JWT token")
        app, _ = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "token"}).get("/api/v2/dags")

        set_cookie_headers = response.headers.get_list("set-cookie")
        assert len(set_cookie_headers) == 2
        assert any("Path=/team-a/" in h for h in set_cookie_headers)
        assert any(
            "Path=/" in h and "Path=/team-a/" not in h and "Max-Age=0" in h for h in set_cookie_headers
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("new_user", [(MagicMock(),), (None,)])
    async def test_generate_new_user_token(self, mock_auth_manager, new_user):
        mock_auth_manager.generate_jwt.return_value = "new_token"
        app, seen = _build_app()
        middleware = JWTRefreshMiddleware(app)
        new_token = await middleware.generate_new_user_token(MagicMock(), new_user)
        if new_user is None:
            assert new_token == ""
        else:
            assert new_token == "new_token"

    @patch(f"{MODULE}.JWTRefreshMiddleware.get_extra_cookies")
    def test_refresh_user_with_extra_cookies(
        self, mock_get_extra_cookies: MagicMock, mock_resolve_user, mock_auth_manager
    ):
        mock_get_extra_cookies.return_value = {"foo": "bar"}
        refreshed_user = MagicMock(spec=BaseUser)
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_auth_manager.generate_jwt.return_value = "new_token"
        app, seen = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}).get("/api/v2/dags")

        # JWTRefreshMiddleware must stamp the trust sentinel so `get_user()` accepts
        # the cached user. Without the marker, the auth dependency falls through
        # to a fresh JWT validation, defeating the refresh.
        assert seen["user"] is refreshed_user
        assert seen["authenticated_via"] is USER_INJECTED_BY_TRUSTED_MIDDLEWARE
        mock_auth_manager.refresh_user.assert_called_once_with(user=mock_resolve_user.return_value)
        mock_auth_manager.generate_jwt.assert_called_once_with(refreshed_user)
        assert (
            response.headers["set-cookie"]
            == "_token=new_token; HttpOnly; Path=/; SameSite=lax, foo=bar; HttpOnly; Path=/; SameSite=lax"
        )

    @patch(f"{MODULE}.JWTRefreshMiddleware.get_extra_cookies")
    def test_refresh_user_clear_extra_cookies(
        self, mock_get_extra_cookies: MagicMock, mock_resolve_user, mock_auth_manager
    ):
        mock_get_extra_cookies.return_value = {"foo": ""}
        refreshed_user = MagicMock(spec=BaseUser)
        mock_auth_manager.refresh_user.return_value = refreshed_user
        mock_auth_manager.generate_jwt.return_value = "new_token"
        app, seen = _build_app()

        response = TestClient(app, cookies={COOKIE_NAME_JWT_TOKEN: "valid_token"}).get("/api/v2/dags")

        # JWTRefreshMiddleware must stamp the trust sentinel so `get_user()` accepts
        # the cached user. Without the marker, the auth dependency falls through
        # to a fresh JWT validation, defeating the refresh.
        assert seen["user"] is refreshed_user
        assert seen["authenticated_via"] is USER_INJECTED_BY_TRUSTED_MIDDLEWARE
        mock_auth_manager.refresh_user.assert_called_once_with(user=mock_resolve_user.return_value)
        mock_auth_manager.generate_jwt.assert_called_once_with(refreshed_user)
        assert (
            response.headers["set-cookie"]
            == '_token=new_token; HttpOnly; Path=/; SameSite=lax, foo=""; HttpOnly; Max-Age=0; Path=/; SameSite=lax'
        )
