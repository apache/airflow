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
import os
import re
import threading
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING
from unittest import mock

import pytest
import structlog.typing
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.routing import Mount
from fastapi.testclient import TestClient
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.middleware.gzip import GZipMiddleware

import airflow.api_fastapi.app as app_module
import airflow.plugins_manager as plugins_manager
from airflow import settings
from airflow.api_fastapi.auth.managers.simple.middleware import SimpleAllAdminMiddleware
from airflow.api_fastapi.common.http_access_log import HttpAccessLogMiddleware
from airflow.api_fastapi.common.http_metrics import HttpMetricsMiddleware
from airflow.exceptions import AirflowConfigException
from airflow.providers_manager import ProvidersManager, WebhookSourceInfo
from airflow.utils.session import create_session_async

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_inbound_deliveries
from tests_common.test_utils.webhooks import (
    FakeHmacSource,
    build_conn_env,
    count_deliveries,
    post_signed_delivery,
)

if TYPE_CHECKING:
    from httpx2 import Response

pytestmark = pytest.mark.db_test


@pytest.fixture
def async_db_app():
    app = FastAPI(lifespan=app_module.lifespan)
    opened = []
    closed = []

    def record_connect(connection, record):
        opened.append((connection, asyncio.get_running_loop()))

    def record_close(connection, record):
        closed.append((connection, asyncio.get_running_loop()))

    event.listen(Engine, "connect", record_connect)
    event.listen(Engine, "close", record_close)

    @app.get("/")
    async def query(fail: bool = False):
        async with create_session_async() as session:
            value = (await session.execute(text("SELECT 1"))).scalar_one()
            if fail:
                raise RuntimeError("request failed")
            return value

    try:
        yield app, opened, closed
    finally:
        event.remove(Engine, "connect", record_connect)
        event.remove(Engine, "close", record_close)


def test_async_connections_are_reused_and_disposed_on_the_client_loop(async_db_app):
    app, opened, closed = async_db_app
    configured_engine = settings.async_engine
    configured_factory = settings.AsyncSession
    for count in (1, 2):
        with TestClient(app) as client:
            assert settings.async_engine is configured_engine
            assert settings.AsyncSession is configured_factory
            assert client.get("/").json() == 1
            assert client.get("/").json() == 1
            assert len(opened) == count
            assert len(closed) == count - 1
        assert closed == opened
        assert settings.async_engine is configured_engine
        assert settings.AsyncSession is configured_factory
    configured_engine.sync_engine.dispose()
    assert opened[0][1] is not opened[1][1]


def test_async_pool_is_disposed_after_a_request_error(async_db_app):
    app, opened, closed = async_db_app
    with pytest.raises(RuntimeError, match="request failed"):
        with TestClient(app) as client:
            client.get("/?fail=true")
    assert len(opened) == 1
    assert closed == opened


@pytest.mark.parametrize("fail_at", ["startup", "shutdown"])
def test_async_pool_is_disposed_after_a_lifespan_error(async_db_app, fail_at):
    app, opened, closed = async_db_app

    @asynccontextmanager
    async def lifespan(app):
        async with create_session_async() as session:
            await session.execute(text("SELECT 1"))
        if fail_at == "startup":
            raise RuntimeError("startup failed")
        yield
        async with create_session_async() as session:
            await session.execute(text("SELECT 1"))
        raise RuntimeError("shutdown failed")

    app.mount("/child", FastAPI(lifespan=lifespan))
    with pytest.raises(RuntimeError, match=f"{fail_at} failed"):
        with TestClient(app):
            pass
    assert len(opened) == 1
    assert closed == opened


def test_main_app_lifespan(client):
    with client() as test_client:
        test_app = test_client.app

        # assert the app was created and lifespan was called
        assert test_app
        assert test_app.state.lifespan_called, "Lifespan not called on Execution API app."


@mock.patch("airflow.api_fastapi.app.init_views")
@mock.patch("airflow.api_fastapi.app.init_plugins")
@mock.patch("airflow.api_fastapi.app.create_task_execution_api_app")
def test_core_api_app(mock_create_task_exec_api, mock_init_plugins, mock_init_views):
    test_app = app_module.create_app(apps="core")

    # Assert that core-related functions were called
    mock_init_views.assert_called_once_with(test_app)
    mock_init_plugins.assert_called_once_with(test_app)

    # Assert that execution-related functions were NOT called
    mock_create_task_exec_api.assert_not_called()


@mock.patch("airflow.api_fastapi.app.init_views")
@mock.patch("airflow.api_fastapi.app.init_plugins")
@mock.patch("airflow.api_fastapi.app.create_task_execution_api_app")
def test_execution_api_app(mock_create_task_exec_api, mock_init_plugins, mock_init_views):
    app_module.create_app(apps="execution")

    # Assert that execution-related functions were called
    mock_create_task_exec_api.assert_called_once()

    # Assert that core-related functions were NOT called
    mock_init_views.assert_not_called()
    mock_init_plugins.assert_not_called()


def test_execution_api_app_lifespan(client, get_execution_app):
    with client(apps="execution") as test_client:
        execution_app = get_execution_app(test_client)
        assert execution_app, "Execution API app not found in FastAPI app."
        assert execution_app.state.lifespan_called, "Lifespan not called on Execution API app."


@mock.patch("airflow.api_fastapi.app.init_views")
@mock.patch("airflow.api_fastapi.app.init_plugins")
@mock.patch("airflow.api_fastapi.app.create_task_execution_api_app")
def test_all_apps(mock_create_task_exec_api, mock_init_plugins, mock_init_views):
    test_app = app_module.create_app(apps="all")

    # Assert that core-related functions were called
    mock_init_views.assert_called_once_with(test_app)
    mock_init_plugins.assert_called_once_with(test_app)

    # Assert that execution-related functions were also called
    mock_create_task_exec_api.assert_called_once_with(dag_bag=test_app.state.dag_bag)


@pytest.mark.parametrize(
    ("apps", "unknown_apps"),
    [
        ("webhook", ["webhook"]),
        ("core,foo", ["foo"]),
        ("core, execution", [" execution"]),
        ("core,", [""]),
    ],
)
def test_create_app_rejects_unknown_apps(apps, unknown_apps):
    with pytest.raises(
        ValueError,
        match=re.escape(
            f"Unknown API apps {unknown_apps} in --apps / AIRFLOW_API_APPS; "
            "valid values are: core, execution, webhooks, all"
        ),
    ):
        app_module.create_app(apps=apps)


@pytest.mark.parametrize("apps", ["all", "core", "execution", "webhooks"])
@conf_vars({("webhooks", "names"): "demo"})
def test_access_log_middleware_installed_outermost_for_every_apps_selection(apps):
    """Both server backends disable their own access logger, so a selection that skips this
    middleware has no access logging at all. It must also stay outermost so it times the full
    request including inner middlewares (GZip compression in particular — see #60165); the
    test default config has no CORS so index 0 is HttpAccessLogMiddleware."""
    installed = [m.cls for m in app_module.create_app(apps=apps).user_middleware]

    assert installed.count(HttpAccessLogMiddleware) == 1
    assert installed[0] is HttpAccessLogMiddleware


@pytest.mark.parametrize("apps", ["all", "core", "execution", "webhooks"])
@conf_vars({("metrics", "statsd_on"): "True", ("webhooks", "names"): "demo"})
def test_api_metrics_middleware_installed_for_every_apps_selection(apps):
    """The execution API is mounted as a sub-app and `--apps execution` never reaches
    ``init_middlewares``, so installing this middleware there would leave the surface every
    running task depends on unmetered. It must also wrap GZipMiddleware so the recorded
    duration includes compression time (see #60165)."""
    installed = [m.cls for m in app_module.create_app(apps=apps).user_middleware]

    assert installed.count(HttpMetricsMiddleware) == 1
    if GZipMiddleware in installed:
        assert installed.index(HttpMetricsMiddleware) < installed.index(GZipMiddleware)


@pytest.mark.parametrize("apps", ["all", "core", "execution", "webhooks"])
@conf_vars({("webhooks", "names"): "demo"})
def test_api_metrics_middleware_skipped_without_a_metrics_backend(apps):
    installed = [m.cls for m in app_module.create_app(apps=apps).user_middleware]

    assert HttpMetricsMiddleware not in installed


@pytest.mark.parametrize("apps", ["all", "all,webhooks"])
@conf_vars({("webhooks", "names"): "demo"})
def test_catch_all_route_last(apps):
    """
    Ensure the catch all route that returns the initial html is the last route in the fastapi app.

    If it's not, it results in any routes/apps added afterwards to not be reachable, as the catch all
    route responds instead.
    """
    test_app = app_module.create_app(apps=apps)
    assert test_app.routes[-1].path == "/{rest_of_path:path}"


@conf_vars({("webhooks", "names"): "demo"})
def test_webhooks_app_alone_serves_only_the_webhooks_mount():
    # Anyone who can reach this process can call it, so a route added at its root must be added on purpose.
    test_app = app_module.create_app(apps="webhooks")

    assert [(type(route), route.path) for route in test_app.routes] == [(Mount, "/webhooks")]
    client = TestClient(test_app)
    assert client.get("/webhooks/_health").json() == {"status": "healthy"}
    assert [client.get(path).status_code for path in ("/docs", "/redoc", "/openapi.json")] == [404] * 3


@pytest.mark.parametrize(
    ("apps", "documented"),
    [("core,webhooks", True), ("execution", True), ("execution,webhooks", False)],
)
@conf_vars({("webhooks", "names"): "demo", ("api", "enable_swagger_ui"): "True"})
def test_api_documentation_is_served_unless_webhooks_are_served_without_the_core_api(apps, documented):
    test_app = app_module.create_app(apps=apps)

    assert (test_app.docs_url, test_app.redoc_url, test_app.openapi_url) == (
        ("/docs", "/redoc", "/openapi.json") if documented else (None, None, None)
    )


@conf_vars({("webhooks", "names"): "demo"})
def test_all_apps_answer_webhook_requests_with_a_json_404():
    test_app = app_module.create_app(apps="all")

    assert "/webhooks" not in {route.path for route in test_app.routes if isinstance(route, Mount)}
    assert not any(path.startswith("/webhooks") for path in test_app.openapi()["paths"])
    client = TestClient(test_app)
    responses = [
        client.get("/webhooks"),
        client.post("/webhooks"),
        client.get("/webhooks/_health"),
        client.post("/webhooks/demo"),
    ]
    assert [(response.status_code, response.json()) for response in responses] == [
        (404, {"error": "Inbound webhooks are served only by airflow api-server --apps webhooks"})
    ] * 4


@conf_vars({("webhooks", "names"): ""})
def test_webhooks_app_without_webhook_names_fails_to_start():
    with pytest.raises(AirflowConfigException, match=re.escape("[webhooks] names lists no webhooks")):
        app_module.create_app(apps="webhooks")


def build_conflict_error(source_type: str) -> str:
    return (
        f"Webhook source type {source_type} is declared by pkg-a, pkg-b, so none of them serves it; set "
        "[webhooks] source_type_owners to the package that serves it"
    )


def build_wrong_owner_error(source_type: str, owner: str, declared_by: str) -> str:
    return (
        f"[webhooks] source_type_owners names {owner} for source type {source_type}, which that package "
        f"does not declare (declared by {declared_by}), so it is not served"
    )


@pytest.mark.parametrize(
    ("source_type_owners", "expected_errors"),
    [
        pytest.param("", [build_conflict_error("github"), build_conflict_error("jira")], id="unresolved"),
        pytest.param("jira=pkg-b", [build_conflict_error("github")], id="resolved"),
        pytest.param(
            "jira=pkg-c",
            [build_conflict_error("github"), build_wrong_owner_error("jira", "pkg-c", "pkg-a, pkg-b")],
            id="conflict-wrong-owner",
        ),
        pytest.param(
            "slack=pkg-b,linear=pkg-b",
            [
                build_conflict_error("github"),
                build_conflict_error("jira"),
                build_wrong_owner_error("slack", "pkg-b", "pkg-a"),
            ],
            id="no-conflict-wrong-owner",
        ),
    ],
)
@mock.patch.object(ProvidersManager, "webhook_sources", new_callable=mock.PropertyMock)
@mock.patch.object(ProvidersManager, "webhook_source_conflicts", new_callable=mock.PropertyMock)
def test_webhooks_app_logs_each_source_type_it_cannot_serve(
    mock_conflicts, mock_sources, source_type_owners, expected_errors, caplog
):
    declarations = [
        WebhookSourceInfo(source_type=source_type, classpath=f"{package}.Source", package_name=package)
        for source_type, packages in (
            ("github", ["pkg-a", "pkg-b"]),
            ("jira", ["pkg-a", "pkg-b"]),
            ("slack", ["pkg-a"]),
        )
        for package in packages
    ]
    mock_sources.return_value = declarations
    mock_conflicts.return_value = {
        source_type: [info for info in declarations if info.source_type == source_type]
        for source_type in ("github", "jira")
    }

    with conf_vars({("webhooks", "names"): "demo", ("webhooks", "source_type_owners"): source_type_owners}):
        app_module.create_app(apps="webhooks")

    assert [entry["event"] for entry in caplog if entry["log_level"] == "error"] == expected_errors


@conf_vars({("webhooks", "names"): "demo", ("api", "secret_key"): ""})
def test_webhooks_app_alone_runs_without_the_api_secret_key(caplog):
    test_app = app_module.create_app(apps="webhooks")

    assert not hasattr(test_app.state, "secret_key")
    assert (
        "`api/secret_key` was empty, using a generated one for now. Please set this in your config"
        not in caplog
    )


@pytest.fixture
def clean_inbound_deliveries():
    clear_db_inbound_deliveries()
    yield
    clear_db_inbound_deliveries()


@pytest.mark.parametrize(("name", "status_code"), [("fake", 202), ("unlisted", 404)])
@pytest.mark.usefixtures("clean_inbound_deliveries")
@mock.patch.dict(os.environ, build_conn_env("fake"))
# Patched rather than read through caplog: test_http_access_log monkeypatches this module-level
# logger, pinning it to the structlog processors of that moment, so caplog misses its events once
# a later test reconfigures structlog.
@mock.patch("airflow.api_fastapi.common.http_access_log.logger", spec=structlog.typing.FilteringBoundLogger)
@mock.patch.object(ProvidersManager, "get_webhook_source_class", autospec=True, return_value=FakeHmacSource)
@conf_vars({("webhooks", "names"): "fake"})
def test_webhook_requests_are_access_logged_with_an_int_status(
    mock_get_webhook_source_class, mock_access_logger, name, status_code
):
    client = TestClient(app_module.create_app(apps="webhooks"), base_url="http://testserver/webhooks")

    post_signed_delivery(client, name=name)

    mock_access_logger.info.assert_called_once()
    logged_status = mock_access_logger.info.call_args.kwargs["status_code"]
    assert (type(logged_status), logged_status) == (int, status_code)


class RefuseRequestsMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        return JSONResponse({"detail": "Refused"}, status_code=403)


class MarkResponsesMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        response = await call_next(request)
        response.headers["X-Plugin-Middleware"] = "passed"
        return response


@pytest.mark.usefixtures("clean_inbound_deliveries")
@mock.patch.dict(os.environ, build_conn_env("fake"))
class TestWebhookDeliveryThroughTheCoreMiddlewares:
    """
    Deliveries to ``--apps all,webhooks`` pass through the core API's middlewares.

    Those middlewares wrap every route, the webhooks mount included.
    """

    @pytest.fixture(autouse=True)
    def config(self):
        with conf_vars(
            {
                ("webhooks", "names"): "fake",
                (
                    "core",
                    "auth_manager",
                ): "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            }
        ):
            yield

    @pytest.fixture(autouse=True)
    def source_classes(self):
        with mock.patch.object(
            ProvidersManager, "get_webhook_source_class", autospec=True, return_value=FakeHmacSource
        ):
            yield

    def assert_delivery_is_recorded(self, app: FastAPI, session) -> Response:
        response = post_signed_delivery(TestClient(app, base_url="http://testserver/webhooks"))

        assert (response.status_code, response.json()) == (
            202,
            {"delivery_id": "delivery-1", "outcome": "no_subscribers"},
        )
        assert count_deliveries(session) == 1
        return response

    @conf_vars({("core", "simple_auth_manager_all_admins"): "True"})
    def test_all_admins_middleware_adding_a_token(self, session):
        app = app_module.create_app(apps="all,webhooks")

        assert SimpleAllAdminMiddleware in [middleware.cls for middleware in app.user_middleware]
        self.assert_delivery_is_recorded(app, session)

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_scoped_plugin(self, session):
        fastapi_apps = [
            {"name": "team_app", "app": FastAPI(), "url_prefix": "/team_a", "team_name": "team_a"}
        ]
        root_middlewares = [
            {"name": "team_middleware", "middleware": RefuseRequestsMiddleware, "team_name": "team_a"}
        ]
        with mock.patch.object(
            plugins_manager,
            "get_fastapi_plugins",
            autospec=True,
            return_value=(fastapi_apps, root_middlewares),
        ):
            app = app_module.create_app(apps="all,webhooks")

        assert "/team_a" in [getattr(route, "path", None) for route in app.routes]
        # Root middlewares of team-scoped plugins are skipped, so this one must not wrap the delivery.
        assert RefuseRequestsMiddleware not in [middleware.cls for middleware in app.user_middleware]
        self.assert_delivery_is_recorded(app, session)

    def test_root_plugin_middleware(self, session):
        root_middlewares = [{"name": "marker", "middleware": MarkResponsesMiddleware}]
        with mock.patch.object(
            plugins_manager, "get_fastapi_plugins", autospec=True, return_value=([], root_middlewares)
        ):
            app = app_module.create_app(apps="all,webhooks")

        response = self.assert_delivery_is_recorded(app, session)
        assert response.headers["X-Plugin-Middleware"] == "passed"


@pytest.mark.parametrize(
    ("invalid_prefix", "expected_message"),
    [
        ("", "'url_prefix' key is empty string for the fastapi app: test"),
        *((prefix, "attempted to use reserved url_prefix") for prefix in app_module.RESERVED_URL_PREFIXES),
    ],
)
def test_plugin_with_invalid_url_prefix(caplog, invalid_prefix, expected_message):
    fastapi_apps = [{"name": "test", "app": FastAPI(), "url_prefix": invalid_prefix}]
    app = FastAPI()
    with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
        app_module.init_plugins(app)

    assert any(expected_message in rec.message for rec in caplog.records)
    assert not any(r.path == invalid_prefix for r in app.routes)


class TestInitPluginsTeamAuthorization:
    """A team-scoped plugin's app must be mounted behind the team authorization
    middleware, since Airflow applies no authorization to plugin apps itself."""

    @staticmethod
    def _mount_for(app, url_prefix):
        return next(route for route in app.routes if getattr(route, "path", None) == url_prefix)

    @staticmethod
    def _has_team_middleware(mount, team_name):
        from airflow.api_fastapi.auth.middlewares.team_authorization import TeamAuthorizationMiddleware

        # Starlette applies a Mount's middleware by wrapping the sub-app, so the mounted
        # app *is* the middleware instance when the plugin is team-scoped.
        return isinstance(mount.app, TeamAuthorizationMiddleware) and mount.app.team_name == team_name

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_plugin_app_is_wrapped(self):
        fastapi_apps = [
            {"name": "team_a_app", "app": FastAPI(), "url_prefix": "/team_a", "team_name": "team_a"}
        ]
        app = FastAPI()
        with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
            app_module.init_plugins(app)

        assert self._has_team_middleware(self._mount_for(app, "/team_a"), "team_a")

    @conf_vars({("core", "multi_team"): "True"})
    def test_global_plugin_app_is_not_wrapped(self):
        fastapi_apps = [{"name": "global_app", "app": FastAPI(), "url_prefix": "/global", "team_name": None}]
        app = FastAPI()
        with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
            app_module.init_plugins(app)

        mount = self._mount_for(app, "/global")
        assert not self._has_team_middleware(mount, None)

    @conf_vars({("core", "multi_team"): "False"})
    def test_team_plugin_app_is_not_wrapped_when_multi_team_disabled(self):
        fastapi_apps = [
            {"name": "team_a_app", "app": FastAPI(), "url_prefix": "/team_a", "team_name": "team_a"}
        ]
        app = FastAPI()
        with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
            app_module.init_plugins(app)

        assert not self._has_team_middleware(self._mount_for(app, "/team_a"), "team_a")

    @conf_vars({("core", "multi_team"): "True"})
    def test_team_plugin_root_middleware_is_skipped(self, caplog):
        root_middlewares = [
            {"name": "team_a_middleware", "middleware": mock.MagicMock(), "team_name": "team_a"}
        ]
        app = FastAPI()
        with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=([], root_middlewares)):
            with mock.patch.object(app, "add_middleware") as mock_add_middleware:
                app_module.init_plugins(app)

        mock_add_middleware.assert_not_called()
        assert any("Skipping root middleware team_a_middleware" in rec.message for rec in caplog.records)

    @conf_vars({("core", "multi_team"): "True"})
    def test_global_root_middleware_is_still_added(self):
        middleware = mock.MagicMock()
        root_middlewares = [{"name": "global_middleware", "middleware": middleware, "team_name": None}]
        app = FastAPI()
        with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=([], root_middlewares)):
            with mock.patch.object(app, "add_middleware") as mock_add_middleware:
                app_module.init_plugins(app)

        mock_add_middleware.assert_called_once_with(middleware)


class TestGetCookiePath:
    def test_default_returns_slash(self):
        """When no base_url is configured, get_cookie_path() should return '/'."""
        with mock.patch.object(app_module, "API_ROOT_PATH", "/"):
            assert app_module.get_cookie_path() == "/"

    def test_empty_returns_slash(self):
        """When API_ROOT_PATH is empty, get_cookie_path() should return '/'."""
        with mock.patch.object(app_module, "API_ROOT_PATH", ""):
            assert app_module.get_cookie_path() == "/"

    def test_subpath(self):
        """When base_url contains a subpath, get_cookie_path() should return it."""
        with mock.patch.object(app_module, "API_ROOT_PATH", "/team-a/"):
            assert app_module.get_cookie_path() == "/team-a/"

    def test_nested_subpath(self):
        """When base_url contains a nested subpath, get_cookie_path() should return it."""
        with mock.patch.object(app_module, "API_ROOT_PATH", "/org/team-a/airflow/"):
            assert app_module.get_cookie_path() == "/org/team-a/airflow/"


def test_create_auth_manager_thread_safety():
    """Concurrent calls to create_auth_manager must return the same singleton instance."""
    call_count = 0
    singleton = None

    class FakeAuthManager:
        def __init__(self):
            nonlocal call_count, singleton
            call_count += 1
            singleton = self

    app_module.purge_cached_app()

    results = []
    barrier = threading.Barrier(10)

    def call_create_auth_manager():
        barrier.wait()
        results.append(app_module.create_auth_manager())

    with mock.patch.object(app_module, "get_auth_manager_cls", return_value=FakeAuthManager):
        threads = [threading.Thread(target=call_create_auth_manager) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert len(results) == 10
    assert all(r is singleton for r in results)
    assert call_count == 1

    app_module.purge_cached_app()


class TestInitializeApiServerStats:
    """
    Ensure that stats subsystem is properly initialized in API server.
    """

    def test_initializes_api_server_stats_with_factory(self):
        """It initializes the Stats singleton in the API server using the configured factory."""
        sentinel_factory = object()
        with (
            mock.patch("airflow._shared.observability.metrics.stats") as mock_stats,
            mock.patch(
                "airflow.observability.metrics.stats_utils.get_stats_factory",
                return_value=sentinel_factory,
            ) as mock_get_factory,
        ):
            app_module._initialize_api_server_stats()

            mock_get_factory.assert_called_once_with()
            mock_stats.initialize.assert_called_once()
            _, kwargs = mock_stats.initialize.call_args
            assert kwargs["factory"] is sentinel_factory
            assert isinstance(kwargs["export_legacy_names"], bool)

    def test_stats_failure_does_not_block_startup(self):
        """A metrics misconfiguration must not prevent the API server from starting."""
        with (
            mock.patch("airflow._shared.observability.metrics.stats") as mock_stats,
            mock.patch("airflow.observability.metrics.stats_utils.get_stats_factory"),
            mock.patch("airflow.api_fastapi.app.log") as mock_logger,
        ):
            mock_stats.initialize.side_effect = RuntimeError("boom")

            # Must not raise.
            app_module._initialize_api_server_stats()

        mock_logger.warning.assert_called_once()

    def test_stats_initialized_during_lifespan(self, client):
        """_initialize_api_server_stats must be called as part of the app lifespan, not just defined."""
        with mock.patch.object(app_module, "_initialize_api_server_stats") as mock_init:
            with client():
                mock_init.assert_called_once()
