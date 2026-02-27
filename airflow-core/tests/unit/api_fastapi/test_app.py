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
from fastapi import FastAPI

import airflow.api_fastapi.app as app_module
import airflow.plugins_manager as plugins_manager

pytestmark = pytest.mark.db_test


def test_main_app_lifespan(client):
    with client() as test_client:
        test_app = test_client.app

        # assert the app was created and lifespan was called
        assert test_app
        assert test_app.state.lifespan_called, "Lifespan not called on Execution API app."


@mock.patch("airflow.api_fastapi.app.init_views")
@mock.patch("airflow.api_fastapi.app.init_plugins")
@mock.patch("airflow.api_fastapi.app.create_task_execution_api_app")
def test_core_api_app(mock_create_task_exec_api, mock_init_plugins, mock_init_views, client):
    test_app = client(apps="core").app

    # Assert that core-related functions were called
    mock_init_views.assert_called_once_with(test_app)
    mock_init_plugins.assert_called_once_with(test_app)

    # Assert that execution-related functions were NOT called
    mock_create_task_exec_api.assert_not_called()


@mock.patch("airflow.api_fastapi.app.init_views")
@mock.patch("airflow.api_fastapi.app.init_plugins")
@mock.patch("airflow.api_fastapi.app.create_task_execution_api_app")
def test_execution_api_app(mock_create_task_exec_api, mock_init_plugins, mock_init_views, client):
    client(apps="execution")

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
def test_all_apps(mock_create_task_exec_api, mock_init_plugins, mock_init_views, client):
    test_app = client(apps="all").app

    # Assert that core-related functions were called
    mock_init_views.assert_called_once_with(test_app)
    mock_init_plugins.assert_called_once_with(test_app)

    # Assert that execution-related functions were also called
    mock_create_task_exec_api.assert_called_once_with()


def test_catch_all_route_last(client):
    """
    Ensure the catch all route that returns the initial html is the last route in the fastapi app.

    If it's not, it results in any routes/apps added afterwards to not be reachable, as the catch all
    route responds instead.
    """
    test_app = client(apps="all").app
    assert test_app.routes[-1].path == "/{rest_of_path:path}"


@pytest.mark.parametrize(
    ("fastapi_apps", "expected_message", "invalid_path"),
    [
        (
            [{"name": "test", "app": FastAPI(), "url_prefix": ""}],
            "'url_prefix' key is empty string for the fastapi app: test",
            "",
        ),
        (
            [{"name": "test", "app": FastAPI(), "url_prefix": next(iter(app_module.RESERVED_URL_PREFIXES))}],
            "attempted to use reserved url_prefix",
            next(iter(app_module.RESERVED_URL_PREFIXES)),
        ),
    ],
)
def test_plugin_with_invalid_url_prefix(caplog, fastapi_apps, expected_message, invalid_path):
    app = FastAPI()
    with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
        app_module.init_plugins(app)

    assert any(expected_message in rec.message for rec in caplog.records)
    assert not any(r.path == invalid_path for r in app.routes)


def test_plugin_with_missing_app_key(caplog):
    """Plugin dict without 'app' key should log error and not mount."""
    fastapi_apps = [{"name": "bad_plugin", "url_prefix": "/test"}]
    app = FastAPI()
    with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
        app_module.init_plugins(app)
    assert any("'app' key is missing" in rec.message for rec in caplog.records)


def test_plugin_with_missing_url_prefix_key(caplog):
    """Plugin dict without 'url_prefix' key should log error and not mount."""
    fastapi_apps = [{"name": "bad_plugin", "app": FastAPI()}]
    app = FastAPI()
    with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
        app_module.init_plugins(app)
    assert any("'url_prefix' key is missing" in rec.message for rec in caplog.records)


def test_valid_plugin_is_mounted():
    """A well-formed plugin should be mounted at its url_prefix."""
    sub = FastAPI()
    fastapi_apps = [{"name": "good_plugin", "app": sub, "url_prefix": "/myplugin"}]
    app = FastAPI()
    with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
        app_module.init_plugins(app)
    paths = [r.path for r in app.routes]
    assert "/myplugin" in paths


@pytest.mark.parametrize("prefix", app_module.RESERVED_URL_PREFIXES)
def test_all_reserved_prefixes_rejected(caplog, prefix):
    """Each reserved prefix should be rejected when used by a plugin."""
    fastapi_apps = [{"name": "test", "app": FastAPI(), "url_prefix": prefix}]
    app = FastAPI()
    with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=(fastapi_apps, [])):
        app_module.init_plugins(app)
    assert any("reserved url_prefix" in rec.message for rec in caplog.records)
    assert not any(r.path == prefix for r in app.routes)


def test_middleware_plugin_with_missing_middleware_key(caplog):
    """Middleware dict without 'middleware' key should log error."""
    middlewares = [{"name": "bad_mw"}]
    app = FastAPI()
    with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=([], middlewares)):
        app_module.init_plugins(app)
    assert any("'middleware' key is missing" in rec.message for rec in caplog.records)


def test_middleware_plugin_non_callable(caplog):
    """Middleware value that is not callable should log error."""
    middlewares = [{"name": "bad_mw", "middleware": "not_callable"}]
    app = FastAPI()
    with mock.patch.object(plugins_manager, "get_fastapi_plugins", return_value=([], middlewares)):
        app_module.init_plugins(app)
    assert any("should be callable" in rec.message for rec in caplog.records)


def test_catch_all_returns_html(client):
    """The SPA catch-all should return HTML for non-API paths."""
    with client(apps="core") as test_client:
        resp = test_client.get("/some/random/ui/path")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")


def test_deprecated_api_v1_returns_404(client):
    """/api/v1/ paths should return 404 with removal message."""
    with client(apps="core") as test_client:
        resp = test_client.get("/api/v1/dags")
        assert resp.status_code == 404
        body = resp.json()
        assert "/api/v1 has been removed" in body["error"]


def test_old_health_endpoint_returns_404(client):
    """/health should return 404 with migration message."""
    with client(apps="core") as test_client:
        resp = test_client.get("/health")
        assert resp.status_code == 404
        body = resp.json()
        assert "/api/v2/monitor/health" in body["error"]


def test_invalid_api_path_returns_404(client):
    """/api/nonexistent should return 404."""
    with client(apps="core") as test_client:
        resp = test_client.get("/api/nonexistent")
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "API route not found"
