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
    with mock.patch.object(plugins_manager, "fastapi_apps", fastapi_apps):
        app_module.init_plugins(app)

    assert any(expected_message in rec.message for rec in caplog.records)
    assert not any(r.path == invalid_path for r in app.routes)
