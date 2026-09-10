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

import mimetypes
from unittest import mock

import pytest
from fastapi.routing import APIRoute
from starlette.routing import Mount

from airflow.providers.edge3.worker_api.app import create_edge_worker_api_app


@pytest.fixture
def app():
    # The React UI bundle (``plugins/www/dist``) is only produced by a JS build step and
    # is not present in a plain source checkout, so the real StaticFiles directory check
    # would fail here regardless of how create_edge_worker_api_app() itself behaves.
    with mock.patch("airflow.providers.edge3.worker_api.app.StaticFiles") as mocked_static_files:
        mocked_static_files.side_effect = lambda *args, **kwargs: mock.MagicMock()
        yield create_edge_worker_api_app()


def _api_route_paths(app) -> set[str]:
    return {route.path for route in app.routes if isinstance(route, APIRoute)}


class TestCreateEdgeWorkerApiApp:
    def test_v1_routers_are_mounted_under_the_v1_prefix(self, app):
        route_paths = _api_route_paths(app)
        assert "/v1/health" in route_paths
        assert "/v1/jobs/fetch/{worker_name}" in route_paths
        assert "/v1/logs/logfile_path/{dag_id}/{task_id}/{run_id}/{try_number}/{map_index}" in route_paths
        assert "/v1/worker/{worker_name}" in route_paths

    def test_ui_router_is_mounted_under_the_ui_prefix(self, app):
        route_paths = _api_route_paths(app)
        assert "/ui/worker" in route_paths
        assert not any(path.startswith("/v1") for path in route_paths if path == "/ui/worker")

    def test_static_and_res_directories_are_mounted(self, app):
        mounts = {route.path: route for route in app.routes if isinstance(route, Mount)}
        assert "/static" in mounts
        assert "/res" in mounts
        assert mounts["/static"].name == "react_static_plugin_files"
        assert mounts["/res"].name == "react_res_plugin_files"

    def test_cjs_mimetype_is_registered_as_javascript(self, app):
        # Serving .cjs with the wrong mimetype breaks the Edge Worker UI in the browser.
        assert mimetypes.guess_type("plugin.cjs")[0] == "application/javascript"
