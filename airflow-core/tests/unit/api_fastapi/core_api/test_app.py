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
from fastapi.testclient import TestClient

from airflow.api_fastapi.core_api.app import init_views

from tests_common.test_utils.db import clear_db_jobs

pytestmark = pytest.mark.db_test


class TestGzipMiddleware:
    @pytest.fixture(autouse=True)
    def setup(self):
        clear_db_jobs()
        yield
        clear_db_jobs()

    def test_gzip_middleware_should_not_be_chunked(self, test_client) -> None:
        response = test_client.get("/api/v2/monitor/health")
        headers = {k.lower(): v for k, v in response.headers.items()}

        # Ensure we do not reintroduce Transfer-Encoding: chunked
        assert "transfer-encoding" not in headers


class TestInitViews:
    """Tests for core_api init_views: static files, SPA catch-all, and deprecated routes."""

    @pytest.fixture
    def views_app(self):
        """Create a minimal FastAPI app with init_views applied."""
        app = FastAPI()
        init_views(app)
        return TestClient(app)

    def test_catch_all_serves_html(self, views_app):
        """Any non-API path should serve the SPA index.html with text/html content type."""
        resp = views_app.get("/dashboard")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_catch_all_nested_path(self, views_app):
        """Deeply nested paths should still hit the catch-all and return HTML."""
        resp = views_app.get("/dags/my_dag/grid")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_catch_all_root_path(self, views_app):
        """The root path / should serve the SPA."""
        resp = views_app.get("/")
        assert resp.status_code == 200
        assert "text/html" in resp.headers.get("content-type", "")

    def test_old_health_returns_404(self, views_app):
        """/health should return 404 with pointer to new endpoint."""
        resp = views_app.get("/health")
        assert resp.status_code == 404
        body = resp.json()
        assert "/api/v2/monitor/health" in body["error"]

    def test_old_api_v1_returns_404(self, views_app):
        """/api/v1/* should return 404 with removal notice."""
        resp = views_app.get("/api/v1/dags")
        assert resp.status_code == 404
        body = resp.json()
        assert "/api/v1 has been removed" in body["error"]

    def test_old_api_v1_nested_path(self, views_app):
        """/api/v1/dags/my_dag should still return the v1 removal notice."""
        resp = views_app.get("/api/v1/dags/my_dag/tasks")
        assert resp.status_code == 404
        body = resp.json()
        assert "/api/v2" in body["error"]

    def test_api_catch_all_returns_404(self, views_app):
        """/api/<anything> should return 'API route not found'."""
        resp = views_app.get("/api/v99/unknown")
        assert resp.status_code == 404
        body = resp.json()
        assert body["error"] == "API route not found"

    def test_static_mount_exists(self, views_app):
        """A /static mount should be present in the app routes."""
        paths = [r.path for r in views_app.app.routes]
        assert "/static" in paths

    def test_route_order_catch_all_is_last(self, views_app):
        """The SPA catch-all route must be the last route to avoid shadowing others."""
        last_route = views_app.app.routes[-1]
        assert last_route.path == "/{rest_of_path:path}"

    def test_dev_mode_creates_i18n_mount(self):
        """In DEV_MODE, an extra /static/i18n/locales mount should be added."""
        with mock.patch.dict("os.environ", {"DEV_MODE": "true"}):
            app = FastAPI()
            init_views(app)
            mount_names = [getattr(r, "name", None) for r in app.routes]
            assert "dev_i18n_static" in mount_names

    def test_non_dev_mode_no_i18n_mount(self):
        """Without DEV_MODE, the dev i18n mount should not be present."""
        with mock.patch.dict("os.environ", {"DEV_MODE": "false"}):
            app = FastAPI()
            init_views(app)
            mount_names = [getattr(r, "name", None) for r in app.routes]
            assert "dev_i18n_static" not in mount_names
