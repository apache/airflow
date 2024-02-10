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

import pytest
import werkzeug.test
import werkzeug.wrappers

from airflow.www.app import create_app
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


@pytest.fixture(scope="module")
def app():
    @conf_vars({("webserver", "base_url"): "http://localhost/test"})
    def factory():
        return create_app(testing=True)

    app = factory()
    app.config["WTF_CSRF_ENABLED"] = False
    return app


@pytest.fixture()
def client(app):
    return werkzeug.test.Client(app, werkzeug.wrappers.response.Response)


def test_mount(client):
    # Test an endpoint that doesn't need auth!
    resp = client.get("/test/health")
    assert resp.status_code == 200
    assert b"healthy" in resp.data


def test_not_found(client):
    resp = client.get("/", follow_redirects=True)
    assert resp.status_code == 404


def test_index(client):
    resp = client.get("/test/")
    assert resp.status_code == 302
    assert resp.headers["Location"] == "/test/home"
