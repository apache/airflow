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
from flask import Flask, url_for

from airflow.providers.fab.www.extensions.init_wsgi_middlewares import (
    get_auth_mount_path,
    init_wsgi_middleware,
)

from tests_common.test_utils.config import conf_vars


def build_client(enable_proxy_fix: bool):
    """Return a test client whose only view reports the URL ``url_for`` builds for it."""
    app = Flask(__name__)

    @app.route("/roles/add")
    def add_role():
        return url_for("add_role")

    with conf_vars({("fab", "enable_proxy_fix"): str(enable_proxy_fix)}):
        init_wsgi_middleware(app)
    return app.test_client()


def test_get_auth_mount_path():
    assert get_auth_mount_path() == "/auth"


@pytest.mark.parametrize(
    ("script_name", "forwarded_prefix", "expected"),
    [
        pytest.param("/auth", None, "/auth/roles/add", id="mounted-no-proxy"),
        pytest.param(
            "/auth", "/myns/myrelease", "/myns/myrelease/auth/roles/add", id="mounted-subpath-proxy"
        ),
        pytest.param(
            "/myns/myrelease/auth",
            "/myns/myrelease",
            "/myns/myrelease/auth/roles/add",
            id="base-url-path-and-subpath-proxy",
        ),
        pytest.param(
            "/myns/myrelease/auth", None, "/myns/myrelease/auth/roles/add", id="base-url-path-no-proxy"
        ),
        pytest.param("/auth", "/auth", "/auth/auth/roles/add", id="proxy-prefix-equals-mount-path"),
        pytest.param("/auth", "/myns/", "/myns/auth/roles/add", id="proxy-prefix-trailing-slash"),
        pytest.param("", "/myns", "/myns/roles/add", id="not-mounted-subpath-proxy"),
        pytest.param("", None, "/roles/add", id="not-mounted-no-proxy"),
    ],
)
def test_proxy_fix_keeps_auth_mount_path(script_name, forwarded_prefix, expected):
    client = build_client(enable_proxy_fix=True)
    headers = {"X-Forwarded-Prefix": forwarded_prefix} if forwarded_prefix else {}

    # The test client derives SCRIPT_NAME from the path of base_url.
    response = client.get("/roles/add", base_url=f"http://localhost{script_name}", headers=headers)

    assert response.get_data(as_text=True) == expected


def test_proxy_fix_disabled_ignores_forwarded_prefix():
    client = build_client(enable_proxy_fix=False)

    response = client.get(
        "/roles/add",
        base_url="http://localhost/auth",
        headers={"X-Forwarded-Prefix": "/myns/myrelease"},
    )

    assert response.get_data(as_text=True) == "/auth/roles/add"
