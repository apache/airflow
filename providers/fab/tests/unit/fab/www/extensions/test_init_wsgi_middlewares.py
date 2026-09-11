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
from werkzeug.test import Client

from airflow.providers.fab.www.extensions.init_wsgi_middlewares import init_wsgi_middleware

from tests_common.test_utils.config import conf_vars

# The FastAPI app serves the Flask app under /auth and passes that mount path down as SCRIPT_NAME.
MOUNT_ENVIRON = {"SCRIPT_NAME": "/auth"}
SUBPATH_HEADERS = {"X-Forwarded-Prefix": "/myns/myrelease"}


def build_client() -> Client:
    app = Flask(__name__)

    @app.route("/roles/list/")
    def roles_list():
        return url_for("roles_add")

    @app.route("/roles/add")
    def roles_add():
        return "add"

    init_wsgi_middleware(app)
    return Client(app)


@conf_vars({("fab", "enable_proxy_fix"): "True"})
@pytest.mark.parametrize(
    ("environ", "headers", "expected_url"),
    [
        pytest.param(MOUNT_ENVIRON, {}, "/auth/roles/add", id="mounted-without-forwarded-prefix"),
        pytest.param(
            MOUNT_ENVIRON,
            SUBPATH_HEADERS,
            "/myns/myrelease/auth/roles/add",
            id="mounted-behind-subpath-proxy",
        ),
        pytest.param({}, SUBPATH_HEADERS, "/myns/myrelease/roles/add", id="unmounted-behind-subpath-proxy"),
        pytest.param({}, {}, "/roles/add", id="unmounted-without-forwarded-prefix"),
        pytest.param(
            MOUNT_ENVIRON,
            {"X-Forwarded-Prefix": "/auth"},
            "/auth/auth/roles/add",
            id="subpath-proxy-prefix-matching-mount-path",
        ),
    ],
)
def test_generated_urls_keep_mount_path_and_forwarded_prefix(environ, headers, expected_url):
    response = build_client().get("/roles/list/", environ_overrides=environ, headers=headers)

    assert response.text == expected_url


@conf_vars({("fab", "enable_proxy_fix"): "False"})
def test_forwarded_prefix_ignored_when_proxy_fix_disabled():
    response = build_client().get("/roles/list/", environ_overrides=MOUNT_ENVIRON, headers=SUBPATH_HEADERS)

    assert response.text == "/auth/roles/add"
