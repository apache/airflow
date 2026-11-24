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
"""Public access in an API server started as ``airflow api-server`` starts it, with no Flask app context."""

from __future__ import annotations

from contextlib import contextmanager
from typing import TYPE_CHECKING, cast

import pytest
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app, create_auth_manager

from tests_common.test_utils.config import conf_vars

if TYPE_CHECKING:
    from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

pytestmark = pytest.mark.db_test


@contextmanager
def run_api_server(tmp_path, *, webserver_config: str, auth_role_public: str = ""):
    config_file = tmp_path / "webserver_config.py"
    config_file.write_text(webserver_config)
    with conf_vars(
        {
            ("core", "auth_manager"): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
            ("fab", "config_file"): str(config_file),
            ("fab", "auth_role_public"): auth_role_public,
        }
    ):
        auth_manager = cast("FabAuthManager", create_auth_manager())
        # The auth manager is shared across tests, so do not leave this server's Flask app on it.
        previous_flask_app = auth_manager.flask_app
        try:
            yield auth_manager, TestClient(create_app(apps="core"), raise_server_exceptions=False)
        finally:
            auth_manager.flask_app = previous_flask_app


def test_legacy_auth_role_public_grants_its_role_to_anonymous_requests(tmp_path):
    with run_api_server(tmp_path, webserver_config="AUTH_ROLE_PUBLIC = 'Viewer'\n") as (_, client):
        assert client.get("/api/v2/pools").status_code == 200
        assert client.delete("/api/v2/pools/default_pool").status_code == 403
