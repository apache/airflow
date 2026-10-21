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
"""Deliveries to ``--apps all,webhooks`` pass through the root middlewares, FAB's included."""

from __future__ import annotations

import os
from unittest import mock

import pytest

pytest.importorskip("airflow.api_fastapi.webhooks")

from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app
from airflow.providers.fab.auth_manager.middleware import FabAuthRolePublicMiddleware
from airflow.providers_manager import ProvidersManager

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_inbound_deliveries
from tests_common.test_utils.webhooks import FakeHmacSource, build_conn_env, post_signed_delivery

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_db():
    clear_db_inbound_deliveries()
    yield
    clear_db_inbound_deliveries()


@mock.patch.dict(os.environ, build_conn_env("fake"))
@mock.patch.object(ProvidersManager, "get_webhook_source_class", autospec=True, return_value=FakeHmacSource)
@conf_vars(
    {
        ("core", "auth_manager"): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        ("fab", "auth_role_public"): "Viewer",
        ("webhooks", "names"): "fake",
    }
)
def test_public_role_middleware_leaves_deliveries_alone(mock_get_webhook_source_class):
    app = create_app(apps="all,webhooks")

    response = post_signed_delivery(TestClient(app, base_url="http://testserver/webhooks"))

    assert FabAuthRolePublicMiddleware in [middleware.cls for middleware in app.user_middleware]
    assert (response.status_code, response.json()) == (
        202,
        {"delivery_id": "delivery-1", "outcome": "no_subscribers"},
    )
