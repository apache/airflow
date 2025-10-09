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
from fastapi.testclient import TestClient

from airflow.api_fastapi.app import create_app
from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLIENT_ID_KEY,
    CONF_CLIENT_SECRET_KEY,
    CONF_REALM_KEY,
    CONF_SECTION_NAME,
)

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def client():
    with conf_vars(
        {
            (
                "core",
                "auth_manager",
            ): "airflow.providers.keycloak.auth_manager.keycloak_auth_manager.KeycloakAuthManager",
            (CONF_SECTION_NAME, CONF_CLIENT_ID_KEY): "test",
            (CONF_SECTION_NAME, CONF_CLIENT_SECRET_KEY): "test",
            (CONF_SECTION_NAME, CONF_REALM_KEY): "test",
            (CONF_SECTION_NAME, "base_url"): "http://host.docker.internal:48080",
        }
    ):
        yield TestClient(create_app())
