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
from keycloak.exceptions import (
    KeycloakAuthenticationError,
    KeycloakConnectionError,
    KeycloakGetError,
    KeycloakPostError,
)

from airflow.providers.keycloak.auth_manager.exceptions import is_keycloak_outage


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        pytest.param(KeycloakConnectionError("Can't connect"), True, id="unreachable"),
        pytest.param(KeycloakPostError("Unavailable", response_code=503), True, id="5xx"),
        pytest.param(KeycloakGetError("Server error", response_code=500), True, id="500"),
        pytest.param(KeycloakPostError("Bad request", response_code=400), False, id="4xx"),
        pytest.param(KeycloakAuthenticationError("Unauthorized", response_code=401), False, id="401"),
        pytest.param(KeycloakPostError("No code"), False, id="no-response-code"),
    ],
)
def test_is_keycloak_outage(error, expected):
    assert is_keycloak_outage(error) is expected
