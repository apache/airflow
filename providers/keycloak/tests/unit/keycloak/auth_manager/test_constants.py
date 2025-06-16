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

from airflow.providers.keycloak.auth_manager.constants import (
    CONF_CLIENT_ID_KEY,
    CONF_CLIENT_SECRET_KEY,
    CONF_REALM_KEY,
    CONF_SECTION_NAME,
    CONF_SERVER_URL_KEY,
)


class TestKeycloakAuthManagerConstants:
    def test_conf_section_name(self):
        assert CONF_SECTION_NAME == "keycloak_auth_manager"

    def test_conf_client_id_key(self):
        assert CONF_CLIENT_ID_KEY == "client_id"

    def test_conf_client_secret_key(self):
        assert CONF_CLIENT_SECRET_KEY == "client_secret"

    def test_conf_realm_key(self):
        assert CONF_REALM_KEY == "realm"

    def test_conf_server_url_key(self):
        assert CONF_SERVER_URL_KEY == "server_url"
