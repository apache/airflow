#!/usr/bin/env python
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

import sys
from pathlib import Path

from airflow.providers.keycloak.auth_manager import __file__ as KEYCLOAK_AUTH_MANAGER_PATH
from airflow.providers.keycloak.auth_manager.keycloak_auth_manager import KeycloakAuthManager
from airflow.providers_manager import ProvidersManager

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from in_container_utils import console, generate_openapi_file, validate_openapi_file

KEYCLOAK_AUTH_MANAGER_OPENAPI_SPEC_FILE = (
    Path(KEYCLOAK_AUTH_MANAGER_PATH).parent / "openapi" / "v2-keycloak-auth-manager-generated.yaml"
)

ProvidersManager().initialize_providers_configuration()

# Generate Keycloak auth manager openapi spec
keycloak_auth_manager_app = KeycloakAuthManager().get_fastapi_app()
if keycloak_auth_manager_app:
    generate_openapi_file(
        app=keycloak_auth_manager_app, file_path=KEYCLOAK_AUTH_MANAGER_OPENAPI_SPEC_FILE, prefix="/auth"
    )
    validate_openapi_file(KEYCLOAK_AUTH_MANAGER_OPENAPI_SPEC_FILE)
else:
    console.print("[red]Keycloak auth manager app not found. Skipping OpenAPI spec generation.[/]")
    sys.exit(1)
