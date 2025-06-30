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

from airflow.providers.fab.auth_manager.api_fastapi import __file__ as FAB_AUTH_MANAGER_API_PATH
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager
from airflow.providers_manager import ProvidersManager

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from in_container_utils import console, generate_openapi_file, validate_openapi_file

FAB_AUTH_MANAGER_OPENAPI_SPEC_FILE = (
    Path(FAB_AUTH_MANAGER_API_PATH).parent / "openapi" / "v2-fab-auth-manager-generated.yaml"
)

ProvidersManager().initialize_providers_configuration()

# Generate FAB auth manager openapi spec
fab_auth_manager_app = FabAuthManager().get_fastapi_app()
if fab_auth_manager_app:
    generate_openapi_file(
        app=fab_auth_manager_app, file_path=FAB_AUTH_MANAGER_OPENAPI_SPEC_FILE, prefix="/auth"
    )
    validate_openapi_file(FAB_AUTH_MANAGER_OPENAPI_SPEC_FILE)
else:
    console.print("[red]FAB auth manager app not found. Skipping OpenAPI spec generation.[/]")
    sys.exit(1)
