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

import os
import sys
from pathlib import Path

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX, create_app
from airflow.api_fastapi.auth.managers.simple import __file__ as SIMPLE_AUTH_MANAGER_PATH
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.core_api import __file__ as CORE_API_PATH

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from in_container_utils import console, generate_openapi_file, validate_openapi_file

OPENAPI_SPEC_FILE = Path(CORE_API_PATH).parent / "openapi" / "v2-rest-api-generated.yaml"
# We need a "combined" spec file to generate the UI code with, but we don't want to include this in the repo
# nor in the rendered docs, so we make this a separate file which is gitignored
OPENAPI_UI_SPEC_FILE = Path(CORE_API_PATH).parent / "openapi" / "_private_ui.yaml"
SIMPLE_AUTH_MANAGER_OPENAPI_SPEC_FILE = (
    Path(SIMPLE_AUTH_MANAGER_PATH).parent / "openapi" / "v2-simple-auth-manager-generated.yaml"
)

# Generate main application openapi spec
# Set the auth manager as SAM. No need to use another one to generate fastapi spec
os.environ["AIRFLOW__CORE__AUTH_MANAGER"] = (
    "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager"
)
generate_openapi_file(app=create_app(), file_path=OPENAPI_SPEC_FILE)
validate_openapi_file(OPENAPI_SPEC_FILE)

generate_openapi_file(app=create_app(), file_path=OPENAPI_UI_SPEC_FILE, only_ui=True)
validate_openapi_file(OPENAPI_UI_SPEC_FILE)

# Generate simple auth manager openapi spec
simple_auth_manager_app = SimpleAuthManager().get_fastapi_app()
if simple_auth_manager_app:
    generate_openapi_file(
        app=simple_auth_manager_app,
        file_path=SIMPLE_AUTH_MANAGER_OPENAPI_SPEC_FILE,
        prefix=AUTH_MANAGER_FASTAPI_APP_PREFIX,
    )
    validate_openapi_file(SIMPLE_AUTH_MANAGER_OPENAPI_SPEC_FILE)
    console.print("[green]OpenAPI spec for SimpleAuthManager generated successfully.[/]")
else:
    console.print(
        "[red]Failed to generate OpenAPI spec for SimpleAuthManager. "
        "The app is not available or the generation failed.[/]"
    )
    sys.exit(1)
