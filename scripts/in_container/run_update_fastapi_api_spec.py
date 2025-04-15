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

from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from fastapi.openapi.utils import get_openapi
from fastapi.routing import APIRoute

from airflow.api_fastapi.app import AUTH_MANAGER_FASTAPI_APP_PREFIX, create_app
from airflow.api_fastapi.auth.managers.simple import __file__ as SIMPLE_AUTH_MANAGER_PATH
from airflow.api_fastapi.auth.managers.simple.simple_auth_manager import SimpleAuthManager
from airflow.api_fastapi.core_api import __file__ as CORE_API_PATH
from airflow.providers.fab.auth_manager.api_fastapi import __file__ as FAB_AUTH_MANAGER_API_PATH
from airflow.providers.fab.auth_manager.fab_auth_manager import FabAuthManager

if TYPE_CHECKING:
    from fastapi import FastAPI

OPENAPI_SPEC_FILE = Path(CORE_API_PATH).parent / "openapi" / "v1-rest-api-generated.yaml"
# We need a "combined" spec file to generate the UI code with, but we don't want to include this in the repo
# nor in the rendered docs, so we make this a separate file which is gitignored
OPENAPI_UI_SPEC_FILE = Path(CORE_API_PATH).parent / "openapi" / "_private_ui.yaml"
SIMPLE_AUTH_MANAGER_OPENAPI_SPEC_FILE = (
    Path(SIMPLE_AUTH_MANAGER_PATH).parent / "openapi" / "v1-simple-auth-manager-generated.yaml"
)
FAB_AUTH_MANAGER_OPENAPI_SPEC_FILE = (
    Path(FAB_AUTH_MANAGER_API_PATH).parent / "openapi" / "v1-fab-auth-manager-generated.yaml"
)


def generate_file(app: FastAPI, file_path: Path, prefix: str = "", only_ui: bool = False):
    if only_ui:
        for route in app.routes:
            if not isinstance(route, APIRoute):
                continue
            route.include_in_schema = route.path.startswith("/ui/")

    with file_path.open("w+") as f:
        openapi_schema = get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            routes=app.routes,
        )
        if prefix:
            openapi_schema["paths"] = {
                prefix + path: path_dict for path, path_dict in openapi_schema["paths"].items()
            }
        yaml.dump(
            openapi_schema,
            f,
            default_flow_style=False,
            sort_keys=False,
        )


# Generate main application openapi spec
generate_file(app=create_app(), file_path=OPENAPI_SPEC_FILE)
generate_file(app=create_app(), file_path=OPENAPI_UI_SPEC_FILE, only_ui=True)

# Generate simple auth manager openapi spec
simple_auth_manager_app = SimpleAuthManager().get_fastapi_app()
if simple_auth_manager_app:
    generate_file(
        app=simple_auth_manager_app,
        file_path=SIMPLE_AUTH_MANAGER_OPENAPI_SPEC_FILE,
        prefix=AUTH_MANAGER_FASTAPI_APP_PREFIX,
    )

# Generate FAB auth manager openapi spec
fab_auth_manager_app = FabAuthManager().get_fastapi_app()
if fab_auth_manager_app:
    generate_file(app=fab_auth_manager_app, file_path=FAB_AUTH_MANAGER_OPENAPI_SPEC_FILE)
