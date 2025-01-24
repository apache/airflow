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

from typing import TYPE_CHECKING

import yaml
from fastapi.openapi.utils import get_openapi

from airflow.api_fastapi.app import create_app
from airflow.auth.managers.simple.simple_auth_manager import SimpleAuthManager

if TYPE_CHECKING:
    from fastapi import FastAPI

OPENAPI_SPEC_FILE = "airflow/api_fastapi/core_api/openapi/v1-generated.yaml"
SIMPLE_AUTH_MANAGER_OPENAPI_SPEC_FILE = "airflow/auth/managers/simple/openapi/v1-generated.yaml"


def generate_file(app: FastAPI, file_path: str, prefix: str = ""):
    # The persisted openapi spec will list all endpoints (public and ui), this
    # is used for code generation.
    for route in app.routes:
        if getattr(route, "name") == "webapp":
            continue
        route.__setattr__("include_in_schema", True)

    with open(file_path, "w+") as f:
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
generate_file(create_app(), OPENAPI_SPEC_FILE)

# Generate simple auth manager openapi spec
simple_auth_manager_app = SimpleAuthManager().get_fastapi_app()
if simple_auth_manager_app:
    generate_file(simple_auth_manager_app, SIMPLE_AUTH_MANAGER_OPENAPI_SPEC_FILE, "/auth")
