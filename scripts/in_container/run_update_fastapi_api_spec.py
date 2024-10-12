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

from inspect import Signature, signature

import yaml
from fastapi.openapi.utils import get_openapi
from fastapi.routing import APIRoute

from airflow.api_fastapi.app import create_app
from airflow.api_fastapi.parameters import SortParam

app = create_app()

OPENAPI_SPEC_FILE = "airflow/api_fastapi/openapi/v1-generated.yaml"
# SortParam class name and model path mapping for default value in openapi spec
SORT_PARAM_CLASS_NAME = SortParam.__name__
SORT_PARAM_MODEL_PATH_MAPPING = {}
# The persisted openapi spec will list all endpoints (public and ui), this
# is used for code generation.
for route in app.routes:
    # Handle dynamic SortParam default value in openapi spec
    if type(route) is APIRoute:
        route: APIRoute = route
        endpoint_signature: Signature = signature(route.endpoint)
        for param_name, param_definition in endpoint_signature.parameters.items():
            if SORT_PARAM_CLASS_NAME in str(param_definition.annotation):
                SORT_PARAM_MODEL_PATH_MAPPING[route.path] = {
                    "param_name": param_name,
                    "param_default_value": str(
                        param_definition.annotation.split(",")[-1].split(")")[0]
                    ).lstrip(" "),
                }

    if getattr(route, "name") == "webapp":
        continue
    route.__setattr__("include_in_schema", True)

with open(OPENAPI_SPEC_FILE, "w+") as f:
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        openapi_version=app.openapi_version,
        description=app.description,
        routes=app.routes,
    )
    # Add default value for any parameter that have SortParam as parameter to handle dynamic default value
    for path, path_info in openapi_schema["paths"].items():
        if path in SORT_PARAM_MODEL_PATH_MAPPING.keys():
            for _method, method_info in path_info.items():
                if method_info.get("parameters"):
                    for param in method_info["parameters"]:
                        if param["name"] == SORT_PARAM_MODEL_PATH_MAPPING[path]["param_name"]:
                            param["schema"].update(
                                {
                                    "default": SortParam.get_primary_key_of_given_model_string(
                                        SORT_PARAM_MODEL_PATH_MAPPING[path]["param_default_value"]
                                    )
                                }
                            )
    yaml.dump(
        openapi_schema,
        f,
        default_flow_style=False,
        sort_keys=False,
    )
