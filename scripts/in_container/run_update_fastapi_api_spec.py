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

import yaml
from fastapi.openapi.utils import get_openapi

from airflow.api_fastapi.app import create_app

app = create_app()

OPENAPI_SPEC_FILE = "airflow/api_fastapi/openapi/v1-generated.yaml"


# The persisted openapi spec will list all endpoints (public and ui), this
# is used for code generation.
for route in app.routes:  # Handle dynamic SortParam default value in openapi spec
    if getattr(route, "name") == "webapp":
        continue
    route.__setattr__("include_in_schema", True)

with open(OPENAPI_SPEC_FILE, "w+") as f:
    yaml.dump(
        get_openapi(
            title=app.title,
            version=app.version,
            openapi_version=app.openapi_version,
            description=app.description,
            routes=app.routes,
        ),
        f,
        default_flow_style=False,
        sort_keys=False,
    )
