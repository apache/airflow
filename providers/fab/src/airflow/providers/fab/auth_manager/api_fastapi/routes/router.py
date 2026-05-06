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

from enum import Enum

from airflow.api_fastapi.common.router import AirflowRouter

FAB_AUTH_TAGS: list[str | Enum] = ["FabAuthManager"]
FAB_AUTH_PREFIX = "/fab/v1"

auth_router = AirflowRouter(tags=FAB_AUTH_TAGS)
fab_router = AirflowRouter(prefix=FAB_AUTH_PREFIX, tags=FAB_AUTH_TAGS)


def register_routes() -> None:
    """Register FastAPI routes by importing modules for side effects."""
    import importlib

    importlib.import_module("airflow.providers.fab.auth_manager.api_fastapi.routes.login")
    importlib.import_module("airflow.providers.fab.auth_manager.api_fastapi.routes.roles")
    importlib.import_module("airflow.providers.fab.auth_manager.api_fastapi.routes.users")
