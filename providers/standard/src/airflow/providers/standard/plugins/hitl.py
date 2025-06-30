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

from typing import Any

from fastapi import FastAPI

from airflow.plugins_manager import AirflowPlugin


def _get_api_endpoint() -> dict[str, Any]:
    from airflow.providers.standard.api_fastapi.core_api.routes.hitl import hitl_router

    hitl_api_app = FastAPI(
        title="Airflow Human-in-the-loop API",
        # TODO: update description
        description="API-90",
    )
    hitl_api_app.include_router(hitl_router)

    return {
        "app": hitl_api_app,
        "url_prefix": "/hitl",
        "name": "Airflow Human in the loop API",
    }


class HumanInTheLoopPlugin(AirflowPlugin):
    """Human in the loop plugin for Airflow."""

    name = "standard_hitl"
    fastapi_apps = [_get_api_endpoint()]
