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

import logging

from fastapi import FastAPI

from airflow.api_fastapi.core_api.app import init_config, init_dag_bag, init_plugins, init_views
from airflow.api_fastapi.execution_api.app import create_task_execution_api_app

log = logging.getLogger(__name__)

app: FastAPI | None = None


def create_app(apps: str = "all") -> FastAPI:
    apps_list = apps.split(",") if apps else ["all"]

    app = FastAPI(
        title="Airflow API",
        description="Airflow API. All endpoints located under ``/public`` can be used safely, are stable and backward compatible. "
        "Endpoints located under ``/ui`` are dedicated to the UI and are subject to breaking change "
        "depending on the need of the frontend. Users should not rely on those but use the public ones instead.",
    )

    if "core" in apps_list or "all" in apps_list:
        init_dag_bag(app)
        init_views(app)
        init_plugins(app)

    if "execution" in apps_list or "all" in apps_list:
        task_exec_api_app = create_task_execution_api_app(app)
        app.mount("/execution", task_exec_api_app)

    init_config(app)

    return app


def cached_app(config=None, testing=False, apps="all") -> FastAPI:
    """Return cached instance of Airflow API app."""
    global app
    if not app:
        app = create_app(apps=apps)
    return app


def purge_cached_app() -> None:
    """Remove the cached version of the app in global state."""
    global app
    app = None
