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
import os
from pathlib import Path
from typing import cast

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import HTMLResponse
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from airflow.settings import AIRFLOW_PATH
from airflow.www.extensions.init_dagbag import get_dag_bag

log = logging.getLogger(__name__)


def init_dag_bag(app: FastAPI) -> None:
    """
    Create global DagBag for the FastAPI application.

    To access it use ``request.app.state.dag_bag``.
    """
    app.state.dag_bag = get_dag_bag()


def init_views(app: FastAPI) -> None:
    """Init views by registering the different routers."""
    from airflow.api_fastapi.core_api.routes.public import public_router
    from airflow.api_fastapi.core_api.routes.ui import ui_router

    app.include_router(ui_router)
    app.include_router(public_router)

    dev_mode = os.environ.get("DEV_MODE", False) == "true"

    directory = Path(AIRFLOW_PATH) / ("airflow/ui/dev" if dev_mode else "airflow/ui/dist")

    # During python tests or when the backend is run without having the frontend build
    # those directories might not exist. App should not fail initializing in those scenarios.
    Path(directory).mkdir(exist_ok=True)

    templates = Jinja2Templates(directory=directory)

    app.mount(
        "/static",
        StaticFiles(
            directory=directory,
            html=True,
        ),
        name="webapp_static_folder",
    )

    @app.get(
        "/webapp/{rest_of_path:path}",
        response_class=HTMLResponse,
        include_in_schema=False,
    )
    def webapp(request: Request, rest_of_path: str):
        return templates.TemplateResponse(
            "/index.html", {"request": request}, media_type="text/html"
        )


def init_plugins(app: FastAPI) -> None:
    """Integrate FastAPI app plugins."""
    from airflow import plugins_manager

    plugins_manager.initialize_fastapi_plugins()

    # After calling initialize_fastapi_plugins, fastapi_apps cannot be None anymore.
    for subapp_dict in cast(list, plugins_manager.fastapi_apps):
        name = subapp_dict.get("name")
        subapp = subapp_dict.get("app")
        if subapp is None:
            log.error("'app' key is missing for the fastapi app: %s", name)
            continue
        url_prefix = subapp_dict.get("url_prefix")
        if url_prefix is None:
            log.error("'url_prefix' key is missing for the fastapi app: %s", name)
            continue

        log.debug("Adding subapplication %s under prefix %s", name, url_prefix)
        app.mount(url_prefix, subapp)
