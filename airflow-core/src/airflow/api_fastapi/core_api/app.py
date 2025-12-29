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
import sys
import warnings
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse
from starlette.staticfiles import StaticFiles
from starlette.templating import Jinja2Templates

from airflow.api_fastapi.auth.tokens import get_signing_key
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.settings import AIRFLOW_PATH

log = logging.getLogger(__name__)

PY313 = sys.version_info >= (3, 13)


def init_views(app: FastAPI) -> None:
    """Init views by registering the different routers."""
    from airflow.api_fastapi.core_api.routes.public import public_router
    from airflow.api_fastapi.core_api.routes.ui import ui_router

    app.include_router(ui_router)
    app.include_router(public_router)

    dev_mode = os.environ.get("DEV_MODE", str(False)) == "true"

    directory = Path(AIRFLOW_PATH) / ("airflow/ui/dev" if dev_mode else "airflow/ui/dist")

    # During python tests or when the backend is run without having the frontend build
    # those directories might not exist. App should not fail initializing in those scenarios.
    Path(directory).mkdir(exist_ok=True)

    templates = Jinja2Templates(directory=directory)

    if dev_mode:
        app.mount(
            "/static/i18n/locales",
            StaticFiles(directory=Path(AIRFLOW_PATH) / "airflow/ui/public/i18n/locales"),
            name="dev_i18n_static",
        )

    app.mount(
        "/static",
        StaticFiles(
            directory=directory,
            html=True,
        ),
        name="webapp_static_folder",
    )

    @app.get("/health", include_in_schema=False)
    def old_health():
        # If someone has the `/health` endpoint from Airflow 2 set up, we want this to be a 404, not serve the
        # default index.html for the SPA.
        #
        # This is a 404, not a redirect, as setups need correcting to account for this, and a redirect might
        # hide the issue
        return JSONResponse(
            status_code=404,
            content={"error": "Moved in Airflow 3. Please change config to check `/api/v2/monitor/health`"},
        )

    @app.get("/api/v1/{_:path}", include_in_schema=False)
    def old_api(_):
        return JSONResponse(
            status_code=404,
            content={
                "error": "/api/v1 has been removed in Airflow 3, please use its upgraded version /api/v2 instead."
            },
        )

    @app.get("/api/{_:path}", include_in_schema=False)
    def api_not_found(_):
        """Catch all route to handle invalid API endpoints."""
        return JSONResponse(status_code=404, content={"error": "API route not found"})

    @app.get("/{rest_of_path:path}", response_class=HTMLResponse, include_in_schema=False)
    def webapp(request: Request, rest_of_path: str):
        return templates.TemplateResponse(
            "/index.html",
            {"request": request, "backend_server_base_url": request.base_url.path},
            media_type="text/html",
        )


def init_flask_plugins(app: FastAPI) -> None:
    """Integrate Flask plugins (plugins from Airflow 2)."""
    from airflow import plugins_manager

    blueprints, appbuilder_views, appbuilder_menu_links = plugins_manager.get_flask_plugins()

    # If no Airflow 2.x plugin is in the environment, no need to go further
    if not blueprints and not appbuilder_views and not appbuilder_menu_links:
        return

    from fastapi.middleware.wsgi import WSGIMiddleware

    try:
        from airflow.providers.fab.www.app import create_app
    except ImportError:
        if PY313:
            log.info(
                "Some Airflow 2 plugins have been detected in your environment. Currently FAB provider "
                "does not support Python 3.13, so you cannot use Airflow 2 plugins with Airflow 3 until "
                "FAB provider will be Python 3.13 compatible."
            )
            return
        raise AirflowException(
            "Some Airflow 2 plugins have been detected in your environment. "
            "To run them with Airflow 3, you must install the FAB provider in your Airflow environment."
        )

    warnings.warn(
        "You have a plugin that is using a FAB view or Flask Blueprint, which was used for the Airflow 2 UI,"
        "and is now deprecated. Please update your plugin to be compatible with the Airflow 3 UI.",
        DeprecationWarning,
        stacklevel=2,
    )

    flask_app = create_app(enable_plugins=True)
    app.mount("/pluginsv2", WSGIMiddleware(flask_app))


def init_config(app: FastAPI) -> None:
    from airflow.configuration import conf

    allow_origins = conf.getlist("api", "access_control_allow_origins")
    allow_methods = conf.getlist("api", "access_control_allow_methods")
    allow_headers = conf.getlist("api", "access_control_allow_headers")

    if allow_origins or allow_methods or allow_headers:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=allow_origins,
            allow_credentials=True,
            allow_methods=allow_methods,
            allow_headers=allow_headers,
        )

    # Compress responses greater than 1kB with optimal compression level as 5
    # with level ranging from 1 to 9 with 1 (fastest, least compression)
    # and 9 (slowest, most compression)
    app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=5)

    app.state.secret_key = get_signing_key("api", "secret_key")


def init_error_handlers(app: FastAPI) -> None:
    from airflow.api_fastapi.common.exceptions import ERROR_HANDLERS

    for handler in ERROR_HANDLERS:
        app.add_exception_handler(handler.exception_cls, handler.exception_handler)


def init_middlewares(app: FastAPI) -> None:
    from airflow.api_fastapi.auth.middlewares.refresh_token import JWTRefreshMiddleware

    app.add_middleware(JWTRefreshMiddleware)
    if conf.getboolean("core", "simple_auth_manager_all_admins"):
        from airflow.api_fastapi.auth.managers.simple.middleware import SimpleAllAdminMiddleware

        app.add_middleware(SimpleAllAdminMiddleware)
