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
from contextlib import AsyncExitStack, asynccontextmanager
from typing import cast
from urllib.parse import urlsplit

from fastapi import FastAPI
from starlette.routing import Mount

from airflow.api_fastapi.common.auth_manager import init_auth_manager
from airflow.api_fastapi.common.dagbag import create_dag_bag
from airflow.api_fastapi.core_api.app import (
    init_config,
    init_error_handlers,
    init_flask_plugins,
    init_middlewares,
    init_views,
)
from airflow.api_fastapi.execution_api.app import create_task_execution_api_app
from airflow.configuration import conf
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

API_BASE_URL = conf.get("api", "base_url", fallback="")
if not API_BASE_URL or not API_BASE_URL.endswith("/"):
    API_BASE_URL += "/"
API_ROOT_PATH = urlsplit(API_BASE_URL).path

# Define the full path on which the potential auth manager fastapi is mounted
AUTH_MANAGER_FASTAPI_APP_PREFIX = f"{API_ROOT_PATH}auth"

log = logging.getLogger(__name__)

app: FastAPI | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with AsyncExitStack() as stack:
        for route in app.routes:
            if isinstance(route, Mount) and isinstance(route.app, FastAPI):
                await stack.enter_async_context(
                    route.app.router.lifespan_context(route.app),
                )
        app.state.lifespan_called = True
        yield


@providers_configuration_loaded
def create_app(apps: str = "all") -> FastAPI:
    apps_list = apps.split(",") if apps else ["all"]

    app = FastAPI(
        title="Airflow API",
        description="Airflow API. All endpoints located under ``/api/v2`` can be used safely, are stable and backward compatible. "
        "Endpoints located under ``/ui`` are dedicated to the UI and are subject to breaking change "
        "depending on the need of the frontend. Users should not rely on those but use the public ones instead.",
        lifespan=lifespan,
        root_path=API_ROOT_PATH.removesuffix("/"),
        version="2",
    )

    dag_bag = create_dag_bag()

    if "execution" in apps_list or "all" in apps_list:
        task_exec_api_app = create_task_execution_api_app()
        task_exec_api_app.state.dag_bag = dag_bag
        init_error_handlers(task_exec_api_app)
        app.mount("/execution", task_exec_api_app)

    if "core" in apps_list or "all" in apps_list:
        app.state.dag_bag = dag_bag
        init_plugins(app)
        init_auth_manager(app)
        init_flask_plugins(app)
        init_views(app)  # Core views need to be the last routes added - it has a catch all route
        init_error_handlers(app)
        init_middlewares(app)

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


def init_plugins(app: FastAPI) -> None:
    """Integrate FastAPI app and middleware plugins."""
    from airflow import plugins_manager

    plugins_manager.initialize_fastapi_plugins()

    # After calling initialize_fastapi_plugins, fastapi_apps cannot be None anymore.
    for subapp_dict in cast("list", plugins_manager.fastapi_apps):
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

    for middleware_dict in cast("list", plugins_manager.fastapi_root_middlewares):
        name = middleware_dict.get("name")
        middleware = middleware_dict.get("middleware")
        args = middleware_dict.get("args", [])
        kwargs = middleware_dict.get("kwargs", {})

        if middleware is None:
            log.error("'middleware' key is missing for the fastapi middleware: %s", name)
            continue

        log.debug("Adding root middleware %s", name)
        app.add_middleware(middleware, *args, **kwargs)
