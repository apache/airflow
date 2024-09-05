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

from fastapi import APIRouter, FastAPI

from airflow.configuration import conf
from airflow.www.app import create_app as create_flask_app
from airflow.www.extensions.init_dagbag import get_dag_bag

app: FastAPI | None = None

log = logging.getLogger(__name__)


def init_dag_bag(app: FastAPI) -> None:
    """
    Create global DagBag for the FastAPI application.

    To access it use ``request.app.state.dag_bag``.
    """
    app.state.dag_bag = get_dag_bag()


def init_flask_app(app: FastAPI, testing: bool = False) -> None:
    """
    Auth providers and permission logic are tightly coupled to Flask.

    Leverage the create_app for flask to initialize the api_auth, app_builder, auth_manager.
    Also limit the backend auth in production mode.
    """
    flask_app = create_flask_app(testing=testing)

    app.state.flask_app = flask_app

    if conf.get("api", "auth_backends") != "airflow.providers.fab.auth_manager.api.auth.backend.basic_auth":
        log.warning(
            "FastAPI UI API only supports 'airflow.providers.fab.auth_manager.api.auth.backend.basic_auth' other backends will be ignored",
        )


def create_app(testing: bool = False) -> FastAPI:
    app = FastAPI(
        description="Internal Rest API for the UI frontend. It is subject to breaking change "
        "depending on the need of the frontend. Users should not rely on this API but use the "
        "public API instead.",
    )

    init_dag_bag(app)
    init_views(app)

    init_flask_app(app, testing)

    return app


def init_views(app) -> None:
    """Init views by registering the different routers."""
    from airflow.api_ui.views.datasets import dataset_router

    root_router = APIRouter(prefix="/ui")

    root_router.include_router(dataset_router)

    app.include_router(root_router)


def cached_app(config=None, testing=False) -> FastAPI:
    """Return cached instance of Airflow UI app."""
    global app
    if not app:
        app = create_app()
    return app


def purge_cached_app() -> None:
    """Remove the cached version of the app in global state."""
    global app
    app = None
