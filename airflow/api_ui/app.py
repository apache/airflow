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

from fastapi import APIRouter, FastAPI

from airflow.www.extensions.init_dagbag import get_dag_bag

app: FastAPI | None = None


def init_dag_bag(app: FastAPI) -> None:
    """
    Create global DagBag for the FastAPI application.

    To access it use ``request.app.state.dag_bag``.
    """
    app.state.dag_bag = get_dag_bag()


def create_app() -> FastAPI:
    app = FastAPI(
        description="Internal Rest API for the UI frontend. It is subject to breaking change "
        "depending on the need of the frontend. Users should not rely on this API but use the "
        "public API instead."
    )

    init_dag_bag(app)

    init_views(app)

    return app


def init_views(app) -> None:
    """Init views by registering the different routers."""
    from airflow.api_ui.views.datasets import dataset_router

    root_router = APIRouter(prefix="/ui")

    root_router.include_router(dataset_router)

    app.include_router(root_router)


def cached_app(config=None, testing=False):
    """Return cached instance of Airflow UI app."""
    global app
    if not app:
        app = create_app()
    return app


def purge_cached_app():
    """Remove the cached version of the app in global state."""
    global app
    app = None
