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
from fastapi.middleware.cors import CORSMiddleware

from airflow.api_fastapi.core_api.app import init_dag_bag, init_plugins, init_views

log = logging.getLogger(__name__)

app: FastAPI | None = None


def create_app(apps: list[str] | None = None) -> FastAPI:
    from airflow.configuration import conf

    app = FastAPI(
        title="Airflow API",
        description="Airflow API. All endpoints located under ``/public`` can be used safely, are stable and backward compatible. "
        "Endpoints located under ``/ui`` are dedicated to the UI and are subject to breaking change "
        "depending on the need of the frontend. Users should not rely on those but use the public ones instead.",
    )

    init_dag_bag(app)

    init_views(app)

    init_plugins(app)

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

    return app


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
