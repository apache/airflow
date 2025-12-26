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

from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from airflow.providers.edge3.worker_api.routes.health import health_router
from airflow.providers.edge3.worker_api.routes.jobs import jobs_router
from airflow.providers.edge3.worker_api.routes.logs import logs_router
from airflow.providers.edge3.worker_api.routes.ui import ui_router
from airflow.providers.edge3.worker_api.routes.worker import worker_router


def create_edge_worker_api_app() -> FastAPI:
    """Create FastAPI app for edge worker API."""
    edge_worker_api_app = FastAPI(
        title="Airflow Edge Worker API",
        description=(
            "This is Airflow Edge Worker API - this is a the access endpoint for workers running on remote "
            "sites serving for Apache Airflow jobs. It also serves for the Edge Worker UI and provides "
            "endpoints for React web app.\n"
            "All endpoints under ``/edge_worker/v1`` are used by remote workers and need a specific JWT token.\n"
            "All endpoints under ``/edge_worker/ui`` are used by UI and can be accessed with normal authentication. "
            "Please assume UI endpoints to change and not be stable."
        ),
    )

    edge_worker_api_app.include_router(jobs_router, prefix="/v1")
    edge_worker_api_app.include_router(logs_router, prefix="/v1")
    edge_worker_api_app.include_router(worker_router, prefix="/v1")
    edge_worker_api_app.include_router(health_router, prefix="/v1")
    edge_worker_api_app.include_router(ui_router, prefix="/ui")

    # Fix mimetypes to serve cjs files correctly
    import mimetypes

    if ".cjs" not in mimetypes.suffix_map:
        mimetypes.add_type("application/javascript", ".cjs")

    www_files = Path(__file__).parents[1] / "plugins" / "www"
    edge_worker_api_app.mount(
        "/static",
        StaticFiles(directory=(www_files / "dist").absolute(), html=True),
        name="react_static_plugin_files",
    )
    edge_worker_api_app.mount(
        "/res",
        StaticFiles(directory=(www_files / "src" / "res").absolute(), html=True),
        name="react_res_plugin_files",
    )

    return edge_worker_api_app
