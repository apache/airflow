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

from fastapi import FastAPI

from airflow.providers.edge3.worker_api.routes.health import health_router
from airflow.providers.edge3.worker_api.routes.jobs import jobs_router
from airflow.providers.edge3.worker_api.routes.logs import logs_router
from airflow.providers.edge3.worker_api.routes.worker import worker_router


def create_edge_worker_api_app() -> FastAPI:
    """Create FastAPI app for edge worker API."""
    edge_worker_api_app = FastAPI(
        title="Airflow Edge Worker API",
        description=(
            "This is Airflow Edge Worker API - which is a the access endpoint for workers running on remote "
            "sites serving for Apache Airflow jobs. It also proxies internal API to edge endpoints. It is "
            "not intended to be used by any external code. You can find more information in AIP-69 "
            "https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=301795932"
        ),
    )

    edge_worker_api_app.include_router(health_router)
    edge_worker_api_app.include_router(jobs_router)
    edge_worker_api_app.include_router(logs_router)
    edge_worker_api_app.include_router(worker_router)
    return edge_worker_api_app
