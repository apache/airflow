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

from cadwyn import VersionedAPIRouter
from fastapi import APIRouter

from airflow.api_fastapi.execution_api.deps import JWTBearerDep
from airflow.api_fastapi.execution_api.routes import (
    asset_events,
    assets,
    connections,
    dag_runs,
    dags,
    health,
    hitl,
    task_instances,
    task_reschedules,
    variables,
    xcoms,
)

execution_api_router = APIRouter()
execution_api_router.include_router(health.router, prefix="/health", tags=["Health"])

# _Every_ single endpoint under here must be authenticated. Some do further checks on top of these
authenticated_router = VersionedAPIRouter(dependencies=[JWTBearerDep])  # type: ignore[list-item]

authenticated_router.include_router(assets.router, prefix="/assets", tags=["Assets"])
authenticated_router.include_router(asset_events.router, prefix="/asset-events", tags=["Asset Events"])
authenticated_router.include_router(connections.router, prefix="/connections", tags=["Connections"])
authenticated_router.include_router(dag_runs.router, prefix="/dag-runs", tags=["Dag Runs"])
authenticated_router.include_router(dags.router, prefix="/dags", tags=["Dags"])
authenticated_router.include_router(task_instances.router, prefix="/task-instances", tags=["Task Instances"])
authenticated_router.include_router(
    task_reschedules.router, prefix="/task-reschedules", tags=["Task Reschedules"]
)
authenticated_router.include_router(variables.router, prefix="/variables", tags=["Variables"])
authenticated_router.include_router(xcoms.router, prefix="/xcoms", tags=["XComs"])
authenticated_router.include_router(hitl.router, prefix="/hitlDetails", tags=["Human in the Loop"])

execution_api_router.include_router(authenticated_router)
