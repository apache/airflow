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

from sqlalchemy import select

from airflow.api_fastapi.common.db.common import SessionDep  # noqa: TC001
from airflow.api_fastapi.common.router import AirflowRouter
from airflow.providers.edge3.models.edge_worker import EdgeWorkerModel
from airflow.providers.edge3.worker_api.datamodels_ui import Worker, WorkerCollectionResponse

ui_router = AirflowRouter(tags=["UI"])


@ui_router.get("/worker")
def worker(
    session: SessionDep,
) -> WorkerCollectionResponse:
    """Return Edge Workers."""
    query = select(EdgeWorkerModel)
    workers: list[EdgeWorkerModel] = session.scalars(query)

    result = [
        Worker(
            worker_name=w.worker_name,
            queues=w.queues,
            state=w.state,
            jobs_active=w.jobs_active,
            sysinfo=w.sysinfo_json or {},
            maintenance_comments=w.maintenance_comment,
        )
        for w in workers
    ]
    return WorkerCollectionResponse(
        workers=result,
        total_entries=len(result),
    )
