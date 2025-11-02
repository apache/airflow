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

from datetime import datetime
from typing import (
    Annotated,
)

from pydantic import Field

from airflow.api_fastapi.core_api.base import BaseModel
from airflow.providers.edge3.worker_api.datamodels import EdgeJobBase, WorkerStateBody
from airflow.utils.state import TaskInstanceState  # noqa: TC001


class Worker(WorkerStateBody):
    """Details of the worker state sent to the scheduler."""

    worker_name: Annotated[str, Field(description="Name of the worker.")]
    first_online: Annotated[datetime | None, Field(description="When the worker was first online.")] = None
    last_heartbeat: Annotated[
        datetime | None, Field(description="When the worker last sent a heartbeat.")
    ] = None


class WorkerCollectionResponse(BaseModel):
    """Worker Collection serializer."""

    workers: list[Worker]
    total_entries: int


class Job(EdgeJobBase):
    """Details of the job sent to the scheduler."""

    state: Annotated[TaskInstanceState, Field(description="State of the job from the view of the executor.")]
    queue: Annotated[
        str,
        Field(description="Queue for which the task is scheduled/running."),
    ]
    queued_dttm: Annotated[datetime | None, Field(description="When the job was queued.")] = None
    edge_worker: Annotated[
        str | None, Field(description="The worker processing the job during execution.")
    ] = None
    last_update: Annotated[datetime | None, Field(description="Last heartbeat of the job.")] = None


class JobCollectionResponse(BaseModel):
    """Job Collection serializer."""

    jobs: list[Job]
    total_entries: int


class MaintenanceRequest(BaseModel):
    """Request body for maintenance operations."""

    maintenance_comment: Annotated[str, Field(description="Comment describing the maintenance reason.")]


class QueueUpdateRequest(BaseModel):
    """Request body for queue operations."""

    queue_name: Annotated[str, Field(description="Name of the queue to add or remove.")]
