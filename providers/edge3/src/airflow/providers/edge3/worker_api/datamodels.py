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
    Any,
)

from fastapi import Path
from pydantic import BaseModel, Field

from airflow.executors.workloads import ExecuteTask  # noqa: TCH001
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.edge3.models.edge_worker import EdgeWorkerState  # noqa: TCH001


class WorkerApiDocs:
    """Documentation collection for the worker API."""

    dag_id = Path(title="Dag ID", description="Identifier of the DAG to which the task belongs.")
    task_id = Path(title="Task ID", description="Task name in the DAG.")
    run_id = Path(title="Run ID", description="Run ID of the DAG execution.")
    try_number = Path(title="Try Number", description="The number of attempt to execute this task.")
    map_index = Path(
        title="Map Index",
        description="For dynamically mapped tasks the mapping number, -1 if the task is not mapped.",
    )
    state = Path(title="Task State", description="State of the assigned task under execution.")


class JsonRpcRequestBase(BaseModel):
    """Base JSON RPC request model to define just the method."""

    method: Annotated[
        str,
        Field(description="Fully qualified python module method name that is called via JSON RPC."),
    ]


class JsonRpcRequest(JsonRpcRequestBase):
    """JSON RPC request model."""

    jsonrpc: Annotated[str, Field(description="JSON RPC Version", examples=["2.0"])]
    params: Annotated[
        dict[str, Any] | None,
        Field(description="Dictionary of parameters passed to the method."),
    ]


class EdgeJobBase(BaseModel):
    """Basic attributes of a job on the edge worker."""

    dag_id: Annotated[
        str, Field(title="Dag ID", description="Identifier of the DAG to which the task belongs.")
    ]
    task_id: Annotated[str, Field(title="Task ID", description="Task name in the DAG.")]
    run_id: Annotated[str, Field(title="Run ID", description="Run ID of the DAG execution.")]
    map_index: Annotated[
        int,
        Field(
            title="Map Index",
            description="For dynamically mapped tasks the mapping number, -1 if the task is not mapped.",
        ),
    ]
    try_number: Annotated[
        int, Field(title="Try Number", description="The number of attempt to execute this task.")
    ]

    @property
    def key(self) -> TaskInstanceKey:
        return TaskInstanceKey(self.dag_id, self.task_id, self.run_id, self.try_number, self.map_index)


class EdgeJobFetched(EdgeJobBase):
    """Job that is to be executed on the edge worker."""

    command: Annotated[
        ExecuteTask,
        Field(
            title="Command",
            description="Command line to use to execute the job in Airflow",
        ),
    ]
    concurrency_slots: Annotated[int, Field(description="Number of concurrency slots the job requires.")]

    @property
    def identifier(self) -> str:
        """Get a human readable identifier for the edge job."""
        return (
            f"dag_id={self.dag_id} "
            f"task_id={self.task_id} "
            f"run_id={self.run_id} "
            f"map_index={self.map_index} "
            f"try_number={self.try_number}"
        )


class WorkerQueuesBase(BaseModel):
    """Queues that a worker supports to run jobs on."""

    queues: Annotated[
        list[str] | None,
        Field(
            None,
            description="List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues.",
        ),
    ]


class WorkerQueuesBody(WorkerQueuesBase):
    """Queues that a worker supports to run jobs on."""

    free_concurrency: Annotated[int, Field(description="Number of free concurrency slots on the worker.")]


class WorkerStateBody(WorkerQueuesBase):
    """Details of the worker state sent to the scheduler."""

    state: Annotated[EdgeWorkerState, Field(description="State of the worker from the view of the worker.")]
    jobs_active: Annotated[int, Field(description="Number of active jobs the worker is running.")] = 0
    queues: Annotated[
        list[str] | None,
        Field(
            description="List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues."
        ),
    ] = None
    sysinfo: Annotated[
        dict[str, str | int],
        Field(
            description="System information of the worker.",
            examples=[
                {
                    "concurrency": 4,
                    "free_concurrency": 3,
                    "airflow_version": "2.0.0",
                    "edge_provider_version": "1.0.0",
                }
            ],
        ),
    ]
    maintenance_comments: Annotated[
        str | None,
        Field(description="Comments about the maintenance state of the worker."),
    ] = None


class WorkerQueueUpdateBody(BaseModel):
    """Changed queues for the worker."""

    new_queues: Annotated[
        list[str] | None,
        Field(description="Additional queues to be added to worker."),
    ]
    remove_queues: Annotated[
        list[str] | None,
        Field(description="Queues to remove from worker."),
    ]


class PushLogsBody(BaseModel):
    """Incremental new log content from worker."""

    log_chunk_time: Annotated[datetime, Field(description="Time of the log chunk at point of sending.")]
    log_chunk_data: Annotated[str, Field(description="Log chunk data as incremental log text.")]


class WorkerRegistrationReturn(BaseModel):
    """The return class for the worker registration."""

    last_update: Annotated[datetime, Field(description="Time of the last update of the worker.")]


class WorkerSetStateReturn(BaseModel):
    """The return class for the worker set state."""

    state: Annotated[EdgeWorkerState, Field(description="State of the worker from the view of the server.")]
    queues: Annotated[
        list[str] | None,
        Field(
            description="List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues."
        ),
    ]
    maintenance_comments: Annotated[
        str | None,
        Field(description="Comments about the maintenance state of the worker."),
    ] = None
