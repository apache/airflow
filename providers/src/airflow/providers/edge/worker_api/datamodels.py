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
from typing import (  # noqa: UP035 - Pydantic not able to parse in Python 3.9
    Annotated,
    Any,
    Dict,
    List,
    Optional,
    Union,
)

from pydantic import BaseModel, Field

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.edge.models.edge_worker import EdgeWorkerState  # noqa: TCH001
from airflow.providers.edge.worker_api.routes._v2_compat import Path


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
        Optional[Dict[str, Any]],  # noqa: UP006, UP007 - Pydantic not able to parse in Python 3.9
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
        list[str], Field(title="Command", description="Command line to use to execute the job.")
    ]
    concurrency_slots: Annotated[int, Field(description="Number of concurrency slots the job requires.")]


class WorkerQueuesBase(BaseModel):
    """Queues that a worker supports to run jobs on."""

    queues: Annotated[
        Optional[List[str]],  # noqa: UP006, UP007 - Pydantic not able to parse in Python 3.9
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
        Optional[List[str]],  # noqa: UP006, UP007 - Pydantic not able to parse in Python 3.9
        Field(
            description="List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues."
        ),
    ] = None
    sysinfo: Annotated[
        dict[str, Union[str, int]],  # noqa: UP007 - Pydantic not able to parse in Python 3.9
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


class WorkerQueueUpdateBody(BaseModel):
    """Changed queues for the worker."""

    new_queues: Annotated[
        Optional[List[str]],  # noqa: UP006, UP007 - Pydantic not able to parse in Python 3.9
        Field(description="Additional queues to be added to worker."),
    ]
    remove_queues: Annotated[
        Optional[List[str]],  # noqa: UP006, UP007 - Pydantic not able to parse in Python 3.9
        Field(description="Queues to remove from worker."),
    ]


class PushLogsBody(BaseModel):
    """Incremental new log content from worker."""

    log_chunk_time: Annotated[datetime, Field(description="Time of the log chunk at point of sending.")]
    log_chunk_data: Annotated[str, Field(description="Log chunk data as incremental log text.")]
