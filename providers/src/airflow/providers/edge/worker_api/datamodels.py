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
from typing import (  # noqa: UP035 - prevent pytest failing in back-compat
    Annotated,
    Any,
    Dict,
    List,
    Optional,
    Union,
)

from pydantic import BaseModel, Field

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
        Optional[Dict[str, Any]],  # noqa: UP006, UP007 - prevent pytest failing in back-compat
        Field(description="Dictionary of parameters passed to the method."),
    ]


class WorkerStateBody(BaseModel):
    """Details of the worker state sent to the scheduler."""

    state: Annotated[EdgeWorkerState, Field(description="State of the worker from the view of the worker.")]
    jobs_active: Annotated[int, Field(description="Number of active jobs the worker is running.")] = 0
    queues: Annotated[
        Optional[List[str]],  # noqa: UP006, UP007 - prevent pytest failing in back-compat
        Field(
            description="List of queues the worker is pulling jobs from. If not provided, worker pulls from all queues."
        ),
    ] = None
    sysinfo: Annotated[
        Dict[str, Union[str, int]],  # noqa: UP006, UP007 - prevent pytest failing in back-compat
        Field(
            description="System information of the worker.",
            examples=[
                {
                    "concurrency": 4,
                    "airflow_version": "2.0.0",
                    "edge_provider_version": "1.0.0",
                }
            ],
        ),
    ]


class WorkerQueueUpdateBody(BaseModel):
    """Changed queues for the worker."""

    new_queues: Annotated[
        Optional[List[str]],  # noqa: UP006, UP007 - prevent pytest failing in back-compat
        Field(description="Additional queues to be added to worker."),
    ]
    remove_queues: Annotated[
        Optional[List[str]],  # noqa: UP006, UP007 - prevent pytest failing in back-compat
        Field(description="Queues to remove from worker."),
    ]


class PushLogsBody(BaseModel):
    """Incremental new log content from worker."""

    log_chunk_time: Annotated[datetime, Field(description="Time of the log chunk at point of sending.")]
    log_chunk_data: Annotated[str, Field(description="Log chunk data as incremental log text.")]
