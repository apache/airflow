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

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, field_validator

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun, TaskInstance, TIRunContext
from airflow.utils.state import CallbackState, TaskInstanceState


class DagCallbackContext(BaseModel):
    """What a worker needs to run a Dag's success or failure callbacks."""

    # No default on the tags: it would drop them from ``required``, and the generated task-sdk
    # client then types them ``Literal | None``, invalid as a tagged-union discriminator.
    type: Literal["DagCallback"]
    dag_id: str
    run_id: str
    is_failure_callback: bool = True
    msg: str | None = None
    dag_run: DagRun | None = None
    last_ti: TaskInstance | None = None


class TaskCallbackContext(BaseModel):
    """What a worker needs to run a task's failure or retry callbacks."""

    type: Literal["TaskCallback"]
    ti: TaskInstance
    task_callback_type: TaskInstanceState | None = None
    msg: str | None = None
    ti_context: TIRunContext | None = None


class EmailCallbackContext(BaseModel):
    """What a worker needs to send a task's failure or retry email."""

    type: Literal["EmailCallback"]
    ti: TaskInstance
    email_type: Literal["failure", "retry"]
    msg: str | None = None
    ti_context: TIRunContext


class CallbackContextResponse(BaseModel):
    """The context of a Dag-attribute callback, read by the worker that runs it."""

    context: Annotated[
        DagCallbackContext | TaskCallbackContext | EmailCallbackContext, Field(discriminator="type")
    ]


class CallbackRunPayload(StrictBaseModel):
    """The attempt exchanging a callback's token, so a retry of its own exchange is recognized."""

    hostname: str
    pid: int
    attempt_id: UUID
    """Generated once per attempt to run the callback, so no other attempt matches it."""


class CallbackTerminalStatePayload(StrictBaseModel):
    """The outcome a worker reports for a callback it ran."""

    state: CallbackState
    output: Annotated[str | None, Field(max_length=2000)] = None

    @field_validator("state", mode="after")
    @classmethod
    def _only_terminal_states(cls, v: CallbackState) -> CallbackState:
        if v not in (CallbackState.SUCCESS, CallbackState.FAILED):
            raise ValueError(f"Workers may only report terminal states (success/failed); got {v.value!r}")
        return v
