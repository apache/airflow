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

from typing import Annotated, Literal, Union

from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    WithJsonSchema,
)

from airflow.api_fastapi.common.types import UtcDateTime
from airflow.utils.state import State, TaskInstanceState as TIState


class TIEnterRunningPayload(BaseModel):
    """Schema for updating TaskInstance to 'RUNNING' state with minimal required fields."""

    model_config = ConfigDict(from_attributes=True)

    state: Annotated[
        Literal[TIState.RUNNING],
        # Specify a default in the schema, but not in code, so Pydantic marks it as required.
        WithJsonSchema({"enum": [TIState.RUNNING], "default": TIState.RUNNING}),
    ]
    hostname: str
    """Hostname where this task has started"""
    unixname: str
    """Local username of the process where this task has started"""
    pid: int
    """Process Identifier on `hostname`"""
    start_date: UtcDateTime
    """When the task started executing"""


class TITerminalStatePayload(BaseModel):
    """Schema for updating TaskInstance to a terminal state (e.g., SUCCESS or FAILED)."""

    state: Annotated[
        Literal[TIState.SUCCESS, TIState.FAILED, TIState.SKIPPED],
        Field(title="TerminalState"),
        WithJsonSchema({"enum": list(State.ran_and_finished_states)}),
    ]

    end_date: UtcDateTime
    """When the task completed executing"""


class TITargetStatePayload(BaseModel):
    """Schema for updating TaskInstance to a target state, excluding terminal and running states."""

    state: Annotated[
        TIState,
        # For the OpenAPI schema generation,
        #   make sure we do not include RUNNING as a valid state here
        WithJsonSchema(
            {
                "enum": [
                    state for state in TIState if state not in (State.ran_and_finished_states | {State.NONE})
                ]
            }
        ),
    ]


def ti_state_discriminator(v: dict[str, str] | BaseModel) -> str:
    """
    Determine the discriminator key for TaskInstance state transitions.

    This function serves as a discriminator for the TIStateUpdate union schema,
    categorizing the payload based on the ``state`` attribute in the input data.
    It returns a key that directs FastAPI to the appropriate subclass (schema)
    based on the requested state.
    """
    if isinstance(v, dict):
        state = v.get("state")
    else:
        state = getattr(v, "state", None)
    if state == TIState.RUNNING:
        return str(state)
    elif state in State.ran_and_finished_states:
        return "_terminal_"
    return "_other_"


# It is called "_terminal_" to avoid future conflicts if we added an actual state named "terminal"
# and "_other_" is a catch-all for all other states that are not covered by the other schemas.
TIStateUpdate = Annotated[
    Union[
        Annotated[TIEnterRunningPayload, Tag("running")],
        Annotated[TITerminalStatePayload, Tag("_terminal_")],
        Annotated[TITargetStatePayload, Tag("_other_")],
    ],
    Discriminator(ti_state_discriminator),
]


class TIHeartbeatInfo(BaseModel):
    """Schema for TaskInstance heartbeat endpoint."""

    hostname: str
    pid: int


class ConnectionResponse(BaseModel):
    """Connection schema for responses with fields that are needed for Runtime."""

    conn_id: str
    conn_type: str
    host: str | None
    schema_: str | None = Field(alias="schema")
    login: str | None
    password: str | None
    port: int | None
    extra: str | None


class VariableResponse(BaseModel):
    """Variable schema for responses with fields that are needed for Runtime."""

    model_config = ConfigDict(from_attributes=True)

    key: str
    val: str | None = Field(alias="value")


# TODO: This is a placeholder for Task Identity Token schema.
class TIToken(BaseModel):
    """Task Identity Token."""

    ti_key: str
