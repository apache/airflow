#
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
r"""
Communication protocol between the Supervisor and the task process
==================================================================

* All communication is done over stdout/stdin in the form of "JSON lines" (each
  message is a single JSON document terminated by `\n` character)
* Messages from the subprocess are all log messages and are sent directly to the log
* No messages are sent to task process except in response to a request. (This is because the task process will
  be running user's code, so we can't read from stdin until we enter our code, such as when requesting an XCom
  value etc.)

The reason this communication protocol exists, rather than the task process speaking directly to the Task
Execution API server is because:

1. To reduce the number of concurrent HTTP connections on the API server.

   The supervisor already has to speak to that to heartbeat the running Task, so having the task speak to its
   parent process and having all API traffic go through that means that the number of HTTP connections is
   "halved". (Not every task will make API calls, so it's not always halved, but it is reduced.)

2. This means that the user Task code doesn't ever directly see the task identity JWT token.

   This is a short lived token tied to one specific task instance try, so it being leaked/exfiltrated is not a
   large risk, but it's easy to not give it to the user code, so lets do that.
"""  # noqa: D400, D205

from __future__ import annotations

from typing import Annotated, Literal, Union

from fastapi import Body
from pydantic import BaseModel, ConfigDict, Field, JsonValue

from airflow.sdk.api.datamodels._generated import (
    ConnectionResponse,
    TaskInstance,
    TerminalTIState,
    TIDeferredStatePayload,
    VariableResponse,
    XComResponse,
)


class StartupDetails(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ti: TaskInstance
    file: str
    requests_fd: int
    """
    The channel for the task to send requests over.

    Responses will come back on stdin
    """
    type: Literal["StartupDetails"] = "StartupDetails"


class XComResult(XComResponse):
    """Response to ReadXCom request."""

    type: Literal["XComResult"] = "XComResult"


class ConnectionResult(ConnectionResponse):
    type: Literal["ConnectionResult"] = "ConnectionResult"


class VariableResult(VariableResponse):
    type: Literal["VariableResult"] = "VariableResult"


ToTask = Annotated[
    Union[StartupDetails, XComResult, ConnectionResult, VariableResult],
    Field(discriminator="type"),
]


class TaskState(BaseModel):
    """
    Update a task's state.

    If a process exits without sending one of these the state will be derived from the exit code:
    - 0 = SUCCESS
    - anything else = FAILED
    """

    state: TerminalTIState
    type: Literal["TaskState"] = "TaskState"


class DeferTask(TIDeferredStatePayload):
    """Update a task instance state to deferred."""

    type: Literal["DeferTask"] = "DeferTask"


class GetXCom(BaseModel):
    key: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1
    type: Literal["GetXCom"] = "GetXCom"


class SetXCom(BaseModel):
    key: str
    value: Annotated[
        # JsonValue can handle non JSON stringified dicts, lists and strings, which is better
        # for the task intuitibe to send to the supervisor
        JsonValue,
        Body(
            description="A JSON-formatted string representing the value to set for the XCom.",
            openapi_examples={
                "simple_value": {
                    "summary": "Simple value",
                    "value": "value1",
                },
                "dict_value": {
                    "summary": "Dictionary value",
                    "value": {"key2": "value2"},
                },
                "list_value": {
                    "summary": "List value",
                    "value": ["value1"],
                },
            },
        ),
    ]
    dag_id: str
    run_id: str
    task_id: str
    map_index: int = -1
    type: Literal["SetXCom"] = "SetXCom"


class GetConnection(BaseModel):
    conn_id: str
    type: Literal["GetConnection"] = "GetConnection"


class GetVariable(BaseModel):
    key: str
    type: Literal["GetVariable"] = "GetVariable"


ToSupervisor = Annotated[
    Union[TaskState, GetXCom, GetConnection, GetVariable, DeferTask, SetXCom],
    Field(discriminator="type"),
]
