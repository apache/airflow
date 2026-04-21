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
"""
Shared request handlers for supervised subprocess comms channels.

These functions implement the supervisor-side logic for message types that are
used by more than one subprocess type (tasks, callbacks, triggerer).  Each
handler accepts a ``Client`` and a request message and returns
``(response_model | None, dump_opts)`` so the caller can forward the result
via ``send_msg``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.sdk.api.datamodels._generated import (
    ConnectionResponse,
    TaskStatesResponse,
    VariableResponse,
    XComResponse,
)
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    DeleteVariable,
    DeleteXCom,
    GetConnection,
    GetPreviousTI,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    MaskSecret,
    PutVariable,
    SetXCom,
    TaskStatesResult,
    VariableKeysResult,
    VariableResult,
    XComResult,
)
from airflow.sdk.log import mask_secret

if TYPE_CHECKING:
    from pydantic import BaseModel

    from airflow.sdk.api.client import Client


def handle_get_connection(client: Client, msg: GetConnection) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch a connection and mask its sensitive fields."""
    conn = client.connections.get(msg.conn_id)
    if isinstance(conn, ConnectionResponse):
        if conn.password:
            mask_secret(conn.password)
        if conn.extra:
            mask_secret(conn.extra)
        return ConnectionResult.from_conn_response(conn), {"exclude_unset": True, "by_alias": True}
    return conn, {}


def handle_get_variable(client: Client, msg: GetVariable) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch a variable and mask its value."""
    var = client.variables.get(msg.key)
    if isinstance(var, VariableResponse):
        if var.value:
            mask_secret(var.value, var.key)
        return VariableResult.from_variable_response(var), {"exclude_unset": True}
    return var, {}


def handle_get_variable_keys(
    client: Client, msg: GetVariableKeys
) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch variable keys filtered by prefix."""
    result = client.variables.keys(prefix=msg.prefix, limit=msg.limit, offset=msg.offset)
    return (
        VariableKeysResult(keys=result.keys, total_entries=result.total_entries, type="VariableKeysResult"),
        {"exclude_unset": True},
    )


def handle_mask_secret(msg: MaskSecret) -> None:
    """Register a value with the secrets masker."""
    mask_secret(msg.value, msg.name)


def handle_put_variable(client: Client, msg: PutVariable) -> tuple[BaseModel | None, dict[str, bool]]:
    """Store a variable value."""
    resp = client.variables.set(msg.key, msg.value, msg.description)
    return resp, {}


def handle_delete_variable(client: Client, msg: DeleteVariable) -> tuple[BaseModel | None, dict[str, bool]]:
    """Delete a variable value."""
    resp = client.variables.delete(msg.key)
    return resp, {}


def handle_get_ti_count(client: Client, msg: GetTICount) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch task instance counts."""
    resp = client.task_instances.get_count(
        dag_id=msg.dag_id,
        map_index=msg.map_index,
        task_ids=msg.task_ids,
        task_group_id=msg.task_group_id,
        logical_dates=msg.logical_dates,
        run_ids=msg.run_ids,
        states=msg.states,
    )
    return resp, {}


def handle_get_task_states(client: Client, msg: GetTaskStates) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch task states and normalize them for supervisor response handling."""
    task_states_map = client.task_instances.get_task_states(
        dag_id=msg.dag_id,
        map_index=msg.map_index,
        task_ids=msg.task_ids,
        task_group_id=msg.task_group_id,
        logical_dates=msg.logical_dates,
        run_ids=msg.run_ids,
    )
    if isinstance(task_states_map, TaskStatesResponse):
        return TaskStatesResult.from_api_response(task_states_map), {}
    return task_states_map, {}


def handle_get_previous_ti(client: Client, msg: GetPreviousTI) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch the previous task instance."""
    resp = client.task_instances.get_previous(
        dag_id=msg.dag_id,
        task_id=msg.task_id,
        logical_date=msg.logical_date,
        map_index=msg.map_index,
        state=msg.state,
    )
    return resp, {}


def handle_set_xcom(client: Client, msg: SetXCom) -> tuple[BaseModel | None, dict[str, bool]]:
    """Store an XCom value."""
    resp = client.xcoms.set(
        msg.dag_id,
        msg.run_id,
        msg.task_id,
        msg.key,
        msg.value,
        msg.map_index,
        dag_result=msg.dag_result,
        mapped_length=msg.mapped_length,
    )
    return resp, {}


def handle_delete_xcom(client: Client, msg: DeleteXCom) -> tuple[BaseModel | None, dict[str, bool]]:
    """Delete an XCom value."""
    resp = client.xcoms.delete(msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index)
    return resp, {}


def handle_get_xcom(client: Client, msg: GetXCom) -> tuple[BaseModel | None, dict[str, bool]]:
    """Fetch an XCom and normalize it for supervisor response handling."""
    xcom = client.xcoms.get(
        msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index, msg.include_prior_dates
    )
    if isinstance(xcom, XComResponse):
        xcom_result = XComResult.from_xcom_response(xcom)
        return xcom_result, {"exclude_unset": True}
    return xcom, {}
