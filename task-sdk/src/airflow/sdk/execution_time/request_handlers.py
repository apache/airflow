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
    VariableResponse,
)
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    GetConnection,
    GetVariable,
    MaskSecret,
    VariableResult,
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


def handle_mask_secret(msg: MaskSecret) -> None:
    """Register a value with the secrets masker."""
    mask_secret(msg.value, msg.name)
