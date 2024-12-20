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

from typing import TYPE_CHECKING, Any

import structlog

from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType

if TYPE_CHECKING:
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.execution_time.comms import ConnectionResult


def _convert_connection_result_conn(conn_result: ConnectionResult):
    from airflow.sdk.definitions.connection import Connection

    # `by_alias=True` is used to convert the `schema` field to `schema_` in the Connection model
    return Connection(**conn_result.model_dump(exclude={"type"}, by_alias=True))


def _get_connection(conn_id: str) -> Connection:
    # TODO: This should probably be moved to a separate module like `airflow.sdk.execution_time.comms`
    #   or `airflow.sdk.execution_time.connection`
    #   A reason to not move it to `airflow.sdk.execution_time.comms` is that it
    #   will make that module depend on Task SDK, which is not ideal because we intend to
    #   keep Task SDK as a separate package than execution time mods.
    from airflow.sdk.execution_time.comms import ErrorResponse, GetConnection
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    log = structlog.get_logger(logger_name="task")
    SUPERVISOR_COMMS.send_request(log=log, msg=GetConnection(conn_id=conn_id))
    msg = SUPERVISOR_COMMS.get_message()
    if isinstance(msg, ErrorResponse):
        raise AirflowRuntimeError(msg)

    if TYPE_CHECKING:
        assert isinstance(msg, ConnectionResult)
    return _convert_connection_result_conn(msg)


class ConnectionAccessor:
    """Wrapper to access Connection entries in template."""

    def __getattr__(self, conn_id: str) -> Any:
        return _get_connection(conn_id)

    def __repr__(self) -> str:
        return "<ConnectionAccessor (dynamic access)>"

    def __eq__(self, other):
        if not isinstance(other, ConnectionAccessor):
            return False
        # All instances of ConnectionAccessor are equal since it is a stateless dynamic accessor
        return True

    def get(self, conn_id: str, default_conn: Any = None) -> Any:
        try:
            return _get_connection(conn_id)
        except AirflowRuntimeError as e:
            if e.error.error == ErrorType.CONNECTION_NOT_FOUND:
                return default_conn
            raise
