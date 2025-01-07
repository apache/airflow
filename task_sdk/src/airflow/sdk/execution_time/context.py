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
from airflow.sdk.types import NOTSET

if TYPE_CHECKING:
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.definitions.variable import Variable
    from airflow.sdk.execution_time.comms import ConnectionResult, VariableResult


def _convert_connection_result_conn(conn_result: ConnectionResult) -> Connection:
    from airflow.sdk.definitions.connection import Connection

    # `by_alias=True` is used to convert the `schema` field to `schema_` in the Connection model
    return Connection(**conn_result.model_dump(exclude={"type"}, by_alias=True))


def _convert_variable_result_to_variable(var_result: VariableResult, deserialize_json: bool) -> Variable:
    from airflow.sdk.definitions.variable import Variable

    if deserialize_json:
        import json

        var_result.value = json.loads(var_result.value)  # type: ignore
    return Variable(**var_result.model_dump(exclude={"type"}))


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


def _get_variable(key: str, deserialize_json: bool) -> Variable:
    # TODO: This should probably be moved to a separate module like `airflow.sdk.execution_time.comms`
    #   or `airflow.sdk.execution_time.variable`
    #   A reason to not move it to `airflow.sdk.execution_time.comms` is that it
    #   will make that module depend on Task SDK, which is not ideal because we intend to
    #   keep Task SDK as a separate package than execution time mods.
    from airflow.sdk.execution_time.comms import ErrorResponse, GetVariable
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    log = structlog.get_logger(logger_name="task")
    SUPERVISOR_COMMS.send_request(log=log, msg=GetVariable(key=key))
    msg = SUPERVISOR_COMMS.get_message()
    if isinstance(msg, ErrorResponse):
        raise AirflowRuntimeError(msg)

    if TYPE_CHECKING:
        assert isinstance(msg, VariableResult)
    return _convert_variable_result_to_variable(msg, deserialize_json)


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


class VariableAccessor:
    """Wrapper to access Variable values in template."""

    def __init__(self, deserialize_json: bool) -> None:
        self._deserialize_json = deserialize_json

    def __eq__(self, other):
        if not isinstance(other, VariableAccessor):
            return False
        # All instances of VariableAccessor are equal since it is a stateless dynamic accessor
        return True

    def __repr__(self) -> str:
        return "<VariableAccessor (dynamic access)>"

    def __getattr__(self, key: str) -> Any:
        return _get_variable(key, self._deserialize_json)

    def get(self, key, default_var: Any = NOTSET) -> Any:
        try:
            return _get_variable(key, self._deserialize_json)
        except AirflowRuntimeError as e:
            if e.error.error == ErrorType.VARIABLE_NOT_FOUND:
                return default_var
            raise


class MacrosAccessor:
    """Wrapper to access Macros module lazily."""

    _macros_module = None

    def __getattr__(self, item: str) -> Any:
        # Lazily load Macros module
        if not self._macros_module:
            import airflow.sdk.definitions.macros

            self._macros_module = airflow.sdk.definitions.macros
        return getattr(self._macros_module, item)

    def __repr__(self) -> str:
        return "<MacrosAccessor (dynamic access to macros)>"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MacrosAccessor):
            return False
        return True
