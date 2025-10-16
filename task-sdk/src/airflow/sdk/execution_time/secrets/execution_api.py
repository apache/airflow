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
"""Secrets backend that routes requests to the Execution API."""

from __future__ import annotations

from typing import TYPE_CHECKING

from airflow.secrets.base_secrets import BaseSecretsBackend

if TYPE_CHECKING:
    from airflow.sdk import Connection


class ExecutionAPISecretsBackend(BaseSecretsBackend):
    """
    Secrets backend for client contexts (workers, DAG processors, triggerers).

    Routes connection and variable requests through SUPERVISOR_COMMS to the
    Execution API server. This backend should only be registered in client
    processes, not in API server/scheduler processes.
    """

    def get_conn_value(self, conn_id: str) -> str | None:
        """
        Get connection URI via SUPERVISOR_COMMS.

        Not used since we override get_connection directly.
        """
        raise NotImplementedError("Use get_connection instead")

    def get_connection(self, conn_id: str) -> Connection | None:  # type: ignore[override]
        """
        Return connection object by routing through SUPERVISOR_COMMS.

        :param conn_id: connection id
        :return: Connection object or None if not found
        """
        from airflow.sdk.execution_time.comms import ErrorResponse, GetConnection
        from airflow.sdk.execution_time.context import _process_connection_result_conn
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        try:
            msg = SUPERVISOR_COMMS.send(GetConnection(conn_id=conn_id))

            if isinstance(msg, ErrorResponse):
                # Connection not found or error occurred
                return None

            # Convert ExecutionAPI response to SDK Connection
            return _process_connection_result_conn(msg)
        except Exception:
            # If SUPERVISOR_COMMS fails for any reason, return None
            # to allow fallback to other backends
            return None

    def get_variable(self, key: str) -> str | None:
        """
        Return variable value by routing through SUPERVISOR_COMMS.

        :param key: Variable key
        :return: Variable value or None if not found
        """
        from airflow.sdk.execution_time.comms import ErrorResponse, GetVariable, VariableResult
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        try:
            msg = SUPERVISOR_COMMS.send(GetVariable(key=key))

            if isinstance(msg, ErrorResponse):
                # Variable not found or error occurred
                return None

            # Extract value from VariableResult
            if isinstance(msg, VariableResult):
                return msg.value  # Already a string | None
            return None
        except Exception:
            # If SUPERVISOR_COMMS fails for any reason, return None
            # to allow fallback to other backends
            return None

    async def aget_connection(self, conn_id: str) -> Connection | None:  # type: ignore[override]
        """
        Return connection object asynchronously via SUPERVISOR_COMMS.

        :param conn_id: connection id
        :return: Connection object or None if not found
        """
        from airflow.sdk.execution_time.comms import ErrorResponse, GetConnection
        from airflow.sdk.execution_time.context import _process_connection_result_conn
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        try:
            msg = await SUPERVISOR_COMMS.asend(GetConnection(conn_id=conn_id))

            if isinstance(msg, ErrorResponse):
                # Connection not found or error occurred
                return None

            # Convert ExecutionAPI response to SDK Connection
            return _process_connection_result_conn(msg)
        except Exception:
            # If SUPERVISOR_COMMS fails for any reason, return None
            # to allow fallback to other backends
            return None

    async def aget_variable(self, key: str) -> str | None:
        """
        Return variable value asynchronously via SUPERVISOR_COMMS.

        :param key: Variable key
        :return: Variable value or None if not found
        """
        from airflow.sdk.execution_time.comms import ErrorResponse, GetVariable, VariableResult
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        try:
            msg = await SUPERVISOR_COMMS.asend(GetVariable(key=key))

            if isinstance(msg, ErrorResponse):
                # Variable not found or error occurred
                return None

            # Extract value from VariableResult
            if isinstance(msg, VariableResult):
                return msg.value  # Already a string | None
            return None
        except Exception:
            # If SUPERVISOR_COMMS fails for any reason, return None
            # to allow fallback to other backends
            return None
