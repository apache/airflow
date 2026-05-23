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

from airflow.sdk.bases.secrets_backend import BaseSecretsBackend
from airflow.sdk.exceptions import AirflowSecretsBackendAccessDenied

if TYPE_CHECKING:
    from airflow.sdk import Connection


class ExecutionAPISecretsBackend(BaseSecretsBackend):
    """
    Secrets backend for client contexts (workers, DAG processors, triggerers).

    Routes connection and variable requests through SUPERVISOR_COMMS to the
    Execution API server. This backend should only be registered in client
    processes, not in API server/scheduler processes.
    """

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        """
        Get connection URI via SUPERVISOR_COMMS.

        Not used since we override get_connection directly.
        """
        raise NotImplementedError("Use get_connection instead")

    def _raise_if_authz_denied(self, msg, *, resource: str, key: str) -> None:
        """
        Raise on an explicit deny response from the Execution API.

        Returning None on a 401/403 would let the secrets-backend dispatcher
        fall through to a less-restrictive backend (e.g. EnvironmentVariablesBackend
        which performs no authorization checks). The Execution API explicitly
        denied this request — we must not silently route around that decision.
        Other ErrorResponse types (NOT_FOUND, transient API_SERVER_ERROR,
        GENERIC_ERROR) keep the existing fallthrough behaviour so the
        not-found-here path remains usable.
        """
        from airflow.sdk.exceptions import ErrorType
        from airflow.sdk.execution_time.comms import ErrorResponse

        if isinstance(msg, ErrorResponse) and msg.error == ErrorType.PERMISSION_DENIED:
            raise AirflowSecretsBackendAccessDenied(
                f"Access denied for {resource} {key!r} by Execution API; refusing to fall back "
                "to a less-restrictive secrets backend."
            )

    def get_connection(self, conn_id: str, team_name: str | None = None) -> Connection | None:  # type: ignore[override]
        """
        Return connection object by routing through SUPERVISOR_COMMS.

        :param conn_id: connection id
        :param team_name: Name of the team associated to the task trying to access the connection.
            Unused here because the team name is inferred from the task ID provided in the execution API JWT token.
        :return: Connection object or None if not found
        :raises AirflowSecretsBackendAccessDenied: when the Execution API explicitly denies access
            (401/403). Subclasses ``PermissionError``. The secrets-backend dispatcher must not fall
            through to an unauthenticated backend in that case.
        """
        from airflow.sdk.execution_time.comms import ErrorResponse, GetConnection
        from airflow.sdk.execution_time.context import _process_connection_result_conn
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        try:
            msg = SUPERVISOR_COMMS.send(GetConnection(conn_id=conn_id))

            self._raise_if_authz_denied(msg, resource="connection", key=conn_id)

            if isinstance(msg, ErrorResponse):
                # Connection not found or transient error — allow fallback.
                return None

            # Convert ExecutionAPI response to SDK Connection
            return _process_connection_result_conn(msg)
        except AirflowSecretsBackendAccessDenied:
            # Re-raise so the dispatcher does NOT fall through.
            raise
        except Exception:
            # If SUPERVISOR_COMMS fails for any non-authz reason, return None
            # to allow fallback to other backends.
            return None

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Return variable value by routing through SUPERVISOR_COMMS.

        :param key: Variable key
        :param team_name: Name of the team associated to the task trying to access the variable.
            Unused here because the team name is inferred from the task ID provided in the execution API JWT token.
        :return: Variable value or None if not found
        :raises AirflowSecretsBackendAccessDenied: when the Execution API explicitly denies access
            (401/403). Subclasses ``PermissionError``. The secrets-backend dispatcher must not fall
            through to an unauthenticated backend in that case.
        """
        from airflow.sdk.execution_time.comms import ErrorResponse, GetVariable, VariableResult
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        try:
            msg = SUPERVISOR_COMMS.send(GetVariable(key=key))

            self._raise_if_authz_denied(msg, resource="variable", key=key)

            if isinstance(msg, ErrorResponse):
                # Variable not found or transient error — allow fallback.
                return None

            # Extract value from VariableResult
            if isinstance(msg, VariableResult):
                return msg.value  # Already a string | None
            return None
        except AirflowSecretsBackendAccessDenied:
            raise
        except Exception:
            # If SUPERVISOR_COMMS fails for any non-authz reason, return None
            # to allow fallback to other backends.
            return None

    async def aget_connection(self, conn_id: str) -> Connection | None:  # type: ignore[override]
        """
        Return connection object asynchronously via SUPERVISOR_COMMS.

        :param conn_id: connection id
        :return: Connection object or None if not found
        :raises AirflowSecretsBackendAccessDenied: see :meth:`get_connection`.
        """
        from airflow.sdk.execution_time.comms import ErrorResponse, GetConnection
        from airflow.sdk.execution_time.context import _process_connection_result_conn
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        try:
            msg = await SUPERVISOR_COMMS.asend(GetConnection(conn_id=conn_id))

            self._raise_if_authz_denied(msg, resource="connection", key=conn_id)

            if isinstance(msg, ErrorResponse):
                # Connection not found or transient error — allow fallback.
                return None

            # Convert ExecutionAPI response to SDK Connection
            return _process_connection_result_conn(msg)
        except AirflowSecretsBackendAccessDenied:
            raise
        except Exception:
            # If SUPERVISOR_COMMS fails for any non-authz reason, return None
            # to allow fallback to other backends.
            return None

    async def aget_variable(self, key: str) -> str | None:
        """
        Return variable value asynchronously via SUPERVISOR_COMMS.

        :param key: Variable key
        :return: Variable value or None if not found
        :raises AirflowSecretsBackendAccessDenied: see :meth:`get_variable`.
        """
        from airflow.sdk.execution_time.comms import ErrorResponse, GetVariable, VariableResult
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        try:
            msg = await SUPERVISOR_COMMS.asend(GetVariable(key=key))

            self._raise_if_authz_denied(msg, resource="variable", key=key)

            if isinstance(msg, ErrorResponse):
                # Variable not found or transient error — allow fallback.
                return None

            # Extract value from VariableResult
            if isinstance(msg, VariableResult):
                return msg.value  # Already a string | None
            return None
        except AirflowSecretsBackendAccessDenied:
            raise
        except Exception:
            # If SUPERVISOR_COMMS fails for any non-authz reason, return None
            # to allow fallback to other backends.
            return None
