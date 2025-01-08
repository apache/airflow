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

import datetime
import sys
from typing import TYPE_CHECKING, Any

import httpx
import rich
import structlog

from airflow.cli.api.datamodels._generated import (
    AssetAliasCollectionResponse,
    AssetAliasResponse,
    AssetCollectionResponse,
    AssetResponse,
    BackfillPostBody,
    BackfillResponse,
    Config,
    ConnectionBody,
    ConnectionBulkBody,
    ConnectionCollectionResponse,
    ConnectionResponse,
    ConnectionTestResponse,
    DAGDetailsResponse,
    DAGResponse,
    DAGRunCollectionResponse,
    DAGRunResponse,
    JobCollectionResponse,
    PoolCollectionResponse,
    PoolPatchBody,
    PoolPostBody,
    PoolPostBulkBody,
    PoolResponse,
    ProviderCollectionResponse,
    TriggerDAGRunPostBody,
    VariableBody,
    VariableCollectionResponse,
    VariableResponse,
    VersionInfo,
)

if TYPE_CHECKING:
    from airflow.cli.api.client import Client
    from airflow.utils.state import DagRunState

log = structlog.get_logger(logger_name=__name__)


# Generic Server Response Error
class ServerResponseError(httpx.HTTPStatusError):
    """Server response error (Generic)."""

    def __init__(self, message: str, *, request: httpx.Request, response: httpx.Response):
        super().__init__(message, request=request, response=response)

    # def_

    @classmethod
    def from_response(cls, response: httpx.Response) -> ServerResponseError | None:
        if response.status_code < 400:
            return None

        if response.headers.get("content-type") != "application/json":
            return None

        if 400 <= response.status_code < 500:
            response.read()
            return cls(
                message=f"Client error message: {response.json()}",
                request=response.request,
                response=response,
            )

        msg = response.json()

        self = cls(message=msg, request=response.request, response=response)
        return self


# Decorator to apply methods to all operations, this is initiated at __init_subclass__ on BaseOperations
SERVER_CONNECTION_REFUSED_ERROR: str = "API Server is not running, please contact your administrator."


def _check_flag_and_exit_if_server_response_error(func):
    """Return decorator to check for ServerResponseError and exit if the server is not running."""

    def _exit_if_server_response_error(response: Any | ServerResponseError):
        if isinstance(response, ServerResponseError):
            rich.print(f"[bold red]Error:[/bold red] {response.response.json()}")
            sys.exit(1)
        return response

    def wrapped(self, *args, **kwargs):
        try:
            if self.exit_in_error:
                return _exit_if_server_response_error(response=func(self, *args, **kwargs))
            else:
                return func(self, *args, **kwargs)
        except httpx.ConnectError as e:
            rich.print(f"error: {e}")
            rich.print(f"[bold red]{SERVER_CONNECTION_REFUSED_ERROR}[/bold red]")
            sys.exit(1)

    return wrapped


class BaseOperations:
    """
    Base class for operations.

    This class is used to decorate all callable methods with a check for ServerResponseError.
    Set exit_in_error false to not exit.
    """

    __slots__ = ("client", "response", "exit_in_error")

    def __init__(self, client: Client, response=None, exit_in_error: bool = True):
        self.client = client
        self.response = response
        self.exit_in_error = exit_in_error

    def __init_subclass__(cls, **kwargs):
        """Decorate all callable methods with a check for ServerResponseError and exit if the server is not running."""
        super().__init_subclass__(**kwargs)
        for attr, value in cls.__dict__.items():
            if callable(value):
                setattr(cls, attr, _check_flag_and_exit_if_server_response_error(value))


# Operations
# TODO: Get all with limit and offset to overcome default 100 limit for all list operations
class AssetsOperations(BaseOperations):
    """Assets operations."""

    def get(self, asset_id: str) -> AssetResponse | ServerResponseError:
        """Get an asset from the API server."""
        try:
            self.response = self.client.get(f"assets/{asset_id}")
            return AssetResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Asset not found",
                asset_id=asset_id,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def get_by_alias(self, alias: str) -> AssetAliasResponse | ServerResponseError:
        """Get an asset by alias from the API server."""
        try:
            self.response = self.client.get(f"assets/aliases/{alias}")
            return AssetAliasResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Asset alias not found",
                alias=alias,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def list(self) -> AssetCollectionResponse | ServerResponseError:
        """List all assets from the API server."""
        try:
            self.response = self.client.get("assets")
            return AssetCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Assets not found",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def list_by_alias(self) -> AssetAliasCollectionResponse | ServerResponseError:
        """List all assets by alias from the API server."""
        try:
            self.response = self.client.get("/assets/aliases")
            return AssetAliasCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Asset aliases not found",
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class BackfillsOperations(BaseOperations):
    """Backfill operations."""

    def create(self, backfill: BackfillPostBody) -> BackfillResponse | ServerResponseError:
        """Create a backfill."""
        try:
            self.response = self.client.post("backfills", data=backfill.model_dump())
            return BackfillResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Backfill already exists",
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class ConfigOperations(BaseOperations):
    """Config operations."""

    def get(self, section: str, option: str) -> Config | ServerResponseError:
        """Get a config from the API server."""
        try:
            self.response = self.client.get(f"/section/{section}/option/{option}")
            return Config.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Config not found",
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class ConnectionsOperations(BaseOperations):
    """Connection operations."""

    def get(self, conn_id: str) -> ConnectionResponse | ServerResponseError:
        """Get a connection from the API server."""
        try:
            self.response = self.client.get(f"connections/{conn_id}")
            return ConnectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Connection not found",
                conn_id=conn_id,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def list(self) -> ConnectionCollectionResponse | ServerResponseError:
        """List all connections from the API server."""
        try:
            self.response = self.client.get("connections")
            return ConnectionCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Connection not found",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def create(
        self,
        connection: ConnectionBody,
    ) -> ConnectionResponse | ServerResponseError:
        """Create a connection."""
        try:
            self.response = self.client.post("connections", json=connection.model_dump())
            return ConnectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Connection already exists",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def create_bulk(
        self, connections: ConnectionBulkBody, overwrite: bool = False
    ) -> ConnectionCollectionResponse | ServerResponseError:
        """Create multiple connections."""
        try:
            self.response = self.client.put("connections/bulk", json=connections.model_dump())
            return ConnectionCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Connection already exists",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def create_defaults(self) -> None | ServerResponseError:
        """Create default connections."""
        try:
            self.response = self.client.post("connections/defaults")
            return None
        except ServerResponseError as e:
            log.error(
                "Failed to create default connections",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def delete(self, conn_id: str) -> str | ServerResponseError:
        """Delete a connection."""
        try:
            self.client.delete(f"connections/{conn_id}")
            return conn_id
        except ServerResponseError as e:
            log.error(
                "Connection not found",
                conn_id=conn_id,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def update(
        self,
        connection: ConnectionBody,
    ) -> ConnectionResponse | ServerResponseError:
        """Update a connection."""
        try:
            self.response = self.client.patch(
                f"connections/{connection.connection_id}", json=connection.model_dump()
            )
            return ConnectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Connection not found",
                conn_id=connection.connection_id,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def test(
        self,
        connection: ConnectionBody,
    ) -> ConnectionTestResponse | ServerResponseError:
        """Test a connection."""
        try:
            self.response = self.client.post("connections/test", json=connection.model_dump())
            return ConnectionTestResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Connection test failed",
                conn_id=connection.connection_id,
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class DagOperations(BaseOperations):
    """Dag operations."""

    def get(self, dag_id: str) -> DAGResponse | ServerResponseError:
        """Get a DAG."""
        try:
            self.response = self.client.get(f"dags/{dag_id}")
            return DAGResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to get dag",
                dag_id=dag_id,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def get_details(self, dag_id: str) -> DAGDetailsResponse | ServerResponseError:
        """Get a DAG details."""
        try:
            self.response = self.client.get(f"dags/{dag_id}/details")
            return DAGDetailsResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to get dag details",
                dag_id=dag_id,
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class DagRunOperations(BaseOperations):
    """Dag run operations."""

    def get(self, dag_run_id: str) -> DAGRunResponse | ServerResponseError:
        """Get a dag run."""
        try:
            self.response = self.client.get(f"dag_runs/{dag_run_id}")
            return DAGRunResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to get dag run",
                dag_run_id=dag_run_id,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def list(
        self,
        dag_id: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        state: DagRunState,
        limit: int,
    ) -> DAGRunCollectionResponse | ServerResponseError:
        """List all dag runs."""
        try:
            params = {
                "start_date": start_date,
                "end_date": end_date,
                "state": state,
                "limit": limit,
            }
            self.response = self.client.get("dag_runs", params=params)  # type: ignore
            return DAGRunCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to list dag runs",
                dag_id=dag_id,
                limit=limit,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def create(
        self, dag_id: str, trigger_dag_run: TriggerDAGRunPostBody
    ) -> DAGRunResponse | ServerResponseError:
        """Create a dag run."""
        try:
            # It is model_dump_json() because it has unparsable json datetime objects
            self.response = self.client.post(f"dag_runs/{dag_id}", json=trigger_dag_run.model_dump_json())
            return DAGRunResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to create dag run",
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class JobsOperations(BaseOperations):
    """Job operations."""

    def list(
        self, job_type: str, hostname: str, limit: int, is_alive: bool
    ) -> JobCollectionResponse | ServerResponseError:
        """List all jobs."""
        try:
            params = {"limit": limit, "job_type": job_type, "hostname": hostname, "is_alive": is_alive}
            self.response = self.client.get("jobs", params=params)  # type: ignore
            return JobCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to list jobs",
                job_type=job_type,
                hostname=hostname,
                limit=limit,
                is_alive=is_alive,
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class PoolsOperations(BaseOperations):
    """Pool operations."""

    def get(self, pool_name: str) -> PoolResponse | ServerResponseError:
        """Get a pool."""
        try:
            self.response = self.client.get(f"pools/{pool_name}")
            return PoolResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to get pool",
                pool_name=pool_name,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def list(self) -> PoolCollectionResponse | ServerResponseError:
        """List all pools."""
        try:
            self.response = self.client.get("pools")
            return PoolCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to list pools",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def create(self, pool: PoolPostBody) -> PoolResponse | ServerResponseError:
        """Create a pool."""
        try:
            self.response = self.client.post("pools", json=pool.model_dump())
            return PoolResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to create pool",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def create_bulk(self, pools: PoolPostBulkBody) -> PoolCollectionResponse | ServerResponseError:
        """Create multiple pools."""
        try:
            self.response = self.client.put("pools/bulk", json=pools.model_dump())
            return PoolCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to create pools",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def delete(self, pool: str) -> str | ServerResponseError:
        """Delete a pool."""
        try:
            self.client.delete(f"pools/{pool}")
            return pool
        except ServerResponseError as e:
            log.error(
                "Failed to delete pool",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def update(self, pool_body: PoolPatchBody) -> PoolResponse | ServerResponseError:
        """Update a pool."""
        try:
            self.response = self.client.patch(f"pools/{pool_body.pool}", json=pool_body.model_dump())
            return PoolResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to update pool",
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class ProvidersOperations(BaseOperations):
    """Provider operations."""

    def list(self) -> ProviderCollectionResponse | ServerResponseError:
        """List all providers."""
        try:
            self.response = self.client.get("providers")
            return ProviderCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to list providers",
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class VariablesOperations(BaseOperations):
    """Variable operations."""

    def get(self, variable_key: str) -> VariableResponse | ServerResponseError:
        """Get a variable."""
        try:
            self.response = self.client.get(f"variables/{variable_key}")
            return VariableResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to get variable",
                variable_key=variable_key,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def list(self) -> VariableCollectionResponse | ServerResponseError:
        """List all variables."""
        try:
            self.response = self.client.get("variables")
            return VariableCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to list variables",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def create(self, variable: VariableBody) -> VariableResponse | ServerResponseError:
        """Create a variable."""
        try:
            self.response = self.client.post("variables", json=variable.model_dump())
            return VariableResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to create variable",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def create_bulk(
        self, variables: VariableCollectionResponse
    ) -> VariableCollectionResponse | ServerResponseError:
        """Create multiple variables."""
        try:
            response_variable_list = []
            for variable_body in variables.variables:
                self.response = self.client.post("variables", json=variable_body.model_dump())
                response_variable_list.append(VariableResponse.model_validate_json(self.response.content))
            return VariableCollectionResponse(
                variables=response_variable_list, total_entries=len(response_variable_list)
            )
        except ServerResponseError as e:
            log.error(
                "Failed to create variables",
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def delete(self, variable_key: str) -> str | ServerResponseError:
        """Delete a variable."""
        try:
            self.client.delete(f"variables/{variable_key}")
            return variable_key
        except ServerResponseError as e:
            log.error(
                "Failed to delete variable",
                variable_key=variable_key,
                detail=e,
                status_code=e.response.status_code,
            )
            return e

    def update(self, variable: VariableBody) -> VariableResponse | ServerResponseError:
        """Update a variable."""
        try:
            self.response = self.client.patch(f"variables/{variable.key}", json=variable.model_dump())
            return VariableResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to update variable",
                detail=e,
                status_code=e.response.status_code,
            )
            return e


class VersionOperations(BaseOperations):
    """Version operations."""

    def get(self) -> VersionInfo | ServerResponseError:
        """Get the version."""
        try:
            self.response = self.client.get("version")
            return VersionInfo.model_validate_json(self.response.content)
        except ServerResponseError as e:
            log.error(
                "Failed to get version",
                detail=e,
                status_code=e.response.status_code,
            )
            return e
