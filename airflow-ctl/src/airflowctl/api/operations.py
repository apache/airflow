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
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

import httpx
import structlog

from airflowctl.api.datamodels.auth_generated import LoginBody, LoginResponse
from airflowctl.api.datamodels.generated import (
    AssetAliasCollectionResponse,
    AssetAliasResponse,
    AssetCollectionResponse,
    AssetEventResponse,
    AssetResponse,
    BackfillCollectionResponse,
    BackfillPostBody,
    BackfillResponse,
    BulkBodyConnectionBody,
    BulkBodyPoolBody,
    BulkBodyVariableBody,
    BulkResponse,
    Config,
    ConnectionBody,
    ConnectionCollectionResponse,
    ConnectionResponse,
    ConnectionTestResponse,
    CreateAssetEventsBody,
    DAGCollectionResponse,
    DAGDetailsResponse,
    DAGPatchBody,
    DAGResponse,
    DAGRunCollectionResponse,
    DAGRunResponse,
    DagStatsCollectionResponse,
    DAGTagCollectionResponse,
    DAGVersionCollectionResponse,
    DagVersionResponse,
    DAGWarningCollectionResponse,
    ImportErrorCollectionResponse,
    ImportErrorResponse,
    JobCollectionResponse,
    PoolBody,
    PoolCollectionResponse,
    PoolPatchBody,
    PoolResponse,
    ProviderCollectionResponse,
    QueuedEventCollectionResponse,
    QueuedEventResponse,
    TriggerDAGRunPostBody,
    VariableBody,
    VariableCollectionResponse,
    VariableResponse,
    VersionInfo,
)
from airflowctl.exceptions import AirflowCtlConnectionException

T = TypeVar("T", bound=Callable)

if TYPE_CHECKING:
    from pydantic import BaseModel

    from airflowctl.api.client import Client

log = structlog.get_logger(logger_name=__name__)


# Generic Server Response Error
class ServerResponseError(httpx.HTTPStatusError):
    """Server response error (Generic)."""

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


def _check_flag_and_exit_if_server_response_error(func):
    """Return decorator to check for ServerResponseError and exit if the server is not running."""

    def _exit_if_server_response_error(response: Any | ServerResponseError):
        if isinstance(response, ServerResponseError):
            raise response
        return response

    def wrapped(self, *args, **kwargs):
        try:
            if self.exit_in_error:
                return _exit_if_server_response_error(response=func(self, *args, **kwargs))
            return func(self, *args, **kwargs)
        except httpx.ConnectError as e:
            if "Connection refused" in str(e):
                raise AirflowCtlConnectionException("Connection refused. Is the API server running?")
            raise AirflowCtlConnectionException(f"Connection error: {e}")

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

    def execute_operation(
        self,
        *,
        method: str,
        url: str,
        data_model: type[BaseModel] | None = None,
        return_entry: str | None = None,
        **kwargs,
    ):
        try:
            print("try")
            self.response = self.client.request(method, url, **kwargs)
            if data_model:
                return data_model.model_validate_json(self.response.content)

            return return_entry
        except ServerResponseError as e:
            raise e


# Login operations
class LoginOperations:
    """Login operations."""

    def __init__(self, client: Client):
        self.client = client

    def login_with_username_and_password(self, login: LoginBody) -> LoginResponse | ServerResponseError:
        """Login to the API server."""
        try:
            return LoginResponse.model_validate_json(
                self.client.post("/token/cli", json=login.model_dump()).content
            )
        except ServerResponseError as e:
            raise e


# Operations
# TODO: Get all with limit and offset to overcome default 100 limit for all list operations
class AssetsOperations(BaseOperations):
    """Assets operations."""

    def get(self, asset_id: str) -> AssetResponse | ServerResponseError:
        """Get an asset from the API server."""
        return super().execute_operation(method="get", url=f"assets/{asset_id}", data_model=AssetResponse)

    def get_by_alias(self, alias: str) -> AssetAliasResponse | ServerResponseError:
        """Get an asset by alias from the API server."""
        return super().execute_operation(
            method="get", url=f"assets/aliases/{alias}", data_model=AssetAliasResponse
        )

    def list(self):
        """List all assets from the API server."""
        return super().execute_operation(method="get", url="assets", data_model=AssetCollectionResponse)

    def list_by_alias(self) -> AssetAliasCollectionResponse | ServerResponseError:
        """List all assets by alias from the API server."""
        return super().execute_operation(
            method="get", url="/assets/aliases", data_model=AssetAliasCollectionResponse
        )

    def create_event(
        self, asset_event_body: CreateAssetEventsBody
    ) -> AssetEventResponse | ServerResponseError:
        """Create an asset event."""
        return super().execute_operation(method="post", url="assets/events", data_model=AssetEventResponse)

    def materialize(self, asset_id: str) -> DAGRunResponse | ServerResponseError:
        """Materialize an asset."""
        return super().execute_operation(
            method="post", url=f"assets/{asset_id}/materialize", data_model=DAGRunResponse
        )

    def get_queued_events(self, asset_id: str) -> QueuedEventCollectionResponse | ServerResponseError:
        """Get queued events for an asset."""
        return super().execute_operation(
            method="get", url=f"assets/{asset_id}/queuedEvents", data_model=QueuedEventCollectionResponse
        )

    def get_dag_queued_events(
        self, dag_id: str, before: str
    ) -> QueuedEventCollectionResponse | ServerResponseError:
        return super().execute_operation(
            method="get",
            url=f"dags/{dag_id}/assets/queuedEvents",
            data_model=QueuedEventCollectionResponse,
            params={"before": before},
        )

    def get_dag_queued_event(self, dag_id: str, asset_id: str) -> QueuedEventResponse | ServerResponseError:
        """Get a queued event for a dag."""
        return super().execute_operation(
            method="get", url=f"dags/{dag_id}/assets/{asset_id}/queuedEvents", data_model=QueuedEventResponse
        )

    def delete_queued_events(self, asset_id: str) -> str | ServerResponseError:
        """Delete a queued event for an asset."""
        return super().execute_operation(
            method="delete", url=f"assets/{asset_id}/queuedEvents/", return_entity=asset_id
        )

    def delete_dag_queued_events(self, dag_id: str, before: str) -> str | ServerResponseError:
        """Delete a queued event for a dag."""
        return super().execute_operation(
            method="delete",
            url=f"assets/dags/{dag_id}/queuedEvents",
            return_delete_entry=dag_id,
            params={"before": before},
        )

    def delete_queued_event(self, dag_id: str, asset_id: str) -> str | ServerResponseError:
        """Delete a queued event for a dag."""
        return super().execute_operation(
            method="delete",
            url=f"assets/dags/{dag_id}/assets/{asset_id}/queuedEvents/",
            return_delete_entry=asset_id,
        )


class BackfillsOperations(BaseOperations):
    """Backfill operations."""

    def create(self, backfill: BackfillPostBody) -> BackfillResponse | ServerResponseError:
        """Create a backfill."""
        return super().execute_operation(
            method="post", url="backfills", data_model=BackfillResponse, data=backfill.model_dump()
        )

    def create_dry_run(self, backfill: BackfillPostBody) -> BackfillResponse | ServerResponseError:
        """Create a dry run backfill."""
        return super().execute_operation(
            method="post", url="backfills/dry_run", data_model=BackfillResponse, data=backfill.model_dump()
        )

    def get(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Get a backfill."""
        return super().execute_operation(
            method="get", url=f"backfills/{backfill_id}", data_model=BackfillResponse
        )

    def list(self) -> BackfillCollectionResponse | ServerResponseError:
        """List all backfills."""
        return super().execute_operation(method="get", url="backfills", data_model=BackfillCollectionResponse)

    def pause(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Pause a backfill."""
        return super().execute_operation(
            method="post", url=f"backfills/{backfill_id}/pause", data_model=BackfillResponse
        )

    def unpause(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Unpause a backfill."""
        return super().execute_operation(
            method="post", url=f"backfills/{backfill_id}/unpause", data_model=BackfillResponse
        )

    def cancel(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Cancel a backfill."""
        return super().execute_operation(
            method="post", url=f"backfills/{backfill_id}/cancel", data_model=BackfillResponse
        )


class ConfigOperations(BaseOperations):
    """Config operations."""

    def get(self, section: str, option: str) -> Config | ServerResponseError:
        """Get a config from the API server."""
        return super().execute_operation(
            method="get", url=f"/config/section/{section}/option/{option}", data_model=Config
        )

    def list(self) -> Config | ServerResponseError:
        """List all configs from the API server."""
        return super().execute_operation(method="get", url="/config", data_model=Config)


class ConnectionsOperations(BaseOperations):
    """Connection operations."""

    def get(self, conn_id: str) -> ConnectionResponse | ServerResponseError:
        """Get a connection from the API server."""
        return super().execute_operation(
            method="get", url=f"connections/{conn_id}", data_model=ConnectionResponse
        )

    def list(self) -> ConnectionCollectionResponse | ServerResponseError:
        """List all connections from the API server."""
        return super().execute_operation(
            method="get", url="connections", data_model=ConnectionCollectionResponse
        )

    def create(
        self,
        connection: ConnectionBody,
    ) -> ConnectionResponse | ServerResponseError:
        """Create a connection."""
        return super().execute_operation(
            method="post", url="connections", data_model=ConnectionResponse, json=connection.model_dump()
        )

    def bulk(self, connections: BulkBodyConnectionBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple connections."""
        try:
            self.response = self.client.patch("connections", json=connections.model_dump())
            return BulkResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e
        # return super().execute_operation(method="patch",url = "connections",data_model = BulkResponse,json=connections.model_dump())

    def create_defaults(self) -> None | ServerResponseError:
        """Create default connections."""
        return super().execute_operation(method="post", url="connections/defaults")

    def delete(self, conn_id: str) -> str | ServerResponseError:
        """Delete a connection."""
        return super().execute_operation(method="delete", url=f"connections/{conn_id}")

    def update(
        self,
        connection: ConnectionBody,
    ) -> ConnectionResponse | ServerResponseError:
        """Update a connection."""
        return super().execute_operation(
            method="patch",
            url=f"connections/{connection.connection_id}",
            data_model=ConnectionResponse,
            json=connection.model_dump(),
        )

    def test(
        self,
        connection: ConnectionBody,
    ) -> ConnectionTestResponse | ServerResponseError:
        """Test a connection."""
        return super().execute_operation(
            method="post", url="connections/test", data_model=ConnectionTestResponse
        )


class DagOperations(BaseOperations):
    """Dag operations."""

    def get(self, dag_id: str) -> DAGResponse | ServerResponseError:
        """Get a DAG."""
        return super().execute_operation(method="get", url="dags/{dag_id}", data_model=DAGResponse)

    def get_details(self, dag_id: str) -> DAGDetailsResponse | ServerResponseError:
        """Get a DAG details."""
        return super().execute_operation(
            method="get", url=f"dags/{dag_id}/details", data_model=DAGDetailsResponse
        )

    def get_tags(self) -> DAGTagCollectionResponse | ServerResponseError:
        """Get all DAG tags."""
        return super().execute_operation(method="get", url="dagTags", data_model=DAGTagCollectionResponse)

    def list(self) -> DAGCollectionResponse | ServerResponseError:
        """List DAGs."""
        return super().execute_operation(method="get", url="dags", data_model=DAGCollectionResponse)

    def patch(self, dag_id: str, dag_body: DAGPatchBody) -> DAGResponse | ServerResponseError:
        return super().execute_operation(method="patch", url=f"dags/{dag_id}", data_model=DAGResponse)

    def delete(self, dag_id: str) -> str | ServerResponseError:
        return super().execute_operation(method="delete", url=f"dags/{dag_id}", return_entry=dag_id)

    def get_import_error(self, import_error_id: str) -> ImportErrorResponse | ServerResponseError:
        return super().execute_operation(
            method="get", url=f"importErrors/{import_error_id}", data_model=ImportErrorResponse
        )

    def list_import_error(self) -> ImportErrorCollectionResponse | ServerResponseError:
        return super().execute_operation(
            method="get", url="importErrors", data_model=ImportErrorCollectionResponse
        )

    def get_stats(self, dag_ids: list) -> DagStatsCollectionResponse | ServerResponseError:  # type: ignore
        return super().execute_operation(
            method="get", url="dagStats", data_model=DagStatsCollectionResponse, params={"dag_ids": dag_ids}
        )

    def get_version(self, dag_id: str, version_number: int) -> DagVersionResponse | ServerResponseError:
        return super().execute_operation(
            method="get", url=f"dags/{dag_id}/dagVersions/{version_number}", data_model=DagVersionResponse
        )

    def list_version(self, dag_id: str) -> DAGVersionCollectionResponse | ServerResponseError:
        return super().execute_operation(
            method="get", url=f"dags/{dag_id}/dagVersions", data_model=DAGVersionCollectionResponse
        )

    def list_warning(self) -> DAGWarningCollectionResponse | ServerResponseError:
        return super().execute_operation(
            method="get", url="dagWarnings", data_model=DAGWarningCollectionResponse
        )


class DagRunOperations(BaseOperations):
    """Dag run operations."""

    def get(self, dag_run_id: str) -> DAGRunResponse | ServerResponseError:
        """Get a dag run."""
        return super().execute_operation(
            method="get", url=f"dag_runs/{dag_run_id}", data_model=DAGRunResponse
        )

    def list(
        self,
        dag_id: str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        state: str,
        limit: int,
    ) -> DAGRunCollectionResponse | ServerResponseError:
        """List all dag runs."""
        params = {
            "start_date": start_date,
            "end_date": end_date,
            "state": state,
            "limit": limit,
        }
        return super().execute_operation(
            method="get", url="dag_runs", data_model=DAGRunCollectionResponse, params=params
        )

    def create(
        self, dag_id: str, trigger_dag_run: TriggerDAGRunPostBody
    ) -> DAGRunResponse | ServerResponseError:
        """Create a dag run."""
        return super().execute_operation(
            method="post",
            url=f"dag_runs/{dag_id}",
            data_model=DAGRunResponse,
            json=trigger_dag_run.model_dump_json(),
        )


class JobsOperations(BaseOperations):
    """Job operations."""

    def list(
        self, job_type: str, hostname: str, is_alive: bool
    ) -> JobCollectionResponse | ServerResponseError:
        """List all jobs."""
        params = {"job_type": job_type, "hostname": hostname, "is_alive": is_alive}
        return super().execute_operation(
            method="get", url="jobs", data_model=JobCollectionResponse, params=params
        )


class PoolsOperations(BaseOperations):
    """Pool operations."""

    def get(self, pool_name: str) -> PoolResponse | ServerResponseError:
        """Get a pool."""
        return super().execute_operation(method="get", url=f"pools/{pool_name}", data_model=PoolResponse)

    def list(self) -> PoolCollectionResponse | ServerResponseError:
        """List all pools."""
        try:
            self.response = self.client.get("pools")
            return PoolCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def create(self, pool: PoolBody) -> PoolResponse | ServerResponseError:
        """Create a pool."""
        return super().execute_operation(
            method="post", url="pools", data_model=PoolResponse, json=pool.model_dump()
        )

    def bulk(self, pools: BulkBodyPoolBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple pools."""
        try:
            self.response = self.client.patch("pools", json=pools.model_dump())
            return BulkResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def delete(self, pool: str) -> str | ServerResponseError:
        """Delete a pool."""
        return super().execute_operation(method="delete", url=f"pools/{pool}", return_entry=pool)

    def update(self, pool_body: PoolPatchBody) -> PoolResponse | ServerResponseError:
        """Update a pool."""
        return super().execute_operation(
            method="patch",
            url=f"pools/{pool_body.pool}",
            data_model=PoolResponse,
            json=pool_body.model_dump(),
        )


class ProvidersOperations(BaseOperations):
    """Provider operations."""

    def list(self) -> ProviderCollectionResponse | ServerResponseError:
        """List all providers."""
        return super().execute_operation(method="get", url="providers", data_model=ProviderCollectionResponse)


class VariablesOperations(BaseOperations):
    """Variable operations."""

    def get(self, variable_key: str) -> VariableResponse | ServerResponseError:
        """Get a variable."""
        return super().execute_operation(
            method="get", url=f"variables/{variable_key}", data_model=VariableResponse
        )

    def list(self) -> VariableCollectionResponse | ServerResponseError:
        """List all variables."""
        try:
            self.response = self.client.get("variables")
            return VariableCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def create(self, variable: VariableBody) -> VariableResponse | ServerResponseError:
        """Create a variable."""
        return super().execute_operation(
            method="post", url="variables", data_model=VariableResponse, json=variable.model_dump()
        )

    def bulk(self, variables: BulkBodyVariableBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple variables."""
        try:
            self.response = self.client.patch("variables", json=variables.model_dump())
            return BulkResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def delete(self, variable_key: str) -> str | ServerResponseError:
        """Delete a variable."""
        return super().execute_operation(
            method="delete", url=f"variables/{variable_key}", return_entry=variable_key
        )

    def update(self, variable: VariableBody) -> VariableResponse | ServerResponseError:
        """Update a variable."""
        return super().execute_operation(
            method="patch",
            url=f"variables/{variable.key}",
            data_model=VariableResponse,
            json=variable.model_dump(),
        )


class VersionOperations(BaseOperations):
    """Version operations."""

    def get(self) -> VersionInfo | ServerResponseError:
        """Get the version."""
        return super().execute_operation(method="get", url="version", data_model=VersionInfo)
