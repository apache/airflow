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
from typing import TYPE_CHECKING, Any, TypeVar

import httpx
import structlog
from pydantic import BaseModel

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

if TYPE_CHECKING:
    from airflowctl.api.client import Client

log = structlog.get_logger(logger_name=__name__)

T = TypeVar("T", bound=BaseModel)


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

    def execute_list(
        self,
        *,
        path: str,
        data_model: type[T],
        offset: int = 0,
        limit: int = 50,
        params: dict | None = None,
    ) -> T | ServerResponseError:
        shared_params = {**(params or {})}
        self.response = self.client.get(path, params=shared_params)
        first_pass = data_model.model_validate_json(self.response.content)
        total_entries = first_pass.total_entries  # type: ignore[attr-defined]
        if total_entries < limit:
            return first_pass
        for key, value in first_pass.model_dump().items():
            if key != "total_entries" and isinstance(value, list):
                break
        entry_list = getattr(first_pass, key)
        offset = offset + limit
        while offset < total_entries:
            self.response = self.client.get(path, params={**shared_params, "offset": offset})
            entry = data_model.model_validate_json(self.response.content)
            offset = offset + limit
            entry_list.extend(getattr(entry, key))
        obj = data_model(**{key: entry_list, "total_entries": total_entries})
        return data_model.model_validate(obj.model_dump())


# Login operations
class LoginOperations:
    """Login operations."""

    def __init__(self, client: Client):
        self.client = client

    def login_with_username_and_password(self, login: LoginBody) -> LoginResponse | ServerResponseError:
        """Login to the API server."""
        try:
            return LoginResponse.model_validate_json(
                self.client.post("/token/cli", json=login.model_dump(mode="json")).content
            )
        except ServerResponseError as e:
            raise e


# Operations
class AssetsOperations(BaseOperations):
    """Assets operations."""

    def get(self, asset_id: str) -> AssetResponse | ServerResponseError:
        """Get an asset from the API server."""
        try:
            self.response = self.client.get(f"assets/{asset_id}")
            return AssetResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def get_by_alias(self, alias: str) -> AssetAliasResponse | ServerResponseError:
        """Get an asset by alias from the API server."""
        try:
            self.response = self.client.get(f"assets/aliases/{alias}")
            return AssetAliasResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def list(self) -> AssetCollectionResponse | ServerResponseError:
        """List all assets from the API server."""
        return super().execute_list(path="assets", data_model=AssetCollectionResponse)

    def list_by_alias(self) -> AssetAliasCollectionResponse | ServerResponseError:
        """List all assets by alias from the API server."""
        return super().execute_list(path="/assets/aliases", data_model=AssetAliasCollectionResponse)

    def create_event(
        self, asset_event_body: CreateAssetEventsBody
    ) -> AssetEventResponse | ServerResponseError:
        """Create an asset event."""
        try:
            # Ensure extra is initialised before sent to API
            if asset_event_body.extra is None:
                asset_event_body.extra = {}
            self.response = self.client.post("assets/events", json=asset_event_body.model_dump(mode="json"))
            return AssetEventResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def materialize(self, asset_id: str) -> DAGRunResponse | ServerResponseError:
        """Materialize an asset."""
        try:
            self.response = self.client.post(f"assets/{asset_id}/materialize")
            return DAGRunResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def get_queued_events(self, asset_id: str) -> QueuedEventCollectionResponse | ServerResponseError:
        """Get queued events for an asset."""
        try:
            self.response = self.client.get(f"assets/{asset_id}/queuedEvents")
            return QueuedEventCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def get_dag_queued_events(
        self, dag_id: str, before: str
    ) -> QueuedEventCollectionResponse | ServerResponseError:
        """Get queued events for a dag."""
        try:
            self.response = self.client.get(f"dags/{dag_id}/assets/queuedEvents", params={"before": before})
            return QueuedEventCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def get_dag_queued_event(self, dag_id: str, asset_id: str) -> QueuedEventResponse | ServerResponseError:
        """Get a queued event for a dag."""
        try:
            self.response = self.client.get(f"dags/{dag_id}/assets/{asset_id}/queuedEvents")
            return QueuedEventResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def delete_queued_events(self, asset_id: str) -> str | ServerResponseError:
        """Delete a queued event for an asset."""
        try:
            self.client.delete(f"assets/{asset_id}/queuedEvents/")
            return asset_id
        except ServerResponseError as e:
            raise e

    def delete_dag_queued_events(self, dag_id: str, before: str) -> str | ServerResponseError:
        """Delete a queued event for a dag."""
        try:
            self.client.delete(f"assets/dags/{dag_id}/queuedEvents", params={"before": before})
            return dag_id
        except ServerResponseError as e:
            raise e

    def delete_queued_event(self, dag_id: str, asset_id: str) -> str | ServerResponseError:
        """Delete a queued event for a dag."""
        try:
            self.client.delete(f"assets/dags/{dag_id}/assets/{asset_id}/queuedEvents/")
            return asset_id
        except ServerResponseError as e:
            raise e


class BackfillOperations(BaseOperations):
    """Backfill operations."""

    def create(self, backfill: BackfillPostBody) -> BackfillResponse | ServerResponseError:
        """Create a backfill."""
        try:
            self.response = self.client.post("backfills", data=backfill.model_dump(mode="json"))
            return BackfillResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def create_dry_run(self, backfill: BackfillPostBody) -> BackfillResponse | ServerResponseError:
        """Create a dry run backfill."""
        try:
            self.response = self.client.post("backfills/dry_run", data=backfill.model_dump(mode="json"))
            return BackfillResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def get(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Get a backfill."""
        try:
            self.response = self.client.get(f"backfills/{backfill_id}")
            return BackfillResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def list(self, dag_id: str) -> BackfillCollectionResponse | ServerResponseError:
        """List all backfills."""
        params = {"dag_id": dag_id}
        return super().execute_list(path="backfills", data_model=BackfillCollectionResponse, params=params)

    def pause(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Pause a backfill."""
        try:
            self.response = self.client.post(f"backfills/{backfill_id}/pause")
            return BackfillResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def unpause(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Unpause a backfill."""
        try:
            self.response = self.client.post(f"backfills/{backfill_id}/unpause")
            return BackfillResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def cancel(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Cancel a backfill."""
        try:
            self.response = self.client.post(f"backfills/{backfill_id}/cancel")
            return BackfillResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e


class ConfigOperations(BaseOperations):
    """Config operations."""

    def get(self, section: str, option: str) -> Config | ServerResponseError:
        """Get a config from the API server."""
        try:
            self.response = self.client.get(f"/config/section/{section}/option/{option}")
            return Config.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def list(self) -> Config | ServerResponseError:
        """List all configs from the API server."""
        try:
            self.response = self.client.get("/config")
            return Config.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e


class ConnectionsOperations(BaseOperations):
    """Connection operations."""

    def get(self, conn_id: str) -> ConnectionResponse | ServerResponseError:
        """Get a connection from the API server."""
        try:
            self.response = self.client.get(f"connections/{conn_id}")
            return ConnectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def list(self) -> ConnectionCollectionResponse | ServerResponseError:
        """List all connections from the API server."""
        return super().execute_list(path="connections", data_model=ConnectionCollectionResponse)

    def create(
        self,
        connection: ConnectionBody,
    ) -> ConnectionResponse | ServerResponseError:
        """Create a connection."""
        try:
            self.response = self.client.post("connections", json=connection.model_dump(mode="json"))
            return ConnectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def bulk(self, connections: BulkBodyConnectionBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple connections."""
        try:
            self.response = self.client.patch("connections", json=connections.model_dump(mode="json"))
            return BulkResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def create_defaults(self) -> None | ServerResponseError:
        """Create default connections."""
        try:
            self.response = self.client.post("connections/defaults")
            return None
        except ServerResponseError as e:
            raise e

    def delete(self, conn_id: str) -> str | ServerResponseError:
        """Delete a connection."""
        try:
            self.client.delete(f"connections/{conn_id}")
            return conn_id
        except ServerResponseError as e:
            raise e

    def update(
        self,
        connection: ConnectionBody,
    ) -> ConnectionResponse | ServerResponseError:
        """Update a connection."""
        try:
            self.response = self.client.patch(
                f"connections/{connection.connection_id}", json=connection.model_dump(mode="json")
            )
            return ConnectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def test(
        self,
        connection: ConnectionBody,
    ) -> ConnectionTestResponse | ServerResponseError:
        """Test a connection."""
        try:
            self.response = self.client.post("connections/test", json=connection.model_dump(mode="json"))
            return ConnectionTestResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e


class DagsOperations(BaseOperations):
    """Dags operations."""

    def get(self, dag_id: str) -> DAGResponse | ServerResponseError:
        """Get a DAG."""
        try:
            self.response = self.client.get(f"dags/{dag_id}")
            return DAGResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def get_details(self, dag_id: str) -> DAGDetailsResponse | ServerResponseError:
        """Get a DAG details."""
        try:
            self.response = self.client.get(f"dags/{dag_id}/details")
            return DAGDetailsResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def get_tags(self) -> DAGTagCollectionResponse | ServerResponseError:
        """Get all DAG tags."""
        return super().execute_list(path="dagTags", data_model=DAGTagCollectionResponse)

    def list(self) -> DAGCollectionResponse | ServerResponseError:
        """List DAGs."""
        return super().execute_list(path="dags", data_model=DAGCollectionResponse)

    def update(self, dag_id: str, dag_body: DAGPatchBody) -> DAGResponse | ServerResponseError:
        try:
            self.response = self.client.patch(f"dags/{dag_id}", json=dag_body.model_dump(mode="json"))
            return DAGResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def delete(self, dag_id: str) -> str | ServerResponseError:
        try:
            self.client.delete(f"dags/{dag_id}")
            return dag_id
        except ServerResponseError as e:
            raise e

    def get_import_error(self, import_error_id: str) -> ImportErrorResponse | ServerResponseError:
        try:
            self.response = self.client.get(f"importErrors/{import_error_id}")
            return ImportErrorResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def list_import_errors(self) -> ImportErrorCollectionResponse | ServerResponseError:
        return super().execute_list(path="importErrors", data_model=ImportErrorCollectionResponse)

    def get_stats(self, dag_ids: list) -> DagStatsCollectionResponse | ServerResponseError:  # type: ignore
        try:
            self.response = self.client.get("dagStats", params={"dag_ids": dag_ids})
            return DagStatsCollectionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def get_version(self, dag_id: str, version_number: int) -> DagVersionResponse | ServerResponseError:
        try:
            self.response = self.client.get(f"dags/{dag_id}/dagVersions/{version_number}")
            return DagVersionResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def list_version(self, dag_id: str) -> DAGVersionCollectionResponse | ServerResponseError:
        return super().execute_list(
            path=f"dags/{dag_id}/dagVersions", data_model=DAGVersionCollectionResponse
        )

    def list_warning(self) -> DAGWarningCollectionResponse | ServerResponseError:
        return super().execute_list(path="dagWarnings", data_model=DAGWarningCollectionResponse)

    def trigger(
        self, dag_id: str, trigger_dag_run: TriggerDAGRunPostBody
    ) -> DAGRunResponse | ServerResponseError:
        """Create a dag run."""
        if trigger_dag_run.conf is None:
            trigger_dag_run.conf = {}
        try:
            self.response = self.client.post(
                f"dags/{dag_id}/dagRuns", json=trigger_dag_run.model_dump(mode="json")
            )
            return DAGRunResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e


class DagRunOperations(BaseOperations):
    """Dag run operations."""

    def get(self, dag_id: str, dag_run_id: str) -> DAGRunResponse | ServerResponseError:
        """Get a dag run."""
        try:
            self.response = self.client.get(f"/dags/{dag_id}/dagRuns/{dag_run_id}")
            return DAGRunResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

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
            "dag_id": dag_id,
        }
        return super().execute_list(
            path=f"/dags/{dag_id}/dagRuns", data_model=DAGRunCollectionResponse, params=params
        )


class JobsOperations(BaseOperations):
    """Job operations."""

    def list(
        self, job_type: str, hostname: str, is_alive: bool
    ) -> JobCollectionResponse | ServerResponseError:
        """List all jobs."""
        params = {"job_type": job_type, "hostname": hostname, "is_alive": is_alive}
        return super().execute_list(path="jobs", data_model=JobCollectionResponse, params=params)


class PoolsOperations(BaseOperations):
    """Pool operations."""

    def get(self, pool_name: str) -> PoolResponse | ServerResponseError:
        """Get a pool."""
        try:
            self.response = self.client.get(f"pools/{pool_name}")
            return PoolResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def list(self) -> PoolCollectionResponse | ServerResponseError:
        """List all pools."""
        return super().execute_list(path="pools", data_model=PoolCollectionResponse)

    def create(self, pool: PoolBody) -> PoolResponse | ServerResponseError:
        """Create a pool."""
        try:
            self.response = self.client.post("pools", json=pool.model_dump(mode="json"))
            return PoolResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def bulk(self, pools: BulkBodyPoolBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple pools."""
        try:
            self.response = self.client.patch("pools", json=pools.model_dump(mode="json"))
            return BulkResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def delete(self, pool: str) -> str | ServerResponseError:
        """Delete a pool."""
        try:
            self.client.delete(f"pools/{pool}")
            return pool
        except ServerResponseError as e:
            raise e

    def update(self, pool_body: PoolPatchBody) -> PoolResponse | ServerResponseError:
        """Update a pool."""
        try:
            self.response = self.client.patch(
                f"pools/{pool_body.pool}", json=pool_body.model_dump(mode="json")
            )
            return PoolResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e


class ProvidersOperations(BaseOperations):
    """Provider operations."""

    def list(self) -> ProviderCollectionResponse | ServerResponseError:
        """List all providers."""
        return super().execute_list(path="providers", data_model=ProviderCollectionResponse)


class VariablesOperations(BaseOperations):
    """Variable operations."""

    def get(self, variable_key: str) -> VariableResponse | ServerResponseError:
        """Get a variable."""
        try:
            self.response = self.client.get(f"variables/{variable_key}")
            return VariableResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def list(self) -> VariableCollectionResponse | ServerResponseError:
        """List all variables."""
        return super().execute_list(path="variables", data_model=VariableCollectionResponse)

    def create(self, variable: VariableBody) -> VariableResponse | ServerResponseError:
        """Create a variable."""
        try:
            self.response = self.client.post("variables", json=variable.model_dump(mode="json"))
            return VariableResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def bulk(self, variables: BulkBodyVariableBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple variables."""
        try:
            self.response = self.client.patch("variables", json=variables.model_dump(mode="json"))
            return BulkResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e

    def delete(self, variable_key: str) -> str | ServerResponseError:
        """Delete a variable."""
        try:
            self.client.delete(f"variables/{variable_key}")
            return variable_key
        except ServerResponseError as e:
            raise e

    def update(self, variable: VariableBody) -> VariableResponse | ServerResponseError:
        """Update a variable."""
        try:
            self.response = self.client.patch(
                f"variables/{variable.key}", json=variable.model_dump(mode="json")
            )
            return VariableResponse.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e


class VersionOperations(BaseOperations):
    """Version operations."""

    def get(self) -> VersionInfo | ServerResponseError:
        """Get the version."""
        try:
            self.response = self.client.get("version")
            return VersionInfo.model_validate_json(self.response.content)
        except ServerResponseError as e:
            raise e
