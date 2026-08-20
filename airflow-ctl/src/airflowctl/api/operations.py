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
import functools
import json
import operator
import types
import typing
from typing import TYPE_CHECKING, Any, TypeVar, cast, get_args, get_origin

import httpx
import structlog
from pydantic import BaseModel, RootModel, ValidationError, create_model
from pydantic.fields import FieldInfo

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
    ClearTaskInstancesBody,
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
    PluginCollectionResponse,
    PluginImportErrorCollectionResponse,
    PoolBody,
    PoolCollectionResponse,
    PoolPatchBody,
    PoolResponse,
    ProviderCollectionResponse,
    QueuedEventCollectionResponse,
    QueuedEventResponse,
    TaskDependencyCollectionResponse,
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
    TriggerDAGRunPostBody,
    VariableBody,
    VariableCollectionResponse,
    VariableResponse,
    VersionInfo,
    XComCollectionResponse,
    XComCreateBody,
    XComResponseNative,
    XComUpdateBody,
)
from airflowctl.exceptions import AirflowCtlConnectionException

if TYPE_CHECKING:
    from airflowctl.api.client import Client

log = structlog.get_logger(logger_name=__name__)

T = TypeVar("T", bound=BaseModel)


def _serialize_query_param(value: Any) -> Any:
    # datetime.datetime subclasses datetime.date, so this covers both.
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def _build_query_params(**values: Any) -> dict[str, Any]:
    return {name: _serialize_query_param(value) for name, value in values.items() if value is not None}


# Generic Server Response Error
class ServerResponseError(httpx.HTTPStatusError):
    """Server response error (Generic)."""

    @classmethod
    def from_response(cls, response: httpx.Response) -> ServerResponseError | None:
        if response.status_code < 400:
            return None

        if response.headers.get("content-type") != "application/json":
            return None

        # httpx runs response event hooks before it reads the body, so the body has to be
        # pulled in explicitly here or ``.json()`` raises ``httpx.ResponseNotRead``.
        response.read()

        error_kind = "Client" if response.status_code < 500 else "Server"
        return cls(
            message=f"{error_kind} error message: {response.json()}",
            request=response.request,
            response=response,
        )


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


# Cache of tolerant twins, keyed by the strict model they were built from.
_TOLERANT_MODELS: dict[type[BaseModel], type[BaseModel]] = {}


def _is_polymorphic_union(annotation: Any) -> bool:
    """
    Return whether ``annotation`` is a union with two or more non-``None`` members.

    E.g. ``AssetExpressionAsset | AssetExpressionAlias``. ``X | None`` (one real member) is a
    plain optional and must still be rewritten. A union of two or more models is different:
    pydantic's smart-union picks whichever member the payload merely *fits*, and every field
    of a tolerant twin fits trivially (they are all optional). Put a twin in such a union and
    a payload shaped for member B can silently validate as an empty member A instead of
    raising -- the wrong-shape response is swallowed rather than surfaced. The cost of
    leaving such a union strict is that a field missing anywhere beneath it is not
    tolerated: it surfaces as a validation error rather than degrading to ``None``.
    """
    if get_origin(annotation) not in (typing.Union, types.UnionType):
        return False
    return len([arg for arg in get_args(annotation) if arg is not type(None)]) >= 2


def _tolerate_missing(annotation: Any) -> Any:
    """Rewrite ``annotation``, swapping any nested response model for its tolerant twin."""
    if _is_polymorphic_union(annotation):
        return annotation
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return _tolerant_model(annotation)
    origin = get_origin(annotation)
    if origin is None:
        return annotation
    args = get_args(annotation)
    rewritten = [_tolerate_missing(arg) for arg in args]
    if all(new_arg is old_arg for new_arg, old_arg in zip(rewritten, args)):
        return annotation
    if origin in (typing.Union, types.UnionType):
        return functools.reduce(operator.or_, rewritten)
    return origin[tuple(rewritten)]


def _tolerant_model(model: type[BaseModel]) -> type[BaseModel]:
    """
    Build (or fetch the cached) tolerant twin of ``model``.

    The twin is a ``pydantic.create_model`` subclass of ``model`` with every required field
    -- including ones nested inside it, recursively -- widened to ``X | None`` defaulting to
    ``None``. A field an older server omits is then left honestly unset instead of being
    synthesized to a type default that would be indistinguishable from a real value. Because
    the twin subclasses the original model, ``isinstance(twin_instance, model)`` still holds.

    A ``RootModel`` wraps a single scalar rather than named fields, so "a field is missing"
    has no meaning for it; it is returned unchanged. Polymorphic unions (see
    ``_is_polymorphic_union``) are never rewritten, so no cycle can form even though the
    generated datamodels do contain mutually self-referential schemas (``AssetExpressionAll``
    <-> ``AssetExpressionAny``): every path back to a model already being built runs through
    such a union.
    """
    if model in _TOLERANT_MODELS:
        return _TOLERANT_MODELS[model]
    if issubclass(model, RootModel):
        return model
    overrides: dict[str, Any] = {}
    for field_name, field in model.model_fields.items():
        annotation = _tolerate_missing(field.annotation)
        if field.is_required():
            overrides[field_name] = (annotation | None, FieldInfo.merge_field_infos(field, default=None))
        elif annotation is not field.annotation:
            overrides[field_name] = (annotation, field)
    twin = create_model(f"Partial{model.__name__}", __base__=model, **overrides) if overrides else model
    _TOLERANT_MODELS[model] = twin
    return twin


def validate_response(content: bytes, data_model: type[T]) -> T:
    """
    Validate a server response, tolerating fields an older server does not send.

    The datamodels are generated from the newest Airflow API spec, so a server on an
    older Airflow line can legitimately omit fields the models declare as required. Any
    such field is left ``None`` -- never synthesized to a type default (``False``, ``0``,
    ``""``, ...), which would be indistinguishable from a real value the server actually
    sent. Any other validation error means the response genuinely disagrees with the
    model, which is a defect on one side or the other and must surface instead of being
    papered over.
    """
    try:
        return data_model.model_validate_json(content)
    except ValidationError as validation_error:
        errors = validation_error.errors()
        if any(error["type"] != "missing" for error in errors):
            raise
        log.warning(
            "Response omitted fields this airflowctl requires; leaving them unset (None) "
            "rather than guessing a value. The server is likely on an older Airflow line "
            "than these datamodels.",
            model=data_model.__name__,
            fields=[".".join(str(part) for part in error["loc"]) for error in errors],
        )
        return cast("T", _tolerant_model(data_model).model_validate_json(content))


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

    def execute_list(self, *, path, data_model, offset=0, limit=50, params=None):
        if limit <= 0:
            raise ValueError(f"limit must be a positive integer, got {limit}")

        shared_params = {"limit": limit, **(params or {})}

        self.response = self.client.get(path, params={**shared_params, "offset": offset})
        first_pass = validate_response(self.response.content, data_model)
        total_entries = first_pass.total_entries  # type: ignore[attr-defined]
        # An older server that omits total_entries leaves it None; treat that as "no more
        # pages" rather than raising on the comparison below.
        if total_entries is None or total_entries < limit:
            return first_pass
        found_key = None
        for key, value in first_pass.model_dump().items():
            if key != "total_entries" and isinstance(value, list):
                found_key = key
                break
        entry_list = getattr(first_pass, found_key)
        offset = offset + limit
        while offset < total_entries:
            self.response = self.client.get(path, params={**shared_params, "offset": offset})
            entry = validate_response(self.response.content, data_model)
            offset = offset + limit
            entry_list.extend(getattr(entry, found_key))
        return data_model(**{found_key: entry_list, "total_entries": total_entries})


# Login operations
class LoginOperations:
    """Login operations."""

    def __init__(self, client: Client):
        self.client = client

    def login_with_username_and_password(self, login: LoginBody) -> LoginResponse | ServerResponseError:
        """Login to the API server."""
        return LoginResponse.model_validate_json(
            self.client.post("/token/cli", json=login.model_dump(mode="json")).content
        )


# Operations
class AssetsOperations(BaseOperations):
    """Assets operations."""

    def get(self, asset_id: str) -> AssetResponse | ServerResponseError:
        """Get an asset from the API server."""
        self.response = self.client.get(f"assets/{asset_id}")
        return validate_response(self.response.content, AssetResponse)

    def get_alias(self, asset_alias_id: str) -> AssetAliasResponse | ServerResponseError:
        """Get an asset alias by its ID from the API server."""
        self.response = self.client.get(f"assets/aliases/{asset_alias_id}")
        return validate_response(self.response.content, AssetAliasResponse)

    def list(self) -> AssetCollectionResponse | ServerResponseError:
        """List all assets from the API server."""
        return super().execute_list(path="assets", data_model=AssetCollectionResponse)

    def list_aliases(self) -> AssetAliasCollectionResponse | ServerResponseError:
        """List all assets aliases from the API server."""
        return super().execute_list(path="/assets/aliases", data_model=AssetAliasCollectionResponse)

    def create_event(
        self, asset_event_body: CreateAssetEventsBody
    ) -> AssetEventResponse | ServerResponseError:
        """Create an asset event."""
        # Ensure extra is initialised before sent to API
        if asset_event_body.extra is None:
            asset_event_body.extra = {}
        self.response = self.client.post(
            "assets/events", json=asset_event_body.model_dump(mode="json", exclude_none=True)
        )
        return validate_response(self.response.content, AssetEventResponse)

    def materialize(self, asset_id: str) -> DAGRunResponse | ServerResponseError:
        """Materialize an asset."""
        self.response = self.client.post(f"assets/{asset_id}/materialize")
        return validate_response(self.response.content, DAGRunResponse)

    def get_queued_events(self, asset_id: str) -> QueuedEventCollectionResponse | ServerResponseError:
        """Get queued events for an asset."""
        self.response = self.client.get(f"assets/{asset_id}/queuedEvents")
        return validate_response(self.response.content, QueuedEventCollectionResponse)

    def get_dag_queued_events(
        self, dag_id: str, before: str
    ) -> QueuedEventCollectionResponse | ServerResponseError:
        """Get queued events for a dag."""
        self.response = self.client.get(f"dags/{dag_id}/assets/queuedEvents", params={"before": before})
        return validate_response(self.response.content, QueuedEventCollectionResponse)

    def get_dag_queued_event(self, dag_id: str, asset_id: str) -> QueuedEventResponse | ServerResponseError:
        """Get a queued event for a dag."""
        self.response = self.client.get(f"dags/{dag_id}/assets/{asset_id}/queuedEvents")
        return validate_response(self.response.content, QueuedEventResponse)

    def delete_queued_events(self, asset_id: str) -> str | ServerResponseError:
        """Delete a queued event for an asset."""
        self.client.delete(f"assets/{asset_id}/queuedEvents")
        return asset_id

    def delete_dag_queued_events(self, dag_id: str, before: str) -> str | ServerResponseError:
        """Delete a queued event for a Dag."""
        self.client.delete(f"dags/{dag_id}/assets/queuedEvents", params={"before": before})
        return dag_id

    def delete_queued_event(self, dag_id: str, asset_id: str) -> str | ServerResponseError:
        """Delete a queued event for a Dag."""
        self.client.delete(f"dags/{dag_id}/assets/{asset_id}/queuedEvents")
        return asset_id


class BackfillOperations(BaseOperations):
    """Backfill operations."""

    def create(self, backfill: BackfillPostBody) -> BackfillResponse | ServerResponseError:
        """Create a backfill."""
        self.response = self.client.post(
            "backfills", json=backfill.model_dump(mode="json", exclude_none=True)
        )
        return validate_response(self.response.content, BackfillResponse)

    def create_dry_run(self, backfill: BackfillPostBody) -> BackfillResponse | ServerResponseError:
        """Create a dry run backfill."""
        self.response = self.client.post(
            "backfills/dry_run", json=backfill.model_dump(mode="json", exclude_none=True)
        )
        return validate_response(self.response.content, BackfillResponse)

    def get(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Get a backfill."""
        self.response = self.client.get(f"backfills/{backfill_id}")
        return validate_response(self.response.content, BackfillResponse)

    def list(self, dag_id: str) -> BackfillCollectionResponse | ServerResponseError:
        """List all backfills."""
        params = {"dag_id": dag_id}
        return super().execute_list(path="backfills", data_model=BackfillCollectionResponse, params=params)

    def pause(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Pause a backfill."""
        self.response = self.client.post(f"backfills/{backfill_id}/pause")
        return validate_response(self.response.content, BackfillResponse)

    def unpause(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Unpause a backfill."""
        self.response = self.client.post(f"backfills/{backfill_id}/unpause")
        return validate_response(self.response.content, BackfillResponse)

    def cancel(self, backfill_id: str) -> BackfillResponse | ServerResponseError:
        """Cancel a backfill."""
        self.response = self.client.post(f"backfills/{backfill_id}/cancel")
        return validate_response(self.response.content, BackfillResponse)


class ConfigOperations(BaseOperations):
    """Config operations."""

    def get(self, section: str, option: str) -> Config | ServerResponseError:
        """Get a config from the API server."""
        self.response = self.client.get(f"/config/section/{section}/option/{option}")
        return validate_response(self.response.content, Config)

    def list(self) -> Config | ServerResponseError:
        """List all configs from the API server."""
        self.response = self.client.get("/config")
        return validate_response(self.response.content, Config)


class ConnectionsOperations(BaseOperations):
    """Connection operations."""

    def get(self, conn_id: str) -> ConnectionResponse | ServerResponseError:
        """Get a connection from the API server."""
        self.response = self.client.get(f"connections/{conn_id}")
        return validate_response(self.response.content, ConnectionResponse)

    def list(self) -> ConnectionCollectionResponse | ServerResponseError:
        """List all connections from the API server."""
        return super().execute_list(path="connections", data_model=ConnectionCollectionResponse)

    def create(
        self,
        connection: ConnectionBody,
    ) -> ConnectionResponse | ServerResponseError:
        """Create a connection."""
        self.response = self.client.post(
            "connections", json=connection.model_dump(mode="json", by_alias=True, exclude_none=True)
        )
        return validate_response(self.response.content, ConnectionResponse)

    def bulk(self, connections: BulkBodyConnectionBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple connections."""
        self.response = self.client.patch(
            "connections", json=connections.model_dump(mode="json", by_alias=True)
        )
        return validate_response(self.response.content, BulkResponse)

    def create_defaults(self) -> None | ServerResponseError:
        """Create default connections."""
        self.response = self.client.post("connections/defaults")
        return None

    def delete(self, conn_id: str) -> str | ServerResponseError:
        """Delete a connection."""
        self.client.delete(f"connections/{conn_id}")
        return conn_id

    def update(
        self,
        connection: ConnectionBody,
    ) -> ConnectionResponse | ServerResponseError:
        """Update a connection."""
        self.response = self.client.patch(
            f"connections/{connection.connection_id}",
            json=connection.model_dump(mode="json", by_alias=True),
        )
        return validate_response(self.response.content, ConnectionResponse)

    def test(
        self,
        connection: ConnectionBody,
    ) -> ConnectionTestResponse | ServerResponseError:
        """Test a connection."""
        self.response = self.client.post(
            "connections/test", json=connection.model_dump(mode="json", by_alias=True)
        )
        return validate_response(self.response.content, ConnectionTestResponse)


class DagsOperations(BaseOperations):
    """Dags operations."""

    def get(self, dag_id: str) -> DAGResponse | ServerResponseError:
        """Get a Dag."""
        self.response = self.client.get(f"dags/{dag_id}")
        return validate_response(self.response.content, DAGResponse)

    def get_details(self, dag_id: str) -> DAGDetailsResponse | ServerResponseError:
        """Get a Dag details."""
        self.response = self.client.get(f"dags/{dag_id}/details")
        return validate_response(self.response.content, DAGDetailsResponse)

    def get_tags(self) -> DAGTagCollectionResponse | ServerResponseError:
        """Get all Dag tags."""
        return super().execute_list(path="dagTags", data_model=DAGTagCollectionResponse)

    def list(self) -> DAGCollectionResponse | ServerResponseError:
        """List DAGs."""
        return super().execute_list(path="dags", data_model=DAGCollectionResponse)

    def update(self, dag_id: str, dag_body: DAGPatchBody) -> DAGResponse | ServerResponseError:
        self.response = self.client.patch(f"dags/{dag_id}", json=dag_body.model_dump(mode="json"))
        return validate_response(self.response.content, DAGResponse)

    def delete(self, dag_id: str) -> str | ServerResponseError:
        self.client.delete(f"dags/{dag_id}")
        return dag_id

    def get_import_error(self, import_error_id: str) -> ImportErrorResponse | ServerResponseError:
        self.response = self.client.get(f"importErrors/{import_error_id}")
        return validate_response(self.response.content, ImportErrorResponse)

    def list_import_errors(self) -> ImportErrorCollectionResponse | ServerResponseError:
        return super().execute_list(path="importErrors", data_model=ImportErrorCollectionResponse)

    def get_stats(self, dag_ids: list) -> DagStatsCollectionResponse | ServerResponseError:  # type: ignore
        self.response = self.client.get("dagStats", params={"dag_ids": dag_ids})
        return validate_response(self.response.content, DagStatsCollectionResponse)

    def get_version(self, dag_id: str, version_number: int) -> DagVersionResponse | ServerResponseError:
        self.response = self.client.get(f"dags/{dag_id}/dagVersions/{version_number}")
        return validate_response(self.response.content, DagVersionResponse)

    def list_version(self, dag_id: str) -> DAGVersionCollectionResponse | ServerResponseError:
        return super().execute_list(
            path=f"dags/{dag_id}/dagVersions", data_model=DAGVersionCollectionResponse
        )

    def list_warning(self) -> DAGWarningCollectionResponse | ServerResponseError:
        return super().execute_list(path="dagWarnings", data_model=DAGWarningCollectionResponse)

    def trigger(
        self, dag_id: str, trigger_dag_run: TriggerDAGRunPostBody
    ) -> DAGRunResponse | ServerResponseError:
        """Create a Dag run."""
        if trigger_dag_run.conf is None:
            trigger_dag_run.conf = {}
        self.response = self.client.post(
            f"dags/{dag_id}/dagRuns", json=trigger_dag_run.model_dump(mode="json")
        )
        return validate_response(self.response.content, DAGRunResponse)


class DagRunOperations(BaseOperations):
    """Dag run operations."""

    def get(
        self, dag_id: str, dag_run_id: str, *, suppress_error_log: bool = False
    ) -> DAGRunResponse | ServerResponseError:
        """Get a Dag run."""
        self.response = self.client.get(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}",
            extensions={"airflowctl_suppress_error_log": suppress_error_log},
        )
        return validate_response(self.response.content, DAGRunResponse)

    def list(
        self,
        state: str | None = None,
        limit: int = 100,
        offset: int | None = None,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        dag_id: str | None = None,
        logical_date_gte: datetime.datetime | None = None,
        logical_date_lte: datetime.datetime | None = None,
        partition_date_gte: datetime.date | None = None,
        partition_date_lte: datetime.date | None = None,
        order_by: str | None = None,
        partition_key_pattern: str | None = None,
        *,
        suppress_error_log: bool = False,
    ) -> DAGRunCollectionResponse | ServerResponseError:
        """
        List Dag runs (at most `limit` results).

        Args:
            state: Filter Dag runs by state (optional; no filter applied when omitted)
            start_date: Filter Dag runs by start date (optional)
            end_date: Filter Dag runs by end date (optional)
            limit: Limit the number of results returned
            offset: Offset to start returning results from
            dag_id: The Dag ID to filter by. If None, retrieves Dag runs for all Dags (using "~").
            logical_date_gte: Filter Dag runs with a logical date greater than or equal to this value.
            logical_date_lte: Filter Dag runs with a logical date less than or equal to this value.
            partition_date_gte: Inclusive lower bound of the partition_date window, as a local
                calendar day in the Dag's timetable timezone.
            partition_date_lte: Inclusive upper bound of the partition_date window, as a local
                calendar day in the Dag's timetable timezone.
            order_by: Order the results by the specified field.
            partition_key_pattern: Filter Dag runs by partition key pattern.
            suppress_error_log: Skip client-side error logging, for callers handling the error themselves.
        """
        # Use "~" for all Dags if dag_id is not specified
        if not dag_id:
            dag_id = "~"

        params = _build_query_params(
            limit=limit,
            offset=offset,
            state=str(state) if state is not None else None,
            start_date=start_date,
            end_date=end_date,
            logical_date_gte=logical_date_gte,
            logical_date_lte=logical_date_lte,
            partition_date_gte=partition_date_gte,
            partition_date_lte=partition_date_lte,
            order_by=order_by,
            partition_key_pattern=partition_key_pattern,
        )

        self.response = self.client.get(
            f"/dags/{dag_id}/dagRuns",
            params=params,
            extensions={"airflowctl_suppress_error_log": suppress_error_log},
        )
        return validate_response(self.response.content, DAGRunCollectionResponse)

    def delete(self, dag_id: str, dag_run_id: str) -> str | ServerResponseError:
        """Delete a Dag run."""
        self.client.delete(f"/dags/{dag_id}/dagRuns/{dag_run_id}")
        return dag_run_id


class JobsOperations(BaseOperations):
    """Job operations."""

    def list(
        self,
        job_type: str | None = None,
        hostname: str | None = None,
        is_alive: bool | None = None,
        dag_id: str | None = None,
        state: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        order_by: str | None = None,
    ) -> JobCollectionResponse | ServerResponseError:
        """List all jobs."""
        params = _build_query_params(
            job_type=job_type or None,
            hostname=hostname or None,
            is_alive=is_alive,
            dag_id=dag_id or None,
            job_state=state or None,
            order_by=order_by or "-start_date",
            limit=limit,
            offset=offset,
        )

        if limit is not None or offset is not None:
            self.response = self.client.get("jobs", params=params)
            return validate_response(self.response.content, JobCollectionResponse)

        return super().execute_list(path="jobs", data_model=JobCollectionResponse, params=params)


class PoolsOperations(BaseOperations):
    """Pool operations."""

    def get(self, pool_name: str) -> PoolResponse | ServerResponseError:
        """Get a pool."""
        self.response = self.client.get(f"pools/{pool_name}")
        return validate_response(self.response.content, PoolResponse)

    def list(self) -> PoolCollectionResponse | ServerResponseError:
        """List all pools."""
        return super().execute_list(path="pools", data_model=PoolCollectionResponse)

    def create(self, pool: PoolBody) -> PoolResponse | ServerResponseError:
        """Create a pool."""
        self.response = self.client.post("pools", json=pool.model_dump(mode="json", exclude_none=True))
        return validate_response(self.response.content, PoolResponse)

    def bulk(self, pools: BulkBodyPoolBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple pools."""
        self.response = self.client.patch("pools", json=pools.model_dump(mode="json"))
        return validate_response(self.response.content, BulkResponse)

    def delete(self, pool: str) -> str | ServerResponseError:
        """Delete a pool."""
        self.client.delete(f"pools/{pool}")
        return pool

    def update(self, pool_body: PoolPatchBody) -> PoolResponse | ServerResponseError:
        """Update a pool."""
        self.response = self.client.patch(f"pools/{pool_body.pool}", json=pool_body.model_dump(mode="json"))
        return validate_response(self.response.content, PoolResponse)


class ProvidersOperations(BaseOperations):
    """Provider operations."""

    def list(self) -> ProviderCollectionResponse | ServerResponseError:
        """List all providers."""
        return super().execute_list(path="providers", data_model=ProviderCollectionResponse)


def _build_task_instance_path(dag_id: str, dag_run_id: str, task_id: str, map_index: int | None) -> str:
    """Build the task instance API path, addressing a mapped task instance when map_index is given."""
    path = f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}"
    if map_index is not None and map_index >= 0:
        path = f"{path}/{map_index}"
    return path


class TaskInstancesOperations(BaseOperations):
    """Task instance operations."""

    def get(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        map_index: int | None = None,
        *,
        suppress_error_log: bool = False,
    ) -> TaskInstanceResponse | ServerResponseError:
        """Get a task instance for a Dag run."""
        path = _build_task_instance_path(
            dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, map_index=map_index
        )
        self.response = self.client.get(
            path,
            extensions={"airflowctl_suppress_error_log": suppress_error_log},
        )
        return validate_response(self.response.content, TaskInstanceResponse)

    def get_dependencies(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        map_index: int | None = None,
        *,
        suppress_error_log: bool = False,
    ) -> TaskDependencyCollectionResponse | ServerResponseError:
        """Get unmet scheduler dependencies for a task instance."""
        path = _build_task_instance_path(
            dag_id=dag_id, dag_run_id=dag_run_id, task_id=task_id, map_index=map_index
        )
        self.response = self.client.get(
            f"{path}/dependencies",
            extensions={"airflowctl_suppress_error_log": suppress_error_log},
        )
        return validate_response(self.response.content, TaskDependencyCollectionResponse)

    def list(self, dag_id: str, dag_run_id: str) -> TaskInstanceCollectionResponse | ServerResponseError:
        """List task instances for a Dag run."""
        return super().execute_list(
            path=f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances",
            data_model=TaskInstanceCollectionResponse,
        )


class TasksOperations(BaseOperations):
    """Tasks operations."""

    def clear(
        self, dag_id: str, clear_task_instances: ClearTaskInstancesBody
    ) -> TaskInstanceCollectionResponse | ServerResponseError:
        """Clear task instances of a Dag; with dry_run (the default) only previews the affected task instances."""
        self.response = self.client.post(
            f"dags/{dag_id}/clearTaskInstances",
            json=clear_task_instances.model_dump(mode="json", exclude_none=True),
        )
        return validate_response(self.response.content, TaskInstanceCollectionResponse)


class VariablesOperations(BaseOperations):
    """Variable operations."""

    def get(self, variable_key: str) -> VariableResponse | ServerResponseError:
        """Get a variable."""
        self.response = self.client.get(f"variables/{variable_key}")
        return validate_response(self.response.content, VariableResponse)

    def list(self) -> VariableCollectionResponse | ServerResponseError:
        """List all variables."""
        return super().execute_list(path="variables", data_model=VariableCollectionResponse)

    def create(self, variable: VariableBody) -> VariableResponse | ServerResponseError:
        """Create a variable."""
        self.response = self.client.post(
            "variables", json=variable.model_dump(mode="json", exclude_none=True)
        )
        return validate_response(self.response.content, VariableResponse)

    def bulk(self, variables: BulkBodyVariableBody) -> BulkResponse | ServerResponseError:
        """CRUD multiple variables."""
        self.response = self.client.patch("variables", json=variables.model_dump(mode="json"))
        return validate_response(self.response.content, BulkResponse)

    def delete(self, variable_key: str) -> str | ServerResponseError:
        """Delete a variable."""
        self.client.delete(f"variables/{variable_key}")
        return variable_key

    def update(self, variable: VariableBody) -> VariableResponse | ServerResponseError:
        """Update a variable."""
        self.response = self.client.patch(f"variables/{variable.key}", json=variable.model_dump(mode="json"))
        return validate_response(self.response.content, VariableResponse)


class VersionOperations(BaseOperations):
    """Version operations."""

    def get(self) -> VersionInfo | ServerResponseError:
        """Get the version."""
        self.response = self.client.get("version")
        return validate_response(self.response.content, VersionInfo)


class XComOperations(BaseOperations):
    """XCom operations."""

    def get(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        key: str,
        map_index: int = None,  # type: ignore
    ) -> XComResponseNative | ServerResponseError:
        """Get an XCom entry."""
        params: dict[str, Any] = {}
        if map_index is not None:
            params["map_index"] = map_index
        self.response = self.client.get(
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{key}",
            params=params,
        )
        return validate_response(self.response.content, XComResponseNative)

    def list(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        map_index: int = None,  # type: ignore
        key: str = None,  # type: ignore
    ) -> XComCollectionResponse | ServerResponseError:
        """List XCom entries."""
        params: dict[str, Any] = {}
        if map_index is not None:
            params["map_index"] = map_index
        if key is not None:
            params["xcom_key"] = key
        return super().execute_list(
            path=f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries",
            data_model=XComCollectionResponse,
            params=params,
        )

    def add(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        key: str,
        value: str,
        map_index: int = None,  # type: ignore
    ) -> XComResponseNative | ServerResponseError:
        """Add an XCom entry."""
        try:
            parsed_value = json.loads(value)
        except (ValueError, TypeError):
            parsed_value = value

        body_dict: dict[str, Any] = {"key": key, "value": parsed_value}
        if map_index is not None:
            body_dict["map_index"] = map_index
        body = XComCreateBody(**body_dict)
        self.response = self.client.post(
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries",
            json=body.model_dump(mode="json", exclude_unset=True, exclude_none=True),
        )
        return validate_response(self.response.content, XComResponseNative)

    def edit(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        key: str,
        value: str,
        map_index: int = None,  # type: ignore
    ) -> XComResponseNative | ServerResponseError:
        """Edit an XCom entry."""
        try:
            parsed_value = json.loads(value)
        except (ValueError, TypeError):
            parsed_value = value

        body_dict: dict[str, Any] = {"value": parsed_value}
        if map_index is not None:
            body_dict["map_index"] = map_index
        body = XComUpdateBody(**body_dict)
        self.response = self.client.patch(
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{key}",
            json=body.model_dump(mode="json", exclude_unset=True, exclude_none=True),
        )
        return validate_response(self.response.content, XComResponseNative)

    def delete(
        self,
        dag_id: str,
        dag_run_id: str,
        task_id: str,
        key: str,
        map_index: int = None,  # type: ignore
    ) -> str | ServerResponseError:
        """Delete an XCom entry."""
        params: dict[str, Any] = {}
        if map_index is not None:
            params["map_index"] = map_index
        self.client.delete(
            f"dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries/{key}",
            params=params,
        )
        return key


class PluginsOperations(BaseOperations):
    """Plugins operations."""

    def list(self) -> PluginCollectionResponse | ServerResponseError:
        """List all plugins from the API server."""
        return super().execute_list(path="plugins", data_model=PluginCollectionResponse)

    def list_import_errors(self) -> PluginImportErrorCollectionResponse | ServerResponseError:
        """List plugin import errors from the API server."""
        self.response = self.client.get("plugins/importErrors")
        return validate_response(self.response.content, PluginImportErrorCollectionResponse)
