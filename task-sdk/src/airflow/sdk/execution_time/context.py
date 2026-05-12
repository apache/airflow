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

import collections
import contextlib
import functools
import inspect
from collections.abc import Generator, Iterable, Iterator, Mapping, Sequence
from datetime import datetime
from functools import cache
from typing import TYPE_CHECKING, Any, Generic, TypeVar, overload
from uuid import UUID

import attrs
import structlog

from airflow.sdk.definitions._internal.contextmanager import _CURRENT_CONTEXT
from airflow.sdk.definitions._internal.types import NOTSET
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetAliasEvent,
    AssetAliasUniqueKey,
    AssetNameRef,
    AssetRef,
    AssetUniqueKey,
    AssetUriRef,
    BaseAssetUniqueKey,
)
from airflow.sdk.exceptions import AirflowNotFoundException, AirflowRuntimeError, ErrorType
from airflow.sdk.log import mask_secret

if TYPE_CHECKING:
    from pydantic.types import JsonValue
    from typing_extensions import Self

    from airflow.sdk import Variable
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.execution_time.comms import (
        AssetEventDagRunReferenceResult,
        AssetEventResult,
        AssetEventsResult,
        AssetResult,
        ConnectionResult,
        OKResponse,
        PrevSuccessfulDagRunResponse,
        ReceiveMsgType,
        VariableResult,
    )
    from airflow.sdk.types import OutletEventAccessorsProtocol


DEFAULT_FORMAT_PREFIX = "airflow.ctx."
ENV_VAR_FORMAT_PREFIX = "AIRFLOW_CTX_"

AIRFLOW_VAR_NAME_FORMAT_MAPPING = {
    "AIRFLOW_CONTEXT_DAG_ID": {
        "default": f"{DEFAULT_FORMAT_PREFIX}dag_id",
        "env_var_format": f"{ENV_VAR_FORMAT_PREFIX}DAG_ID",
    },
    "AIRFLOW_CONTEXT_TASK_ID": {
        "default": f"{DEFAULT_FORMAT_PREFIX}task_id",
        "env_var_format": f"{ENV_VAR_FORMAT_PREFIX}TASK_ID",
    },
    "AIRFLOW_CONTEXT_LOGICAL_DATE": {
        "default": f"{DEFAULT_FORMAT_PREFIX}logical_date",
        "env_var_format": f"{ENV_VAR_FORMAT_PREFIX}LOGICAL_DATE",
    },
    "AIRFLOW_CONTEXT_TRY_NUMBER": {
        "default": f"{DEFAULT_FORMAT_PREFIX}try_number",
        "env_var_format": f"{ENV_VAR_FORMAT_PREFIX}TRY_NUMBER",
    },
    "AIRFLOW_CONTEXT_DAG_RUN_ID": {
        "default": f"{DEFAULT_FORMAT_PREFIX}dag_run_id",
        "env_var_format": f"{ENV_VAR_FORMAT_PREFIX}DAG_RUN_ID",
    },
    "AIRFLOW_CONTEXT_DAG_OWNER": {
        "default": f"{DEFAULT_FORMAT_PREFIX}dag_owner",
        "env_var_format": f"{ENV_VAR_FORMAT_PREFIX}DAG_OWNER",
    },
    "AIRFLOW_CONTEXT_DAG_EMAIL": {
        "default": f"{DEFAULT_FORMAT_PREFIX}dag_email",
        "env_var_format": f"{ENV_VAR_FORMAT_PREFIX}DAG_EMAIL",
    },
    "AIRFLOW_CONTEXT_TEAM_NAME": {
        "default": f"{DEFAULT_FORMAT_PREFIX}team_name",
        "env_var_format": f"{ENV_VAR_FORMAT_PREFIX}TEAM_NAME",
    },
}


log = structlog.get_logger(logger_name="task")

T = TypeVar("T")


def _process_connection_result_conn(conn_result: ReceiveMsgType | None) -> Connection:
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.execution_time.comms import ErrorResponse

    if isinstance(conn_result, ErrorResponse):
        raise AirflowRuntimeError(conn_result)

    if TYPE_CHECKING:
        assert isinstance(conn_result, ConnectionResult)

    # `by_alias=True` is used to convert the `schema` field to `schema_` in the Connection model
    return Connection(**conn_result.model_dump(exclude={"type"}, by_alias=True))


def _mask_connection_secrets(conn: Connection) -> None:
    """Mask sensitive connection fields from logs."""
    if conn.password:
        mask_secret(conn.password)
    if conn.extra:
        mask_secret(conn.extra)


def _convert_variable_result_to_variable(var_result: VariableResult, deserialize_json: bool) -> Variable:
    from airflow.sdk.definitions.variable import Variable

    if deserialize_json:
        import json

        var_result.value = json.loads(var_result.value)  # type: ignore
    return Variable(**var_result.model_dump(exclude={"type"}))


def _get_connection(conn_id: str) -> Connection:
    from airflow.sdk.execution_time.cache import SecretCache
    from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

    # Check cache first (optional; only on dag processor)
    try:
        uri = SecretCache.get_connection_uri(conn_id)
        from airflow.sdk.definitions.connection import Connection

        conn = Connection.from_uri(uri, conn_id=conn_id)
        _mask_connection_secrets(conn)
        return conn
    except SecretCache.NotPresentException:
        pass  # continue to backends

    # Iterate over configured backends (which may include SupervisorCommsSecretsBackend
    # in worker contexts or MetastoreBackend in API server contexts)
    backends = ensure_secrets_backend_loaded()
    for secrets_backend in backends:
        try:
            conn = secrets_backend.get_connection(conn_id=conn_id)  # type: ignore[assignment]
            if conn:
                SecretCache.save_connection_uri(conn_id, conn.get_uri())
                _mask_connection_secrets(conn)
                return conn
        except Exception:
            log.debug(
                "Unable to retrieve connection from secrets backend (%s). "
                "Checking subsequent secrets backend.",
                type(secrets_backend).__name__,
            )

    # If no backend found the connection, raise an error

    raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")


async def _async_get_connection(conn_id: str) -> Connection:
    from asgiref.sync import sync_to_async

    from airflow.sdk.execution_time.cache import SecretCache

    # Check cache first
    try:
        uri = SecretCache.get_connection_uri(conn_id)
        from airflow.sdk.definitions.connection import Connection

        conn = Connection.from_uri(uri, conn_id=conn_id)
        _mask_connection_secrets(conn)
        return conn
    except SecretCache.NotPresentException:
        pass  # continue to backends

    from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

    # Try secrets backends
    backends = ensure_secrets_backend_loaded()
    for secrets_backend in backends:
        try:
            # Use async method if available, otherwise wrap sync method
            # getattr avoids triggering AsyncMock coroutine creation under Python 3.13
            async_method = getattr(secrets_backend, "aget_connection", None)
            if async_method is not None:
                maybe_awaitable = async_method(conn_id)
                conn = await maybe_awaitable if inspect.isawaitable(maybe_awaitable) else maybe_awaitable
            else:
                conn = await sync_to_async(secrets_backend.get_connection)(conn_id)  # type: ignore[assignment]

            if conn:
                SecretCache.save_connection_uri(conn_id, conn.get_uri())
                _mask_connection_secrets(conn)
                return conn
        except Exception:
            # If one backend fails, try the next one
            log.debug(
                "Unable to retrieve connection from secrets backend (%s). "
                "Checking subsequent secrets backend.",
                type(secrets_backend).__name__,
            )

    # If no backend found the connection, raise an error

    raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")


def _get_variable(key: str, deserialize_json: bool) -> Any:
    from airflow.sdk.execution_time.cache import SecretCache
    from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

    # Check cache first
    try:
        var_val = SecretCache.get_variable(key)
        if var_val is not None:
            if deserialize_json:
                import json

                var_val = json.loads(var_val)
            if isinstance(var_val, str):
                mask_secret(var_val, key)
            return var_val
    except SecretCache.NotPresentException:
        pass  # Continue to check backends

    backends = ensure_secrets_backend_loaded()

    # Iterate over backends if not in cache (or expired)
    for secrets_backend in backends:
        try:
            var_val = secrets_backend.get_variable(key=key)
            if var_val is not None:
                # Save raw value before deserialization to maintain cache consistency
                SecretCache.save_variable(key, var_val)
                if deserialize_json:
                    import json

                    var_val = json.loads(var_val)
                if isinstance(var_val, str):
                    mask_secret(var_val, key)
                return var_val
        except Exception:
            log.exception(
                "Unable to retrieve variable from secrets backend (%s). Checking subsequent secrets backend.",
                type(secrets_backend).__name__,
            )

    # If no backend found the variable, raise a not found error (mirrors _get_connection)
    from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
    from airflow.sdk.execution_time.comms import ErrorResponse

    raise AirflowRuntimeError(
        ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"message": f"Variable {key} not found"})
    )


def _set_variable(key: str, value: Any, description: str | None = None, serialize_json: bool = False) -> None:
    # TODO: This should probably be moved to a separate module like `airflow.sdk.execution_time.comms`
    #   or `airflow.sdk.execution_time.variable`
    #   A reason to not move it to `airflow.sdk.execution_time.comms` is that it
    #   will make that module depend on Task SDK, which is not ideal because we intend to
    #   keep Task SDK as a separate package than execution time mods.
    import json

    from airflow.sdk.execution_time.cache import SecretCache
    from airflow.sdk.execution_time.comms import PutVariable
    from airflow.sdk.execution_time.secrets.execution_api import ExecutionAPISecretsBackend
    from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    # check for write conflicts on the worker
    for secrets_backend in ensure_secrets_backend_loaded():
        if isinstance(secrets_backend, ExecutionAPISecretsBackend):
            continue
        try:
            var_val = secrets_backend.get_variable(key=key)
            if var_val is not None:
                _backend_name = type(secrets_backend).__name__
                log.warning(
                    "The variable %s is defined in the %s secrets backend, which takes "
                    "precedence over reading from the API Server. The value from the API Server will be "
                    "updated, but to read it you have to delete the conflicting variable "
                    "from %s",
                    key,
                    _backend_name,
                    _backend_name,
                )
        except Exception:
            log.exception(
                "Unable to retrieve variable from secrets backend (%s). Checking subsequent secrets backend.",
                type(secrets_backend).__name__,
            )

    try:
        if serialize_json:
            value = json.dumps(value, indent=2)
    except Exception as e:
        log.exception(e)

    SUPERVISOR_COMMS.send(PutVariable(key=key, value=value, description=description))

    # Invalidate cache after setting the variable
    SecretCache.invalidate_variable(key)


def _delete_variable(key: str) -> None:
    # TODO: This should probably be moved to a separate module like `airflow.sdk.execution_time.comms`
    #   or `airflow.sdk.execution_time.variable`
    #   A reason to not move it to `airflow.sdk.execution_time.comms` is that it
    #   will make that module depend on Task SDK, which is not ideal because we intend to
    #   keep Task SDK as a separate package than execution time mods.
    from airflow.sdk.execution_time.cache import SecretCache
    from airflow.sdk.execution_time.comms import DeleteVariable
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    msg = SUPERVISOR_COMMS.send(DeleteVariable(key=key))
    if TYPE_CHECKING:
        assert isinstance(msg, OKResponse)

    # Invalidate cache after deleting the variable
    SecretCache.invalidate_variable(key)


class ConnectionAccessor:
    """Wrapper to access Connection entries in template."""

    def __getattr__(self, conn_id: str) -> Any:
        from airflow.sdk.definitions.connection import Connection

        return Connection.get(conn_id)

    def __repr__(self) -> str:
        return "<ConnectionAccessor (dynamic access)>"

    def __eq__(self, other):
        if not isinstance(other, ConnectionAccessor):
            return False
        # All instances of ConnectionAccessor are equal since it is a stateless dynamic accessor
        return True

    def __hash__(self):
        return hash(self.__class__.__name__)

    def get(self, conn_id: str, default_conn: Any = None) -> Any:
        try:
            return _get_connection(conn_id)
        except AirflowRuntimeError as e:
            if e.error.error == ErrorType.CONNECTION_NOT_FOUND:
                return default_conn
            raise
        except AirflowNotFoundException:
            return default_conn


class VariableAccessor:
    """Wrapper to access Variable values in template."""

    def __init__(self, deserialize_json: bool) -> None:
        self._deserialize_json = deserialize_json

    def __eq__(self, other):
        if not isinstance(other, VariableAccessor):
            return False
        # All instances of VariableAccessor are equal since it is a stateless dynamic accessor
        return True

    def __hash__(self):
        return hash(self.__class__.__name__)

    def __repr__(self) -> str:
        return "<VariableAccessor (dynamic access)>"

    def __getattr__(self, key: str) -> Any:
        return _get_variable(key, self._deserialize_json)

    def get(self, key, default: Any = NOTSET) -> Any:
        try:
            return _get_variable(key, self._deserialize_json)
        except AirflowRuntimeError as e:
            if e.error.error == ErrorType.VARIABLE_NOT_FOUND:
                return default
            raise


class TaskStateAccessor:
    """Accessor for task state scoped to the current task instance. Available as ``context['task_state']`` at task execution time."""

    def __init__(self, ti_id: UUID) -> None:
        self._ti_id = ti_id

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TaskStateAccessor):
            return False
        return self._ti_id == other._ti_id

    def __hash__(self) -> int:
        return hash(self._ti_id)

    def __repr__(self) -> str:
        return f"<TaskStateAccessor ti_id={self._ti_id}>"

    # TODO: ``__getattr__`` for jinja template access like ``{{ task_state.job_id }}``
    # is not implemented yet cos it's unclear whether task state values will be
    # used in templates.

    def get(self, key: str) -> str | None:
        """Return the stored value, or ``None`` if the key does not exist."""
        from airflow.sdk.execution_time.comms import ErrorResponse, GetTaskState, TaskStateResult
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        resp = SUPERVISOR_COMMS.send(GetTaskState(ti_id=self._ti_id, key=key))
        if isinstance(resp, ErrorResponse) and resp.error != ErrorType.TASK_STATE_NOT_FOUND:
            raise AirflowRuntimeError(resp)
        if isinstance(resp, TaskStateResult):
            return resp.value
        return None

    def set(self, key: str, value: str) -> None:
        """Write or overwrite the value for the given key."""
        from airflow.sdk.execution_time.comms import SetTaskState
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        SUPERVISOR_COMMS.send(SetTaskState(ti_id=self._ti_id, key=key, value=value))

    def delete(self, key: str) -> None:
        """Delete a single key. No-op if the key does not exist."""
        from airflow.sdk.execution_time.comms import DeleteTaskState
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        SUPERVISOR_COMMS.send(DeleteTaskState(ti_id=self._ti_id, key=key))

    def clear(self, all_map_indices: bool = False) -> None:
        """
        Delete all keys for this task instance.

        Pass ``all_map_indices=True`` to wipe state across every mapped
        instance of the task (fleet-wide reset). Defaults to clearing only
        this task instance's own state.
        """
        from airflow.sdk.execution_time.comms import ClearTaskState
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        SUPERVISOR_COMMS.send(ClearTaskState(ti_id=self._ti_id, all_map_indices=all_map_indices))


class AssetStateAccessor:
    """
    Accessor for asset state scoped to a single asset.

    Obtained via ``context['asset_state'][MY_ASSET]`` or, as sugar for single-inlet
    tasks, directly as ``context['asset_state']``.
    """

    def __init__(self, *, name: str | None = None, uri: str | None = None) -> None:
        if not name and not uri:
            raise ValueError("Either `name` or `uri` must be provided")
        self._name = name
        self._uri = uri

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AssetStateAccessor):
            return False
        return self._name == other._name and self._uri == other._uri

    def __hash__(self) -> int:
        return hash((self._name, self._uri))

    def __repr__(self) -> str:
        if self._name is not None:
            return f"<AssetStateAccessor name={self._name!r}>"
        return f"<AssetStateAccessor uri={self._uri!r}>"

    def get(self, key: str) -> str | None:
        """Return the stored value, or ``None`` if the key does not exist."""
        from airflow.sdk.execution_time.comms import (
            AssetStateResult,
            ErrorResponse,
            GetAssetStateByName,
            GetAssetStateByUri,
            ToSupervisor,
        )
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg: ToSupervisor
        if self._name:
            msg = GetAssetStateByName(name=self._name, key=key)
        elif self._uri:
            msg = GetAssetStateByUri(uri=self._uri, key=key)
        resp = SUPERVISOR_COMMS.send(msg)
        if isinstance(resp, ErrorResponse) and resp.error != ErrorType.ASSET_STATE_NOT_FOUND:
            raise AirflowRuntimeError(resp)
        if isinstance(resp, AssetStateResult):
            return resp.value
        return None

    def set(self, key: str, value: str) -> None:
        """Write or overwrite the value for the given key."""
        from airflow.sdk.execution_time.comms import SetAssetStateByName, SetAssetStateByUri, ToSupervisor
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg: ToSupervisor
        if self._name:
            msg = SetAssetStateByName(name=self._name, key=key, value=value)
        elif self._uri:
            msg = SetAssetStateByUri(uri=self._uri, key=key, value=value)
        SUPERVISOR_COMMS.send(msg)

    def delete(self, key: str) -> None:
        """Delete a single key. No-op if the key does not exist."""
        from airflow.sdk.execution_time.comms import (
            DeleteAssetStateByName,
            DeleteAssetStateByUri,
            ToSupervisor,
        )
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg: ToSupervisor
        if self._name:
            msg = DeleteAssetStateByName(name=self._name, key=key)
        elif self._uri:
            msg = DeleteAssetStateByUri(uri=self._uri, key=key)
        SUPERVISOR_COMMS.send(msg)

    def clear(self) -> None:
        """Delete all state keys for this asset."""
        from airflow.sdk.execution_time.comms import ClearAssetStateByName, ClearAssetStateByUri, ToSupervisor
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg: ToSupervisor
        if self._name:
            msg = ClearAssetStateByName(name=self._name)
        elif self._uri:
            msg = ClearAssetStateByUri(uri=self._uri)
        SUPERVISOR_COMMS.send(msg)


class AssetStateAccessors:
    """
    Mapping of asset state accessors for all concrete inlets of a task.

    Available as ``context['asset_state']``. Subscript by asset to get a per asset
    accessor as: ``context['asset_state'][MY_ASSET].get('watermark')``.

    For tasks with exactly one concrete inlet, the accessor methods (``get``, ``set``,
    ``delete``, ``clear``) can be called directly without subscripting.
    """

    def __init__(self, inlets: list) -> None:
        self._by_name: dict[str, AssetStateAccessor] = {}
        self._by_uri: dict[str, AssetStateAccessor] = {}

        for inlet in inlets:
            if isinstance(inlet, (Asset, AssetNameRef)):
                self._by_name[inlet.name] = AssetStateAccessor(name=inlet.name)
            elif isinstance(inlet, AssetUriRef):
                self._by_uri[inlet.uri] = AssetStateAccessor(uri=inlet.uri)
            elif isinstance(inlet, AssetAlias):
                from airflow.sdk.execution_time.comms import AssetsByAliasResult, GetAssetsByAlias
                from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

                resp = SUPERVISOR_COMMS.send(GetAssetsByAlias(alias_name=inlet.name))
                if isinstance(resp, AssetsByAliasResult):
                    for asset in resp.assets:
                        self._by_name[asset.name] = AssetStateAccessor(name=asset.name)

        self._total = len(self._by_name) + len(self._by_uri)

    def __getitem__(self, key: Asset | AssetNameRef | AssetUriRef) -> AssetStateAccessor:
        try:
            if isinstance(key, (Asset, AssetNameRef)):
                return self._by_name[key.name]
            if isinstance(key, AssetUriRef):
                return self._by_uri[key.uri]
        except KeyError:
            raise KeyError(f"{key!r} is not in this task's inlets")
        raise TypeError(f"Expected Asset, AssetNameRef, or AssetUriRef; got {type(key).__name__}")

    def _single_accessor(self) -> AssetStateAccessor:
        if self._total != 1:
            raise ValueError(
                f"Task has {self._total} concrete inlets — use context['asset_state'][MY_ASSET] to specify which"
            )
        if self._by_name:
            return next(iter(self._by_name.values()))
        return next(iter(self._by_uri.values()))

    def get(self, key: str) -> str | None:
        """Return the stored value for the single-inlet task, or ``None`` if not found."""
        return self._single_accessor().get(key)

    def set(self, key: str, value: str) -> None:
        """Write or overwrite the value for the single-inlet task."""
        self._single_accessor().set(key, value)

    def delete(self, key: str) -> None:
        """Delete a single key for the single-inlet task."""
        self._single_accessor().delete(key)

    def clear(self) -> None:
        """Delete all state keys for the single-inlet task."""
        self._single_accessor().clear()

    def __repr__(self) -> str:
        parts = [f"name={k!r}" for k in self._by_name] + [f"uri={k!r}" for k in self._by_uri]
        return f"<AssetStateAccessors [{', '.join(parts)}]>"


class MacrosAccessor:
    """Wrapper to access Macros module lazily."""

    _macros_module = None

    def __getattr__(self, item: str) -> Any:
        # Lazily load Macros module
        if not self._macros_module:
            import airflow.sdk.execution_time.macros

            self._macros_module = airflow.sdk.execution_time.macros
        return getattr(self._macros_module, item)

    def __repr__(self) -> str:
        return "<MacrosAccessor (dynamic access to macros)>"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MacrosAccessor):
            return False
        return True

    def __hash__(self):
        return hash(self.__class__.__name__)


class _AssetRefResolutionMixin:
    _asset_ref_cache: dict[AssetRef, tuple[AssetUniqueKey, dict[str, JsonValue]]] = {}

    def _resolve_asset_ref(self, ref: AssetRef) -> tuple[AssetUniqueKey, dict[str, JsonValue]]:
        with contextlib.suppress(KeyError):
            return self._asset_ref_cache[ref]

        refs_to_cache: list[AssetRef]
        if isinstance(ref, AssetNameRef):
            asset = self._get_asset_from_db(name=ref.name)
            refs_to_cache = [ref, AssetUriRef(asset.uri)]
        elif isinstance(ref, AssetUriRef):
            asset = self._get_asset_from_db(uri=ref.uri)
            refs_to_cache = [ref, AssetNameRef(asset.name)]
        else:
            raise TypeError(f"Unimplemented asset ref: {type(ref)}")
        unique_key = AssetUniqueKey.from_asset(asset)
        for ref in refs_to_cache:
            self._asset_ref_cache[ref] = (unique_key, asset.extra)
        return (unique_key, asset.extra)

    # TODO: This is temporary to avoid code duplication between here & airflow/models/taskinstance.py
    @staticmethod
    def _get_asset_from_db(name: str | None = None, uri: str | None = None) -> Asset:
        from airflow.sdk.definitions.asset import Asset
        from airflow.sdk.execution_time.comms import (
            ErrorResponse,
            GetAssetByName,
            GetAssetByUri,
            ToSupervisor,
        )
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        msg: ToSupervisor
        if name:
            msg = GetAssetByName(name=name)
        elif uri:
            msg = GetAssetByUri(uri=uri)
        else:
            raise ValueError("Either name or uri must be provided")

        resp = SUPERVISOR_COMMS.send(msg)
        if isinstance(resp, ErrorResponse):
            raise AirflowRuntimeError(resp)

        if TYPE_CHECKING:
            assert isinstance(resp, AssetResult)
        return Asset(**resp.model_dump(exclude={"type"}))


@attrs.define
class OutletEventAccessor(_AssetRefResolutionMixin):
    """Wrapper to access an outlet asset event in template."""

    key: BaseAssetUniqueKey
    extra: dict[str, JsonValue] = attrs.Factory(dict)
    asset_alias_events: list[AssetAliasEvent] = attrs.field(factory=list)

    def add(self, asset: Asset | AssetRef, extra: dict[str, JsonValue] | None = None) -> None:
        """Add an AssetEvent to an existing Asset."""
        if not isinstance(self.key, AssetAliasUniqueKey):
            return

        if isinstance(asset, AssetRef):
            asset_key, asset_extra = self._resolve_asset_ref(asset)
        else:
            asset_key = AssetUniqueKey.from_asset(asset)
            asset_extra = asset.extra

        asset_alias_name = self.key.name
        event = AssetAliasEvent(
            source_alias_name=asset_alias_name,
            dest_asset_key=asset_key,
            dest_asset_extra=asset_extra,
            extra=extra or {},
        )
        self.asset_alias_events.append(event)


class _AssetEventAccessorsMixin(Generic[T]):
    @overload
    def for_asset(self, *, name: str, uri: str) -> T: ...

    @overload
    def for_asset(self, *, name: str) -> T: ...

    @overload
    def for_asset(self, *, uri: str) -> T: ...

    def for_asset(self, *, name: str | None = None, uri: str | None = None) -> T:
        if name and uri:
            return self[Asset(name=name, uri=uri)]
        if name:
            return self[Asset.ref(name=name)]
        if uri:
            return self[Asset.ref(uri=uri)]

        raise ValueError("name and uri cannot both be None")

    def for_asset_alias(self, *, name: str) -> T:
        return self[AssetAlias(name=name)]

    def __getitem__(self, key: Asset | AssetAlias | AssetRef) -> T:
        raise NotImplementedError


class OutletEventAccessors(
    _AssetRefResolutionMixin,
    Mapping["Asset | AssetAlias", OutletEventAccessor],
    _AssetEventAccessorsMixin[OutletEventAccessor],
):
    """Lazy mapping of outlet asset event accessors."""

    def __init__(self) -> None:
        self._dict: dict[BaseAssetUniqueKey, OutletEventAccessor] = {}

    def __str__(self) -> str:
        return f"OutletEventAccessors(_dict={self._dict})"

    def __iter__(self) -> Iterator[Asset | AssetAlias]:
        return (
            key.to_asset() if isinstance(key, AssetUniqueKey) else key.to_asset_alias() for key in self._dict
        )

    def __len__(self) -> int:
        return len(self._dict)

    def __getitem__(self, key: Asset | AssetAlias | AssetRef) -> OutletEventAccessor:
        hashable_key: BaseAssetUniqueKey
        if isinstance(key, Asset):
            hashable_key = AssetUniqueKey.from_asset(key)
        elif isinstance(key, AssetAlias):
            hashable_key = AssetAliasUniqueKey.from_asset_alias(key)
        elif isinstance(key, AssetRef):
            hashable_key, _ = self._resolve_asset_ref(key)
        else:
            raise TypeError(f"Key should be either an asset or an asset alias, not {type(key)}")

        if hashable_key not in self._dict:
            self._dict[hashable_key] = OutletEventAccessor(extra={}, key=hashable_key)
        return self._dict[hashable_key]


@attrs.define(init=False)
class InletEventsAccessor(Sequence["AssetEventResult"]):
    _after: str | datetime | None
    _before: str | datetime | None
    _ascending: bool
    _limit: int | None
    _asset_name: str | None
    _asset_uri: str | None
    _alias_name: str | None

    def __init__(
        self, asset_name: str | None = None, asset_uri: str | None = None, alias_name: str | None = None
    ):
        self._asset_name = asset_name
        self._asset_uri = asset_uri
        self._alias_name = alias_name
        self._after = None
        self._before = None
        self._ascending = True
        self._limit = None

    def after(self, after: str) -> Self:
        self._after = after
        self._reset_cache()
        return self

    def before(self, before: str) -> Self:
        self._before = before
        self._reset_cache()
        return self

    def ascending(self, ascending: bool = True) -> Self:
        self._ascending = ascending
        self._reset_cache()
        return self

    def limit(self, limit: int) -> Self:
        self._limit = limit
        self._reset_cache()
        return self

    @functools.cached_property
    def _asset_events(self) -> list[AssetEventResult]:
        from airflow.sdk.execution_time.comms import (
            ErrorResponse,
            GetAssetEventByAsset,
            GetAssetEventByAssetAlias,
            ToSupervisor,
        )
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        query_dict: dict[str, Any] = {
            "after": self._after,
            "before": self._before,
            "ascending": self._ascending,
            "limit": self._limit,
        }

        msg: ToSupervisor
        if self._alias_name is not None:
            msg = GetAssetEventByAssetAlias(alias_name=self._alias_name, **query_dict)
        else:
            if self._asset_name is None and self._asset_uri is None:
                raise ValueError("Either asset_name or asset_uri must be provided")
            msg = GetAssetEventByAsset(name=self._asset_name, uri=self._asset_uri, **query_dict)
        resp = SUPERVISOR_COMMS.send(msg)
        if isinstance(resp, ErrorResponse):
            raise AirflowRuntimeError(resp)

        if TYPE_CHECKING:
            assert isinstance(resp, AssetEventsResult)

        return list(resp.iter_asset_event_results())

    def _reset_cache(self) -> None:
        with contextlib.suppress(AttributeError):
            del self._asset_events

    def __iter__(self) -> Iterator[AssetEventResult]:
        return iter(self._asset_events)

    def __len__(self) -> int:
        return len(self._asset_events)

    @overload
    def __getitem__(self, key: int) -> AssetEventResult: ...

    @overload
    def __getitem__(self, key: slice) -> Sequence[AssetEventResult]: ...

    def __getitem__(self, key: int | slice) -> AssetEventResult | Sequence[AssetEventResult]:
        return self._asset_events[key]


@attrs.define(init=False)
class InletEventsAccessors(
    Mapping["int | Asset | AssetAlias | AssetRef", Any],
    _AssetEventAccessorsMixin[Sequence["AssetEventResult"]],
):
    """Lazy mapping of inlet asset event accessors."""

    _inlets: list[Any]
    _assets: dict[AssetUniqueKey, Asset]
    _asset_aliases: dict[AssetAliasUniqueKey, AssetAlias]

    def __init__(self, inlets: list) -> None:
        self._inlets = inlets
        self._assets = {}
        self._asset_aliases = {}

        for inlet in inlets:
            if isinstance(inlet, Asset):
                self._assets[AssetUniqueKey.from_asset(inlet)] = inlet
            elif isinstance(inlet, AssetAlias):
                self._asset_aliases[AssetAliasUniqueKey.from_asset_alias(inlet)] = inlet
            elif isinstance(inlet, AssetNameRef):
                asset = OutletEventAccessors._get_asset_from_db(name=inlet.name)
                self._assets[AssetUniqueKey.from_asset(asset)] = asset
            elif isinstance(inlet, AssetUriRef):
                asset = OutletEventAccessors._get_asset_from_db(uri=inlet.uri)
                self._assets[AssetUniqueKey.from_asset(asset)] = asset

    def __iter__(self) -> Iterator[Asset | AssetAlias]:
        return iter(self._inlets)

    def __len__(self) -> int:
        return len(self._inlets)

    def __getitem__(
        self,
        key: int | Asset | AssetAlias | AssetRef,
    ) -> InletEventsAccessor:
        from airflow.sdk.definitions.asset import Asset

        if isinstance(key, int):  # Support index access; it's easier for trivial cases.
            obj = self._inlets[key]
            if not isinstance(obj, (Asset, AssetAlias, AssetRef)):
                raise IndexError(key)
        else:
            obj = key

        if isinstance(obj, Asset):
            asset = self._assets[AssetUniqueKey.from_asset(obj)]
            return InletEventsAccessor(asset_name=asset.name, asset_uri=asset.uri)
        if isinstance(obj, AssetNameRef):
            try:
                asset = next(a for k, a in self._assets.items() if k.name == obj.name)
            except StopIteration:
                raise KeyError(obj) from None
            return InletEventsAccessor(asset_name=asset.name)
        if isinstance(obj, AssetUriRef):
            try:
                asset = next(a for k, a in self._assets.items() if k.uri == obj.uri)
            except StopIteration:
                raise KeyError(obj) from None
            return InletEventsAccessor(asset_uri=asset.uri)
        if isinstance(obj, AssetAlias):
            asset_alias = self._asset_aliases[AssetAliasUniqueKey.from_asset_alias(obj)]
            return InletEventsAccessor(alias_name=asset_alias.name)
        raise TypeError(f"`key` is of unknown type ({type(key).__name__})")


@attrs.define
class TriggeringAssetEventsAccessor(
    _AssetRefResolutionMixin,
    Mapping["Asset | AssetAlias | AssetRef", Sequence["AssetEventDagRunReferenceResult"]],
    _AssetEventAccessorsMixin[Sequence["AssetEventDagRunReferenceResult"]],
):
    """Lazy mapping of triggering asset events."""

    _events: Mapping[BaseAssetUniqueKey, Sequence[AssetEventDagRunReferenceResult]]

    @classmethod
    def build(cls, events: Iterable[AssetEventDagRunReferenceResult]) -> TriggeringAssetEventsAccessor:
        coll: dict[BaseAssetUniqueKey, list[AssetEventDagRunReferenceResult]] = collections.defaultdict(list)
        for event in events:
            coll[AssetUniqueKey(name=event.asset.name, uri=event.asset.uri)].append(event)
            for alias in event.source_aliases:
                coll[AssetAliasUniqueKey(name=alias.name)].append(event)
        return cls(coll)

    def __str__(self) -> str:
        return f"TriggeringAssetEventAccessor(_events={self._events})"

    def __iter__(self) -> Iterator[Asset | AssetAlias]:
        return (
            key.to_asset() if isinstance(key, AssetUniqueKey) else key.to_asset_alias()
            for key in self._events
        )

    def __len__(self) -> int:
        return len(self._events)

    def __getitem__(self, key: Asset | AssetAlias | AssetRef) -> Sequence[AssetEventDagRunReferenceResult]:
        hashable_key: BaseAssetUniqueKey
        if isinstance(key, Asset):
            hashable_key = AssetUniqueKey.from_asset(key)
        elif isinstance(key, AssetRef):
            hashable_key, _ = self._resolve_asset_ref(key)
        elif isinstance(key, AssetAlias):
            hashable_key = AssetAliasUniqueKey.from_asset_alias(key)
        else:
            raise TypeError(f"Key should be either an asset or an asset alias, not {type(key)}")

        return self._events[hashable_key]


@cache  # Prevent multiple API access.
def get_previous_dagrun_success(ti_id: UUID) -> PrevSuccessfulDagRunResponse:
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import (
        GetPrevSuccessfulDagRun,
        PrevSuccessfulDagRunResponse,
        PrevSuccessfulDagRunResult,
    )

    msg = task_runner.SUPERVISOR_COMMS.send(GetPrevSuccessfulDagRun(ti_id=ti_id))

    if TYPE_CHECKING:
        assert isinstance(msg, PrevSuccessfulDagRunResult)
    return PrevSuccessfulDagRunResponse(**msg.model_dump(exclude={"type"}))


@contextlib.contextmanager
def set_current_context(context: Context) -> Generator[Context, None, None]:
    """
    Set the current execution context to the provided context object.

    This method should be called once per Task execution, before calling operator.execute.
    """
    _CURRENT_CONTEXT.append(context)
    try:
        yield context
    finally:
        expected_state = _CURRENT_CONTEXT.pop()
        if expected_state != context:
            log.warning(
                "Current context is not equal to the state at context stack.",
                expected=context,
                got=expected_state,
            )


def context_update_for_unmapped(context: Context, task: BaseOperator) -> None:
    """
    Update context after task unmapping.

    Since ``get_template_context()`` is called before unmapping, the context
    contains information about the mapped task. We need to do some in-place
    updates to ensure the template context reflects the unmapped task instead.

    :meta private:
    """
    from airflow.sdk.definitions.param import process_params

    context["task"] = context["ti"].task = task
    context["params"] = process_params(
        context["dag"], task, context["dag_run"].conf, suppress_exception=False
    )


def context_to_airflow_vars(context: Mapping[str, Any], in_env_var_format: bool = False) -> dict[str, str]:
    """
    Return values used to externally reconstruct relations between dags, dag_runs, tasks and task_instances.

    Given a context, this function provides a dictionary of values that can be used to
    externally reconstruct relations between dags, dag_runs, tasks and task_instances.
    Default to abc.def.ghi format and can be made to ABC_DEF_GHI format if
    in_env_var_format is set to True.

    :param context: The context for the task_instance of interest.
    :param in_env_var_format: If returned vars should be in ABC_DEF_GHI format.
    :return: task_instance context as dict.
    """
    from datetime import datetime

    from airflow import settings

    params = {}
    if in_env_var_format:
        name_format = "env_var_format"
    else:
        name_format = "default"

    task = context.get("task")
    task_instance = context.get("task_instance")
    dag_run = context.get("dag_run")

    ops = [
        (task, "email", "AIRFLOW_CONTEXT_DAG_EMAIL"),
        (task, "owner", "AIRFLOW_CONTEXT_DAG_OWNER"),
        (task_instance, "dag_id", "AIRFLOW_CONTEXT_DAG_ID"),
        (task_instance, "task_id", "AIRFLOW_CONTEXT_TASK_ID"),
        (dag_run, "logical_date", "AIRFLOW_CONTEXT_LOGICAL_DATE"),
        (task_instance, "try_number", "AIRFLOW_CONTEXT_TRY_NUMBER"),
        (dag_run, "run_id", "AIRFLOW_CONTEXT_DAG_RUN_ID"),
        (dag_run, "team_name", "AIRFLOW_CONTEXT_TEAM_NAME"),
    ]

    context_params = settings.get_airflow_context_vars(context)
    for key_raw, value in context_params.items():
        if not isinstance(key_raw, str):
            raise TypeError(f"key <{key_raw}> must be string")
        if not isinstance(value, str):
            raise TypeError(f"value of key <{key_raw}> must be string, not {type(value)}")

        if in_env_var_format and not key_raw.startswith(ENV_VAR_FORMAT_PREFIX):
            key = ENV_VAR_FORMAT_PREFIX + key_raw.upper()
        elif not key_raw.startswith(DEFAULT_FORMAT_PREFIX):
            key = DEFAULT_FORMAT_PREFIX + key_raw
        else:
            key = key_raw
        params[key] = value

    for subject, attr, mapping_key in ops:
        _attr = getattr(subject, attr, None)
        if subject and _attr:
            mapping_value = AIRFLOW_VAR_NAME_FORMAT_MAPPING[mapping_key][name_format]
            if isinstance(_attr, str):
                params[mapping_value] = _attr
            elif isinstance(_attr, datetime):
                params[mapping_value] = _attr.isoformat()
            elif isinstance(_attr, list):
                # os env variable value needs to be string
                params[mapping_value] = ",".join(_attr)
            else:
                params[mapping_value] = str(_attr)

    return params


def context_get_outlet_events(context: Context) -> OutletEventAccessorsProtocol:
    try:
        outlet_events = context["outlet_events"]
    except KeyError:
        outlet_events = context["outlet_events"] = OutletEventAccessors()
    return outlet_events
