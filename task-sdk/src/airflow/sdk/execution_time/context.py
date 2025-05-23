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
from collections.abc import Generator, Iterable, Iterator, Mapping, Sequence
from functools import cache
from typing import TYPE_CHECKING, Any, Generic, TypeVar, Union

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
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.sdk.execution_time.secrets_masker import mask_secret

if TYPE_CHECKING:
    from uuid import UUID

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
}


log = structlog.get_logger(logger_name="task")

T = TypeVar("T")


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
    from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

    # TODO: check cache first
    # enabled only if SecretCache.init() has been called first

    # iterate over configured backends if not in cache (or expired)
    for secrets_backend in ensure_secrets_backend_loaded():
        try:
            conn = secrets_backend.get_connection(conn_id=conn_id)
            if conn:
                return conn
        except Exception:
            log.exception(
                "Unable to retrieve connection from secrets backend (%s). "
                "Checking subsequent secrets backend.",
                type(secrets_backend).__name__,
            )

    log.debug(
        "Connection not found in any of the configured Secrets Backends. Trying to retrieve from API server",
        conn_id=conn_id,
    )

    # TODO: This should probably be moved to a separate module like `airflow.sdk.execution_time.comms`
    #   or `airflow.sdk.execution_time.connection`
    #   A reason to not move it to `airflow.sdk.execution_time.comms` is that it
    #   will make that module depend on Task SDK, which is not ideal because we intend to
    #   keep Task SDK as a separate package than execution time mods.
    from airflow.sdk.execution_time.comms import ErrorResponse, GetConnection
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    # Since Triggers can hit this code path via `sync_to_async` (which uses threads internally)
    # we need to make sure that we "atomically" send a request and get the response to that
    # back so that two triggers don't end up interleaving requests and create a possible
    # race condition where the wrong trigger reads the response.
    with SUPERVISOR_COMMS.lock:
        SUPERVISOR_COMMS.send_request(log=log, msg=GetConnection(conn_id=conn_id))
        msg = SUPERVISOR_COMMS.get_message()

    if isinstance(msg, ErrorResponse):
        raise AirflowRuntimeError(msg)

    if TYPE_CHECKING:
        assert isinstance(msg, ConnectionResult)
    return _convert_connection_result_conn(msg)


def _get_variable(key: str, deserialize_json: bool) -> Any:
    # TODO: check cache first
    # enabled only if SecretCache.init() has been called first
    from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded

    var_val = None
    # iterate over backends if not in cache (or expired)
    for secrets_backend in ensure_secrets_backend_loaded():
        try:
            var_val = secrets_backend.get_variable(key=key)  # type: ignore[assignment]
            if var_val is not None:
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

    log.debug(
        "Variable not found in any of the configured Secrets Backends. Trying to retrieve from API server",
        key=key,
    )

    # TODO: This should probably be moved to a separate module like `airflow.sdk.execution_time.comms`
    #   or `airflow.sdk.execution_time.variable`
    #   A reason to not move it to `airflow.sdk.execution_time.comms` is that it
    #   will make that module depend on Task SDK, which is not ideal because we intend to
    #   keep Task SDK as a separate package than execution time mods.
    from airflow.sdk.execution_time.comms import ErrorResponse, GetVariable
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    # Since Triggers can hit this code path via `sync_to_async` (which uses threads internally)
    # we need to make sure that we "atomically" send a request and get the response to that
    # back so that two triggers don't end up interleaving requests and create a possible
    # race condition where the wrong trigger reads the response.
    with SUPERVISOR_COMMS.lock:
        SUPERVISOR_COMMS.send_request(log=log, msg=GetVariable(key=key))
        msg = SUPERVISOR_COMMS.get_message()

    if isinstance(msg, ErrorResponse):
        raise AirflowRuntimeError(msg)

    if TYPE_CHECKING:
        assert isinstance(msg, VariableResult)
    variable = _convert_variable_result_to_variable(msg, deserialize_json)
    return variable.value


def _set_variable(key: str, value: Any, description: str | None = None, serialize_json: bool = False) -> None:
    # TODO: This should probably be moved to a separate module like `airflow.sdk.execution_time.comms`
    #   or `airflow.sdk.execution_time.variable`
    #   A reason to not move it to `airflow.sdk.execution_time.comms` is that it
    #   will make that module depend on Task SDK, which is not ideal because we intend to
    #   keep Task SDK as a separate package than execution time mods.
    import json

    from airflow.sdk.execution_time.comms import PutVariable
    from airflow.sdk.execution_time.supervisor import ensure_secrets_backend_loaded
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    # check for write conflicts on the worker
    for secrets_backend in ensure_secrets_backend_loaded():
        try:
            var_val = secrets_backend.get_variable(key=key)
            if var_val is not None:
                _backend_name = type(secrets_backend).__name__
                log.warning(
                    "The variable %s is defined in the %s secrets backend, which takes "
                    "precedence over reading from the database. The value in the database will be "
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

    # It is best to have lock everywhere or nowhere on the SUPERVISOR_COMMS, lock was
    # primarily added for triggers but it doesn't make sense to have it in some places
    # and not in the rest. A lot of this will be simplified by https://github.com/apache/airflow/issues/46426
    with SUPERVISOR_COMMS.lock:
        SUPERVISOR_COMMS.send_request(log=log, msg=PutVariable(key=key, value=value, description=description))


def _delete_variable(key: str) -> None:
    # TODO: This should probably be moved to a separate module like `airflow.sdk.execution_time.comms`
    #   or `airflow.sdk.execution_time.variable`
    #   A reason to not move it to `airflow.sdk.execution_time.comms` is that it
    #   will make that module depend on Task SDK, which is not ideal because we intend to
    #   keep Task SDK as a separate package than execution time mods.
    from airflow.sdk.execution_time.comms import DeleteVariable
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    # It is best to have lock everywhere or nowhere on the SUPERVISOR_COMMS, lock was
    # primarily added for triggers but it doesn't make sense to have it in some places
    # and not in the rest. A lot of this will be simplified by https://github.com/apache/airflow/issues/46426
    with SUPERVISOR_COMMS.lock:
        SUPERVISOR_COMMS.send_request(log=log, msg=DeleteVariable(key=key))
        msg = SUPERVISOR_COMMS.get_message()
    if TYPE_CHECKING:
        assert isinstance(msg, OKResponse)


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

    def get(self, key, default: Any = NOTSET) -> Any:
        try:
            return _get_variable(key, self._deserialize_json)
        except AirflowRuntimeError as e:
            if e.error.error == ErrorType.VARIABLE_NOT_FOUND:
                return default
            raise


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


class _AssetRefResolutionMixin:
    _asset_ref_cache: dict[AssetRef, AssetUniqueKey] = {}

    def _resolve_asset_ref(self, ref: AssetRef) -> AssetUniqueKey:
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
            self._asset_ref_cache[ref] = unique_key
        return unique_key

    # TODO: This is temporary to avoid code duplication between here & airflow/models/taskinstance.py
    @staticmethod
    def _get_asset_from_db(name: str | None = None, uri: str | None = None) -> Asset:
        from airflow.sdk.definitions.asset import Asset
        from airflow.sdk.execution_time.comms import ErrorResponse, GetAssetByName, GetAssetByUri
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        if name:
            SUPERVISOR_COMMS.send_request(log=log, msg=GetAssetByName(name=name))
        elif uri:
            SUPERVISOR_COMMS.send_request(log=log, msg=GetAssetByUri(uri=uri))
        else:
            raise ValueError("Either name or uri must be provided")

        msg = SUPERVISOR_COMMS.get_message()
        if isinstance(msg, ErrorResponse):
            raise AirflowRuntimeError(msg)

        if TYPE_CHECKING:
            assert isinstance(msg, AssetResult)
        return Asset(**msg.model_dump(exclude={"type"}))


@attrs.define
class TriggeringAssetEventsAccessor(
    _AssetRefResolutionMixin,
    Mapping[Union[Asset, AssetAlias, AssetRef], Sequence["AssetEventDagRunReferenceResult"]],
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
            hashable_key = self._resolve_asset_ref(key)
        elif isinstance(key, AssetAlias):
            hashable_key = AssetAliasUniqueKey.from_asset_alias(key)
        else:
            raise TypeError(f"Key should be either an asset or an asset alias, not {type(key)}")

        return self._events[hashable_key]


@attrs.define
class OutletEventAccessor(_AssetRefResolutionMixin):
    """Wrapper to access an outlet asset event in template."""

    key: BaseAssetUniqueKey
    extra: dict[str, Any] = attrs.Factory(dict)
    asset_alias_events: list[AssetAliasEvent] = attrs.field(factory=list)

    def add(self, asset: Asset | AssetRef, extra: dict[str, Any] | None = None) -> None:
        """Add an AssetEvent to an existing Asset."""
        if not isinstance(self.key, AssetAliasUniqueKey):
            return

        if isinstance(asset, AssetRef):
            asset_key = self._resolve_asset_ref(asset)
        else:
            asset_key = AssetUniqueKey.from_asset(asset)

        asset_alias_name = self.key.name
        event = AssetAliasEvent(
            source_alias_name=asset_alias_name,
            dest_asset_key=asset_key,
            extra=extra or {},
        )
        self.asset_alias_events.append(event)


class _AssetEventAccessorsMixin(Generic[T]):
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
    Mapping[Union[Asset, AssetAlias], OutletEventAccessor],
    _AssetEventAccessorsMixin,
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
            hashable_key = self._resolve_asset_ref(key)
        else:
            raise TypeError(f"Key should be either an asset or an asset alias, not {type(key)}")

        if hashable_key not in self._dict:
            self._dict[hashable_key] = OutletEventAccessor(extra={}, key=hashable_key)
        return self._dict[hashable_key]


@attrs.define(init=False)
class InletEventsAccessors(Mapping[Union[int, Asset, AssetAlias, AssetRef], Any], _AssetEventAccessorsMixin):
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

    def __getitem__(self, key: int | Asset | AssetAlias | AssetRef) -> list[AssetEventResult]:
        from airflow.sdk.definitions.asset import Asset
        from airflow.sdk.execution_time.comms import (
            ErrorResponse,
            GetAssetEventByAsset,
            GetAssetEventByAssetAlias,
        )
        from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

        if isinstance(key, int):  # Support index access; it's easier for trivial cases.
            obj = self._inlets[key]
            if not isinstance(obj, (Asset, AssetAlias, AssetRef)):
                raise IndexError(key)
        else:
            obj = key

        if isinstance(obj, Asset):
            asset = self._assets[AssetUniqueKey.from_asset(obj)]
            SUPERVISOR_COMMS.send_request(log=log, msg=GetAssetEventByAsset(name=asset.name, uri=asset.uri))
        elif isinstance(obj, AssetNameRef):
            try:
                asset = next(a for k, a in self._assets.items() if k.name == obj.name)
            except StopIteration:
                raise KeyError(obj) from None
            SUPERVISOR_COMMS.send_request(log=log, msg=GetAssetEventByAsset(name=asset.name, uri=None))
        elif isinstance(obj, AssetUriRef):
            try:
                asset = next(a for k, a in self._assets.items() if k.uri == obj.uri)
            except StopIteration:
                raise KeyError(obj) from None
            SUPERVISOR_COMMS.send_request(log=log, msg=GetAssetEventByAsset(name=None, uri=asset.uri))
        elif isinstance(obj, AssetAlias):
            asset_alias = self._asset_aliases[AssetAliasUniqueKey.from_asset_alias(obj)]
            SUPERVISOR_COMMS.send_request(log=log, msg=GetAssetEventByAssetAlias(alias_name=asset_alias.name))

        msg = SUPERVISOR_COMMS.get_message()
        if isinstance(msg, ErrorResponse):
            raise AirflowRuntimeError(msg)

        if TYPE_CHECKING:
            assert isinstance(msg, AssetEventsResult)

        return list(msg.iter_asset_event_results())


@cache  # Prevent multiple API access.
def get_previous_dagrun_success(ti_id: UUID) -> PrevSuccessfulDagRunResponse:
    from airflow.sdk.execution_time.comms import (
        GetPrevSuccessfulDagRun,
        PrevSuccessfulDagRunResponse,
        PrevSuccessfulDagRunResult,
    )
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    SUPERVISOR_COMMS.send_request(log=log, msg=GetPrevSuccessfulDagRun(ti_id=ti_id))
    msg = SUPERVISOR_COMMS.get_message()

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
    ]

    context_params = settings.get_airflow_context_vars(context)
    for key, value in context_params.items():
        if not isinstance(key, str):
            raise TypeError(f"key <{key}> must be string")
        if not isinstance(value, str):
            raise TypeError(f"value of key <{key}> must be string, not {type(value)}")

        if in_env_var_format:
            if not key.startswith(ENV_VAR_FORMAT_PREFIX):
                key = ENV_VAR_FORMAT_PREFIX + key.upper()
        else:
            if not key.startswith(DEFAULT_FORMAT_PREFIX):
                key = DEFAULT_FORMAT_PREFIX + key
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
