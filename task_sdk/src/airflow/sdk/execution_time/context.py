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

import contextlib
from collections.abc import Generator, Iterator, Mapping
from functools import cache
from typing import TYPE_CHECKING, Any, Union

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

if TYPE_CHECKING:
    from uuid import UUID

    from airflow.sdk.definitions.baseoperator import BaseOperator
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.variable import Variable
    from airflow.sdk.execution_time.comms import (
        AssetResult,
        ConnectionResult,
        PrevSuccessfulDagRunResponse,
        VariableResult,
    )

log = structlog.get_logger(logger_name="task")


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


@attrs.define
class OutletEventAccessor:
    """Wrapper to access an outlet asset event in template."""

    key: BaseAssetUniqueKey
    extra: dict[str, Any] = attrs.Factory(dict)
    asset_alias_events: list[AssetAliasEvent] = attrs.field(factory=list)

    def add(self, asset: Asset, extra: dict[str, Any] | None = None) -> None:
        """Add an AssetEvent to an existing Asset."""
        if not isinstance(self.key, AssetAliasUniqueKey):
            return

        asset_alias_name = self.key.name
        event = AssetAliasEvent(
            source_alias_name=asset_alias_name,
            dest_asset_key=AssetUniqueKey.from_asset(asset),
            extra=extra or {},
        )
        self.asset_alias_events.append(event)


class OutletEventAccessors(Mapping[Union[Asset, AssetAlias], OutletEventAccessor]):
    """Lazy mapping of outlet asset event accessors."""

    _asset_ref_cache: dict[AssetRef, AssetUniqueKey] = {}

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

    def __getitem__(self, key: Asset | AssetAlias) -> OutletEventAccessor:
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
