#
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
"""Jinja2 template rendering context helper."""

from __future__ import annotations

from collections.abc import (
    Container,
    Iterator,
    Mapping,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Union,
    cast,
)

import attrs
from sqlalchemy import and_, select

from airflow.models.asset import (
    AssetAliasModel,
    AssetEvent,
    AssetModel,
    fetch_active_assets_by_name,
    fetch_active_assets_by_uri,
)
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetAliasUniqueKey,
    AssetNameRef,
    AssetRef,
    AssetUniqueKey,
    AssetUriRef,
)
from airflow.sdk.definitions.context import Context
from airflow.sdk.execution_time.context import (
    ConnectionAccessor as ConnectionAccessorSDK,
    OutletEventAccessors as OutletEventAccessorsSDK,
    VariableAccessor as VariableAccessorSDK,
)
from airflow.utils.db import LazySelectSequence
from airflow.utils.session import create_session
from airflow.utils.types import NOTSET

if TYPE_CHECKING:
    from sqlalchemy.engine import Row
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.expression import Select, TextClause

    from airflow.sdk.types import OutletEventAccessorsProtocol

# NOTE: Please keep this in sync with the following:
# * Context in task_sdk/src/airflow/sdk/definitions/context.py
# * Table in docs/apache-airflow/templates-ref.rst
KNOWN_CONTEXT_KEYS: set[str] = {
    "conn",
    "dag",
    "dag_run",
    "data_interval_end",
    "data_interval_start",
    "ds",
    "ds_nodash",
    "expanded_ti_count",
    "exception",
    "inlets",
    "inlet_events",
    "logical_date",
    "macros",
    "map_index_template",
    "outlets",
    "outlet_events",
    "params",
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
    "prev_start_date_success",
    "prev_end_date_success",
    "reason",
    "run_id",
    "start_date",
    "task",
    "task_reschedule_count",
    "task_instance",
    "task_instance_key_str",
    "test_mode",
    "templates_dict",
    "ti",
    "triggering_asset_events",
    "ts",
    "ts_nodash",
    "ts_nodash_with_tz",
    "try_number",
    "var",
}


class VariableAccessor(VariableAccessorSDK):
    """Wrapper to access Variable values in template."""

    def __getattr__(self, key: str) -> Any:
        from airflow.models.variable import Variable

        return Variable.get(key, deserialize_json=self._deserialize_json)

    def get(self, key, default: Any = NOTSET) -> Any:
        from airflow.models.variable import Variable

        if default is NOTSET:
            return Variable.get(key, deserialize_json=self._deserialize_json)
        return Variable.get(key, default, deserialize_json=self._deserialize_json)


class ConnectionAccessor(ConnectionAccessorSDK):
    """Wrapper to access Connection entries in template."""

    def __getattr__(self, conn_id: str) -> Any:
        from airflow.models.connection import Connection

        return Connection.get_connection_from_secrets(conn_id)

    def get(self, conn_id: str, default_conn: Any = None) -> Any:
        from airflow.exceptions import AirflowNotFoundException
        from airflow.models.connection import Connection

        try:
            return Connection.get_connection_from_secrets(conn_id)
        except AirflowNotFoundException:
            return default_conn


class OutletEventAccessors(OutletEventAccessorsSDK):
    """
    Lazy mapping of outlet asset event accessors.

    :meta private:
    """

    @staticmethod
    def _get_asset_from_db(name: str | None = None, uri: str | None = None) -> Asset:
        if name:
            with create_session() as session:
                asset = session.scalar(
                    select(AssetModel).where(AssetModel.name == name, AssetModel.active.has())
                )
        elif uri:
            with create_session() as session:
                asset = session.scalar(
                    select(AssetModel).where(AssetModel.uri == uri, AssetModel.active.has())
                )
        else:
            raise ValueError("Either name or uri must be provided")

        return asset.to_public()


class LazyAssetEventSelectSequence(LazySelectSequence[AssetEvent]):
    """
    List-like interface to lazily access AssetEvent rows.

    :meta private:
    """

    @staticmethod
    def _rebuild_select(stmt: TextClause) -> Select:
        return select(AssetEvent).from_statement(stmt)

    @staticmethod
    def _process_row(row: Row) -> AssetEvent:
        return row[0]


@attrs.define(init=False)
class InletEventsAccessors(Mapping[Union[int, Asset, AssetAlias, AssetRef], LazyAssetEventSelectSequence]):
    """
    Lazy mapping for inlet asset events accessors.

    :meta private:
    """

    _inlets: list[Any]
    _assets: dict[AssetUniqueKey, Asset]
    _asset_aliases: dict[AssetAliasUniqueKey, AssetAlias]
    _session: Session

    def __init__(self, inlets: list, *, session: Session) -> None:
        self._inlets = inlets
        self._session = session
        self._assets = {}
        self._asset_aliases = {}

        _asset_ref_names: list[str] = []
        _asset_ref_uris: list[str] = []
        for inlet in inlets:
            if isinstance(inlet, Asset):
                self._assets[AssetUniqueKey.from_asset(inlet)] = inlet
            elif isinstance(inlet, AssetAlias):
                self._asset_aliases[AssetAliasUniqueKey.from_asset_alias(inlet)] = inlet
            elif isinstance(inlet, AssetNameRef):
                _asset_ref_names.append(inlet.name)
            elif isinstance(inlet, AssetUriRef):
                _asset_ref_uris.append(inlet.uri)

        if _asset_ref_names:
            for _, asset in fetch_active_assets_by_name(_asset_ref_names, self._session).items():
                self._assets[AssetUniqueKey.from_asset(asset)] = asset
        if _asset_ref_uris:
            for _, asset in fetch_active_assets_by_uri(_asset_ref_uris, self._session).items():
                self._assets[AssetUniqueKey.from_asset(asset)] = asset

    def __iter__(self) -> Iterator[Asset | AssetAlias]:
        return iter(self._inlets)

    def __len__(self) -> int:
        return len(self._inlets)

    def __getitem__(self, key: int | Asset | AssetAlias | AssetRef) -> LazyAssetEventSelectSequence:
        if isinstance(key, int):  # Support index access; it's easier for trivial cases.
            obj = self._inlets[key]
            if not isinstance(obj, (Asset, AssetAlias, AssetRef)):
                raise IndexError(key)
        else:
            obj = key

        if isinstance(obj, Asset):
            asset = self._assets[AssetUniqueKey.from_asset(obj)]
            join_clause = AssetEvent.asset
            where_clause = and_(AssetModel.name == asset.name, AssetModel.uri == asset.uri)
        elif isinstance(obj, AssetAlias):
            asset_alias = self._asset_aliases[AssetAliasUniqueKey.from_asset_alias(obj)]
            join_clause = AssetEvent.source_aliases
            where_clause = AssetAliasModel.name == asset_alias.name
        elif isinstance(obj, AssetNameRef):
            try:
                asset = next(a for k, a in self._assets.items() if k.name == obj.name)
            except StopIteration:
                raise KeyError(obj) from None
            join_clause = AssetEvent.asset
            where_clause = and_(AssetModel.name == asset.name, AssetModel.active.has())
        elif isinstance(obj, AssetUriRef):
            try:
                asset = next(a for k, a in self._assets.items() if k.uri == obj.uri)
            except StopIteration:
                raise KeyError(obj) from None
            join_clause = AssetEvent.asset
            where_clause = and_(AssetModel.uri == asset.uri, AssetModel.active.has())
        else:
            raise ValueError(key)

        return LazyAssetEventSelectSequence.from_select(
            select(AssetEvent).join(join_clause).where(where_clause),
            order_by=[AssetEvent.timestamp],
            session=self._session,
        )


def context_merge(context: Context, *args: Any, **kwargs: Any) -> None:
    """
    Merge parameters into an existing context.

    Like ``dict.update()`` , this take the same parameters, and updates
    ``context`` in-place.

    This is implemented as a free function because the ``Context`` type is
    "faked" as a ``TypedDict`` in ``context.pyi``, which cannot have custom
    functions.

    :meta private:
    """
    if not context:
        context = Context()

    context.update(*args, **kwargs)


def context_copy_partial(source: Context, keys: Container[str]) -> Context:
    """
    Create a context by copying items under selected keys in ``source``.

    :meta private:
    """
    new = {k: v for k, v in source.items() if k in keys}
    return cast(Context, new)


def context_get_outlet_events(context: Context) -> OutletEventAccessorsProtocol:
    try:
        return context["outlet_events"]
    except KeyError:
        return OutletEventAccessors()
