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

# This stub exists to "fake" the Context class as a TypedDict to provide
# better typehint and editor support.
#
# Unfortunately 'conn', 'macros', 'var.json', and 'var.value' need to be
# annotated as Any and loose discoverability because we don't know what
# attributes are injected at runtime, and giving them a class would trigger
# undefined attribute errors from Mypy. Hopefully there will be a mechanism to
# declare "these are defined, but don't error if others are accessed" someday.
from __future__ import annotations

from collections.abc import Collection, Container, Iterable, Iterator, Mapping, Sequence
from typing import Any, overload

from pendulum import DateTime
from sqlalchemy.orm import Session

from airflow.configuration import AirflowConfigParser
from airflow.models.asset import AssetEvent
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.param import ParamsDict
from airflow.models.taskinstance import TaskInstance
from airflow.sdk.definitions.asset import Asset, AssetAlias
from airflow.serialization.pydantic.asset import AssetEventPydantic
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.typing_compat import TypedDict

KNOWN_CONTEXT_KEYS: set[str]

class _VariableAccessors(TypedDict):
    json: Any
    value: Any

class VariableAccessor:
    def __init__(self, *, deserialize_json: bool) -> None: ...
    def get(self, key, default: Any = ...) -> Any: ...

class ConnectionAccessor:
    def get(self, key: str, default_conn: Any = None) -> Any: ...

class AssetAliasEvent:
    source_alias_name: str
    dest_asset_uri: str
    extra: dict[str, Any]
    def __init__(self, source_alias_name: str, dest_asset_uri: str, extra: dict[str, Any]) -> None: ...

class OutletEventAccessor:
    def __init__(
        self,
        *,
        extra: dict[str, Any],
        key: Asset | AssetAlias,
        asset_alias_events: list[AssetAliasEvent],
    ) -> None: ...
    def add(self, asset: Asset, extra: dict[str, Any] | None = None) -> None: ...
    extra: dict[str, Any]
    key: Asset | AssetAlias
    asset_alias_events: list[AssetAliasEvent]

class OutletEventAccessors(Mapping[Asset | AssetAlias, OutletEventAccessor]):
    def __iter__(self) -> Iterator[Asset | AssetAlias]: ...
    def __len__(self) -> int: ...
    def __getitem__(self, key: Asset | AssetAlias) -> OutletEventAccessor: ...

class InletEventsAccessor(Sequence[AssetEvent]):
    @overload
    def __getitem__(self, key: int) -> AssetEvent: ...
    @overload
    def __getitem__(self, key: slice) -> Sequence[AssetEvent]: ...
    def __len__(self) -> int: ...

class InletEventsAccessors(Mapping[Asset | AssetAlias, InletEventsAccessor]):
    def __init__(self, inlets: list, *, session: Session) -> None: ...
    def __iter__(self) -> Iterator[Asset | AssetAlias]: ...
    def __len__(self) -> int: ...
    def __getitem__(self, key: int | Asset | AssetAlias) -> InletEventsAccessor: ...

# NOTE: Please keep this in sync with the following:
# * KNOWN_CONTEXT_KEYS in airflow/utils/context.py
# * Table in docs/apache-airflow/templates-ref.rst
class Context(TypedDict, total=False):
    conf: AirflowConfigParser
    conn: Any
    dag: DAG
    dag_run: DagRun | DagRunPydantic
    data_interval_end: DateTime
    data_interval_start: DateTime
    outlet_events: OutletEventAccessors
    ds: str
    ds_nodash: str
    exception: BaseException | str | None
    expanded_ti_count: int | None
    inlets: list
    inlet_events: InletEventsAccessors
    logical_date: DateTime
    macros: Any
    map_index_template: str
    outlets: list
    params: ParamsDict
    prev_data_interval_start_success: DateTime | None
    prev_data_interval_end_success: DateTime | None
    prev_start_date_success: DateTime | None
    prev_end_date_success: DateTime | None
    reason: str | None
    run_id: str
    task: BaseOperator
    task_instance: TaskInstance
    task_instance_key_str: str
    test_mode: bool
    templates_dict: Mapping[str, Any] | None
    ti: TaskInstance
    triggering_asset_events: Mapping[str, Collection[AssetEvent | AssetEventPydantic]]
    ts: str
    ts_nodash: str
    ts_nodash_with_tz: str
    try_number: int | None
    var: _VariableAccessors

class AirflowContextDeprecationWarning(DeprecationWarning): ...

@overload
def context_merge(context: Context, additions: Mapping[str, Any], **kwargs: Any) -> None: ...
@overload
def context_merge(context: Context, additions: Iterable[tuple[str, Any]], **kwargs: Any) -> None: ...
@overload
def context_merge(context: Context, **kwargs: Any) -> None: ...
def context_update_for_unmapped(context: Context, task: BaseOperator) -> None: ...
def context_copy_partial(source: Context, keys: Container[str]) -> Context: ...
def lazy_mapping_from_context(source: Context) -> Mapping[str, Any]: ...
def context_get_outlet_events(context: Context) -> OutletEventAccessors: ...
