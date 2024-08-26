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

from typing import Any, Collection, Container, Iterable, Iterator, Mapping, Sequence, overload

from pendulum import DateTime
from sqlalchemy.orm import Session

from airflow.assets import AssetAlias, AssetAliasEvent, Dataset
from airflow.configuration import AirflowConfigParser
from airflow.models.asset import AssetEvent
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.param import ParamsDict
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.serialization.pydantic.dataset import DatasetEventPydantic
from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
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

class OutletEventAccessor:
    def __init__(
        self,
        *,
        extra: dict[str, Any],
        raw_key: str | Dataset | AssetAlias,
        asset_alias_events: list[AssetAliasEvent],
    ) -> None: ...
    def add(self, dataset: Dataset | str, extra: dict[str, Any] | None = None) -> None: ...
    extra: dict[str, Any]
    raw_key: str | Dataset | AssetAlias
    asset_alias_events: list[AssetAliasEvent]

class OutletEventAccessors(Mapping[str, OutletEventAccessor]):
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __getitem__(self, key: str | Dataset | AssetAlias) -> OutletEventAccessor: ...

class InletEventsAccessor(Sequence[AssetEvent]):
    @overload
    def __getitem__(self, key: int) -> AssetEvent: ...
    @overload
    def __getitem__(self, key: slice) -> Sequence[AssetEvent]: ...
    def __len__(self) -> int: ...

class InletEventsAccessors(Mapping[str, InletEventsAccessor]):
    def __init__(self, inlets: list, *, session: Session) -> None: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def __getitem__(self, key: int | str | Dataset | AssetAlias) -> InletEventsAccessor: ...

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
    execution_date: DateTime
    expanded_ti_count: int | None
    inlets: list
    inlet_events: InletEventsAccessors
    logical_date: DateTime
    macros: Any
    map_index_template: str
    next_ds: str | None
    next_ds_nodash: str | None
    next_execution_date: DateTime | None
    outlets: list
    params: ParamsDict
    prev_data_interval_start_success: DateTime | None
    prev_data_interval_end_success: DateTime | None
    prev_ds: str | None
    prev_ds_nodash: str | None
    prev_execution_date: DateTime | None
    prev_execution_date_success: DateTime | None
    prev_start_date_success: DateTime | None
    prev_end_date_success: DateTime | None
    reason: str | None
    run_id: str
    task: BaseOperator
    task_instance: TaskInstance | TaskInstancePydantic
    task_instance_key_str: str
    test_mode: bool
    templates_dict: Mapping[str, Any] | None
    ti: TaskInstance | TaskInstancePydantic
    tomorrow_ds: str
    tomorrow_ds_nodash: str
    triggering_dataset_events: Mapping[str, Collection[AssetEvent | DatasetEventPydantic]]
    ts: str
    ts_nodash: str
    ts_nodash_with_tz: str
    try_number: int | None
    var: _VariableAccessors
    yesterday_ds: str
    yesterday_ds_nodash: str

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
