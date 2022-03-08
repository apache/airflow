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

from typing import Any, Container, Iterable, Mapping, Optional, Set, Tuple, Union, overload

from pendulum import DateTime

from airflow.configuration import AirflowConfigParser
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.param import ParamsDict
from airflow.models.taskinstance import TaskInstance
from airflow.typing_compat import TypedDict

KNOWN_CONTEXT_KEYS: Set[str]

class _VariableAccessors(TypedDict):
    json: Any
    value: Any

class VariableAccessor:
    def __init__(self, *, deserialize_json: bool) -> None: ...
    def get(self, key, default: Any = ...) -> Any: ...

class ConnectionAccessor:
    def get(self, key: str, default_conn: Any = None) -> Any: ...

# NOTE: Please keep this in sync with KNOWN_CONTEXT_KEYS in airflow/utils/context.py.
class Context(TypedDict):
    conf: AirflowConfigParser
    conn: Any
    dag: DAG
    dag_run: DagRun
    data_interval_end: DateTime
    data_interval_start: DateTime
    ds: str
    ds_nodash: str
    execution_date: DateTime
    exception: Union[Exception, str, None]
    inlets: list
    logical_date: DateTime
    macros: Any
    next_ds: Optional[str]
    next_ds_nodash: Optional[str]
    next_execution_date: Optional[DateTime]
    outlets: list
    params: ParamsDict
    prev_data_interval_start_success: Optional[DateTime]
    prev_data_interval_end_success: Optional[DateTime]
    prev_ds: Optional[str]
    prev_ds_nodash: Optional[str]
    prev_execution_date: Optional[DateTime]
    prev_execution_date_success: Optional[DateTime]
    prev_start_date_success: Optional[DateTime]
    run_id: str
    task: BaseOperator
    task_instance: TaskInstance
    task_instance_key_str: str
    test_mode: bool
    templates_dict: Optional[Mapping[str, Any]]
    ti: TaskInstance
    tomorrow_ds: str
    tomorrow_ds_nodash: str
    ts: str
    ts_nodash: str
    ts_nodash_with_tz: str
    try_number: Optional[int]
    var: _VariableAccessors
    yesterday_ds: str
    yesterday_ds_nodash: str

class AirflowContextDeprecationWarning(DeprecationWarning): ...

@overload
def context_merge(source: Context, additions: Mapping[str, Any], **kwargs: Any) -> None: ...
@overload
def context_merge(source: Context, additions: Iterable[Tuple[str, Any]], **kwargs: Any) -> None: ...
@overload
def context_merge(source: Context, **kwargs: Any) -> None: ...
def context_copy_partial(source: Context, keys: Container[str]) -> Context: ...
def lazy_mapping_from_context(source: Context) -> Mapping[str, Any]: ...
