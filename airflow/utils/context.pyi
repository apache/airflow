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

from typing import Any, Optional

from pendulum import DateTime

from airflow.configuration import AirflowConfigParser
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.param import ParamsDict
from airflow.models.taskinstance import TaskInstance
from airflow.typing_compat import TypedDict

class _VariableAccessors(TypedDict):
    json: Any
    value: Any

class Context(TypedDict, total=False):
    conf: AirflowConfigParser
    conn: Any
    dag: DAG
    dag_run: DagRun
    data_interval_end: DateTime
    data_interval_start: DateTime
    ds: str
    ds_nodash: str
    execution_date: DateTime
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
    ti: TaskInstance
    tomorrow_ds: str
    tomorrow_ds_nodash: str
    ts: str
    ts_nodash: str
    ts_nodash_with_tz: str
    var: _VariableAccessors
    yesterday_ds: str
    yesterday_ds_nodash: str
