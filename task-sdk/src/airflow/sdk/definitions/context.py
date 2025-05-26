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
from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any, NamedTuple, TypedDict

if TYPE_CHECKING:
    from pendulum import DateTime

    from airflow.models.operator import Operator
    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.execution_time.context import InletEventsAccessors
    from airflow.sdk.types import (
        DagRunProtocol,
        OutletEventAccessorsProtocol,
        RuntimeTaskInstanceProtocol,
    )


class Context(TypedDict, total=False):
    """Jinja2 template context for task rendering."""

    conn: Any
    dag: DAG
    dag_run: DagRunProtocol
    data_interval_end: DateTime | None
    data_interval_start: DateTime | None
    outlet_events: OutletEventAccessorsProtocol
    ds: str
    ds_nodash: str
    expanded_ti_count: int | None
    exception: None | str | BaseException
    inlets: list
    inlet_events: InletEventsAccessors
    logical_date: DateTime
    macros: Any
    map_index_template: str | None
    outlets: list
    params: dict[str, Any]
    prev_data_interval_start_success: DateTime | None
    prev_data_interval_end_success: DateTime | None
    prev_start_date_success: DateTime | None
    prev_end_date_success: DateTime | None
    reason: str | None
    run_id: str
    start_date: DateTime
    # TODO: Remove Operator from below once we have MappedOperator to the Task SDK
    #   and once we can remove context related code from the Scheduler/models.TaskInstance
    task: BaseOperator | Operator
    task_reschedule_count: int
    task_instance: RuntimeTaskInstanceProtocol
    task_instance_key_str: str
    # `templates_dict` is only set in PythonOperator
    templates_dict: dict[str, Any] | None
    test_mode: bool
    ti: RuntimeTaskInstanceProtocol
    # triggering_asset_events: Mapping[str, Collection[AssetEvent | AssetEventPydantic]]
    triggering_asset_events: Any
    try_number: int | None
    ts: str
    ts_nodash: str
    ts_nodash_with_tz: str
    var: Any


def get_current_context() -> Context:
    """
    Retrieve the execution context dictionary without altering user method's signature.

    This is the simplest method of retrieving the execution context dictionary.

    **Old style:**

    .. code:: python

        def my_task(**context):
            ti = context["ti"]

    **New style:**

    .. code:: python

        from airflow.sdk import get_current_context


        def my_task():
            context = get_current_context()
            ti = context["ti"]

    Current context will only have value if this method was called after an operator
    was starting to execute.
    """
    from airflow.sdk.definitions._internal.contextmanager import _get_current_context

    return _get_current_context()


class AirflowParsingContext(NamedTuple):
    """
    Context of parsing for the DAG.

    If these values are not None, they will contain the specific DAG and Task ID that Airflow is requesting to
    execute. You can use these for optimizing dynamically generated DAG files.
    """

    dag_id: str | None
    task_id: str | None


_AIRFLOW_PARSING_CONTEXT_DAG_ID = "_AIRFLOW_PARSING_CONTEXT_DAG_ID"
_AIRFLOW_PARSING_CONTEXT_TASK_ID = "_AIRFLOW_PARSING_CONTEXT_TASK_ID"


def get_parsing_context() -> AirflowParsingContext:
    """Return the current (DAG) parsing context info."""
    return AirflowParsingContext(
        dag_id=os.environ.get(_AIRFLOW_PARSING_CONTEXT_DAG_ID),
        task_id=os.environ.get(_AIRFLOW_PARSING_CONTEXT_TASK_ID),
    )
