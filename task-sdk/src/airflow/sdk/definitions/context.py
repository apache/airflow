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

import copy
import os
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, NamedTuple, TypedDict, cast

if TYPE_CHECKING:
    import jinja2
    from pendulum import DateTime

    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.execution_time.context import InletEventsAccessors
    from airflow.sdk.types import (
        DagRunProtocol,
        Operator,
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


KNOWN_CONTEXT_KEYS: set[str] = set(Context.__annotations__.keys())


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
    Context of parsing for the Dag.

    If these values are not None, they will contain the specific Dag and Task ID that Airflow is requesting to
    execute. You can use these for optimizing dynamically generated Dag files.

    You can obtain the current values via :py:func:`.get_parsing_context`.
    """

    dag_id: str | None
    task_id: str | None


_AIRFLOW_PARSING_CONTEXT_DAG_ID = "_AIRFLOW_PARSING_CONTEXT_DAG_ID"
_AIRFLOW_PARSING_CONTEXT_TASK_ID = "_AIRFLOW_PARSING_CONTEXT_TASK_ID"


def get_parsing_context() -> AirflowParsingContext:
    """Return the current (Dag) parsing context info."""
    return AirflowParsingContext(
        dag_id=os.environ.get(_AIRFLOW_PARSING_CONTEXT_DAG_ID),
        task_id=os.environ.get(_AIRFLOW_PARSING_CONTEXT_TASK_ID),
    )


# The 'template' argument is typed as Any because the jinja2.Template is too
# dynamic to be effectively type-checked.
def render_template(template: Any, context: MutableMapping[str, Any], *, native: bool) -> Any:
    """
    Render a Jinja2 template with given Airflow context.

    The default implementation of ``jinja2.Template.render()`` converts the
    input context into dict eagerly many times, which triggers deprecation
    messages in our custom context class. This takes the implementation apart
    and retain the context mapping without resolving instead.

    :param template: A Jinja2 template to render.
    :param context: The Airflow task context to render the template with.
    :param native: If set to *True*, render the template into a native type. A
        Dag can enable this with ``render_template_as_native_obj=True``.
    :returns: The render result.
    """
    context = copy.copy(context)
    env = template.environment
    if template.globals:
        context.update((k, v) for k, v in template.globals.items() if k not in context)
    try:
        nodes = template.root_render_func(env.context_class(env, context, template.name, template.blocks))
    except Exception:
        env.handle_exception()  # Rewrite traceback to point to the template.
    if native:
        import jinja2.nativetypes

        return jinja2.nativetypes.native_concat(nodes)
    return "".join(nodes)


def render_template_as_native(template: jinja2.Template, context: Context) -> Any:
    """Shorthand to ``render_template(native=True)`` with better typing support."""
    return render_template(template, cast("MutableMapping[str, Any]", context), native=True)


def render_template_to_string(template: jinja2.Template, context: Context) -> str:
    """Shorthand to ``render_template(native=False)`` with better typing support."""
    return render_template(template, cast("MutableMapping[str, Any]", context), native=False)
