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

from typing_extensions import NotRequired

if TYPE_CHECKING:
    import jinja2
    from pendulum import DateTime

    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.execution_time.context import (
        AssetStateStoreAccessors,
        InletEventsAccessors,
        TaskStateStoreAccessor,
    )
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
    data_interval_end: NotRequired[DateTime | None]
    data_interval_start: NotRequired[DateTime | None]
    outlet_events: OutletEventAccessorsProtocol
    ds: str
    ds_nodash: str
    expanded_ti_count: NotRequired[int | None]
    exception: NotRequired[None | str | BaseException]
    inlets: list
    inlet_events: InletEventsAccessors
    logical_date: DateTime
    macros: Any
    map_index_template: NotRequired[str | None]
    outlets: list
    params: dict[str, Any]
    partition_key: NotRequired[str | None]
    partition_date: NotRequired[DateTime | None]
    prev_data_interval_start_success: NotRequired[DateTime | None]
    prev_data_interval_end_success: NotRequired[DateTime | None]
    prev_start_date_success: NotRequired[DateTime | None]
    prev_end_date_success: NotRequired[DateTime | None]
    reason: NotRequired[str | None]
    run_id: str
    # TODO: Remove Operator from below once we have MappedOperator to the Task SDK
    #   and once we can remove context related code from the Scheduler/models.TaskInstance
    task: BaseOperator | Operator
    task_reschedule_count: int
    task_instance: RuntimeTaskInstanceProtocol
    task_instance_key_str: str
    task_state_store: TaskStateStoreAccessor
    asset_state_store: AssetStateStoreAccessors
    # `templates_dict` is only set in PythonOperator
    templates_dict: NotRequired[dict[str, Any] | None]
    test_mode: bool
    ti: RuntimeTaskInstanceProtocol
    # triggering_asset_events: Mapping[str, Collection[AssetEvent | AssetEventPydantic]]
    triggering_asset_events: Any
    try_number: NotRequired[int | None]
    ts: str
    ts_nodash: str
    ts_nodash_with_tz: str
    var: Any


KNOWN_CONTEXT_KEYS: set[str] = set(Context.__annotations__.keys())


def clone_context(context: Context) -> Context:
    """
    Create a safe, per-task copy of an execution ``Context`` for concurrent execution.

    The execution context is a mutable mapping that contains many nested
    structures (``params``, ``templates_dict``, ``outlet_events``, ``dag_run``,
    etc.). When running the same logical task concurrently (for example when
    the ``IterableOperator`` spawns multiple indexed task instances that run in
    parallel using threads, processes or asyncio tasks), those mutable objects
    could be mutated by one indexed runtime and unintentionally observed by
    another. That leads to subtle race conditions, corrupted state, and
    flakiness in task execution.

    ``clone_context`` returns a new :class:`Context` mapping where the top-level
    mapping is copied and specific mutable sub-objects that are commonly
    mutated during execution are deep-copied or shallow-copied as appropriate:

    - ``params`` and ``templates_dict`` are deep-copied because they are
      dictionaries that users and operators commonly mutate.
    - ``inlets`` and ``outlets`` are converted to new lists (shallow copy)
      because the sequence identity must be isolated but the elements are
      typically read-only accessor objects.
    - ``dag_run`` is deep-copied because it carries nested state that must
      not be shared between concurrent executions.
    - ``outlet_events`` is intentionally **not** copied so that events emitted
      by sub-tasks are captured in the parent's accessor and serialized when
      the parent task completes. ``OutletEventAccessors.__getitem__`` uses
      ``dict.setdefault`` to guarantee that concurrent sub-tasks always share
      one accessor per asset key rather than silently overwriting each other's
      accumulated events.

    Use cases
    - Multithreading: when using thread-based executors (``concurrent.futures``
      ThreadPoolExecutor) multiple threads share memory; cloning prevents
      concurrent mutation of shared structures.
    - Async concurrency: when running coroutine-based tasks concurrently in
      the same event loop, tasks may still mutate shared mappings; cloning
      avoids interference.
    - Multiprocessing: while processes do not share memory, cloning keeps the
      semantics consistent and avoids accidentally capturing references that
      would be pickled.

    Performance
    - The implementation intentionally copies only a small set of commonly
      mutated fields rather than performing a blanket deep copy of the entire
      context to keep the operation cheap. If future code stores additional
      mutable state in the context that needs isolation, this function should
      be extended appropriately.

    :param context: The original execution context to clone.
    :returns: A new :class:`Context` safe to hand to a concurrently running task.

    :meta private:
    """
    cloned_context = Context()
    cloned_context.update(context)
    cloned_context["params"] = copy.deepcopy(context.get("params", {}))
    cloned_context["inlets"] = list(context.get("inlets", []))
    cloned_context["outlets"] = list(context.get("outlets", []))
    templates_dict = cloned_context.get("templates_dict")
    if templates_dict is not None:
        cloned_context["templates_dict"] = copy.deepcopy(templates_dict)
    cloned_context["inlet_events"] = context["inlet_events"]
    # outlet_events is intentionally NOT copied - sub-tasks must emit into the
    # parent's accessor so events are serialized when the parent completes.
    cloned_context["dag_run"] = context["dag_run"]
    return cloned_context


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
