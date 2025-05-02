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

import datetime
import logging
from abc import abstractmethod
from collections.abc import (
    Callable,
    Collection,
    Iterable,
    Iterator,
)
from typing import TYPE_CHECKING, Any, ClassVar

import methodtools

from airflow.sdk.definitions._internal.mixins import DependencyMixin
from airflow.sdk.definitions._internal.node import DAGNode
from airflow.sdk.definitions._internal.templater import Templater
from airflow.sdk.definitions.context import Context
from airflow.utils.setup_teardown import SetupTeardownContext
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import WeightRule

if TYPE_CHECKING:
    import jinja2

    from airflow.sdk.bases.operator import BaseOperator
    from airflow.sdk.bases.operatorlink import BaseOperatorLink
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.definitions.taskgroup import MappedTaskGroup
    from airflow.sdk.types import Operator

TaskStateChangeCallback = Callable[[Context], None]

DEFAULT_OWNER: str = "airflow"
DEFAULT_POOL_SLOTS: int = 1
DEFAULT_POOL_NAME = "default_pool"
DEFAULT_PRIORITY_WEIGHT: int = 1
# Databases do not support arbitrary precision integers, so we need to limit the range of priority weights.
# postgres: -2147483648 to +2147483647 (see https://www.postgresql.org/docs/current/datatype-numeric.html)
# mysql: -2147483648 to +2147483647 (see https://dev.mysql.com/doc/refman/8.4/en/integer-types.html)
# sqlite: -9223372036854775808 to +9223372036854775807 (see https://sqlite.org/datatype3.html)
MINIMUM_PRIORITY_WEIGHT: int = -2147483648
MAXIMUM_PRIORITY_WEIGHT: int = 2147483647
DEFAULT_EXECUTOR: str | None = None
DEFAULT_QUEUE: str = "default"
DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST: bool = False
DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING: bool = False
DEFAULT_RETRIES: int = 0
DEFAULT_RETRY_DELAY: datetime.timedelta = datetime.timedelta(seconds=300)
MAX_RETRY_DELAY: int = 24 * 60 * 60

# TODO: Task-SDK -- these defaults should be overridable from the Airflow config
DEFAULT_TRIGGER_RULE: TriggerRule = TriggerRule.ALL_SUCCESS
DEFAULT_WEIGHT_RULE: WeightRule = WeightRule.DOWNSTREAM
DEFAULT_TASK_EXECUTION_TIMEOUT: datetime.timedelta | None = None

log = logging.getLogger(__name__)


class NotMapped(Exception):
    """Raise if a task is neither mapped nor has any parent mapped groups."""


class AbstractOperator(Templater, DAGNode):
    """
    Common implementation for operators, including unmapped and mapped.

    This base class is more about sharing implementations, not defining a common
    interface. Unfortunately it's difficult to use this as the common base class
    for typing due to BaseOperator carrying too much historical baggage.

    The union type ``from airflow.models.operator import Operator`` is easier
    to use for typing purposes.

    :meta private:
    """

    operator_class: type[BaseOperator] | dict[str, Any]

    priority_weight: int

    # Defines the operator level extra links.
    operator_extra_links: Collection[BaseOperatorLink] = ()

    owner: str
    task_id: str

    outlets: list
    inlets: list

    trigger_rule: TriggerRule
    _needs_expansion: bool | None = None
    _on_failure_fail_dagrun = False
    is_setup: bool = False
    is_teardown: bool = False

    HIDE_ATTRS_FROM_UI: ClassVar[frozenset[str]] = frozenset(
        (
            "log",
            "dag",  # We show dag_id, don't need to show this too
            "node_id",  # Duplicates task_id
            "task_group",  # Doesn't have a useful repr, no point showing in UI
            "inherits_from_empty_operator",  # impl detail
            "inherits_from_skipmixin",  # impl detail
            # Decide whether to start task execution from triggerer
            "start_trigger_args",
            "start_from_trigger",
            # For compatibility with TG, for operators these are just the current task, no point showing
            "roots",
            "leaves",
            # These lists are already shown via *_task_ids
            "upstream_list",
            "downstream_list",
            # Not useful, implementation detail, already shown elsewhere
            "global_operator_extra_link_dict",
            "operator_extra_link_dict",
        )
    )

    def get_dag(self) -> DAG | None:
        raise NotImplementedError()

    @property
    def task_type(self) -> str:
        raise NotImplementedError()

    @property
    def operator_name(self) -> str:
        raise NotImplementedError()

    @property
    def inherits_from_empty_operator(self) -> bool:
        raise NotImplementedError()

    _is_sensor: bool = False
    _is_mapped: bool = False
    _can_skip_downstream: bool = False

    @property
    def dag_id(self) -> str:
        """Returns dag id if it has one or an adhoc + owner."""
        dag = self.get_dag()
        if dag:
            return dag.dag_id
        return f"adhoc_{self.owner}"

    @property
    def node_id(self) -> str:
        return self.task_id

    @property
    @abstractmethod
    def task_display_name(self) -> str: ...

    @property
    def is_mapped(self):
        return self._is_mapped

    @property
    def label(self) -> str | None:
        if self.task_display_name and self.task_display_name != self.task_id:
            return self.task_display_name
        # Prefix handling if no display is given is cloned from taskmixin for compatibility
        tg = self.task_group
        if tg and tg.node_id and tg.prefix_group_id:
            # "task_group_id.task_id" -> "task_id"
            return self.task_id[len(tg.node_id) + 1 :]
        return self.task_id

    @property
    def on_failure_fail_dagrun(self):
        """
        Whether the operator should fail the dagrun on failure.

        :meta private:
        """
        return self._on_failure_fail_dagrun

    @on_failure_fail_dagrun.setter
    def on_failure_fail_dagrun(self, value):
        """
        Setter for on_failure_fail_dagrun property.

        :meta private:
        """
        if value is True and self.is_teardown is not True:
            raise ValueError(
                f"Cannot set task on_failure_fail_dagrun for "
                f"'{self.task_id}' because it is not a teardown task."
            )
        self._on_failure_fail_dagrun = value

    @property
    def inherits_from_skipmixin(self):
        """Used to determine if an Operator is inherited from SkipMixin or its subclasses (e.g., BranchMixin)."""
        return getattr(self, "_can_skip_downstream", False)

    def as_setup(self):
        self.is_setup = True
        return self

    def as_teardown(
        self,
        *,
        setups: BaseOperator | Iterable[BaseOperator] | None = None,
        on_failure_fail_dagrun: bool | None = None,
    ):
        self.is_teardown = True
        self.trigger_rule = TriggerRule.ALL_DONE_SETUP_SUCCESS
        if on_failure_fail_dagrun is not None:
            self.on_failure_fail_dagrun = on_failure_fail_dagrun
        if setups is not None:
            setups = [setups] if isinstance(setups, DependencyMixin) else setups
            for s in setups:
                s.is_setup = True
                s >> self
        return self

    def __enter__(self):
        if not self.is_setup and not self.is_teardown:
            raise RuntimeError("Only setup/teardown tasks can be used as context managers.")
        SetupTeardownContext.push_setup_teardown_task(self)
        return SetupTeardownContext

    def __exit__(self, exc_type, exc_val, exc_tb):
        SetupTeardownContext.set_work_task_roots_and_leaves()

    def get_flat_relative_ids(self, *, upstream: bool = False) -> set[str]:
        """
        Get a flat set of relative IDs, upstream or downstream.

        Will recurse each relative found in the direction specified.

        :param upstream: Whether to look for upstream or downstream relatives.
        """
        dag = self.get_dag()
        if not dag:
            return set()

        relatives: set[str] = set()

        # This is intentionally implemented as a loop, instead of calling
        # get_direct_relative_ids() recursively, since Python has significant
        # limitation on stack level, and a recursive implementation can blow up
        # if a DAG contains very long routes.
        task_ids_to_trace = self.get_direct_relative_ids(upstream)
        while task_ids_to_trace:
            task_ids_to_trace_next: set[str] = set()
            for task_id in task_ids_to_trace:
                if task_id in relatives:
                    continue
                task_ids_to_trace_next.update(dag.task_dict[task_id].get_direct_relative_ids(upstream))
                relatives.add(task_id)
            task_ids_to_trace = task_ids_to_trace_next

        return relatives

    def get_flat_relatives(self, upstream: bool = False) -> Collection[Operator]:
        """Get a flat list of relatives, either upstream or downstream."""
        dag = self.get_dag()
        if not dag:
            return set()
        return [dag.task_dict[task_id] for task_id in self.get_flat_relative_ids(upstream=upstream)]

    def get_upstreams_follow_setups(self) -> Iterable[Operator]:
        """All upstreams and, for each upstream setup, its respective teardowns."""
        for task in self.get_flat_relatives(upstream=True):
            yield task
            if task.is_setup:
                for t in task.downstream_list:
                    if t.is_teardown and t != self:
                        yield t

    def get_upstreams_only_setups_and_teardowns(self) -> Iterable[Operator]:
        """
        Only *relevant* upstream setups and their teardowns.

        This method is meant to be used when we are clearing the task (non-upstream) and we need
        to add in the *relevant* setups and their teardowns.

        Relevant in this case means, the setup has a teardown that is downstream of ``self``,
        or the setup has no teardowns.
        """
        downstream_teardown_ids = {
            x.task_id for x in self.get_flat_relatives(upstream=False) if x.is_teardown
        }
        for task in self.get_flat_relatives(upstream=True):
            if not task.is_setup:
                continue
            has_no_teardowns = not any(True for x in task.downstream_list if x.is_teardown)
            # if task has no teardowns or has teardowns downstream of self
            if has_no_teardowns or task.downstream_task_ids.intersection(downstream_teardown_ids):
                yield task
                for t in task.downstream_list:
                    if t.is_teardown and t != self:
                        yield t

    def get_upstreams_only_setups(self) -> Iterable[Operator]:
        """
        Return relevant upstream setups.

        This method is meant to be used when we are checking task dependencies where we need
        to wait for all the upstream setups to complete before we can run the task.
        """
        for task in self.get_upstreams_only_setups_and_teardowns():
            if task.is_setup:
                yield task

    # TODO: Task-SDK -- Should the following methods removed?
    #   get_template_env
    #   _render
    def get_template_env(self, dag: DAG | None = None) -> jinja2.Environment:
        """Get the template environment for rendering templates."""
        if dag is None:
            dag = self.get_dag()
        return super().get_template_env(dag=dag)

    def _render(self, template, context, dag: DAG | None = None):
        if dag is None:
            dag = self.get_dag()
        return super()._render(template, context, dag=dag)

    def _do_render_template_fields(
        self,
        parent: Any,
        template_fields: Iterable[str],
        context: Context,
        jinja_env: jinja2.Environment,
        seen_oids: set[int],
    ) -> None:
        """Override the base to use custom error logging."""
        for attr_name in template_fields:
            try:
                value = getattr(parent, attr_name)
            except AttributeError:
                raise AttributeError(
                    f"{attr_name!r} is configured as a template field "
                    f"but {parent.task_type} does not have this attribute."
                )
            try:
                if not value:
                    continue
            except Exception:
                # This may happen if the templated field points to a class which does not support `__bool__`,
                # such as Pandas DataFrames:
                # https://github.com/pandas-dev/pandas/blob/9135c3aaf12d26f857fcc787a5b64d521c51e379/pandas/core/generic.py#L1465
                log.info(
                    "Unable to check if the value of type '%s' is False for task '%s', field '%s'.",
                    type(value).__name__,
                    self.task_id,
                    attr_name,
                )
                # We may still want to render custom classes which do not support __bool__
                pass

            try:
                if callable(value):
                    rendered_content = value(context=context, jinja_env=jinja_env)
                else:
                    rendered_content = self.render_template(value, context, jinja_env, seen_oids)
            except Exception:
                # TODO: Mask the value. Depends on https://github.com/apache/airflow/issues/45438
                log.exception(
                    "Exception rendering Jinja template for task '%s', field '%s'. Template: %r",
                    self.task_id,
                    attr_name,
                    value,
                )
                raise
            else:
                setattr(parent, attr_name, rendered_content)

    def _iter_all_mapped_downstreams(self) -> Iterator[MappedOperator | MappedTaskGroup]:
        """
        Return mapped nodes that are direct dependencies of the current task.

        For now, this walks the entire DAG to find mapped nodes that has this
        current task as an upstream. We cannot use ``downstream_list`` since it
        only contains operators, not task groups. In the future, we should
        provide a way to record an DAG node's all downstream nodes instead.

        Note that this does not guarantee the returned tasks actually use the
        current task for task mapping, but only checks those task are mapped
        operators, and are downstreams of the current task.

        To get a list of tasks that uses the current task for task mapping, use
        :meth:`iter_mapped_dependants` instead.
        """
        from airflow.sdk.definitions.mappedoperator import MappedOperator
        from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup

        def _walk_group(group: TaskGroup) -> Iterable[tuple[str, DAGNode]]:
            """
            Recursively walk children in a task group.

            This yields all direct children (including both tasks and task
            groups), and all children of any task groups.
            """
            for key, child in group.children.items():
                yield key, child
                if isinstance(child, TaskGroup):
                    yield from _walk_group(child)

        dag = self.get_dag()
        if not dag:
            raise RuntimeError("Cannot check for mapped dependants when not attached to a DAG")
        for key, child in _walk_group(dag.task_group):
            if key == self.node_id:
                continue
            if not isinstance(child, (MappedOperator, MappedTaskGroup)):
                continue
            if self.node_id in child.upstream_task_ids:
                yield child

    def iter_mapped_dependants(self) -> Iterator[MappedOperator | MappedTaskGroup]:
        """
        Return mapped nodes that depend on the current task the expansion.

        For now, this walks the entire DAG to find mapped nodes that has this
        current task as an upstream. We cannot use ``downstream_list`` since it
        only contains operators, not task groups. In the future, we should
        provide a way to record an DAG node's all downstream nodes instead.
        """
        return (
            downstream
            for downstream in self._iter_all_mapped_downstreams()
            if any(p.node_id == self.node_id for p in downstream.iter_mapped_dependencies())
        )

    def iter_mapped_task_groups(self) -> Iterator[MappedTaskGroup]:
        """
        Return mapped task groups this task belongs to.

        Groups are returned from the innermost to the outmost.

        :meta private:
        """
        if (group := self.task_group) is None:
            return
        yield from group.iter_mapped_task_groups()

    def get_closest_mapped_task_group(self) -> MappedTaskGroup | None:
        """
        Get the mapped task group "closest" to this task in the DAG.

        :meta private:
        """
        return next(self.iter_mapped_task_groups(), None)

    def get_needs_expansion(self) -> bool:
        """
        Return true if the task is MappedOperator or is in a mapped task group.

        :meta private:
        """
        if self._needs_expansion is None:
            if self.get_closest_mapped_task_group() is not None:
                self._needs_expansion = True
            else:
                self._needs_expansion = False
        return self._needs_expansion

    @methodtools.lru_cache(maxsize=None)
    def get_parse_time_mapped_ti_count(self) -> int:
        """
        Return the number of mapped task instances that can be created on DAG run creation.

        This only considers literal mapped arguments, and would return *None*
        when any non-literal values are used for mapping.

        :raise NotFullyPopulated: If non-literal mapped arguments are encountered.
        :raise NotMapped: If the operator is neither mapped, nor has any parent
            mapped task groups.
        :return: Total number of mapped TIs this task should have.
        """
        group = self.get_closest_mapped_task_group()
        if group is None:
            raise NotMapped()
        return group.get_parse_time_mapped_ti_count()
