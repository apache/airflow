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
import inspect
from abc import abstractproperty
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Collection, Iterable, Iterator, Sequence

import methodtools
from sqlalchemy import select

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.models.expandinput import NotFullyPopulated
from airflow.models.taskmixin import DAGNode, DependencyMixin
from airflow.template.templater import Templater
from airflow.utils.context import Context
from airflow.utils.db import exists_query
from airflow.utils.log.secrets_masker import redact
from airflow.utils.setup_teardown import SetupTeardownContext
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.task_group import MappedTaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import NOTSET, ArgNotSet
from airflow.utils.weight_rule import WeightRule

TaskStateChangeCallback = Callable[[Context], None]

if TYPE_CHECKING:
    import jinja2  # Slow import.
    from sqlalchemy.orm import Session

    from airflow.models.baseoperator import BaseOperator
    from airflow.models.baseoperatorlink import BaseOperatorLink
    from airflow.models.dag import DAG
    from airflow.models.mappedoperator import MappedOperator
    from airflow.models.operator import Operator
    from airflow.models.taskinstance import TaskInstance
    from airflow.task.priority_strategy import PriorityWeightStrategy
    from airflow.utils.task_group import TaskGroup

DEFAULT_OWNER: str = conf.get_mandatory_value("operators", "default_owner")
DEFAULT_POOL_SLOTS: int = 1
DEFAULT_PRIORITY_WEIGHT: int = 1
DEFAULT_QUEUE: str = conf.get_mandatory_value("operators", "default_queue")
DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST: bool = conf.getboolean(
    "scheduler", "ignore_first_depends_on_past_by_default"
)
DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING: bool = False
DEFAULT_RETRIES: int = conf.getint("core", "default_task_retries", fallback=0)
DEFAULT_RETRY_DELAY: datetime.timedelta = datetime.timedelta(
    seconds=conf.getint("core", "default_task_retry_delay", fallback=300)
)
MAX_RETRY_DELAY: int = conf.getint("core", "max_task_retry_delay", fallback=24 * 60 * 60)

DEFAULT_WEIGHT_RULE: WeightRule = WeightRule(
    conf.get("core", "default_task_weight_rule", fallback=WeightRule.DOWNSTREAM)
)
DEFAULT_TRIGGER_RULE: TriggerRule = TriggerRule.ALL_SUCCESS
DEFAULT_TASK_EXECUTION_TIMEOUT: datetime.timedelta | None = conf.gettimedelta(
    "core", "default_task_execution_timeout"
)


class NotMapped(Exception):
    """Raise if a task is neither mapped nor has any parent mapped groups."""


class AbstractOperator(Templater, DAGNode):
    """Common implementation for operators, including unmapped and mapped.

    This base class is more about sharing implementations, not defining a common
    interface. Unfortunately it's difficult to use this as the common base class
    for typing due to BaseOperator carrying too much historical baggage.

    The union type ``from airflow.models.operator import Operator`` is easier
    to use for typing purposes.

    :meta private:
    """

    operator_class: type[BaseOperator] | dict[str, Any]

    weight_rule: PriorityWeightStrategy
    priority_weight: int

    # Defines the operator level extra links.
    operator_extra_links: Collection[BaseOperatorLink]

    owner: str
    task_id: str

    outlets: list
    inlets: list
    trigger_rule: TriggerRule

    _on_failure_fail_dagrun = False

    HIDE_ATTRS_FROM_UI: ClassVar[frozenset[str]] = frozenset(
        (
            "log",
            "dag",  # We show dag_id, don't need to show this too
            "node_id",  # Duplicates task_id
            "task_group",  # Doesn't have a useful repr, no point showing in UI
            "inherits_from_empty_operator",  # impl detail
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

    @abstractproperty
    def task_display_name(self) -> str: ...

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
    def is_setup(self) -> bool:
        raise NotImplementedError()

    @is_setup.setter
    def is_setup(self, value: bool) -> None:
        raise NotImplementedError()

    @property
    def is_teardown(self) -> bool:
        raise NotImplementedError()

    @is_teardown.setter
    def is_teardown(self, value: bool) -> None:
        raise NotImplementedError()

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

    def as_setup(self):
        self.is_setup = True
        return self

    def as_teardown(
        self,
        *,
        setups: BaseOperator | Iterable[BaseOperator] | ArgNotSet = NOTSET,
        on_failure_fail_dagrun=NOTSET,
    ):
        self.is_teardown = True
        self.trigger_rule = TriggerRule.ALL_DONE_SETUP_SUCCESS
        if on_failure_fail_dagrun is not NOTSET:
            self.on_failure_fail_dagrun = on_failure_fail_dagrun
        if not isinstance(setups, ArgNotSet):
            setups = [setups] if isinstance(setups, DependencyMixin) else setups
            for s in setups:
                s.is_setup = True
                s >> self
        return self

    def get_direct_relative_ids(self, upstream: bool = False) -> set[str]:
        """Get direct relative IDs to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_task_ids
        return self.downstream_task_ids

    def get_flat_relative_ids(self, *, upstream: bool = False) -> set[str]:
        """Get a flat set of relative IDs, upstream or downstream.

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

    def _iter_all_mapped_downstreams(self) -> Iterator[MappedOperator | MappedTaskGroup]:
        """Return mapped nodes that are direct dependencies of the current task.

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
        from airflow.models.mappedoperator import MappedOperator
        from airflow.utils.task_group import TaskGroup

        def _walk_group(group: TaskGroup) -> Iterable[tuple[str, DAGNode]]:
            """Recursively walk children in a task group.

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
        """Return mapped nodes that depend on the current task the expansion.

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
        """Return mapped task groups this task belongs to.

        Groups are returned from the innermost to the outmost.

        :meta private:
        """
        if (group := self.task_group) is None:
            return
        yield from group.iter_mapped_task_groups()

    def get_closest_mapped_task_group(self) -> MappedTaskGroup | None:
        """Get the mapped task group "closest" to this task in the DAG.

        :meta private:
        """
        return next(self.iter_mapped_task_groups(), None)

    def unmap(self, resolve: None | dict[str, Any] | tuple[Context, Session]) -> BaseOperator:
        """Get the "normal" operator from current abstract operator.

        MappedOperator uses this to unmap itself based on the map index. A non-
        mapped operator (i.e. BaseOperator subclass) simply returns itself.

        :meta private:
        """
        raise NotImplementedError()

    @property
    def priority_weight_total(self) -> int:
        """
        Total priority weight for the task. It might include all upstream or downstream tasks.

        Depending on the weight rule:

        - WeightRule.ABSOLUTE - only own weight
        - WeightRule.DOWNSTREAM - adds priority weight of all downstream tasks
        - WeightRule.UPSTREAM - adds priority weight of all upstream tasks
        """
        from airflow.task.priority_strategy import (
            _AbsolutePriorityWeightStrategy,
            _DownstreamPriorityWeightStrategy,
            _UpstreamPriorityWeightStrategy,
        )

        if type(self.weight_rule) == _AbsolutePriorityWeightStrategy:
            return self.priority_weight
        elif type(self.weight_rule) == _DownstreamPriorityWeightStrategy:
            upstream = False
        elif type(self.weight_rule) == _UpstreamPriorityWeightStrategy:
            upstream = True
        else:
            upstream = False
        dag = self.get_dag()
        if dag is None:
            return self.priority_weight
        return self.priority_weight + sum(
            dag.task_dict[task_id].priority_weight
            for task_id in self.get_flat_relative_ids(upstream=upstream)
        )

    @cached_property
    def operator_extra_link_dict(self) -> dict[str, Any]:
        """Returns dictionary of all extra links for the operator."""
        op_extra_links_from_plugin: dict[str, Any] = {}
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()
        if plugins_manager.operator_extra_links is None:
            raise AirflowException("Can't load operators")
        for ope in plugins_manager.operator_extra_links:
            if ope.operators and self.operator_class in ope.operators:
                op_extra_links_from_plugin.update({ope.name: ope})

        operator_extra_links_all = {link.name: link for link in self.operator_extra_links}
        # Extra links defined in Plugins overrides operator links defined in operator
        operator_extra_links_all.update(op_extra_links_from_plugin)

        return operator_extra_links_all

    @cached_property
    def global_operator_extra_link_dict(self) -> dict[str, Any]:
        """Returns dictionary of all global extra links."""
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()
        if plugins_manager.global_operator_extra_links is None:
            raise AirflowException("Can't load operators")
        return {link.name: link for link in plugins_manager.global_operator_extra_links}

    @cached_property
    def extra_links(self) -> list[str]:
        return sorted(set(self.operator_extra_link_dict).union(self.global_operator_extra_link_dict))

    def get_extra_links(self, ti: TaskInstance, link_name: str) -> str | None:
        """For an operator, gets the URLs that the ``extra_links`` entry points to.

        :meta private:

        :raise ValueError: The error message of a ValueError will be passed on through to
            the fronted to show up as a tooltip on the disabled link.
        :param ti: The TaskInstance for the URL being searched for.
        :param link_name: The name of the link we're looking for the URL for. Should be
            one of the options specified in ``extra_links``.
        """
        link: BaseOperatorLink | None = self.operator_extra_link_dict.get(link_name)
        if not link:
            link = self.global_operator_extra_link_dict.get(link_name)
            if not link:
                return None

        parameters = inspect.signature(link.get_link).parameters
        old_signature = all(name != "ti_key" for name, p in parameters.items() if p.kind != p.VAR_KEYWORD)

        if old_signature:
            return link.get_link(self.unmap(None), ti.dag_run.logical_date)  # type: ignore[misc]
        return link.get_link(self.unmap(None), ti_key=ti.key)

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
            raise NotMapped
        return group.get_parse_time_mapped_ti_count()

    def get_mapped_ti_count(self, run_id: str, *, session: Session) -> int:
        """
        Return the number of mapped TaskInstances that can be created at run time.

        This considers both literal and non-literal mapped arguments, and the
        result is therefore available when all depended tasks have finished. The
        return value should be identical to ``parse_time_mapped_ti_count`` if
        all mapped arguments are literal.

        :raise NotFullyPopulated: If upstream tasks are not all complete yet.
        :raise NotMapped: If the operator is neither mapped, nor has any parent
            mapped task groups.
        :return: Total number of mapped TIs this task should have.
        """
        group = self.get_closest_mapped_task_group()
        if group is None:
            raise NotMapped
        return group.get_mapped_ti_count(run_id, session=session)

    def expand_mapped_task(self, run_id: str, *, session: Session) -> tuple[Sequence[TaskInstance], int]:
        """Create the mapped task instances for mapped task.

        :raise NotMapped: If this task does not need expansion.
        :return: The newly created mapped task instances (if any) in ascending
            order by map index, and the maximum map index value.
        """
        from sqlalchemy import func, or_

        from airflow.models.baseoperator import BaseOperator
        from airflow.models.mappedoperator import MappedOperator
        from airflow.models.taskinstance import TaskInstance
        from airflow.settings import task_instance_mutation_hook

        if not isinstance(self, (BaseOperator, MappedOperator)):
            raise RuntimeError(f"cannot expand unrecognized operator type {type(self).__name__}")

        try:
            total_length: int | None = self.get_mapped_ti_count(run_id, session=session)
        except NotFullyPopulated as e:
            # It's possible that the upstream tasks are not yet done, but we
            # don't have upstream of upstreams in partial DAGs (possible in the
            # mini-scheduler), so we ignore this exception.
            if not self.dag or not self.dag.partial:
                self.log.error(
                    "Cannot expand %r for run %s; missing upstream values: %s",
                    self,
                    run_id,
                    sorted(e.missing),
                )
            total_length = None

        state: TaskInstanceState | None = None
        unmapped_ti: TaskInstance | None = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index == -1,
                or_(TaskInstance.state.in_(State.unfinished), TaskInstance.state.is_(None)),
            )
        ).one_or_none()

        all_expanded_tis: list[TaskInstance] = []

        if unmapped_ti:
            # The unmapped task instance still exists and is unfinished, i.e. we
            # haven't tried to run it before.
            if total_length is None:
                # If the DAG is partial, it's likely that the upstream tasks
                # are not done yet, so the task can't fail yet.
                if not self.dag or not self.dag.partial:
                    unmapped_ti.state = TaskInstanceState.UPSTREAM_FAILED
            elif total_length < 1:
                # If the upstream maps this to a zero-length value, simply mark
                # the unmapped task instance as SKIPPED (if needed).
                self.log.info(
                    "Marking %s as SKIPPED since the map has %d values to expand",
                    unmapped_ti,
                    total_length,
                )
                unmapped_ti.state = TaskInstanceState.SKIPPED
            else:
                zero_index_ti_exists = exists_query(
                    TaskInstance.dag_id == self.dag_id,
                    TaskInstance.task_id == self.task_id,
                    TaskInstance.run_id == run_id,
                    TaskInstance.map_index == 0,
                    session=session,
                )
                if not zero_index_ti_exists:
                    # Otherwise convert this into the first mapped index, and create
                    # TaskInstance for other indexes.
                    unmapped_ti.map_index = 0
                    self.log.debug("Updated in place to become %s", unmapped_ti)
                    all_expanded_tis.append(unmapped_ti)
                    session.flush()
                else:
                    self.log.debug("Deleting the original task instance: %s", unmapped_ti)
                    session.delete(unmapped_ti)
                state = unmapped_ti.state

        if total_length is None or total_length < 1:
            # Nothing to fixup.
            indexes_to_map: Iterable[int] = ()
        else:
            # Only create "missing" ones.
            current_max_mapping = session.scalar(
                select(func.max(TaskInstance.map_index)).where(
                    TaskInstance.dag_id == self.dag_id,
                    TaskInstance.task_id == self.task_id,
                    TaskInstance.run_id == run_id,
                )
            )
            indexes_to_map = range(current_max_mapping + 1, total_length)

        for index in indexes_to_map:
            # TODO: Make more efficient with bulk_insert_mappings/bulk_save_mappings.
            ti = TaskInstance(self, run_id=run_id, map_index=index, state=state)
            self.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti)
            ti = session.merge(ti)
            ti.refresh_from_task(self)  # session.merge() loses task information.
            all_expanded_tis.append(ti)

        # Coerce the None case to 0 -- these two are almost treated identically,
        # except the unmapped ti (if exists) is marked to different states.
        total_expanded_ti_count = total_length or 0

        # Any (old) task instances with inapplicable indexes (>= the total
        # number we need) are set to "REMOVED".
        query = select(TaskInstance).where(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.run_id == run_id,
            TaskInstance.map_index >= total_expanded_ti_count,
        )
        query = with_row_locks(query, of=TaskInstance, session=session, skip_locked=True)
        to_update = session.scalars(query)
        for ti in to_update:
            ti.state = TaskInstanceState.REMOVED
        session.flush()
        return all_expanded_tis, total_expanded_ti_count - 1

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        """Template all attributes listed in *self.template_fields*.

        If the operator is mapped, this should return the unmapped, fully
        rendered, and map-expanded operator. The mapped operator should not be
        modified. However, *context* may be modified in-place to reference the
        unmapped operator for template rendering.

        If the operator is not mapped, this should modify the operator in-place.
        """
        raise NotImplementedError()

    def _render(self, template, context, dag: DAG | None = None):
        if dag is None:
            dag = self.get_dag()
        return super()._render(template, context, dag=dag)

    def get_template_env(self, dag: DAG | None = None) -> jinja2.Environment:
        """Get the template environment for rendering templates."""
        if dag is None:
            dag = self.get_dag()
        return super().get_template_env(dag=dag)

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
                self.log.info(
                    "Unable to check if the value of type '%s' is False for task '%s', field '%s'.",
                    type(value).__name__,
                    self.task_id,
                    attr_name,
                )
                # We may still want to render custom classes which do not support __bool__
                pass

            try:
                rendered_content = self.render_template(
                    value,
                    context,
                    jinja_env,
                    seen_oids,
                )
            except Exception:
                value_masked = redact(name=attr_name, value=value)
                self.log.exception(
                    "Exception rendering Jinja template for task '%s', field '%s'. Template: %r",
                    self.task_id,
                    attr_name,
                    value_masked,
                )
                raise
            else:
                setattr(parent, attr_name, rendered_content)

    def __enter__(self):
        if not self.is_setup and not self.is_teardown:
            raise AirflowException("Only setup/teardown tasks can be used as context managers.")
        SetupTeardownContext.push_setup_teardown_task(self)
        return SetupTeardownContext

    def __exit__(self, exc_type, exc_val, exc_tb):
        SetupTeardownContext.set_work_task_roots_and_leaves()
