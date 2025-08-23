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

import functools
import operator
from typing import TYPE_CHECKING, Any, ClassVar, TypeAlias, TypeGuard

import attrs
import methodtools
import structlog
from sqlalchemy.orm import Session

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator as TaskSDKBaseOperator
from airflow.sdk.definitions._internal.abstractoperator import (
    DEFAULT_EXECUTOR,
    DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
    DEFAULT_OWNER,
    DEFAULT_POOL_NAME,
    DEFAULT_POOL_SLOTS,
    DEFAULT_PRIORITY_WEIGHT,
    DEFAULT_QUEUE,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_TRIGGER_RULE,
    DEFAULT_WEIGHT_RULE,
    NotMapped,
    TaskStateChangeCallbackAttrType,
)
from airflow.sdk.definitions._internal.node import DAGNode
from airflow.sdk.definitions.mappedoperator import MappedOperator as TaskSDKMappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.serialization.serialized_objects import DEFAULT_OPERATOR_DEPS, SerializedBaseOperator
from airflow.task.priority_strategy import PriorityWeightStrategy, validate_and_load_priority_weight_strategy

if TYPE_CHECKING:
    import datetime
    from collections.abc import Collection, Iterator, Sequence

    import pendulum

    from airflow.models import TaskInstance
    from airflow.models.dag import DAG as SchedulerDAG
    from airflow.models.expandinput import SchedulerExpandInput
    from airflow.sdk import BaseOperatorLink, Context, StartTriggerArgs
    from airflow.sdk.definitions.operator_resources import Resources
    from airflow.sdk.definitions.param import ParamsDict
    from airflow.task.trigger_rule import TriggerRule
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep

    Operator: TypeAlias = "SerializedBaseOperator | MappedOperator"

log = structlog.get_logger(__name__)


def is_mapped(task: Operator) -> TypeGuard[MappedOperator]:
    return task.is_mapped


@attrs.define(
    kw_only=True,
    # Disable custom __getstate__ and __setstate__ generation since it interacts
    # badly with Airflow's DAG serialization and pickling. When a mapped task is
    # deserialized, subclasses are coerced into MappedOperator, but when it goes
    # through DAG pickling, all attributes defined in the subclasses are dropped
    # by attrs's custom state management. Since attrs does not do anything too
    # special here (the logic is only important for slots=True), we use Python's
    # built-in implementation, which works (as proven by good old BaseOperator).
    getstate_setstate=False,
    repr=False,
)
# TODO (GH-52141): Duplicate DAGNode in the scheduler.
class MappedOperator(DAGNode):
    """Object representing a mapped operator in a DAG."""

    operator_class: dict[str, Any]
    partial_kwargs: dict[str, Any] = attrs.field(init=False, factory=dict)

    # Needed for serialization.
    task_id: str
    params: ParamsDict | dict = attrs.field(init=False, factory=dict)
    operator_extra_links: Collection[BaseOperatorLink]
    template_ext: Sequence[str]
    template_fields: Collection[str]
    template_fields_renderers: dict[str, str]
    ui_color: str
    ui_fgcolor: str
    _is_empty: bool = attrs.field(alias="is_empty", init=False, default=False)
    _can_skip_downstream: bool = attrs.field(alias="can_skip_downstream")
    _is_sensor: bool = attrs.field(alias="is_sensor", default=False)
    _task_module: str
    _task_type: str
    _operator_name: str
    start_trigger_args: StartTriggerArgs | None
    start_from_trigger: bool
    _needs_expansion: bool = True

    dag: SchedulerDAG = attrs.field(init=False)
    task_group: TaskGroup = attrs.field(init=False)
    start_date: pendulum.DateTime | None = attrs.field(init=False, default=None)
    end_date: pendulum.DateTime | None = attrs.field(init=False, default=None)
    upstream_task_ids: set[str] = attrs.field(factory=set, init=False)
    downstream_task_ids: set[str] = attrs.field(factory=set, init=False)

    _disallow_kwargs_override: bool
    """Whether execution fails if ``expand_input`` has duplicates to ``partial_kwargs``.

    If *False*, values from ``expand_input`` under duplicate keys override those
    under corresponding keys in ``partial_kwargs``.
    """

    _expand_input_attr: str
    """Where to get kwargs to calculate expansion length against.

    This should be a name to call ``getattr()`` on.
    """

    deps: frozenset[BaseTIDep] = attrs.field(init=False, default=DEFAULT_OPERATOR_DEPS)

    is_mapped: ClassVar[bool] = True

    @property
    def node_id(self) -> str:
        return self.task_id

    @property
    def roots(self) -> Sequence[DAGNode]:
        """Required by DAGNode."""
        return [self]

    @property
    def leaves(self) -> Sequence[DAGNode]:
        """Required by DAGNode."""
        return [self]

    # TODO (GH-52141): Review if any of the properties below are used in the
    # SDK and the scheduler, and remove those not needed.

    @property
    def task_type(self) -> str:
        """Implementing Operator."""
        return self._task_type

    @property
    def operator_name(self) -> str:
        return self._operator_name

    @property
    def task_display_name(self) -> str:
        return self.partial_kwargs.get("task_display_name") or self.task_id

    @property
    def doc_md(self) -> str | None:
        return self.partial_kwargs.get("doc_md")

    @property
    def map_index_template(self) -> str | None:
        return self.partial_kwargs.get("map_index_template")

    @property
    def inherits_from_empty_operator(self) -> bool:
        """Implementing an empty Operator."""
        return self._is_empty

    @property
    def inherits_from_skipmixin(self) -> bool:
        return self._can_skip_downstream

    @property
    def owner(self) -> str:
        return self.partial_kwargs.get("owner", DEFAULT_OWNER)

    @property
    def trigger_rule(self) -> TriggerRule:
        return self.partial_kwargs.get("trigger_rule", DEFAULT_TRIGGER_RULE)

    @property
    def is_setup(self) -> bool:
        return bool(self.partial_kwargs.get("is_setup"))

    @property
    def is_teardown(self) -> bool:
        return bool(self.partial_kwargs.get("is_teardown"))

    @property
    def depends_on_past(self) -> bool:
        return bool(self.partial_kwargs.get("depends_on_past"))

    @property
    def ignore_first_depends_on_past(self) -> bool:
        value = self.partial_kwargs.get("ignore_first_depends_on_past", DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST)
        return bool(value)

    @property
    def wait_for_downstream(self) -> bool:
        return bool(self.partial_kwargs.get("wait_for_downstream"))

    @property
    def retries(self) -> int:
        return self.partial_kwargs.get("retries", DEFAULT_RETRIES)

    @property
    def queue(self) -> str:
        return self.partial_kwargs.get("queue", DEFAULT_QUEUE)

    @property
    def pool(self) -> str:
        return self.partial_kwargs.get("pool", DEFAULT_POOL_NAME)

    @property
    def pool_slots(self) -> int:
        return self.partial_kwargs.get("pool_slots", DEFAULT_POOL_SLOTS)

    @property
    def resources(self) -> Resources | None:
        return self.partial_kwargs.get("resources")

    @property
    def max_active_tis_per_dag(self) -> int | None:
        return self.partial_kwargs.get("max_active_tis_per_dag")

    @property
    def max_active_tis_per_dagrun(self) -> int | None:
        return self.partial_kwargs.get("max_active_tis_per_dagrun")

    @property
    def on_execute_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_execute_callback") or []

    @property
    def on_failure_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_failure_callback") or []

    @property
    def on_retry_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_retry_callback") or []

    @property
    def on_success_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_success_callback") or []

    @property
    def on_skipped_callback(self) -> TaskStateChangeCallbackAttrType:
        return self.partial_kwargs.get("on_skipped_callback") or []

    @property
    def run_as_user(self) -> str | None:
        return self.partial_kwargs.get("run_as_user")

    @property
    def priority_weight(self) -> int:
        return self.partial_kwargs.get("priority_weight", DEFAULT_PRIORITY_WEIGHT)

    @property
    def retry_delay(self) -> datetime.timedelta:
        return self.partial_kwargs.get("retry_delay", DEFAULT_RETRY_DELAY)

    @property
    def retry_exponential_backoff(self) -> bool:
        return bool(self.partial_kwargs.get("retry_exponential_backoff"))

    @property
    def weight_rule(self) -> PriorityWeightStrategy:
        return validate_and_load_priority_weight_strategy(
            self.partial_kwargs.get("weight_rule", DEFAULT_WEIGHT_RULE)
        )

    @property
    def executor(self) -> str | None:
        return self.partial_kwargs.get("executor", DEFAULT_EXECUTOR)

    @property
    def executor_config(self) -> dict:
        return self.partial_kwargs.get("executor_config", {})

    @property
    def execution_timeout(self) -> datetime.timedelta | None:
        return self.partial_kwargs.get("execution_timeout")

    @property
    def inlets(self) -> list[Any]:
        return self.partial_kwargs.get("inlets", [])

    @property
    def outlets(self) -> list[Any]:
        return self.partial_kwargs.get("outlets", [])

    @property
    def on_failure_fail_dagrun(self) -> bool:
        return bool(self.partial_kwargs.get("on_failure_fail_dagrun"))

    @on_failure_fail_dagrun.setter
    def on_failure_fail_dagrun(self, v) -> None:
        self.partial_kwargs["on_failure_fail_dagrun"] = bool(v)

    def get_serialized_fields(self):
        return TaskSDKMappedOperator.get_serialized_fields()

    @functools.cached_property
    def operator_extra_link_dict(self) -> dict[str, BaseOperatorLink]:
        """Returns dictionary of all extra links for the operator."""
        op_extra_links_from_plugin: dict[str, Any] = {}
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()
        if plugins_manager.operator_extra_links is None:
            raise AirflowException("Can't load operators")
        operator_class_type = self.operator_class["task_type"]  # type: ignore
        for ope in plugins_manager.operator_extra_links:
            if ope.operators and any(operator_class_type in cls.__name__ for cls in ope.operators):
                op_extra_links_from_plugin.update({ope.name: ope})

        operator_extra_links_all = {link.name: link for link in self.operator_extra_links}
        # Extra links defined in Plugins overrides operator links defined in operator
        operator_extra_links_all.update(op_extra_links_from_plugin)

        return operator_extra_links_all

    @functools.cached_property
    def global_operator_extra_link_dict(self) -> dict[str, Any]:
        """Returns dictionary of all global extra links."""
        from airflow import plugins_manager

        plugins_manager.initialize_extra_operators_links_plugins()
        if plugins_manager.global_operator_extra_links is None:
            raise AirflowException("Can't load operators")
        return {link.name: link for link in plugins_manager.global_operator_extra_links}

    @functools.cached_property
    def extra_links(self) -> list[str]:
        return sorted(set(self.operator_extra_link_dict).union(self.global_operator_extra_link_dict))

    def get_extra_links(self, ti: TaskInstance, name: str) -> str | None:
        """
        For an operator, gets the URLs that the ``extra_links`` entry points to.

        :meta private:

        :raise ValueError: The error message of a ValueError will be passed on through to
            the fronted to show up as a tooltip on the disabled link.
        :param ti: The TaskInstance for the URL being searched for.
        :param name: The name of the link we're looking for the URL for. Should be
            one of the options specified in ``extra_links``.
        """
        link = self.operator_extra_link_dict.get(name) or self.global_operator_extra_link_dict.get(name)
        if not link:
            return None
        return link.get_link(self, ti_key=ti.key)  # type: ignore[arg-type] # TODO: GH-52141 - BaseOperatorLink.get_link expects BaseOperator but receives MappedOperator

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def _get_specified_expand_input(self) -> SchedulerExpandInput:
        """Input received from the expand call on the operator."""
        return getattr(self, self._expand_input_attr)

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def iter_mapped_task_groups(self) -> Iterator[MappedTaskGroup]:
        """
        Return mapped task groups this task belongs to.

        Groups are returned from the innermost to the outmost.

        :meta private:
        """
        if (group := self.task_group) is None:
            return
        yield from group.iter_mapped_task_groups()

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def get_closest_mapped_task_group(self) -> MappedTaskGroup | None:
        """
        Get the mapped task group "closest" to this task in the DAG.

        :meta private:
        """
        return next(self.iter_mapped_task_groups(), None)

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def get_needs_expansion(self) -> bool:
        """
        Return true if the task is MappedOperator or is in a mapped task group.

        :meta private:
        """
        return self._needs_expansion

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    @methodtools.lru_cache(maxsize=1)
    def get_parse_time_mapped_ti_count(self) -> int:
        current_count = self._get_specified_expand_input().get_parse_time_mapped_ti_count()

        def _get_parent_count() -> int:
            if (group := self.get_closest_mapped_task_group()) is None:
                raise NotMapped()
            return group.get_parse_time_mapped_ti_count()

        try:
            parent_count = _get_parent_count()
        except NotMapped:
            return current_count
        return parent_count * current_count

    def iter_mapped_dependencies(self) -> Iterator[Operator]:
        """Upstream dependencies that provide XComs used by this task for task mapping."""
        from airflow.models.xcom_arg import SchedulerXComArg

        for op, _ in SchedulerXComArg.iter_xcom_references(self._get_specified_expand_input()):
            yield op

    def expand_start_from_trigger(self, *, context: Context) -> bool:
        if not self.partial_kwargs.get("start_from_trigger", self.start_from_trigger):
            return False
        # TODO (GH-52141): Implement this.
        log.warning(
            "Starting a mapped task from triggerer is currently unsupported",
            task_id=self.task_id,
            dag_id=self.dag_id,
        )
        return False

    # TODO (GH-52141): Move the implementation in SDK MappedOperator here.
    def expand_start_trigger_args(self, *, context: Context) -> StartTriggerArgs | None:
        raise NotImplementedError


@functools.singledispatch
def get_mapped_ti_count(task: DAGNode, run_id: str, *, session: Session) -> int:
    raise NotImplementedError(f"Not implemented for {type(task)}")


# Still accept TaskSDKBaseOperator because some tests don't go through serialization.
# TODO (GH-52141): Rewrite tests so we can drop SDK references at some point.
@get_mapped_ti_count.register(SerializedBaseOperator)
@get_mapped_ti_count.register(TaskSDKBaseOperator)
def _(task: SerializedBaseOperator | TaskSDKBaseOperator, run_id: str, *, session: Session) -> int:
    group = task.get_closest_mapped_task_group()
    if group is None:
        raise NotMapped()
    return get_mapped_ti_count(group, run_id, session=session)


# Still accept TaskSDKMappedOperator because some tests don't go through serialization.
# TODO (GH-52141): Rewrite tests so we can drop SDK references at some point.
@get_mapped_ti_count.register(MappedOperator)
@get_mapped_ti_count.register(TaskSDKMappedOperator)
def _(task: MappedOperator | TaskSDKMappedOperator, run_id: str, *, session: Session) -> int:
    from airflow.serialization.serialized_objects import BaseSerialization, _ExpandInputRef

    exp_input = task._get_specified_expand_input()
    # TODO (GH-52141): 'task' here should be scheduler-bound and returns scheduler expand input.
    if not hasattr(exp_input, "get_total_map_length"):
        if TYPE_CHECKING:
            assert isinstance(task.dag, SchedulerDAG)
        current_count = (
            _ExpandInputRef(
                exp_input.EXPAND_INPUT_TYPE,
                BaseSerialization.deserialize(BaseSerialization.serialize(exp_input.value)),
            )
            .deref(task.dag)
            .get_total_map_length(run_id, session=session)
        )
    else:
        current_count = exp_input.get_total_map_length(run_id, session=session)

    group = task.get_closest_mapped_task_group()
    if group is None:
        return current_count
    parent_count = get_mapped_ti_count(group, run_id, session=session)
    return parent_count * current_count


@get_mapped_ti_count.register
def _(group: TaskGroup, run_id: str, *, session: Session) -> int:
    """
    Return the number of instances a task in this group should be mapped to at run time.

    This considers both literal and non-literal mapped arguments, and the
    result is therefore available when all depended tasks have finished. The
    return value should be identical to ``parse_time_mapped_ti_count`` if
    all mapped arguments are literal.

    If this group is inside mapped task groups, all the nested counts are
    multiplied and accounted.

    :raise NotFullyPopulated: If upstream tasks are not all complete yet.
    :return: Total number of mapped TIs this task should have.
    """
    from airflow.serialization.serialized_objects import BaseSerialization, _ExpandInputRef

    def iter_mapped_task_group_lengths(group) -> Iterator[int]:
        while group is not None:
            if isinstance(group, MappedTaskGroup):
                exp_input = group._expand_input
                # TODO (GH-52141): 'group' here should be scheduler-bound and returns scheduler expand input.
                if not hasattr(exp_input, "get_total_map_length"):
                    if TYPE_CHECKING:
                        assert isinstance(group.dag, SchedulerDAG)
                    exp_input = _ExpandInputRef(
                        exp_input.EXPAND_INPUT_TYPE,
                        BaseSerialization.deserialize(BaseSerialization.serialize(exp_input.value)),
                    ).deref(group.dag)
                yield exp_input.get_total_map_length(run_id, session=session)
            group = group.parent_group

    return functools.reduce(operator.mul, iter_mapped_task_group_lengths(group))
