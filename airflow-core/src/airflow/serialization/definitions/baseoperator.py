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
import functools
from typing import TYPE_CHECKING, Any

import methodtools

from airflow.exceptions import AirflowException
from airflow.serialization.definitions.node import DAGNode
from airflow.serialization.definitions.param import SerializedParamsDict
from airflow.serialization.enums import DagAttributeTypes
from airflow.task.priority_strategy import PriorityWeightStrategy, validate_and_load_priority_weight_strategy
from airflow.ti_deps.deps.mapped_task_upstream_dep import MappedTaskUpstreamDep
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep

if TYPE_CHECKING:
    from collections.abc import Collection, Iterable, Iterator, Sequence

    from airflow.models.taskinstance import TaskInstance
    from airflow.sdk import Context
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
    from airflow.serialization.definitions.operatorlink import XComOperatorLink
    from airflow.serialization.definitions.taskgroup import SerializedMappedTaskGroup, SerializedTaskGroup
    from airflow.task.trigger_rule import TriggerRule
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
    from airflow.triggers.base import StartTriggerArgs

DEFAULT_OPERATOR_DEPS: frozenset[BaseTIDep] = frozenset(
    (
        NotInRetryPeriodDep(),
        PrevDagrunDep(),
        TriggerRuleDep(),
        NotPreviouslySkippedDep(),
        MappedTaskUpstreamDep(),
    )
)


class SerializedBaseOperator(DAGNode):
    """
    Serialized representation of a BaseOperator instance.

    See :mod:`~airflow.serialization.serialized_objects.OperatorSerialization`
    for more details on operator serialization.
    """

    _can_skip_downstream: bool
    _is_empty: bool
    _needs_expansion: bool
    _task_display_name: str | None = None
    _weight_rule: str | PriorityWeightStrategy = "downstream"

    allow_nested_operators: bool = True
    dag: SerializedDAG | None = None
    depends_on_past: bool = False
    do_xcom_push: bool = True
    doc: str | None = None
    doc_md: str | None = None
    doc_json: str | None = None
    doc_yaml: str | None = None
    doc_rst: str | None = None
    downstream_task_ids: set[str] = set()
    email: str | Sequence[str] | None = None

    # These two are deprecated.
    email_on_retry: bool = True
    email_on_failure: bool = True

    execution_timeout: datetime.timedelta | None = None
    executor: str | None = None
    executor_config: dict = {}
    ignore_first_depends_on_past: bool = False

    inlets: Sequence = []
    is_setup: bool = False
    is_teardown: bool = False

    map_index_template: str | None = None
    max_active_tis_per_dag: int | None = None
    max_active_tis_per_dagrun: int | None = None
    max_retry_delay: datetime.timedelta | float | None = None
    multiple_outputs: bool = False

    # Boolean flags for callback existence
    has_on_execute_callback: bool = False
    has_on_failure_callback: bool = False
    has_on_retry_callback: bool = False
    has_on_success_callback: bool = False
    has_on_skipped_callback: bool = False

    operator_extra_links: Collection[XComOperatorLink] = []
    on_failure_fail_dagrun: bool = False

    outlets: Sequence = []
    owner: str = "airflow"
    params: SerializedParamsDict = SerializedParamsDict()
    pool: str = "default_pool"
    pool_slots: int = 1
    priority_weight: int = 1
    queue: str = "default"

    resources: dict[str, Any] | None = None
    retries: int = 0
    retry_delay: datetime.timedelta = datetime.timedelta(seconds=300)
    retry_exponential_backoff: float = 0
    run_as_user: str | None = None
    task_group: SerializedTaskGroup | None = None

    start_date: datetime.datetime | None = None
    end_date: datetime.datetime | None = None

    start_from_trigger: bool = False
    start_trigger_args: StartTriggerArgs | None = None

    task_type: str = "BaseOperator"
    template_ext: Sequence[str] = []
    template_fields: Collection[str] = []
    template_fields_renderers: dict[str, str] = {}

    trigger_rule: str | TriggerRule = "all_success"

    # TODO: Remove the following, they aren't used anymore
    ui_color: str = "#fff"
    ui_fgcolor: str = "#000"

    wait_for_downstream: bool = False
    wait_for_past_depends_before_skipping: bool = False

    is_mapped = False

    def __init__(self, *, task_id: str, _airflow_from_mapped: bool = False) -> None:
        super().__init__()
        self._BaseOperator__from_mapped = _airflow_from_mapped
        self.task_id = task_id
        self.deps = DEFAULT_OPERATOR_DEPS
        self._operator_name: str | None = None

    # Disable hashing.
    __hash__ = None  # type: ignore[assignment]

    def __eq__(self, other) -> bool:
        return NotImplemented

    def __repr__(self) -> str:
        return f"<SerializedTask({self.task_type}): {self.task_id}>"

    @classmethod
    def get_serialized_fields(cls):
        """Fields to deserialize from the serialized JSON object."""
        return frozenset(
            (
                "_logger_name",
                "_needs_expansion",
                "_task_display_name",
                "allow_nested_operators",
                "depends_on_past",
                "do_xcom_push",
                "doc",
                "doc_json",
                "doc_md",
                "doc_rst",
                "doc_yaml",
                "downstream_task_ids",
                "email",
                "email_on_failure",
                "email_on_retry",
                "end_date",
                "execution_timeout",
                "executor",
                "executor_config",
                "ignore_first_depends_on_past",
                "inlets",
                "is_setup",
                "is_teardown",
                "map_index_template",
                "max_active_tis_per_dag",
                "max_active_tis_per_dagrun",
                "max_retry_delay",
                "multiple_outputs",
                "has_on_execute_callback",
                "has_on_failure_callback",
                "has_on_retry_callback",
                "has_on_skipped_callback",
                "has_on_success_callback",
                "on_failure_fail_dagrun",
                "outlets",
                "owner",
                "params",
                "pool",
                "pool_slots",
                "priority_weight",
                "queue",
                "resources",
                "retries",
                "retry_delay",
                "retry_exponential_backoff",
                "run_as_user",
                "start_date",
                "start_from_trigger",
                "start_trigger_args",
                "task_id",
                "task_type",
                "template_ext",
                "template_fields",
                "template_fields_renderers",
                "trigger_rule",
                "ui_color",
                "ui_fgcolor",
                "wait_for_downstream",
                "wait_for_past_depends_before_skipping",
                "weight_rule",
            )
        )

    @property
    def node_id(self) -> str:
        return self.task_id

    def get_dag(self) -> SerializedDAG | None:
        return self.dag

    @property
    def roots(self) -> Sequence[DAGNode]:
        """Required by DAGNode."""
        return [self]

    @property
    def leaves(self) -> Sequence[DAGNode]:
        """Required by DAGNode."""
        return [self]

    @functools.cached_property
    def operator_extra_link_dict(self) -> dict[str, XComOperatorLink]:
        """All extra links for the operator."""
        return {link.name: link for link in self.operator_extra_links}

    @functools.cached_property
    def global_operator_extra_link_dict(self) -> dict[str, Any]:
        """All global extra links."""
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
        return link.get_link(self, ti_key=ti.key)

    @property
    def inherits_from_empty_operator(self) -> bool:
        return self._is_empty

    @property
    def inherits_from_skipmixin(self) -> bool:
        return self._can_skip_downstream

    @property
    def operator_name(self) -> str:
        # Overwrites operator_name of BaseOperator to use _operator_name instead of
        # __class__.operator_name.
        return self._operator_name or self.task_type

    @operator_name.setter
    def operator_name(self, operator_name: str):
        self._operator_name = operator_name

    @property
    def task_display_name(self) -> str:
        return self._task_display_name or self.task_id

    def expand_start_trigger_args(self, *, context: Context) -> StartTriggerArgs | None:
        return self.start_trigger_args

    @property
    def weight_rule(self) -> PriorityWeightStrategy:
        if isinstance(self._weight_rule, PriorityWeightStrategy):
            return self._weight_rule
        return validate_and_load_priority_weight_strategy(self._weight_rule)

    def __getattr__(self, name):
        # Handle missing attributes with task_type instead of SerializedBaseOperator
        # Don't intercept special methods that Python internals might check
        if name.startswith("__") and name.endswith("__"):
            # For special methods, raise the original error
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")
        # For regular attributes, use task_type in the error message
        raise AttributeError(f"'{self.task_type}' object has no attribute '{name}'")

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        return DagAttributeTypes.OP, self.task_id

    def expand_start_from_trigger(self, *, context: Context) -> bool:
        """
        Get the start_from_trigger value of the current abstract operator.

        Since a BaseOperator is not mapped to begin with, this simply returns
        the original value of start_from_trigger.

        :meta private:
        """
        return self.start_from_trigger

    def _iter_all_mapped_downstreams(self) -> Iterator[SerializedMappedOperator | SerializedMappedTaskGroup]:
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
        from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
        from airflow.serialization.definitions.taskgroup import SerializedMappedTaskGroup, SerializedTaskGroup

        def _walk_group(group: SerializedTaskGroup) -> Iterable[tuple[str, DAGNode]]:
            """
            Recursively walk children in a task group.

            This yields all direct children (including both tasks and task
            groups), and all children of any task groups.
            """
            for key, child in group.children.items():
                yield key, child
                if isinstance(child, SerializedTaskGroup):
                    yield from _walk_group(child)

        if not (dag := self.dag):
            raise RuntimeError("Cannot check for mapped dependants when not attached to a DAG")
        for key, child in _walk_group(dag.task_group):
            if key == self.node_id:
                continue
            if not isinstance(child, SerializedMappedOperator | SerializedMappedTaskGroup):
                continue
            if self.node_id in child.upstream_task_ids:
                yield child

    def iter_mapped_dependants(self) -> Iterator[SerializedMappedOperator | SerializedMappedTaskGroup]:
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

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def iter_mapped_task_groups(self) -> Iterator[SerializedMappedTaskGroup]:
        """
        Return mapped task groups this task belongs to.

        Groups are returned from the innermost to the outmost.

        :meta private:
        """
        if (group := self.task_group) is None:
            return
        yield from group.iter_mapped_task_groups()

    # TODO (GH-52141): Copied from sdk. Find a better place for this to live in.
    def get_closest_mapped_task_group(self) -> SerializedMappedTaskGroup | None:
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
        """
        Return the number of mapped task instances that can be created on DAG run creation.

        This only considers literal mapped arguments, and would return *None*
        when any non-literal values are used for mapping.

        :raise NotFullyPopulated: If non-literal mapped arguments are encountered.
        :raise NotMapped: If the operator is neither mapped, nor has any parent
            mapped task groups.
        :return: Total number of mapped TIs this task should have.
        """
        from airflow.exceptions import NotMapped

        group = self.get_closest_mapped_task_group()
        if group is None:
            raise NotMapped()
        return group.get_parse_time_mapped_ti_count()
