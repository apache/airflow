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
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import attrs
import structlog
from sqlalchemy.orm import Session

from airflow.exceptions import AirflowException
from airflow.sdk.bases.operator import BaseOperator as TaskSDKBaseOperator
from airflow.sdk.definitions._internal.abstractoperator import NotMapped
from airflow.sdk.definitions.mappedoperator import MappedOperator as TaskSDKMappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.serialization.serialized_objects import DEFAULT_OPERATOR_DEPS, SerializedBaseOperator

if TYPE_CHECKING:
    from collections.abc import Iterator

    from airflow.models import TaskInstance
    from airflow.models.dag import DAG as SchedulerDAG
    from airflow.sdk import BaseOperatorLink
    from airflow.sdk.definitions._internal.node import DAGNode
    from airflow.sdk.definitions.context import Context
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep

log = structlog.get_logger(__name__)


def _prevent_duplicates(kwargs1: dict[str, Any], kwargs2: Mapping[str, Any], *, fail_reason: str) -> None:
    """
    Ensure *kwargs1* and *kwargs2* do not contain common keys.

    :raises TypeError: If common keys are found.
    """
    duplicated_keys = set(kwargs1).intersection(kwargs2)
    if not duplicated_keys:
        return
    if len(duplicated_keys) == 1:
        raise TypeError(f"{fail_reason} argument: {duplicated_keys.pop()}")
    duplicated_keys_display = ", ".join(sorted(duplicated_keys))
    raise TypeError(f"{fail_reason} arguments: {duplicated_keys_display}")


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
class MappedOperator(TaskSDKMappedOperator):
    """Object representing a mapped operator in a DAG."""

    deps: frozenset[BaseTIDep] = attrs.field(init=False, default=DEFAULT_OPERATOR_DEPS)

    def expand_start_from_trigger(self, *, context: Context, session: Session) -> bool:
        """
        Get the start_from_trigger value of the current abstract operator.

        MappedOperator uses this to unmap start_from_trigger to decide whether to start the task
        execution directly from triggerer.

        :meta private:
        """
        if self.partial_kwargs.get("start_from_trigger", self.start_from_trigger):
            log.warning(
                "Starting a mapped task from triggerer is currently unsupported",
                task_id=self.task_id,
                dag_id=self.dag_id,
            )

        # This is intentional. start_from_trigger does not work correctly with
        # sdk-db separation yet, so it is disabled unconditionally for now.
        # TODO: TaskSDK: Implement this properly.
        return False

        # start_from_trigger only makes sense when start_trigger_args exists.
        if not self.start_trigger_args:
            return False

        mapped_kwargs, _ = self._expand_mapped_kwargs(context)
        if self._disallow_kwargs_override:
            _prevent_duplicates(
                self.partial_kwargs,
                mapped_kwargs,
                fail_reason="unmappable or already specified",
            )

        # Ordering is significant; mapped kwargs should override partial ones.
        return mapped_kwargs.get(
            "start_from_trigger", self.partial_kwargs.get("start_from_trigger", self.start_from_trigger)
        )

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
        return link.get_link(self, ti_key=ti.key)


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
    if isinstance(exp_input, _ExpandInputRef):
        exp_input = exp_input.deref(task.dag)
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
