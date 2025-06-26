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
from typing import TYPE_CHECKING, Any

import attrs
import structlog

from airflow.exceptions import AirflowException
from airflow.sdk.definitions._internal.abstractoperator import (
    AbstractOperator as TaskSDKAbstractOperator,
    NotMapped,
)
from airflow.sdk.definitions.mappedoperator import MappedOperator as TaskSDKMappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.triggers.base import StartTriggerArgs
from airflow.utils.helpers import prevent_duplicates

if TYPE_CHECKING:
    from collections.abc import Iterator

    from sqlalchemy.orm.session import Session

    from airflow.models import TaskInstance
    from airflow.models.dag import DAG as SchedulerDAG
    from airflow.sdk import BaseOperatorLink
    from airflow.sdk.definitions._internal.node import DAGNode
    from airflow.sdk.definitions.context import Context
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep

log = structlog.get_logger(__name__)


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
class MappedOperator(TaskSDKMappedOperator):  # type: ignore[misc] # It complains about weight_rule being different
    """Object representing a mapped operator in a DAG."""

    deps: frozenset[BaseTIDep] = attrs.field(init=False)

    @deps.default
    def _deps(self):
        from airflow.models.abstractoperator import DEFAULT_OPERATOR_DEPS

        return DEFAULT_OPERATOR_DEPS

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
        return False
        # start_from_trigger only makes sense when start_trigger_args exists.
        if not self.start_trigger_args:
            return False

        mapped_kwargs, _ = self._expand_mapped_kwargs(context)
        if self._disallow_kwargs_override:
            prevent_duplicates(
                self.partial_kwargs,
                mapped_kwargs,
                fail_reason="unmappable or already specified",
            )

        # Ordering is significant; mapped kwargs should override partial ones.
        return mapped_kwargs.get(
            "start_from_trigger", self.partial_kwargs.get("start_from_trigger", self.start_from_trigger)
        )

    def expand_start_trigger_args(self, *, context: Context, session: Session) -> StartTriggerArgs | None:
        """
        Get the kwargs to create the unmapped start_trigger_args.

        This method is for allowing mapped operator to start execution from triggerer.
        """
        if not self.start_trigger_args:
            return None

        mapped_kwargs, _ = self._expand_mapped_kwargs(context)
        if self._disallow_kwargs_override:
            prevent_duplicates(
                self.partial_kwargs,
                mapped_kwargs,
                fail_reason="unmappable or already specified",
            )

        # Ordering is significant; mapped kwargs should override partial ones.
        trigger_kwargs = mapped_kwargs.get(
            "trigger_kwargs",
            self.partial_kwargs.get("trigger_kwargs", self.start_trigger_args.trigger_kwargs),
        )
        next_kwargs = mapped_kwargs.get(
            "next_kwargs",
            self.partial_kwargs.get("next_kwargs", self.start_trigger_args.next_kwargs),
        )
        timeout = mapped_kwargs.get(
            "trigger_timeout", self.partial_kwargs.get("trigger_timeout", self.start_trigger_args.timeout)
        )
        return StartTriggerArgs(
            trigger_cls=self.start_trigger_args.trigger_cls,
            trigger_kwargs=trigger_kwargs,
            next_method=self.start_trigger_args.next_method,
            next_kwargs=next_kwargs,
            timeout=timeout,
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
        return link.get_link(self, ti_key=ti.key)  # type: ignore[arg-type]


@functools.singledispatch
def get_mapped_ti_count(task: DAGNode, run_id: str, *, session: Session) -> int:
    raise NotImplementedError(f"Not implemented for {type(task)}")


# https://github.com/python/cpython/issues/86153
# While we support Python 3.9 we can't rely on the type hint, we need to pass the type explicitly to
# register.
@get_mapped_ti_count.register(TaskSDKAbstractOperator)
def _(task: TaskSDKAbstractOperator, run_id: str, *, session: Session) -> int:
    group = task.get_closest_mapped_task_group()
    if group is None:
        raise NotMapped()
    return get_mapped_ti_count(group, run_id, session=session)


@get_mapped_ti_count.register(MappedOperator)
def _(task: MappedOperator, run_id: str, *, session: Session) -> int:
    from airflow.serialization.serialized_objects import BaseSerialization, _ExpandInputRef

    exp_input = task._get_specified_expand_input()
    if isinstance(exp_input, _ExpandInputRef):
        exp_input = exp_input.deref(task.dag)
    # TODO: TaskSDK This is only needed to support `dag.test()` etc until we port it over to use the
    # task sdk runner.
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


@get_mapped_ti_count.register(TaskGroup)
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
                # TODO: TaskSDK This is only needed to support `dag.test()` etc
                # until we port it over to use the task sdk runner.
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
