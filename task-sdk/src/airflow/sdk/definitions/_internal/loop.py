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

from collections.abc import Callable
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup

if TYPE_CHECKING:
    from airflow.sdk.definitions.decorators.task_group import _TaskGroupFactory


def _validate_max_iterations(instance, attribute, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("max_iterations must be a positive integer")


@attrs.define(kw_only=True, repr=False)
class LoopTaskGroup(TaskGroup):
    """Private loop definition; runtime iteration creation is owned by the scheduler."""

    max_iterations: int = attrs.field(validator=_validate_max_iterations)
    has_until: bool = attrs.field()
    terminal_task_id: str = attrs.field(init=False, eq=False)
    gate_task_id: str = attrs.field(init=False, eq=False)

    def __attrs_post_init__(self):
        parent = self.parent_group
        while parent is not None:
            if isinstance(parent, LoopTaskGroup):
                raise NotImplementedError("Nested loops are not supported")
            if isinstance(parent, MappedTaskGroup):
                raise NotImplementedError("A loop cannot be inside a mapped task group")
            parent = parent.parent_group
        super().__attrs_post_init__()


def check_dag_result_outside_loop(operator: Any) -> None:
    """Raise if the operator sits in a loop group, whose tasks cannot be the Dag result."""
    group = operator.task_group
    while group is not None:
        if isinstance(group, LoopTaskGroup):
            raise ValueError(
                f"Task {operator.task_id!r} is inside a loop and cannot be a Dag result; "
                "publish the result through a task outside the loop"
            )
        group = group.parent_group


class LoopGateOperator(BaseOperator):
    """Definition of a loop gate task."""

    def __init__(self, *, until: Callable[..., bool] | None = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.until = until


def create_loop(
    factory: _TaskGroupFactory,
    /,
    *,
    max_iterations: int,
    until: Callable[..., bool] | None = None,
) -> LoopTaskGroup:
    """Construct a loop group and its gate."""
    from airflow.sdk.bases.decorator import get_unique_task_id

    if until is not None and not callable(until):
        raise TypeError("until must be callable")
    group = LoopTaskGroup(
        add_suffix_on_collision=True,
        **factory.tg_kwargs,
        max_iterations=max_iterations,
        has_until=until is not None,
    )
    with group:
        factory.apply_function_doc(group)
        factory.function(**factory.partial_kwargs)
        terminals = {task.task_id: task for task in group.get_leaves()}
        if len(terminals) != 1:
            raise ValueError("A loop body must have exactly one terminal task definition")
        terminal = next(iter(terminals.values()))
        gate_name = getattr(until, "__name__", "__loop_gate")
        if not gate_name.isidentifier() or not gate_name.isascii():
            gate_name = "__loop_gate"
        gate = LoopGateOperator(
            task_id=get_unique_task_id(gate_name, task_group=group),
            until=until,
        )
        terminal >> gate
        group.terminal_task_id = terminal.task_id
        group.gate_task_id = gate.task_id
    factory._task_group_created = True
    return group
