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
"""
Check and normalize the task loops of a Dag a language SDK serialized.

Python checks a loop where the author writes it. A language SDK may check too, but the Dag processor is
what decides, so every language gets the same errors and the same graph.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

from airflow._shared.dagnode.cycle import detect_cycle
from airflow.models.task_coordinates import LOOP_GATE_OPERATOR
from airflow.serialization.definitions.mappedoperator import SerializedMappedOperator
from airflow.serialization.definitions.taskgroup import SerializedLoopTaskGroup, SerializedMappedTaskGroup

if TYPE_CHECKING:
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.definitions.taskgroup import SerializedTaskGroup


def normalize_loops(data: dict[str, Any], dag: SerializedDAG) -> None:
    """
    Raise ``ValueError`` for a loop core cannot run, and make each loop's gate its only way out.

    A task serialized as a gate outside a loop is refused too, in a Dag with no loop as well.

    *dag* is *data* deserialized. The edges are changed in *data*, which is what gets stored: the
    terminal task is put before the gate, and an edge from a body task to a task outside the loop
    leaves from the gate instead, as Python's ``create_loop`` does.
    """
    groups = dag.task_group.get_task_group_dict().values()
    loops = [group for group in groups if isinstance(group, SerializedLoopTaskGroup)]
    if not loops:
        _check_gates_are_in_their_loops(dag)
        return
    downstream = {
        task["__var"]["task_id"]: set(task["__var"].get("downstream_task_ids", []))
        for task in data["dag"]["tasks"]
    }
    members_by_loop = [(loop, _check_loop(loop, dag, downstream)) for loop in loops]
    _check_gates_are_in_their_loops(dag)
    _check_expansions_over_loop_tasks(dag, groups)
    for loop, members in members_by_loop:
        _rewire_edges_out(loop, members, downstream)
    # The Dag as sent was checked for a cycle; the edges changed here can still close one.
    if (task_id := detect_cycle(downstream, downstream.__getitem__)) is not None:
        raise ValueError(f"Making each loop's gate its only way out makes a cycle through {task_id!r}")
    for task in data["dag"]["tasks"]:
        edges = downstream[task["__var"]["task_id"]]
        if edges:
            task["__var"]["downstream_task_ids"] = sorted(edges)
        else:
            task["__var"].pop("downstream_task_ids", None)


def _check_gates_are_in_their_loops(dag: SerializedDAG) -> None:
    """Raise for a task serialized as a gate that is not the gate of its loop."""
    for task in dag.task_dict.values():
        if task.task_type != LOOP_GATE_OPERATOR:
            continue
        loop = _find_loop(task.task_group)
        if loop is None or loop.gate_task_id != task.task_id:
            raise ValueError(f"Task {task.task_id!r} is serialized as {LOOP_GATE_OPERATOR} but gates no loop")


def _check_loop(
    loop: SerializedLoopTaskGroup, dag: SerializedDAG, downstream: dict[str, set[str]]
) -> set[str]:
    """Raise unless *loop* is one core can run, and return the ids of the tasks it holds."""
    parent = loop.parent_group
    while parent is not None:
        if isinstance(parent, SerializedLoopTaskGroup):
            raise ValueError(
                f"Loop {loop.group_id!r} is inside loop {parent.group_id!r}; loops cannot be nested"
            )
        if isinstance(parent, SerializedMappedTaskGroup):
            raise ValueError(f"Loop {loop.group_id!r} cannot be inside mapped task group {parent.group_id!r}")
        parent = parent.parent_group
    tasks = list(loop.iter_tasks())
    members = {task.task_id for task in tasks}
    if loop.gate_task_id not in members:
        raise ValueError(
            f"Loop {loop.group_id!r} names gate {loop.gate_task_id!r}, which is not a task of it"
        )
    gate = dag.get_task(loop.gate_task_id)
    # Core finds a gate by its task type, so a gate serialized under another type would never end its loop.
    if gate.task_type != LOOP_GATE_OPERATOR:
        raise ValueError(
            f"The gate {loop.gate_task_id!r} of loop {loop.group_id!r} has to be serialized as "
            f"{LOOP_GATE_OPERATOR}, not {gate.task_type}"
        )
    # One gate instance makes one decision per iteration.
    if isinstance(gate, SerializedMappedOperator):
        raise ValueError(f"The gate {loop.gate_task_id!r} of loop {loop.group_id!r} cannot be mapped")
    for task in tasks:
        if task.returns_dag_result:
            raise ValueError(
                f"Task {task.task_id!r} is inside loop {loop.group_id!r} and cannot be a Dag result"
            )
    body = members - {loop.gate_task_id}
    leaves = sorted(task_id for task_id in body if not downstream[task_id] & body)
    if leaves != [loop.terminal_task_id]:
        raise ValueError(
            f"The body of loop {loop.group_id!r} has to end in exactly one task, its terminal task "
            f"{loop.terminal_task_id!r}, but it ends in {leaves}"
        )
    return members


def _find_loop(group: SerializedTaskGroup | None) -> SerializedLoopTaskGroup | None:
    while group is not None:
        if isinstance(group, SerializedLoopTaskGroup):
            return group
        group = group.parent_group
    return None


# TODO: Share these loop checks between Python and language SDK Dags;
# tracked at https://github.com/apache/airflow/issues/76903
def _check_expansions_over_loop_tasks(dag: SerializedDAG, groups: Iterable[SerializedTaskGroup]) -> None:
    """Raise for a mapped task or group that expands over a task of a loop it is not in."""
    consumers: list[tuple[str | None, SerializedLoopTaskGroup | None, Any]] = [
        (task.task_id, _find_loop(task.task_group), task.iter_mapped_dependencies())
        for task in dag.task_dict.values()
        if isinstance(task, SerializedMappedOperator)
    ]
    consumers.extend(
        (group.group_id, _find_loop(group), group.iter_mapped_dependencies())
        for group in groups
        if isinstance(group, SerializedMappedTaskGroup)
    )
    for consumer_id, consumer_loop, producers in consumers:
        for producer in producers:
            loop = _find_loop(producer.task_group)
            if loop is not None and (consumer_loop is None or consumer_loop.group_id != loop.group_id):
                raise ValueError(
                    f"{consumer_id!r} cannot expand over {producer.task_id!r}, a task in loop "
                    f"{loop.group_id!r}; only tasks in the same loop can expand over its output"
                )


def _rewire_edges_out(
    loop: SerializedLoopTaskGroup, members: set[str], downstream: dict[str, set[str]]
) -> None:
    gate = loop.gate_task_id
    downstream[loop.terminal_task_id].add(gate)
    for task_id in members - {gate}:
        outside = downstream[task_id] - members
        downstream[task_id] -= outside
        downstream[gate] |= outside
