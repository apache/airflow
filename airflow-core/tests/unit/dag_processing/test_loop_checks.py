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
import re

import pytest

from airflow.dag_processing.loop_checks import normalize_loops
from airflow.sdk import DAG, BaseOperator, task, task_group
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.serialization.serialized_objects import DagSerialization


def _serialize_loop_dag() -> dict:
    """``body.first >> body.terminal`` in a loop whose gate leads to ``outside``, serialized."""

    @task_group
    def body():
        BaseOperator(task_id="first") >> BaseOperator(task_id="terminal")

    with DAG("looped", schedule=None) as dag:
        create_loop(body, max_iterations=2) >> BaseOperator(task_id="outside")
    return DagSerialization.to_dict(dag)


def _get_downstream(data: dict) -> dict[str, list[str]]:
    return {
        task["__var"]["task_id"]: task["__var"].get("downstream_task_ids", [])
        for task in data["dag"]["tasks"]
    }


def _set_downstream(data: dict, task_id: str, downstream: list[str]) -> None:
    task = next(task for task in data["dag"]["tasks"] if task["__var"]["task_id"] == task_id)
    task["__var"]["downstream_task_ids"] = downstream


def _normalize(data: dict) -> None:
    normalize_loops(data, DagSerialization.validate_serialized_dag(data))


def test_a_loop_edge_out_of_the_body_is_moved_to_leave_from_the_gate():
    data = _serialize_loop_dag()
    # As an SDK that leaves rewiring to core sends it: the terminal leads straight out of the loop.
    _set_downstream(data, "body.terminal", ["outside"])
    _set_downstream(data, "body.__loop_gate", [])

    _normalize(data)

    assert _get_downstream(data) == {
        "body.first": ["body.terminal"],
        "body.terminal": ["body.__loop_gate"],
        "body.__loop_gate": ["outside"],
        "outside": [],
    }


@pytest.mark.parametrize(
    "send",
    [
        pytest.param(lambda data: None, id="as-python-stores-it"),
        pytest.param(lambda data: _set_downstream(data, "outside", []), id="empty-edge-list"),
    ],
)
def test_a_loop_already_wired_through_its_gate_is_left_as_python_stores_it(send):
    data = _serialize_loop_dag()
    expected = copy.deepcopy(data)
    send(data)

    _normalize(data)

    assert data == expected


def _name_another_terminal(data: dict) -> None:
    data["dag"]["task_group"]["children"]["body"][1]["loop"]["terminal_task_id"] = "body.first"


def _name_a_missing_gate(data: dict) -> None:
    data["dag"]["task_group"]["children"]["body"][1]["loop"]["gate_task_id"] = "body.missing"


def _end_in_two_tasks(data: dict) -> None:
    _set_downstream(data, "body.first", [])


def _set_task_field(data: dict, task_id: str, key: str, value) -> None:
    task = next(task for task in data["dag"]["tasks"] if task["__var"]["task_id"] == task_id)
    task["__var"][key] = value


def _close_a_cycle_through_the_gate(data: dict) -> None:
    # No cycle as sent; putting the terminal before the gate closes one.
    _set_downstream(data, "body.terminal", ["outside"])
    _set_downstream(data, "body.__loop_gate", ["body.first"])


@pytest.mark.parametrize(
    ("break_loop", "error"),
    [
        pytest.param(_name_another_terminal, "has to end in exactly one task", id="wrong-terminal"),
        pytest.param(_end_in_two_tasks, "but it ends in ['body.first', 'body.terminal']", id="two-leaves"),
        pytest.param(_name_a_missing_gate, "which is not a task of it", id="missing-gate"),
        pytest.param(_close_a_cycle_through_the_gate, "makes a cycle", id="cycle-after-rewiring"),
        pytest.param(
            lambda data: _set_task_field(data, "body.__loop_gate", "task_type", "TypeScriptOperator"),
            "has to be serialized as LoopGateOperator, not TypeScriptOperator",
            id="gate-of-another-type",
        ),
        pytest.param(
            lambda data: _set_task_field(data, "body.first", "task_type", "LoopGateOperator"),
            "'body.first' is serialized as LoopGateOperator but gates no loop",
            id="body-task-typed-as-gate",
        ),
        pytest.param(
            lambda data: _set_task_field(data, "body.terminal", "returns_dag_result", True),
            "'body.terminal' is inside loop 'body' and cannot be a Dag result",
            id="dag-result-in-loop",
        ),
    ],
)
def test_a_loop_core_cannot_run_is_refused(break_loop, error):
    data = _serialize_loop_dag()
    break_loop(data)

    with pytest.raises(ValueError, match=re.escape(error)):
        _normalize(data)


def _serialize_expand_over_a_loop_task(producer_id: str) -> dict:
    @task
    def produce():
        return [1]

    @task
    def consume(value):
        return value

    @task_group
    def body():
        produce()

    with DAG("expand_outside", schedule=None) as dag:
        create_loop(body, max_iterations=2)
        consume.expand(value=dag.get_task(producer_id).output)
    return DagSerialization.to_dict(dag)


def _serialize_mapped_group_over_a_loop_task() -> dict:
    @task
    def produce():
        return [1]

    @task
    def work(value):
        return value

    @task_group
    def body():
        produce()

    @task_group
    def consume(value):
        work(value)

    with DAG("mapped_group_outside", schedule=None) as dag:
        create_loop(body, max_iterations=2)
        consume.expand(value=dag.get_task("body.produce").output)
    return DagSerialization.to_dict(dag)


def _serialize_loop_in_a_mapped_group() -> dict:
    @task
    def work(value):
        return value

    @task_group
    def mapped(value):
        @task_group
        def inner():
            work(value)

        inner()

    with DAG("loop_in_mapped", schedule=None) as dag:
        mapped.expand(value=[1, 2])
    data = DagSerialization.to_dict(dag)
    # Python refuses to build this, so the loop is added as an SDK would have written it.
    inner = data["dag"]["task_group"]["children"]["mapped"][1]["children"]["mapped.inner"][1]
    inner["loop"] = {
        "max_iterations": 2,
        "has_until": False,
        "terminal_task_id": "mapped.inner.work",
        "gate_task_id": "mapped.inner.work",
    }
    return data


def _serialize_mapped_gate() -> dict:
    @task
    def work(value):
        return value

    @task_group
    def body():
        work.expand(value=[1, 2])

    with DAG("mapped_gate", schedule=None) as dag:
        create_loop(body, max_iterations=2)
    data = DagSerialization.to_dict(dag)
    # Swap the roles, so the mapped task is the gate and the loop is otherwise one core can run.
    loop = data["dag"]["task_group"]["children"]["body"][1]["loop"]
    loop["gate_task_id"], loop["terminal_task_id"] = "body.work", "body.__loop_gate"
    _set_task_field(data, "body.work", "task_type", "LoopGateOperator")
    _set_task_field(data, "body.__loop_gate", "task_type", "EmptyOperator")
    _set_downstream(data, "body.work", [])
    return data


def _serialize_nested_loop() -> dict:
    @task_group
    def inner():
        BaseOperator(task_id="terminal")

    @task_group
    def outer():
        create_loop(inner, max_iterations=2)

    with DAG("nested", schedule=None) as dag:
        outer()
    data = DagSerialization.to_dict(dag)
    outer_group = data["dag"]["task_group"]["children"]["outer"][1]
    outer_group["loop"] = {
        **outer_group["children"]["outer.inner"][1]["loop"],
        "max_iterations": 3,
    }
    return data


def _serialize_gate_outside_any_loop() -> dict:
    with DAG("no_loop", schedule=None) as dag:
        BaseOperator(task_id="alone")
    data = DagSerialization.to_dict(dag)
    _set_task_field(data, "alone", "task_type", "LoopGateOperator")
    return data


@pytest.mark.parametrize(
    ("serialize", "error"),
    [
        pytest.param(
            lambda: _serialize_expand_over_a_loop_task("body.produce"),
            "'consume' cannot expand over 'body.produce', a task in loop 'body'",
            id="expand-from-outside",
        ),
        pytest.param(
            lambda: _serialize_expand_over_a_loop_task("body.__loop_gate"),
            "'consume' cannot expand over 'body.__loop_gate', a task in loop 'body'",
            id="expand-over-the-gate-from-outside",
        ),
        pytest.param(
            _serialize_mapped_group_over_a_loop_task,
            "'consume' cannot expand over 'body.produce', a task in loop 'body'",
            id="mapped-group-expands-from-outside",
        ),
        pytest.param(
            _serialize_loop_in_a_mapped_group,
            "Loop 'mapped.inner' cannot be inside mapped task group 'mapped'",
            id="inside-a-mapped-group",
        ),
        pytest.param(
            _serialize_mapped_gate,
            "The gate 'body.work' of loop 'body' cannot be mapped",
            id="mapped-gate",
        ),
        pytest.param(
            _serialize_gate_outside_any_loop,
            "'alone' is serialized as LoopGateOperator but gates no loop",
            id="gate-in-a-dag-with-no-loop",
        ),
        pytest.param(
            _serialize_nested_loop,
            "Loop 'outer.inner' is inside loop 'outer'",
            id="nested-loop",
        ),
    ],
)
def test_a_loop_placement_core_cannot_run_is_refused(serialize, error):
    with pytest.raises(ValueError, match=re.escape(error)):
        _normalize(serialize())
