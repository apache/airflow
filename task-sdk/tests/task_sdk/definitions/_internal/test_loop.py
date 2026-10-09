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

from functools import partial

import pytest

from airflow.sdk import (
    DAG,
    BaseOperator,
    Label,
    TaskGroup,
    TriggerRule,
    chain,
    cross_downstream,
    task,
    task_group,
)
from airflow.sdk.definitions._internal.loop import LoopGateOperator, create_loop


@pytest.mark.parametrize("mapped", [False, True])
def test_loop_body_accepts_omitted_keyword_only_loop_context(mapped):
    @task
    def terminal(value, *, loop):
        return value + loop.index

    @task_group
    def body():
        with TaskGroup("nested"):
            if mapped:
                terminal.expand(value=[1, 2])
            else:
                terminal(1)

    with DAG("loop_context_argument", schedule=None) as dag:
        group = create_loop(body, max_iterations=2)

    assert group.terminal_task_id == "body.nested.terminal"
    assert dag.get_task(group.gate_task_id).upstream_task_ids == {group.terminal_task_id}


def test_ordinary_task_loop_argument_remains_required():
    @task
    def terminal(*, loop):
        return loop

    with DAG("ordinary_loop_argument", schedule=None):
        with pytest.raises(TypeError, match="missing a required (keyword-only )?argument: 'loop'"):
            terminal()
        result = terminal(loop="user value")

    assert result.operator.op_kwargs == {"loop": "user value"}


def test_loop_context_cannot_be_replaced_by_mapped_input():
    @task
    def terminal(*, loop):
        return loop.index

    @task_group
    def body():
        terminal.expand(loop=["user value"])

    with DAG("mapped_loop_context", schedule=None):
        with pytest.raises(ValueError, match="task context variable 'loop'"):
            create_loop(body, max_iterations=2)


def test_loop_dependencies_follow_terminal_instead_of_body_return():
    @task_group
    def body():
        """Loop documentation."""
        first = BaseOperator(task_id="first")
        second = BaseOperator(task_id="second")
        terminal = BaseOperator(task_id="terminal", trigger_rule=TriggerRule.ONE_SUCCESS)
        [first, second] >> terminal
        return first

    with DAG("loop_dependencies", schedule=None) as dag:
        start = BaseOperator(task_id="start")
        loop = create_loop(body, max_iterations=3)
        finish = BaseOperator(task_id="finish")
        start >> loop >> finish

    gate = dag.get_task(loop.gate_task_id)
    assert isinstance(gate, LoopGateOperator)
    assert gate.task_id == "body.__loop_gate"
    assert gate.until is None
    assert loop.max_iterations == 3
    assert loop.terminal_task_id == "body.terminal"
    assert loop.doc_md == "Loop documentation."
    assert gate.upstream_task_ids == {"body.terminal"}
    assert gate.downstream_task_ids == {"finish"}
    assert gate.trigger_rule == TriggerRule.ALL_SUCCESS
    assert dag.get_task("body.first").upstream_task_ids == {"start"}
    assert dag.get_task("body.second").upstream_task_ids == {"start"}
    assert dag.get_task("body.terminal").trigger_rule == TriggerRule.ONE_SUCCESS


def test_shared_terminal_with_two_teardowns_is_one_terminal_definition():
    @task_group
    def body():
        setup = BaseOperator(task_id="setup")
        work = BaseOperator(task_id="work")
        first = BaseOperator(task_id="first").as_teardown(setups=setup)
        second = BaseOperator(task_id="second").as_teardown(setups=setup)
        setup >> work >> [first, second]

    with DAG("loop_teardowns", schedule=None) as dag:
        loop = create_loop(body, max_iterations=2)

    assert loop.terminal_task_id == "body.work"
    assert dag.get_task(loop.gate_task_id).upstream_task_ids == {"body.work"}
    assert dag.get_task("body.work").downstream_task_ids == {
        "body.first",
        "body.second",
        loop.gate_task_id,
    }


@pytest.mark.parametrize(
    ("terminal_id", "gate_task_id"),
    [("terminal", "refine.converged"), ("converged", "refine.converged__1")],
)
def test_conditional_loop_names_gate_after_condition_and_preserves_arguments(terminal_id, gate_task_id):
    @task_group(group_id="refine")
    def body(queue):
        BaseOperator(task_id=terminal_id, queue=queue)

    def converged(loop):
        return loop.result

    with DAG("conditional_loop", schedule=None) as dag:
        loop = create_loop(body.partial(queue="compute"), max_iterations=4, until=converged)

    assert loop.gate_task_id == gate_task_id
    assert dag.get_task(loop.gate_task_id).until is converged
    assert dag.get_task(loop.terminal_task_id).queue == "compute"


@pytest.mark.parametrize("condition", [lambda loop: True, partial(bool)])
def test_condition_without_task_compatible_name_uses_internal_gate_name(condition):
    @task_group
    def body():
        BaseOperator(task_id="terminal")

    with DAG("unnamed_condition", schedule=None) as dag:
        loop = create_loop(body, max_iterations=2, until=condition)

    assert loop.gate_task_id == "body.__loop_gate"
    assert dag.get_task(loop.gate_task_id).until is condition


@pytest.mark.parametrize("trigger_rule", [TriggerRule.ALL_DONE, TriggerRule.ONE_SUCCESS])
def test_gate_inherits_body_trigger_rule_default(trigger_rule):
    @task_group(default_args={"trigger_rule": trigger_rule})
    def body():
        BaseOperator(task_id="terminal")

    with DAG("body_trigger_rule", schedule=None) as dag:
        loop = create_loop(body, max_iterations=2)

    assert dag.get_task(loop.terminal_task_id).trigger_rule == trigger_rule
    assert dag.get_task(loop.gate_task_id).trigger_rule == trigger_rule


@pytest.mark.parametrize("max_iterations", [0, -1, True, 1.5, "2", None])
def test_invalid_loop_limit_is_rejected(max_iterations):
    @task_group
    def body():
        BaseOperator(task_id="task")

    with DAG("invalid_limit", schedule=None):
        with pytest.raises((TypeError, ValueError), match="positive integer"):
            create_loop(body, max_iterations=max_iterations)


@pytest.mark.parametrize("until", [False, 3, "condition"])
def test_noncallable_condition_is_rejected(until):
    @task_group
    def body():
        BaseOperator(task_id="task")

    with DAG("invalid_condition", schedule=None):
        with pytest.raises(TypeError, match="callable"):
            create_loop(body, max_iterations=2, until=until)


@pytest.mark.parametrize("kind", ["function", "partial", "object"])
def test_async_condition_is_rejected(kind):
    @task_group
    def body():
        BaseOperator(task_id="terminal")

    async def until(*, loop):
        return True

    class AsyncCondition:
        async def __call__(self, *, loop):
            return True

    condition = {"function": until, "partial": partial(until), "object": AsyncCondition()}[kind]
    with DAG("async_condition", schedule=None):
        with pytest.raises(TypeError, match="synchronous"):
            create_loop(body, max_iterations=2, until=condition)


@pytest.mark.parametrize("terminal_count", [0, 2])
def test_loop_requires_one_terminal_task_definition(terminal_count):
    @task_group
    def body():
        for index in range(terminal_count):
            BaseOperator(task_id=f"task_{index}")

    with DAG("invalid_terminals", schedule=None):
        with pytest.raises(ValueError, match="exactly one terminal"):
            create_loop(body, max_iterations=2)


def test_mapped_terminal_inside_ordinary_nested_group_is_allowed():
    @task
    def terminal(value):
        return value

    @task_group
    def body():
        with TaskGroup("nested"):
            terminal.expand(value=[1, 2])

    with DAG("mapped_terminal", schedule=None) as dag:
        loop = create_loop(body, max_iterations=2)

    assert loop.terminal_task_id == "body.nested.terminal"
    assert dag.get_task(loop.gate_task_id).upstream_task_ids == {"body.nested.terminal"}


def test_nested_loop_is_rejected_through_ordinary_groups():
    @task_group
    def inner():
        BaseOperator(task_id="terminal")

    @task_group
    def outer():
        with TaskGroup("nested"):
            create_loop(inner, max_iterations=2)

    with DAG("nested_loop", schedule=None):
        with pytest.raises(NotImplementedError, match="Nested loops"):
            create_loop(outer, max_iterations=2)


def test_mapping_whole_loop_is_rejected():
    @task_group
    def body():
        BaseOperator(task_id="terminal")

    @task_group
    def mapped(value):
        create_loop(body, max_iterations=2)

    with DAG("mapped_loop", schedule=None):
        with pytest.raises(NotImplementedError, match="mapped task group"):
            mapped.expand(value=[1, 2])


def make_edge_dag():
    @task_group
    def body():
        BaseOperator(task_id="first") >> BaseOperator(task_id="second")

    with DAG("loop_edges", schedule=None) as dag:
        loop = create_loop(body, max_iterations=3)
        BaseOperator(task_id="before")
        BaseOperator(task_id="after")
    return dag, loop


def link_with_rshift(member, consumer):
    member >> consumer


def link_with_lshift(member, consumer):
    consumer << member


def link_with_set_downstream(member, consumer):
    member.set_downstream(consumer)


def link_with_set_upstream(member, consumer):
    consumer.set_upstream(member)


def link_with_list(member, consumer):
    [member] >> consumer


def link_with_label(member, consumer):
    member >> Label("loop result") >> consumer


def link_with_chain(member, consumer):
    chain(member, consumer)


def link_with_cross_downstream(member, consumer):
    cross_downstream([member], [consumer])


def link_with_xcom_arg(member, consumer):
    consumer.set_upstream(member.output)


@pytest.mark.parametrize(
    "link",
    [
        link_with_rshift,
        link_with_lshift,
        link_with_set_downstream,
        link_with_set_upstream,
        link_with_list,
        link_with_label,
        link_with_chain,
        link_with_cross_downstream,
        link_with_xcom_arg,
    ],
)
def test_outside_task_depends_on_the_gate_not_on_the_loop_task(link):
    dag, loop = make_edge_dag()

    link(dag.get_task("body.first"), dag.get_task("after"))

    assert dag.get_task("after").upstream_task_ids == {loop.gate_task_id}
    assert "after" in dag.get_task(loop.gate_task_id).downstream_task_ids
    assert dag.get_task("body.first").downstream_task_ids == {"body.second"}


def test_task_group_edge_to_an_outside_task_uses_the_gate():
    dag, loop = make_edge_dag()

    loop >> dag.get_task("after")

    assert dag.get_task("after").upstream_task_ids == {loop.gate_task_id}


def test_edges_that_stay_inside_or_enter_the_loop_are_unchanged():
    dag, loop = make_edge_dag()

    with dag, loop:
        extra = BaseOperator(task_id="extra")
    dag.get_task("body.first") >> extra
    dag.get_task("before") >> dag.get_task("body.first")
    dag.get_task("before") >> loop

    assert extra.upstream_task_ids == {"body.first"}
    assert dag.get_task("body.first").upstream_task_ids == {"before"}
    assert dag.get_task("body.second").upstream_task_ids == {"body.first"}
    assert dag.get_task(loop.gate_task_id).upstream_task_ids == {"body.second"}
    assert dag.get_task("before").downstream_task_ids == {"body.first"}


def test_xcom_argument_of_a_loop_task_keeps_reading_the_task_but_waits_for_the_gate():
    @task
    def produce():
        return 1

    @task
    def report(value):
        return value

    @task_group
    def body():
        produce()

    with DAG("loop_xcom_edge", schedule=None):
        loop = create_loop(body, max_iterations=2)
        consumer = report(loop["produce"].output)

    assert [arg.operator.task_id for arg in consumer.operator.op_args] == ["body.produce"]
    assert consumer.operator.upstream_task_ids == {loop.gate_task_id}


def test_mapped_consumer_expanded_over_a_loop_task_waits_for_the_gate():
    @task
    def produce():
        return [1, 2]

    @task
    def use(value):
        return value

    @task_group
    def body():
        produce()

    with DAG("loop_expand_edge", schedule=None):
        loop = create_loop(body, max_iterations=2)
        consumer = use.expand(value=loop["produce"].output)

    assert consumer.operator.upstream_task_ids == {loop.gate_task_id}


def test_edge_to_an_outside_task_made_while_the_body_is_built_waits_for_the_gate():
    with DAG("loop_early_edge", schedule=None) as dag:
        outside = BaseOperator(task_id="outside")

        @task_group
        def body():
            BaseOperator(task_id="first") >> outside

        loop = create_loop(body, max_iterations=2)

    assert outside.upstream_task_ids == {loop.gate_task_id}
    assert dag.get_task("body.first").downstream_task_ids == {loop.gate_task_id}
    assert outside.task_id in dag.get_task(loop.gate_task_id).downstream_task_ids
