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

import re
from uuid import UUID

import pytest

from airflow.sdk import DAG, BaseOperator, LoopDecision, TaskInstanceState, task, task_group
from airflow.sdk._shared.secrets_masker import _secrets_masker
from airflow.sdk.api.datamodels._generated import LoopContext, LoopDecisionPayload
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import (
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    SucceedTask,
    TaskState,
    XComCountResponse,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.sdk.execution_time.loop import LoopMaxIterationsExceeded

LOOP_REGION_ID = UUID("0199a8c0-0000-7000-8000-000000000001")
FORKED_REGION_ID = UUID("0199a8c0-0000-7000-8000-000000000002")


@pytest.fixture
def loop_ti(create_runtime_ti):
    def make(*, index=1, max_iterations=3, until=None, map_index=-1):
        @task_group
        def body():
            BaseOperator(task_id="terminal")

        with DAG("loop_runtime", schedule=None) as dag:
            group = create_loop(body, max_iterations=max_iterations, until=until)
        ti = create_runtime_ti(task=dag.get_task(group.gate_task_id), map_index=map_index)
        ti._ti_context_from_server.loop = LoopContext(
            node_id=group.group_id,
            index=index,
            max_iterations=max_iterations,
            terminal_task_id=group.terminal_task_id,
            terminal_is_mapped=False,
            region_id=LOOP_REGION_ID,
            trajectory=1,
        )
        return ti

    return make


def test_first_iteration_previous_does_not_read_xcom(loop_ti, mock_supervisor_comms):
    ti = loop_ti(map_index=7)
    loop = ti.get_template_context()["loop"]

    assert loop.index == 1
    assert loop.max_iterations == 3
    assert loop.previous is None
    assert ti.map_index == 7
    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.parametrize("mapped", [False, True])
def test_body_callable_receives_loop_context_after_unmapping(create_runtime_ti, mapped):
    @task
    def terminal(value, *, loop, ti):
        return value, loop.index, ti.map_index

    @task_group
    def body():
        if mapped:
            terminal.expand(value=[5])
        else:
            terminal(5)

    with DAG("loop_body_runtime", schedule=None) as dag:
        group = create_loop(body, max_iterations=4)
    operator = dag.get_task(group.terminal_task_id)
    if mapped:
        operator = operator.unmap({"op_kwargs": {"value": 5}})
    ti = create_runtime_ti(task=operator, map_index=0 if mapped else -1)
    ti._ti_context_from_server.loop = LoopContext(
        node_id=group.group_id,
        index=2,
        max_iterations=4,
        terminal_task_id=group.terminal_task_id,
        terminal_is_mapped=mapped,
        region_id=LOOP_REGION_ID,
        trajectory=1,
    )

    assert operator.execute(ti.get_template_context()) == (5, 2, 0 if mapped else -1)


def test_task_instance_exposes_the_loop_iteration_and_its_trajectory(loop_ti):
    ti = loop_ti(index=2)
    ti._ti_context_from_server.loop = ti._ti_context_from_server.loop.model_copy(
        update={"trajectory": 2, "forked_from_region_id": FORKED_REGION_ID, "resumes_from_index": 2}
    )

    loop = ti.loop

    assert loop is not None
    assert (loop.index, loop.max_iterations) == (2, 3)
    assert (loop.region_id, loop.trajectory) == (LOOP_REGION_ID, 2)
    assert (loop.forked_from_region_id, loop.resumes_from_index) == (FORKED_REGION_ID, 2)


def test_task_instance_outside_a_loop_has_no_loop(create_runtime_ti):
    ti = create_runtime_ti(task=BaseOperator(task_id="plain"))

    assert ti.loop is None
    assert ti.loop_decision is None


@pytest.mark.parametrize("expanded_ti_count", [None, 4])
def test_task_instance_exposes_the_size_of_its_expansion(create_runtime_ti, expanded_ti_count):
    ti = create_runtime_ti(task=BaseOperator(task_id="mapped"))
    ti._ti_context_from_server.expanded_ti_count = expanded_ti_count

    assert ti.expanded_ti_count == expanded_ti_count


@pytest.mark.parametrize(
    ("index", "until", "expected"),
    [
        (1, lambda: LoopDecision(stop=False, reason="not yet"), LoopDecision(stop=False, reason="not yet")),
        (3, lambda: True, LoopDecision(stop=True)),
        (3, lambda: False, LoopDecision(stop=False)),
    ],
    ids=["continue", "until_met", "cap_reached"],
)
def test_gate_exposes_its_decision(loop_ti, mock_supervisor_comms, index, until, expected):
    ti = loop_ti(index=index, until=until)

    task_runner.run(ti, context=ti.get_template_context(), log=ti.task.log)

    assert ti.loop_decision == expected


@pytest.mark.parametrize("value", [0, False, [], ""])
def test_previous_and_current_result_preserve_falsey_values(loop_ti, mock_supervisor_comms, value):
    ti = loop_ti(index=2)
    mock_supervisor_comms.send.return_value = XComResult(key="return_value", value=value)
    loop = ti.get_template_context()["loop"]

    assert loop.previous == value
    previous = mock_supervisor_comms.send.call_args.args[0]
    assert isinstance(previous, GetXCom)
    assert previous.task_id == "body.terminal"
    assert previous.previous_iteration is True
    assert loop.result == value
    assert mock_supervisor_comms.send.call_args.args[0].previous_iteration is False


@pytest.mark.parametrize(
    ("index", "until", "expected"),
    [
        (1, None, LoopDecisionPayload(stop=False)),
        # max_iterations - 1: a zero-based cap check stops here instead, so this pins the base.
        (2, None, LoopDecisionPayload(stop=False)),
        (3, None, LoopDecisionPayload(stop=True)),
        (1, lambda: False, LoopDecisionPayload(stop=False)),
        (1, lambda: True, LoopDecisionPayload(stop=True)),
        (3, lambda: True, LoopDecisionPayload(stop=True)),
        (1, lambda: LoopDecision(stop=True), LoopDecisionPayload(stop=True)),
        (
            1,
            lambda: LoopDecision(stop=False, reason="score 0.7, needs > 0.9"),
            LoopDecisionPayload(stop=False, reason="score 0.7, needs > 0.9"),
        ),
        (
            1,
            lambda: LoopDecision(stop=True, reason="é" * 256),
            LoopDecisionPayload(stop=True, reason="é" * 256),
        ),
        (1, lambda: LoopDecision(stop=True, reason=""), LoopDecisionPayload(stop=True)),
        (
            1,
            lambda: LoopDecision(stop=True, reason="token hunter2 expired"),
            LoopDecisionPayload(stop=True, reason="token *** expired"),
        ),
    ],
    ids=[
        "fixed_continue",
        "fixed_continue_below_cap",
        "fixed_stop_at_cap",
        "bool_continue",
        "bool_stop",
        "bool_stop_at_cap",
        "decision_without_reason",
        "decision_with_reason",
        "reason_at_byte_limit",
        "empty_reason_is_no_reason",
        "secret_in_reason_is_masked",
    ],
)
@pytest.mark.enable_redact
def test_gate_sends_its_decision_with_success(loop_ti, mock_supervisor_comms, index, until, expected):
    _secrets_masker().add_mask("hunter2", None)
    ti = loop_ti(index=index, until=until)

    state, message, error = task_runner.run(ti, context=ti.get_template_context(), log=ti.task.log)

    assert (state, error) == (TaskInstanceState.SUCCESS, None)
    assert isinstance(message, SucceedTask)
    assert message.loop_decision == expected


def _raise_value_error():
    raise ValueError("condition failed")


@pytest.mark.parametrize(
    ("until", "error", "message"),
    [
        (_raise_value_error, ValueError, "condition failed"),
        (lambda: None, AirflowFailException, "got NoneType"),
        (lambda: 0, AirflowFailException, "got int"),
        (lambda: 1, AirflowFailException, "got int"),
        (lambda: [], AirflowFailException, "got list"),
        (lambda: "yes", AirflowFailException, "got str"),
        (lambda: LoopDecision(stop=1), AirflowFailException, "LoopDecision stop must be a bool, got int"),  # type: ignore[arg-type]
        (
            lambda: LoopDecision(stop=None, reason="r"),  # type: ignore[arg-type]
            AirflowFailException,
            "LoopDecision stop must be a bool, got NoneType",
        ),
        (
            lambda: LoopDecision(stop=True, reason=0.2),  # type: ignore[arg-type]
            AirflowFailException,
            "LoopDecision reason must be a str or None, got float",
        ),
        (
            lambda: LoopDecision(stop=True, reason=b"r"),  # type: ignore[arg-type]
            AirflowFailException,
            "LoopDecision reason must be a str or None, got bytes",
        ),
        (lambda: LoopDecision(stop=True, reason="x" * 513), AirflowFailException, "512 bytes"),
        (lambda: LoopDecision(stop=True, reason="é" * 257), AirflowFailException, "512 bytes"),
        (lambda: LoopDecision(stop=True, reason="\ud800"), AirflowFailException, "valid UTF-8"),
    ],
    ids=[
        "until_raises",
        "none",
        "zero",
        "one",
        "list",
        "str",
        "int_stop",
        "none_stop",
        "float_reason",
        "bytes_reason",
        "ascii_reason_over_limit",
        "utf8_reason_over_limit",
        "unpaired_surrogate_reason",
    ],
)
def test_gate_fails_without_a_decision(loop_ti, mock_supervisor_comms, until, error, message):
    ti = loop_ti(index=1, until=until)

    state, sent, raised = task_runner.run(ti, context=ti.get_template_context(), log=ti.task.log)

    assert state == TaskInstanceState.FAILED
    assert isinstance(raised, error)
    assert re.search(message, str(raised))
    assert isinstance(sent, TaskState)
    assert sent.loop_decision is None


def test_unmet_condition_before_the_cap_continues_instead_of_failing(loop_ti, mock_supervisor_comms):
    """The pass before the cap has an iteration left, so a false condition just continues."""
    ti = loop_ti(index=2, until=lambda: False)

    state, message, error = task_runner.run(ti, context=ti.get_template_context(), log=ti.task.log)

    assert (state, error) == (TaskInstanceState.SUCCESS, None)
    assert isinstance(message, SucceedTask)
    assert message.loop_decision == LoopDecisionPayload(stop=False)


@pytest.mark.parametrize(
    ("result", "message", "reason"),
    [
        (False, r"max_iterations=3$", None),
        (
            LoopDecision(stop=False, reason="error 0.2, needs < 1e-06"),
            r"max_iterations=3: error 0.2, needs",
            "error 0.2, needs < 1e-06",
        ),
    ],
    ids=["bool", "decision_with_reason"],
)
def test_gate_fails_without_retrying_when_the_condition_is_false_at_the_cap(
    loop_ti, mock_supervisor_comms, result, message, reason
):
    ti = loop_ti(index=3, until=lambda: result)

    state, sent, error = task_runner.run(ti, context=ti.get_template_context(), log=ti.task.log)

    assert state == TaskInstanceState.FAILED
    assert isinstance(error, LoopMaxIterationsExceeded)
    assert re.search(message, str(error))
    assert isinstance(sent, TaskState)
    assert sent.loop_decision == LoopDecisionPayload(stop=False, reason=reason)


@pytest.mark.parametrize("previous_iteration", [False, True])
@pytest.mark.parametrize("values", [[], [0], [0, False, ""]])
def test_mapped_terminal_keeps_iteration_across_lazy_reads(
    loop_ti, mock_supervisor_comms, previous_iteration, values
):
    ti = loop_ti(index=2)
    ti._ti_context_from_server.loop.terminal_is_mapped = True

    def respond(message):
        assert message.previous_iteration is previous_iteration
        if isinstance(message, GetXComCount):
            return XComCountResponse(len=len(values))
        if isinstance(message, GetXComSequenceItem):
            return XComSequenceIndexResult(root=values[message.offset])
        assert isinstance(message, GetXComSequenceSlice)
        return XComSequenceSliceResult(root=values[slice(message.start, message.stop, message.step)])

    mock_supervisor_comms.send.side_effect = respond
    context = ti.get_template_context()["loop"]
    result = context.previous if previous_iteration else context.result

    assert isinstance(result, LazyXComSequence)
    assert len(result) == len(values)
    assert result[:] == values
    assert result[::-1] == values[::-1]
    if values:
        assert result[-1] == values[-1]
