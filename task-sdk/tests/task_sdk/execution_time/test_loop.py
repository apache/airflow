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

import pytest

from airflow.sdk import DAG, BaseOperator, TaskInstanceState, task, task_group
from airflow.sdk.api.datamodels._generated import LoopContext
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.definitions._internal.loop import create_loop
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import (
    DeleteXCom,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    SetXCom,
    XComCountResponse,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.sdk.execution_time.loop import LoopMaxIterationsExceeded


@pytest.fixture
def loop_ti(create_runtime_ti):
    def make(*, index=0, max_iterations=3, until=None, map_index=-1):
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
        )
        return ti

    return make


def test_first_iteration_previous_does_not_read_xcom(loop_ti, mock_supervisor_comms):
    ti = loop_ti(map_index=7)
    loop = ti.get_template_context()["loop"]

    assert loop.index == 0
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
    )

    assert operator.execute(ti.get_template_context()) == (5, 2, 0 if mapped else -1)


@pytest.mark.parametrize("value", [0, False, [], ""])
def test_previous_and_current_result_preserve_falsey_values(loop_ti, mock_supervisor_comms, value):
    ti = loop_ti(index=1)
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
    ("index", "condition", "decision"),
    [(0, None, "continue"), (2, None, "stop"), (0, False, "continue"), (0, True, "stop"), (2, True, "stop")],
)
def test_gate_publishes_successful_decision(loop_ti, mock_supervisor_comms, index, condition, decision):
    def until(*, loop):
        assert loop.index == index
        return condition

    ti = loop_ti(index=index, until=until if condition is not None else None)
    context = ti.get_template_context()

    ti.task.execute(context)

    message = mock_supervisor_comms.send.call_args.args[0]
    assert isinstance(message, SetXCom)
    assert message.key == "_airflow_loop_decision"
    assert message.value == decision


@pytest.mark.parametrize(
    ("raises", "error", "message"),
    [(False, LoopMaxIterationsExceeded, "max_iterations"), (True, ValueError, "condition failed")],
)
def test_unsuccessful_gate_does_not_publish_decision(loop_ti, mock_supervisor_comms, raises, error, message):
    def until(*, loop):
        if raises:
            raise ValueError("condition failed")
        return False

    ti = loop_ti(index=2, until=until)
    with pytest.raises(error, match=message):
        ti.task.execute(ti.get_template_context())

    mock_supervisor_comms.send.assert_not_called()


def test_gate_fails_without_retrying_when_the_condition_is_false_at_the_cap(loop_ti, mock_supervisor_comms):
    ti = loop_ti(index=2, until=lambda: False)

    with pytest.raises(LoopMaxIterationsExceeded, match="max_iterations=3") as exc_info:
        ti.task.execute(ti.get_template_context())

    assert isinstance(exc_info.value, AirflowFailException)
    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.parametrize("result", [None, 0, 1, [], "yes"])
def test_gate_fails_when_until_does_not_return_a_bool(loop_ti, mock_supervisor_comms, result):
    ti = loop_ti(index=0, until=lambda: result)

    with pytest.raises(AirflowFailException, match=f"got {type(result).__name__}"):
        ti.task.execute(ti.get_template_context())

    mock_supervisor_comms.send.assert_not_called()


@pytest.mark.parametrize("previous_iteration", [False, True])
@pytest.mark.parametrize("values", [[], [0], [0, False, ""]])
def test_mapped_terminal_keeps_iteration_across_lazy_reads(
    loop_ti, mock_supervisor_comms, previous_iteration, values
):
    ti = loop_ti(index=1)
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


def test_loop_decision_bypasses_custom_backend(loop_ti, mock_supervisor_comms, mocker):
    serialize = mocker.patch("airflow.sdk.execution_time.xcom.XCom.serialize_value", autospec=True)
    ti = loop_ti()

    ti.task.execute(ti.get_template_context())

    serialize.assert_not_called()
    assert mock_supervisor_comms.send.call_args.args[0].value == "continue"


def test_gate_retry_clears_old_signal_without_custom_backend(loop_ti, mock_supervisor_comms, monkeypatch):
    class CustomXCom(BaseXCom):
        @classmethod
        def purge(cls, xcom, *args):
            raise AssertionError("Control metadata must not reach custom backend")

    ti = loop_ti()
    ti._ti_context_from_server.xcom_keys_to_clear = ["_airflow_loop_decision"]
    monkeypatch.setattr(task_runner, "XCom", CustomXCom)
    mock_supervisor_comms.send.side_effect = lambda message: (
        XComResult(key="_airflow_loop_decision", value="stop") if isinstance(message, GetXCom) else None
    )

    state, _, error = task_runner.run(ti, context=ti.get_template_context(), log=ti.task.log)

    assert error is None
    assert state == TaskInstanceState.SUCCESS
    decisions = [
        call.args[0]
        for call in mock_supervisor_comms.send.call_args_list
        if call.args and isinstance(call.args[0], SetXCom)
    ]
    assert [decision.value for decision in decisions] == ["continue"]
    deletes = [
        call.args[0]
        for call in mock_supervisor_comms.send.call_args_list
        if call.args and isinstance(call.args[0], DeleteXCom)
    ]
    assert [delete.key for delete in deletes] == ["_airflow_loop_decision"]
