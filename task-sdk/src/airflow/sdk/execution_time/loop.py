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

from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk.bases.decorator import determine_kwargs
from airflow.sdk.definitions._internal.loop import LOOP_DECISION_KEY
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from airflow.sdk.exceptions import AirflowException
from airflow.sdk.execution_time.comms import SetXCom
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.sdk.execution_time.xcom import XCom

if TYPE_CHECKING:
    from airflow.sdk.api.datamodels._generated import LoopContext
    from airflow.sdk.definitions._internal.loop import LoopGateOperator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance


class LoopMaxIterationsExceeded(AirflowException):
    """The loop condition remained false at its iteration cap."""


@attrs.define(frozen=True)
class LoopContextAccessor:
    _descriptor: LoopContext
    _ti: RuntimeTaskInstance

    @property
    def index(self) -> int:
        return self._descriptor.index

    @property
    def max_iterations(self) -> int:
        return self._descriptor.max_iterations

    @property
    def previous(self) -> Any:
        return None if self.index == 0 else self._result(previous_iteration=True)

    @property
    def result(self) -> Any:
        return self._result(previous_iteration=False)

    def _result(self, *, previous_iteration: bool) -> Any:
        task = self._ti.task.dag.get_task(self._descriptor.terminal_task_id)
        if self._descriptor.terminal_is_mapped:
            return LazyXComSequence(
                xcom_arg=PlainXComArg(task), ti=self._ti, previous_iteration=previous_iteration
            )
        return XCom.get_one(
            key=XCom.XCOM_RETURN_KEY,
            dag_id=self._ti.dag_id,
            run_id=self._ti.run_id,
            task_id=task.task_id,
            _previous_iteration=previous_iteration,
        )


def execute_loop_gate(gate: LoopGateOperator, context: Context) -> None:
    # task_runner imports LoopContextAccessor from this module.
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    loop = context["loop"]
    if gate.until is None:
        stop = loop.index + 1 >= loop.max_iterations
    else:
        stop = bool(gate.until(**determine_kwargs(gate.until, (), context)))
        if not stop and loop.index + 1 >= loop.max_iterations:
            raise LoopMaxIterationsExceeded(
                f"Loop condition remained false at max_iterations={loop.max_iterations}"
            )
    decision = "stop" if stop else "continue"
    gate.log.info("Loop iteration %s: %s (max_iterations=%s)", loop.index, decision, loop.max_iterations)
    ti = context["ti"]
    SUPERVISOR_COMMS.send(
        SetXCom(
            dag_id=ti.dag_id,
            run_id=ti.run_id,
            task_id=ti.task_id,
            key=LOOP_DECISION_KEY,
            value=decision,
            loop_decision=True,
        )
    )
