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

from typing import TYPE_CHECKING, Any, cast

import attrs

from airflow.sdk._shared.secrets_masker import redact
from airflow.sdk.api.datamodels._generated import LoopDecisionPayload
from airflow.sdk.bases.decorator import determine_kwargs
from airflow.sdk.definitions._internal.loop import LOOP_DECISION_REASON_MAX_BYTES
from airflow.sdk.definitions.loop import LoopDecision
from airflow.sdk.definitions.xcom_arg import PlainXComArg
from airflow.sdk.exceptions import AirflowFailException
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.sdk.execution_time.xcom import XCom

if TYPE_CHECKING:
    from uuid import UUID

    from airflow.sdk.api.datamodels._generated import LoopContext
    from airflow.sdk.definitions._internal.loop import LoopGateOperator
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance


class LoopMaxIterationsExceeded(AirflowFailException):
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
    def region_id(self) -> UUID:
        return self._descriptor.region_id

    @property
    def trajectory(self) -> int:
        return self._descriptor.trajectory

    @property
    def forked_from_region_id(self) -> UUID | None:
        return self._descriptor.forked_from_region_id

    @property
    def resumes_from_index(self) -> int | None:
        return self._descriptor.resumes_from_index

    @property
    def previous(self) -> Any:
        # Iterations count from one, so the first pass has no previous result.
        return None if self.index == 1 else self._result(previous_iteration=True)

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
    loop = context["loop"]
    ti = cast("RuntimeTaskInstance", context["ti"])
    reason: str | None = None
    if gate.until is None:
        stop = loop.index >= loop.max_iterations
    else:
        match result := gate.until(**determine_kwargs(gate.until, (), context)):
            case bool():
                stop = result
            case LoopDecision(stop=bool(), reason=None | str()):
                stop, reason = result.stop, result.reason or None
            case LoopDecision(stop=bool()):
                raise AirflowFailException(
                    f"LoopDecision reason must be a str or None, got {type(result.reason).__name__}"
                )
            case LoopDecision():
                raise AirflowFailException(
                    f"LoopDecision stop must be a bool, got {type(result.stop).__name__}"
                )
            case _:
                raise AirflowFailException(
                    f"until must return a bool or LoopDecision, got {type(result).__name__}"
                )
    if reason is not None:
        # Mask here, where we can know what the task has masked.
        reason = cast("str", redact(reason))
        try:
            encoded = reason.encode()
        except UnicodeEncodeError:
            raise AirflowFailException("LoopDecision reason must be valid UTF-8")
        size = len(encoded)
        if size > LOOP_DECISION_REASON_MAX_BYTES:
            raise AirflowFailException(
                f"LoopDecision reason must be at most {LOOP_DECISION_REASON_MAX_BYTES} bytes of UTF-8"
            )
        if "\x00" in reason:
            raise AirflowFailException("LoopDecision reason cannot contain NUL bytes")
    ti._loop_decision = LoopDecisionPayload(stop=stop, reason=reason)
    suffix = "" if reason is None else f": {reason}"
    if not stop and loop.index >= loop.max_iterations:
        raise LoopMaxIterationsExceeded(
            f"Loop condition remained false at max_iterations={loop.max_iterations}{suffix}"
        )
    gate.log.info(
        "Loop iteration %s: %s (max_iterations=%s)%s",
        loop.index,
        "stop" if stop else "continue",
        loop.max_iterations,
        suffix,
    )
