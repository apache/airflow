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

import asyncio
import json
import os
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, cast
from unittest.mock import AsyncMock, MagicMock, Mock, PropertyMock, call, create_autospec, patch

import jinja2
import pytest
from pydantic import BaseModel
from task_sdk.definitions.conftest import make_xcom_arg

from airflow.sdk import (
    DAG,
    Asset,
    BaseAsyncOperator,
    BaseOperator,
    BaseXCom,
    TaskInstanceState,
    get_current_context,
)
from airflow.sdk.bases.operator import ExecutorSafeguard, event_loop
from airflow.sdk.bases.xcom import XComIterable
from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_RETRIES
from airflow.sdk.definitions._internal.expandinput import (
    DictOfListsExpandInput,
    ExpandInput,
    ListOfDictsExpandInput,
    Resolved,
)
from airflow.sdk.definitions._internal.fingerprint import InputFingerprint, to_canonical_json
from airflow.sdk.definitions.context import clone_context
from airflow.sdk.definitions.iterableoperator import (
    CheckpointBatch,
    CheckpointBatches,
    Checkpoints,
    EarlierCheckpoints,
    IndexedTaskInstanceNotStarted,
    IndexedTaskLifecycle,
    IndexedTaskOutcome,
    IndexedTaskOutcomes,
    IndexedTaskUnmapper,
    IterableOperator,
    IterationRun,
    IterationState,
    keeps_failure_to_its_indexed_task,
)
from airflow.sdk.definitions.xcom_arg import _ConcatResult, _MapResult, _ZipResult
from airflow.sdk.exceptions import (
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSkipException,
    AirflowTaskTerminated,
    AirflowTaskTimeout,
    DagRunTriggerException,
    DownstreamTasksSkipped,
    TaskDeferred,
    UnmappableXComTypePushed,
)
from airflow.sdk.execution_time.comms import DeadlockImminentError
from airflow.sdk.execution_time.context import InletEventsAccessors, OutletEventAccessors
from airflow.sdk.execution_time.executor import AsyncAwareExecutor
from airflow.sdk.execution_time.lazy_sequence import LazyXComSequence
from airflow.sdk.execution_time.task_runner import (
    IndexedTaskInstance,
    IndexedTaskRunner,
    IndexedTaskState,
    RuntimeTaskInstance,
)
from airflow.sdk.execution_time.xcom import XCom
from airflow.sdk.serde import serialize

from tests_common.test_utils.mock_context import mock_context as _mock_context_base

if TYPE_CHECKING:
    from airflow.sdk.definitions._internal.expandinput import ExpandInput
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    from tests_common.test_utils.compat import Context


class CheckpointModel(BaseModel):
    """A pydantic result whose required field a checkpoint written before it was added lacks."""

    added_since: int


@dataclass
class RefusingConstructor:
    """A result class whose constructor refuses the data a checkpoint holds."""

    value: int

    def __post_init__(self):
        raise TypeError("the constructor refuses this value")


def fingerprint_of(mapped_kwargs) -> str | None:
    """The fingerprint of an index whose operator takes no partial kwarg from an upstream."""
    return InputFingerprint({}, stop_requested=lambda: False, log=Mock()).of(mapped_kwargs, Mock())


def fingerprint_of_a_run(iterable_op: IterableOperator) -> InputFingerprint:
    """The input fingerprint one run of ``iterable_op`` uses, with the run's stop flag and logger."""
    return InputFingerprint(
        iterable_op.partial_kwargs, stop_requested=iterable_op._state.stop_requested, log=iterable_op.log
    )


def run_of(iterable_op: IterableOperator, context, *, trust_checkpoints: bool = False) -> IterationRun:
    """
    One run of ``iterable_op`` in ``context``, as ``execute`` makes it.

    ``trust_checkpoints`` stands for what a retry's :class:`Checkpoints` decide when the run enters them.
    """
    state = iterable_op._state
    run = IterationRun(
        operator=iterable_op,
        unmapper=IndexedTaskUnmapper(
            iterable_op, iterable_op._operator, context, stop_requested=state.stop_requested
        ),
        state=state,
        context=context,
    )
    run.checkpoints.trust_checkpoints = trust_checkpoints
    return run


def leave_checkpoint(run: IterationRun, task: IndexedTaskInstance, checkpoint: Any) -> None:
    """Leave ``checkpoint`` for ``task`` as an earlier attempt does, read as the run's ``Checkpoints`` read on entry."""
    store = run.context["task_state_store"]
    cast("MockTaskStateStoreAccessor", store).seed_checkpoint(task.index, checkpoint)
    run.checkpoints.earlier.read(store)


def written_checkpoint(run: IterationRun, task: IndexedTaskInstance) -> Any:
    """Write what the run left pending, as the end of its loop does, and return ``task``'s checkpoint as stored."""
    run.checkpoints.batches.flush()
    return cast("MockTaskStateStoreAccessor", run.context["task_state_store"]).checkpoint(task.index)


class MockTaskStateStoreAccessor:
    """Minimal in-memory stand-in for ``TaskStateStoreAccessor``, exposing the async
    ``aget``/``aset`` methods used by ``IterableOperator`` to write its checkpoint batches while the
    indexed tasks run (see ``CheckpointBatches``), plus the sync ``get``/``set``/``delete`` used to read
    them back, write the last batch and the completion marker (see ``Checkpoints``)."""

    def __init__(self):
        self._data: dict[str, Any] = {}

    async def aget(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    async def aset(self, key: str, value: Any, **kwargs) -> None:
        self._data[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)

    def set(self, key: str, value: Any, **kwargs) -> None:
        self._data[key] = value

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def __contains__(self, key: str) -> bool:
        return key in self._data

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def _batches(self) -> Iterator[dict[str, Any]]:
        number = 0
        while (batch := self._data.get(f"{CheckpointBatch.KEY_PREFIX}{number}")) is not None:
            yield batch
            number += 1

    def seed_checkpoint(self, index: int, checkpoint: Any) -> None:
        """Leave ``checkpoint`` for ``index`` as an earlier attempt does: in a batch after the others."""
        number = sum(1 for _ in self._batches())
        self._data[f"{CheckpointBatch.KEY_PREFIX}{number}"] = {str(index): checkpoint}

    def checkpoint(self, index: int) -> Any:
        """The latest checkpoint the batches hold for ``index``, as stored, or None."""
        latest = None
        for batch in self._batches():
            latest = batch.get(str(index), latest)
        return latest


@contextmanager
def mock_context(task, run_id: str | None = None) -> Iterator[Context]:
    """Create a mock context for IterableOperator tests.

    The context includes the task state store, asset event accessors, DAG/run
    information, and a mocked XCom backend.
    """
    task_state_store = MockTaskStateStoreAccessor()
    context = _mock_context_base(task=task, run_id=run_id)
    context["dag"] = task.dag  # type: ignore[typeddict-item]
    context["dag_run"] = SimpleNamespace(conf={})  # type: ignore[typeddict-item]
    context["task_state_store"] = task_state_store  # type: ignore[typeddict-item]
    context["outlet_events"] = OutletEventAccessors()
    context["inlet_events"] = InletEventsAccessors(inlets=[])

    def _set(
        cls,
        key,
        value,
        *,
        dag_id,
        task_id,
        run_id,
        map_index=-1,
        **kwargs,
    ):
        context["ti"].xcom_push(key=key, value=value)

    async def _aset(
        cls,
        key,
        value,
        *,
        dag_id,
        task_id,
        run_id,
        map_index=-1,
        **kwargs,
    ):
        context["ti"].xcom_push(key=key, value=value)

    def _push(cls, ti, key, value, **kwargs):
        context["ti"].xcom_push(key=key, value=value)

    async def _apush(cls, ti, key, value, **kwargs):
        context["ti"].xcom_push(key=key, value=value)

    def _get_one(
        cls,
        *,
        key,
        dag_id,
        task_id,
        run_id,
        map_index=None,
        include_prior_dates=False,
        **kwargs,
    ):
        return context["ti"].xcom_pull(
            task_ids=task_id,
            dag_id=dag_id,
            key=key,
        )

    def _get_all(cls, *, key, dag_id, task_id, run_id, include_prior_dates=False, **kwargs):
        """One map index in these tests, so the values of all of them are the one value, or none."""
        value = context["ti"].xcom_pull(task_ids=task_id, dag_id=dag_id, key=key)
        return None if value is None else [value]

    with (
        patch.object(XCom, "set", classmethod(_set)),
        patch.object(XCom, "aset", classmethod(_aset)),
        patch.object(XCom, "_push", classmethod(_push)),
        patch.object(XCom, "_apush", classmethod(_apush)),
        patch.object(XCom, "get_one", classmethod(_get_one)),
        patch.object(XCom, "get_all", classmethod(_get_all)),
        patch.object(RuntimeTaskInstance, "task_state_store", property(lambda self: task_state_store)),
    ):
        yield context


class MockOperator(BaseOperator):
    """Mock operator for testing IterableOperator expansion."""

    template_fields = ("arg1", "arg2", "arg3")

    def __init__(
        self,
        arg1=None,
        arg2=None,
        arg3=None,
        fail_on_first_attempt=False,
        raise_exception: BaseException | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2
        self.arg3 = arg3
        self.fail_on_first_attempt = fail_on_first_attempt
        self.raise_exception = raise_exception

    def execute(self, context):
        """Execute the operator and return passed arguments as tuple if do_xcom_push is True."""
        expected = clone_context(context)

        if self.raise_exception is not None:
            raise self.raise_exception
        if self.fail_on_first_attempt:
            self.fail_on_first_attempt = False
            raise RuntimeError
        if not self.do_xcom_push:
            return None

        assert context == expected, "Context was unexpectedly mutated during task execution"
        return self.arg1, self.arg2, self.arg3


class MockOperatorWithCustomName(MockOperator):
    """MockOperator subclass with a custom display name, mimicking a @task-decorated callable,
    used to verify IterableOperator.operator_name forwards the wrapped operator's own
    operator_name rather than falling back to this wrapper's task_type."""

    custom_operator_name = "@mock_task"


class MockOutletEventOperator(BaseOperator):
    """Operator that records an outlet asset event on execute, used to test that
    IterableOperator merges/replays each indexed task's outlet events (see ``IndexedTaskLifecycle.run``)."""

    template_fields = ()

    def __init__(self, extra_value: str = "v", **kwargs):
        super().__init__(**kwargs)
        self.extra_value = extra_value

    def execute(self, context):
        context["outlet_events"][Asset(name="a", uri="s3://bucket/a")].extra["value"] = self.extra_value
        return "done"


class MockOnKillOperator(BaseOperator):
    """Operator that records whether ``on_kill()`` was called on it, used to test that
    IterableOperator.on_kill() propagates to currently in-flight indexed tasks (see ``on_kill``)."""

    template_fields = ()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.killed = False

    def execute(self, context):
        return "done"

    def on_kill(self):
        self.killed = True


class MockDeferredOperator(BaseOperator):
    """Operator that immediately defers on execute, simulating a deferrable operator."""

    template_fields = ()

    def execute(self, context):
        raise TaskDeferred(trigger=None, method_name="execute_complete")  # type: ignore[arg-type]


class MockRescheduleSensor(BaseOperator):
    """Operator that raises AirflowRescheduleException on execute, simulating a reschedule-mode sensor."""

    template_fields = ()

    def execute(self, context):
        from datetime import timedelta

        from airflow.sdk import timezone

        raise AirflowRescheduleException(timezone.utcnow() + timedelta(seconds=60))


class MockStateStoreOperator(BaseOperator):
    """Sync operator that keeps its own state in the task state store, as a paginated fetch would."""

    template_fields = ("offset",)

    def __init__(self, offset=None, **kwargs):
        super().__init__(**kwargs)
        self.offset = offset

    def execute(self, context):
        store = context["task_state_store"]
        assert store is context["ti"].task_state_store
        store.set("last_offset", self.offset)
        return store.get("last_offset")


class MockAsyncStateStoreOperator(BaseAsyncOperator):
    """Async twin of MockStateStoreOperator."""

    template_fields = ("offset",)

    def __init__(self, offset=None, **kwargs):
        super().__init__(**kwargs)
        self.offset = offset

    async def aexecute(self, context):
        store = context["task_state_store"]
        await store.aset("last_offset", self.offset)
        return await store.aget("last_offset")


class MockAttemptOperator(BaseAsyncOperator):
    """
    Async operator whose result tells which attempt produced it.

    For the values in ``times_out_on`` the parent's execution timeout strikes instead, which ends
    the whole iteration there and leaves the indexed tasks after it unreached, as a crash would. Async,
    because that timeout lands on the main thread, where async operators run.
    """

    template_fields = ("arg1",)
    times_out_on: set = set()

    def __init__(self, arg1=None, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1

    async def aexecute(self, context):
        if self.arg1 in self.times_out_on:
            raise AirflowTaskTimeout("the iteration ran out of time")
        return f"{self.arg1}@attempt{context['ti'].try_number}"


FIRED_CALLBACKS: list = []


class MockCallbackTimeoutOperator(BaseAsyncOperator):
    """
    Async operator in which the parent's execution timeout strikes; records which callback fired.

    Async, because the parent's limit lands on the main thread, where async operators run; a sync
    operator raising ``AirflowTaskTimeout`` in its worker thread raised its own.
    """

    template_fields = ()

    def __init__(self, **kwargs):
        kwargs["on_failure_callback"] = lambda context: FIRED_CALLBACKS.append("failure")
        kwargs["on_retry_callback"] = lambda context: FIRED_CALLBACKS.append("retry")
        super().__init__(**kwargs)

    async def aexecute(self, context):
        raise AirflowTaskTimeout("the task ran out of time")


class MockCallbackAsyncOperator(BaseAsyncOperator):
    """Async operator that defers for ``arg1="defer"`` and sleeps otherwise; records its callbacks."""

    template_fields = ("arg1",)

    def __init__(self, arg1=None, **kwargs):
        kwargs["on_failure_callback"] = lambda context: FIRED_CALLBACKS.append(("failure", self.arg1))
        kwargs["on_retry_callback"] = lambda context: FIRED_CALLBACKS.append(("retry", self.arg1))
        super().__init__(**kwargs)
        self.arg1 = arg1

    async def aexecute(self, context):
        if self.arg1 == "defer":
            raise TaskDeferred(trigger=None, method_name="execute_complete")  # type: ignore[arg-type]
        await asyncio.sleep(60)


class MockPushingOperator(BaseOperator):
    """Operator that pushes an extra XCom next to its return value, and fails for ``fail=True``."""

    template_fields = ("arg1",)
    executed: list = []

    def __init__(self, arg1=None, fail=False, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.fail = fail

    def execute(self, context):
        type(self).executed.append(self.arg1)
        context["ti"].xcom_push(key="foo", value=f"foo-of-{self.arg1}")
        if self.fail:
            raise RuntimeError("sibling failed")
        return self.arg1


class MockReturnValuePushingOperator(BaseOperator):
    """Operator that pushes its result itself under ``return_value`` and returns None."""

    template_fields = ("arg1",)

    def __init__(self, arg1=None, fail=False, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.fail = fail

    def execute(self, context):
        context["ti"].xcom_push(key="return_value", value=f"pushed-{self.arg1}")
        if self.fail:
            raise RuntimeError("sibling failed")


class MockClearingStateStoreOperator(BaseOperator):
    template_fields = ()

    def execute(self, context):
        context["task_state_store"].clear()


def wait_until(condition, timeout: float = 5.0) -> bool:
    """Poll ``condition`` until it holds; on_kill() kills in a thread of its own."""
    deadline = time.monotonic() + timeout
    while not condition():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.01)
    return True


def create_mapped_operator(
    dag: DAG,
    expand_input: ExpandInput,
    task_id: str = "my_task",
    retries: int = DEFAULT_RETRIES,
    do_xcom_push: bool = True,
    task_concurrency: int | None = None,
    execution_timeout: timedelta | None = None,
    operator_class: type[BaseOperator] = MockOperator,
) -> MappedOperator:
    """
    Create a MappedOperator and assign it to a DAG.

    :param expand_input: The input to expand
    :param dag: The DAG to assign the operator to
    :param task_id: Task ID for the operator
    :param do_xcom_push: Whether to push XCom (default True)
    :param operator_class: Operator class to wrap (default MockOperator)
    """
    return operator_class.partial(
        task_id=task_id,
        dag=dag,
        retries=retries,
        task_concurrency=task_concurrency,
        do_xcom_push=do_xcom_push,
        execution_timeout=execution_timeout,
    )._expand(
        expand_input,
        strict=True,
        register_with_dag=False,
    )


def create_iterable_operator(
    dag: DAG,
    expand_input: ExpandInput,
    task_id: str = "my_task",
    task_concurrency: int | None = None,
    retries: int = DEFAULT_RETRIES,
    do_xcom_push: bool = True,
    operator_class: type[BaseOperator] = MockOperator,
) -> IterableOperator:
    """Create an IterableOperator with a MappedOperator and ExpandInput."""
    mapped_op = create_mapped_operator(
        dag=dag,
        expand_input=expand_input,
        task_id=task_id,
        retries=retries,
        do_xcom_push=do_xcom_push,
        task_concurrency=task_concurrency,
        operator_class=operator_class,
    )
    return IterableOperator(
        operator=mapped_op,
        expand_input=expand_input,
        dag=dag,
    )


def _items(expand_input: ExpandInput) -> list:
    """Every item of the input in index order, the way IterableOperator.execute reads it."""

    async def read():
        length, aget = await expand_input.aresolve({})
        return [await aget(index) for index in range(length)]

    return asyncio.run(read())


class TestIterableOperator:
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}]),
            ([{"a": 1, "b": 2}], [{"a": 1, "b": 2}]),
            ([], []),
        ],
    )
    def test_list_of_dicts_expand_input_aresolve(self, actual, expected):
        """Test IterableOperator with ListOfDictsExpandInput expand_input."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(actual)
            iterable_op = create_iterable_operator(dag, expand_input)

            assert _items(iterable_op.expand_input) == expected

    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}]),
            (
                {"a": [1, 2], "b": [10, 20]},
                [{"a": 1, "b": 10}, {"a": 1, "b": 20}, {"a": 2, "b": 10}, {"a": 2, "b": 20}],
            ),
            ({"a": [1, 2]}, [{"a": 1}, {"a": 2}]),
            (
                {"a": {"x": 1, "y": 2}},
                [{"a": ("x", 1)}, {"a": ("y", 2)}],
            ),
        ],
    )
    def test_dict_of_lists_expand_input_aresolve(self, actual, expected):
        """Test IterableOperator with DictOfListsExpandInput expand_input.

        A dict value expands to its (key, value) pairs (not just its keys), matching
        the classic .expand() resolve() path's handling of dict values.
        """
        with DAG("test_dag") as dag:
            expand_input = DictOfListsExpandInput(actual)
            iterable_op = create_iterable_operator(dag, expand_input)

            assert _items(iterable_op.expand_input) == expected

    def test_task_type(self):
        """
        IterableOperator reports its own class as its type, so whatever resolves a class by it
        (OpenLineage's extractors, the task's class reference) finds the class that runs, while
        the name it is shown under is the wrapped operator's.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = create_iterable_operator(dag, expand_input)

            assert isinstance(iterable_op, IterableOperator)
            assert iterable_op.task_type == "IterableOperator"
            assert iterable_op.operator_name == "MockOperator"

    def test_operator_name(self):
        """Test that IterableOperator forwards the wrapped operator's operator_name (e.g. a
        @task-decorated callable's custom_operator_name), not just its own task_type."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = create_iterable_operator(
                dag, expand_input, operator_class=MockOperatorWithCustomName
            )

            assert isinstance(iterable_op, IterableOperator)
            assert iterable_op.task_type == "IterableOperator"
            assert iterable_op.operator_name == "@mock_task"

    def test_forwards_params_weight_rule_and_retry_policy(self):
        """Test that IterableOperator forwards params, weight_rule, and retry_policy from the
        wrapped operator onto its own DAG node, not just onto the generated indexed tasks."""
        from airflow.sdk import WeightRule
        from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy

        retry_policy = ExceptionRetryPolicy(rules=[])
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            mapped_op = MockOperator.partial(
                task_id="my_task",
                dag=dag,
                params={"p": 1},
                weight_rule=WeightRule.UPSTREAM,
                retry_policy=retry_policy,
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            assert iterable_op.params["p"] == 1
            assert iterable_op.weight_rule == WeightRule.UPSTREAM
            assert iterable_op.retry_policy is retry_policy

    def test_default_args_callbacks_and_hooks_stay_with_the_indexed_tasks(self):
        """
        Callbacks and execute hooks in ``default_args`` reach the indexed tasks, not the iterated task.

        ``_apply_defaults`` fills every ``BaseOperator.__init__`` parameter the call leaves out
        from ``default_args``; the IterableOperator passes them as ``None``, so a DAG-level
        ``on_failure_callback`` runs once per failed indexed task, as through ``.partial()``, and not once
        more for the task with the parent's context.
        """
        callback = Mock()
        with DAG(
            "test_dag",
            default_args={
                "on_failure_callback": callback,
                "on_success_callback": callback,
                "on_execute_callback": callback,
                "pre_execute": callback,
                "post_execute": callback,
            },
        ):
            iterated = MockOperator.partial(task_id="op").iterate(arg1=["a", "b"])

        assert iterated.on_failure_callback == []
        assert iterated.on_success_callback == []
        assert iterated.on_execute_callback == []
        assert iterated._pre_execute_hook is None
        assert iterated._post_execute_hook is None
        item = iterated._operator.unmap({"arg1": "a"})
        assert item.on_failure_callback == [callback]
        assert item.on_success_callback == [callback]
        assert item._pre_execute_hook is callback

    def test_forwards_do_xcom_push(self):
        """Test that IterableOperator forwards do_xcom_push from the wrapped operator's
        partial_kwargs onto its own DAG node."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = create_iterable_operator(dag, expand_input, do_xcom_push=False)

            assert iterable_op.do_xcom_push is False

    def test_forwards_is_setup_is_teardown_and_on_failure_fail_dagrun(self):
        """Test that IterableOperator forwards is_setup/is_teardown/on_failure_fail_dagrun from
        the wrapped operator's partial_kwargs, mirroring what unmap() reads (via
        ``_get_unmap_kwargs``) to apply the same flags to each generated indexed task."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            mapped_op = create_mapped_operator(dag, expand_input)
            # unmap() only ever applies these three flags to indexed tasks by reading them off
            # partial_kwargs (see MappedOperator._get_unmap_kwargs), so that's the only place
            # IterableOperator needs to source them from too.
            mapped_op.partial_kwargs["is_teardown"] = True
            mapped_op.partial_kwargs["on_failure_fail_dagrun"] = True
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            assert iterable_op.is_setup is False
            assert iterable_op.is_teardown is True
            assert iterable_op.on_failure_fail_dagrun is True

    def test_applies_upstream_relationship_for_partial_kwargs_template_fields(self):
        """Test that an XComArg passed via a partial kwarg matching the wrapped operator's own
        template_fields is wired as an upstream dependency of the IterableOperator, mirroring what
        MappedOperator.__attrs_post_init__ does for a normal mapped task."""
        with DAG("test_dag") as dag:
            upstream = MockOperator(task_id="upstream", dag=dag)
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            mapped_op = MockOperator.partial(
                task_id="my_task",
                dag=dag,
                arg1=upstream.output,
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            assert upstream.task_id in iterable_op.upstream_task_ids

    def test_iterate_inside_mapped_task_group_raises_not_implemented_error(self):
        """Test that wrapping an operator expanded inside a mapped task group with IterableOperator
        raises NotImplementedError, since operator expansion in an expanded task group is not
        supported (mirrors MappedOperator's own guard for the analogous .expand() case)."""
        from airflow.decorators import task_group

        with DAG("test_dag") as dag:

            @task_group
            def tg(va):
                expand_input = ListOfDictsExpandInput([{"arg1": 1}])
                mapped_op = MockOperator.partial(
                    task_id="my_task",
                    dag=dag,
                )._expand(expand_input, strict=True, register_with_dag=False)

                with pytest.raises(NotImplementedError, match="operator expansion in an expanded task group"):
                    IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            tg.expand(va=[["a", "b"], [4]])

    def test_task_retries(self):
        """Test that IterableOperator inherits retries from the wrapped operator, since
        the whole IterableOperator is now retried via Airflow's standard retry mechanism."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = create_iterable_operator(dag, expand_input, retries=3)

            assert isinstance(iterable_op, IterableOperator)
            assert iterable_op.retries == 3
            assert iterable_op.task_retries == 3

    def test_task_id(self):
        """Test that IterableOperator inherits task_id from operator."""
        with DAG("test_dag") as dag:
            task_id = "my_task"
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id=task_id)

            assert iterable_op.task_id == task_id

    def test_with_task_concurrency(self):
        """Test that IterableOperator respects task_concurrency parameter."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = create_iterable_operator(dag, expand_input, task_concurrency=4)

            assert iterable_op.max_workers == 4

    def test_direct_instantiation_rejects_task_concurrency(self):
        """A directly instantiated operator can never reach IterableOperator,
        so task_concurrency must be rejected instead of silently accepted as a dead value."""
        with pytest.raises(TypeError, match="which is now max_active_tis_per_dag"):
            MockOperator(task_id="my_task", task_concurrency=4)

    def test_expand_rejects_task_concurrency(self):
        """.expand() produces a plain MappedOperator, never an IterableOperator, so
        task_concurrency (only meaningful for .iterate()/.iterate_kwargs()) must be rejected."""
        with DAG("test_dag"):
            with pytest.raises(TypeError, match="which is now max_active_tis_per_dag"):
                MockOperator.partial(task_id="my_task", task_concurrency=4).expand(arg1=[1, 2, 3])

    def test_expand_kwargs_rejects_task_concurrency(self):
        """.expand_kwargs() produces a plain MappedOperator, never an IterableOperator, so
        task_concurrency (only meaningful for .iterate()/.iterate_kwargs()) must be rejected."""
        with DAG("test_dag"):
            with pytest.raises(TypeError, match="which is now max_active_tis_per_dag"):
                MockOperator.partial(task_id="my_task", task_concurrency=4).expand_kwargs(
                    [{"arg1": 1}, {"arg1": 2}]
                )

    def test_iterate_accepts_task_concurrency(self):
        """.iterate() is the one public entry point where task_concurrency is meaningful: it
        produces an IterableOperator, which reads task_concurrency out of partial_kwargs as
        max_workers rather than forwarding it to BaseOperator.__init__."""
        with DAG("test_dag"):
            iterable_op = MockOperator.partial(task_id="my_task", task_concurrency=4).iterate(arg1=[1, 2, 3])

            assert isinstance(iterable_op, IterableOperator)
            assert iterable_op.max_workers == 4

    def test_iterate_kwargs_accepts_task_concurrency(self):
        """.iterate_kwargs() is the list-of-dicts counterpart to .iterate() and must accept
        task_concurrency the same way."""
        with DAG("test_dag"):
            iterable_op = MockOperator.partial(task_id="my_task", task_concurrency=4).iterate_kwargs(
                [{"arg1": 1}, {"arg1": 2}]
            )

            assert isinstance(iterable_op, IterableOperator)
            assert iterable_op.max_workers == 4

    @pytest.mark.parametrize("invalid_value", [0, -1, -10])
    def test_task_concurrency_validation_rejects_non_positive_values(self, invalid_value):
        """Test that IterableOperator raises ValueError for task_concurrency < 1."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            with pytest.raises(ValueError, match=f"task_concurrency must be at least 1, got {invalid_value}"):
                create_iterable_operator(dag, expand_input, task_concurrency=invalid_value)

    def test_partial_kwargs_not_mutated(self):
        """Test that creating IterableOperator does not mutate the original MappedOperator's partial_kwargs."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            mapped_op = create_mapped_operator(dag, expand_input, task_concurrency=4)
            original_partial_kwargs = mapped_op.partial_kwargs.copy()

            IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            # Verify that mapped_op.partial_kwargs was not mutated
            assert mapped_op.partial_kwargs == original_partial_kwargs
            assert "task_concurrency" in mapped_op.partial_kwargs

    def test_expand_input_stored(self):
        """Test that IterableOperator stores expand_input correctly."""
        with DAG("test_dag") as dag:
            expand_input_data = ListOfDictsExpandInput([{"a": 1}, {"a": 2}])
            iterable_op = create_iterable_operator(dag, expand_input_data)

            assert iterable_op.expand_input is expand_input_data
            assert isinstance(iterable_op.expand_input, (ListOfDictsExpandInput, DictOfListsExpandInput))

    def test_partial_kwargs_stored(self):
        """Test that IterableOperator stores partial_kwargs from operator."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = create_iterable_operator(dag, expand_input)

            assert hasattr(iterable_op, "partial_kwargs")
            assert isinstance(iterable_op.partial_kwargs, dict)

    def test_xcom_push_delegates_to_task(self):
        """An indexed task's result is pushed through task.axcom_push under the default XCom return key."""
        from unittest import mock

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(dag, expand_input)

        task = mock.MagicMock()
        task.axcom_push = mock.AsyncMock()
        task.task_id = "my_task"
        task.index = 0

        lifecycle = IndexedTaskLifecycle(run_of(iterable_op, MagicMock()), task, executor=Mock())
        asyncio.run(lifecycle._push_result("result_value"))

        task.axcom_push.assert_awaited_once_with(key=BaseXCom.XCOM_RETURN_KEY, value="result_value")

    def test_execute_list_of_dicts(self):
        """Test executing IterableOperator with ListOfDictsExpandInput."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_list_of_dicts")

            with mock_context(task=iterable_op) as context:
                result = iterable_op.execute(context=context)
                materialized = list(result)
                assert materialized == [(1, None, None), (2, None, None)]

    @pytest.mark.parametrize(
        "operator_class",
        [MockStateStoreOperator, MockAsyncStateStoreOperator],
        ids=["sync", "async"],
    )
    def test_execute_gives_each_iteration_its_own_task_state_store_keys(self, operator_class):
        """Iterations share one task instance, so a key an iteration stores carries its index."""
        with DAG("test_dag") as dag:
            expand_input = DictOfListsExpandInput({"offset": [10, 20, 30]})
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="exec_state_store", operator_class=operator_class
            )

            with mock_context(task=iterable_op) as context:
                results = list(iterable_op.execute(context=context))
                store = context["task_state_store"]

        assert results == [10, 20, 30]
        assert {key: store[key] for key in ("last_offset_0", "last_offset_1", "last_offset_2")} == {
            "last_offset_0": 10,
            "last_offset_1": 20,
            "last_offset_2": 30,
        }
        assert "last_offset" not in store
        # the operator's own checkpoints keep their unsuffixed keys in the same store
        assert all(store.checkpoint(index) is not None for index in range(3))

    @pytest.mark.parametrize("is_async", [False, True], ids=["sync", "async"])
    def test_execute_runs_each_indexed_task_with_the_executor_safeguard_tracker_set(self, is_async):
        """An indexed task's code runs with the safeguard's tracker set to its operator, sync or async alike."""

        class TrackerSyncOperator(BaseOperator):
            def execute(self, context):
                return ExecutorSafeguard.tracker.get(None) is self

        class TrackerAsyncOperator(BaseAsyncOperator):
            async def aexecute(self, context):
                return ExecutorSafeguard.tracker.get(None) is self

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{}, {}]),
                task_id="exec_tracker",
                operator_class=TrackerAsyncOperator if is_async else TrackerSyncOperator,
            )

            with mock_context(task=iterable_op) as context:
                results = list(iterable_op.execute(context=context))

        assert results == [True, True]

    def test_task_state_store_clear_is_refused_inside_an_iteration(self):
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="exec_clear", operator_class=MockClearingStateStoreOperator
            )

            with mock_context(task=iterable_op) as context:
                with pytest.raises(RuntimeError, match="not available inside an iterated task"):
                    iterable_op.execute(context=context)

    def test_execute_marks_iteration_completed_once_every_index_succeeds(self):
        """
        Once every indexed task has succeeded, ``IterationRun.run`` writes a single completion marker
        instead of deleting the per-index checkpoints one by one, and leaves any state written by
        user code inside the iterated task untouched.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_completed")

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                store._data["last_offset"] = 42

                list(iterable_op.execute(context=context))

                assert store["last_offset"] == 42
                assert store["_iterable_completed"]["completed"] is True
                assert store.checkpoint(0)["status"] == "success"
                assert store.checkpoint(1)["status"] == "success"

    def test_execute_reruns_every_index_after_a_clear_that_follows_success(self):
        """
        A manual clear does not reset the parent TI's ``try_number`` (see ``clear_task_instances``),
        so the next attempt looks like a retry. The completion marker left by the previous fully
        successful run tells ``IterationRun.run`` to ignore the stale ``SUCCESS`` checkpoints and run every
        index again, recording first where the rerun starts so a crash mid-rerun resumes from the
        new checkpoints only.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_rerun")

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 2
                # The clear raised max_tries (try_number + retries); the completed run recorded the old one.
                context["ti"].max_tries = 1
                store = context["task_state_store"]
                store._data["_iterable_completed"] = {
                    "completed": True,
                    "try_number": 1,
                    "max_tries": 0,
                    "since": 0,
                }
                for index in (0, 1):
                    store.seed_checkpoint(
                        index, IndexedTaskState(status=TaskInstanceState.SUCCESS, result="stale").serialize()
                    )

                materialized = list(iterable_op.execute(context=context))

                assert materialized == [(1, None, None), (2, None, None)]
                assert IndexedTaskState.deserialize(store.checkpoint(0)).result == (1, None, None)
                assert store["_iterable_completed"] == {
                    "completed": True,
                    "try_number": 2,
                    "max_tries": 1,
                    "since": 2,
                }

    def test_execute_after_a_crashed_rerun_does_not_replay_results_from_before_the_clear(self, monkeypatch):
        """
        A task that succeeded is cleared and its rerun stops part-way. The next attempt resumes
        what the rerun finished and runs the rest again: an index the rerun did not get to still
        holds its checkpoint from before the clear, and replaying that one is not what clearing
        the task asked for.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}, {"arg1": 3}])
            iterable_op = create_iterable_operator(
                dag,
                expand_input,
                task_id="exec_crashed_rerun",
                operator_class=MockAttemptOperator,
                task_concurrency=1,
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                first_run = list(iterable_op.execute(context=context))
                assert first_run == ["1@attempt1", "2@attempt1", "3@attempt1"]

                # Cleared (which raises max_tries); the rerun finishes the first indexed task and stops at the
                # second.
                context["ti"].try_number = 2
                context["ti"].max_tries = 1
                monkeypatch.setattr(MockAttemptOperator, "times_out_on", {2})
                with pytest.raises(AirflowTaskTimeout):
                    iterable_op.execute(context=context)

                context["ti"].try_number = 3
                monkeypatch.setattr(MockAttemptOperator, "times_out_on", set())
                materialized = list(iterable_op.execute(context=context))

                assert materialized == ["1@attempt2", "2@attempt3", "3@attempt3"]
                assert context["task_state_store"]["_iterable_completed"] == {
                    "completed": True,
                    "try_number": 3,
                    "max_tries": 1,
                    "since": 2,
                }

    def test_execute_resumes_from_checkpoints_on_retry_after_failure(self):
        """
        On a genuine retry (no completion marker) an index checkpointed as ``SUCCESS`` is skipped and
        its checkpointed result is pushed again, while an index left ``UP_FOR_RETRY`` runs, and the
        returned iterable yields both.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_resume")

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 2
                store = context["task_state_store"]
                store.seed_checkpoint(
                    0,
                    IndexedTaskState(
                        status=TaskInstanceState.SUCCESS,
                        result="from_checkpoint",
                        fingerprint=fingerprint_of({"arg1": 1}),
                    ).serialize(),
                )
                store.seed_checkpoint(1, IndexedTaskState(status=TaskInstanceState.UP_FOR_RETRY).serialize())

                materialized = list(iterable_op.execute(context=context))

                assert materialized == ["from_checkpoint", (2, None, None)]
                assert store.checkpoint(1)["status"] == "success"
                assert store["_iterable_completed"]["completed"] is True

    def test_a_retry_that_cannot_read_its_checkpoints_runs_nothing_and_keeps_them(self):
        """
        A retry reads the batches once, when it starts. When the store cannot be read, the attempt fails
        before any indexed task runs, as running one could repeat work its checkpoint says is done, and
        the checkpoints stay for the next retry.
        """
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}]),
                task_id="checkpoint_read_fails",
                task_concurrency=1,
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 2
                store = context["task_state_store"]
                store.seed_checkpoint(
                    0,
                    IndexedTaskState(
                        status=TaskInstanceState.SUCCESS,
                        result="from_checkpoint",
                        fingerprint=fingerprint_of({"arg1": 1}),
                    ).serialize(),
                )

                def get_failing(key, default=None):
                    raise RuntimeError("state store down")

                store.get = get_failing
                with (
                    patch.object(MockOperator, "execute", autospec=True) as execute,
                    pytest.raises(RuntimeError, match="state store down"),
                ):
                    iterable_op.execute(context=context)

        execute.assert_not_called()
        assert store.checkpoint(0)["result"] == "from_checkpoint"
        assert store.checkpoint(1) is None
        assert "_iterable_completed" not in store

    @pytest.mark.parametrize(
        ("classname", "version", "data"),
        [
            pytest.param("not_allowed.Thing", 0, {}, id="class-off-the-allow-list"),
            pytest.param(f"{__name__}.CheckpointModel", 1, {}, id="pydantic-model-gained-a-required-field"),
            pytest.param(f"{__name__}.RefusingConstructor", 0, {"value": 1}, id="constructor-raises"),
        ],
    )
    def test_a_checkpoint_serde_refuses_is_run_again_instead_of_failing_every_retry(
        self, monkeypatch, classname, version, data
    ):
        """
        A checkpoint serde fails to restore is refused on every attempt: the indexed task runs again as
        if there were none, rather than failing each retry until the checkpoint expires. Whether the
        class is off the allow-list, a pydantic model that changed since, or one whose constructor
        refuses the data.
        """
        from airflow.sdk._shared.serialization import CLASSNAME, DATA, VERSION

        monkeypatch.setattr(
            "airflow.sdk.serde._extra_allowed",
            {f"{__name__}.CheckpointModel", f"{__name__}.RefusingConstructor"},
        )
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}]), task_id="checkpoint_refused"
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 2
                store = context["task_state_store"]
                store.seed_checkpoint(
                    0,
                    {
                        **IndexedTaskState(
                            status=TaskInstanceState.SUCCESS,
                            fingerprint=fingerprint_of({"arg1": 1}),
                        ).serialize(),
                        "result": {CLASSNAME: classname, VERSION: version, DATA: data},
                    },
                )

                materialized = list(iterable_op.execute(context=context))

        assert materialized == [(1, None, None)]
        assert store.checkpoint(0)["status"] == "success"

    def test_a_failed_replay_fails_only_its_indexed_task_and_the_next_retry_replays_it(self):
        """
        Pushing a SUCCESS checkpoint's result again can fail: the indexed task fails this attempt, its
        checkpoint stays SUCCESS and the next indexed task still runs, so the following retry replays the
        result instead of running the indexed task.
        """
        original_push_result = IndexedTaskLifecycle._push_result

        async def axcom_push_failing_the_first_item(self, value):
            if self.task.index == 0:
                raise RuntimeError("xcom backend down")
            return await original_push_result(self, value)

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}]),
                task_id="replay_fails",
                task_concurrency=1,
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 2
                store = context["task_state_store"]
                store.seed_checkpoint(
                    0,
                    IndexedTaskState(
                        status=TaskInstanceState.SUCCESS,
                        result="from_checkpoint",
                        fingerprint=fingerprint_of({"arg1": 1}),
                    ).serialize(),
                )

                with patch.object(IndexedTaskLifecycle, "_push_result", axcom_push_failing_the_first_item):
                    with pytest.raises(RuntimeError, match="xcom backend down"):
                        iterable_op.execute(context=context)
                status_after_the_failed_replay = store.checkpoint(0)["status"]
                ran_next_item = store.checkpoint(1)["status"]

                context["ti"].try_number = 3
                materialized = list(iterable_op.execute(context=context))

        assert status_after_the_failed_replay == "success"
        assert ran_next_item == "success"
        assert materialized[0] == "from_checkpoint"

    def test_execute_runs_an_index_again_when_its_input_changed(self):
        """
        A retry can run on another input than the attempt that wrote the checkpoints: the upstream
        was cleared together with this task and produced other items. The index whose item changed
        runs again instead of replaying a result computed from the old item; the other is resumed.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": "new"}, {"arg1": 2}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_input_changed")

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 2
                store = context["task_state_store"]
                store.seed_checkpoint(
                    0,
                    IndexedTaskState(
                        status=TaskInstanceState.SUCCESS,
                        result="from_the_old_item",
                        fingerprint=fingerprint_of({"arg1": "old"}),
                    ).serialize(),
                )
                store.seed_checkpoint(
                    1,
                    IndexedTaskState(
                        status=TaskInstanceState.SUCCESS,
                        result="from_checkpoint",
                        fingerprint=fingerprint_of({"arg1": 2}),
                    ).serialize(),
                )

                materialized = list(iterable_op.execute(context=context))

                assert materialized == [("new", None, None), "from_checkpoint"]
                assert store.checkpoint(0)["fingerprint"] == fingerprint_of({"arg1": "new"})

    def test_fingerprint(self):
        """The digest follows the content, not the key order, and gives up on what serde cannot serialize."""
        assert fingerprint_of({"a": 1, "b": [1, 2]}) == fingerprint_of({"b": [1, 2], "a": 1})
        assert fingerprint_of({"a": 1}) != fingerprint_of({"a": 2})
        assert fingerprint_of({"a": object()}) is None

    def test_fingerprint_of_a_set_is_the_same_in_every_process(self):
        """
        serde writes a set's members in iteration order, which for strings follows the hash seed: a
        retry on a restarted worker must still digest the same input to the same fingerprint.
        """
        script = (
            "from airflow.sdk.definitions._internal.fingerprint import InputFingerprint\n"
            "fingerprint = InputFingerprint({}, stop_requested=lambda: False, log=None)\n"
            "print(fingerprint.of({'tags': {'alpha', 'beta', 'gamma', 'delta'}, "
            "'nested': [frozenset({'x', 'y', 'z'})]}, None))\n"
        )
        digests = {
            subprocess.run(
                [sys.executable, "-c", script],
                env={**os.environ, "PYTHONHASHSEED": seed},
                capture_output=True,
                text=True,
                check=True,
            ).stdout.strip()
            for seed in ("1", "2", "3")
        }

        assert len(digests) == 1
        assert digests.pop() == fingerprint_of(
            {"tags": {"delta", "gamma", "beta", "alpha"}, "nested": [frozenset({"z", "y", "x"})]}
        )

    def test_upstream_inputs_are_serialized_once_per_run(self):
        """The partial kwargs from an upstream are the same for every index: one serialization serves them all."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([{"arg1": "a"}]))
        iterable_op.partial_kwargs = {"arg2": make_xcom_arg(None)}
        rendered = SimpleNamespace(arg2=[{"id": i} for i in range(5)])
        fingerprint = fingerprint_of_a_run(iterable_op)

        with patch(
            "airflow.sdk.definitions._internal.fingerprint.to_canonical_json", wraps=to_canonical_json
        ) as canonical:
            texts = {fingerprint.upstream_inputs.json(rendered) for _ in range(3)}

        assert canonical.call_count == 1
        assert texts == {to_canonical_json({"arg2": rendered.arg2})}

    def test_fingerprint_follows_the_upstream_inputs(self):
        """Other upstream inputs give another fingerprint; none leaves the input's own digest as it was."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([{"arg1": "a"}]))
        iterable_op.partial_kwargs = {"arg2": make_xcom_arg(None)}

        def with_upstream(value):
            return fingerprint_of_a_run(iterable_op).of({"arg1": 1}, SimpleNamespace(arg2=value))

        own = fingerprint_of({"arg1": 1})

        assert InputFingerprint({}, stop_requested=lambda: False, log=Mock()).of({"arg1": 1}, Mock()) == own
        assert with_upstream([1]) != own
        assert with_upstream([1]) != with_upstream([2])
        assert with_upstream(object()) is None

    def test_an_upstream_input_serde_cannot_serialize_leaves_no_fingerprint(self):
        """Like an input of its own serde refuses, the checkpoint is then honoured by index alone."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([{"arg1": "a"}]))
        iterable_op.partial_kwargs = {"arg2": make_xcom_arg(None)}
        rendered = SimpleNamespace(arg2=object())
        fingerprint = fingerprint_of_a_run(iterable_op)

        assert fingerprint.upstream_inputs.json(rendered) is None
        assert fingerprint.of({"arg1": "a"}, rendered) is None

    def test_canonical_json_of_a_value_without_a_set_is_serdes_own(self):
        """Only sets are reordered, so a checkpoint written before keeps its fingerprint."""
        value = {"b": [3, 1, 2], "a": ("x", {"k": None}), "c": "text"}

        assert to_canonical_json(value) == json.dumps(serialize(value), sort_keys=True)

    def test_execute_does_not_leak_unmapped_operator_into_parent_context(self):
        """
        Regression test: unmapping an indexed task must not mutate the parent context's own `ti`.

        ``context_update_for_unmapped`` sets ``context["ti"].task = task`` in place. Since
        ``context.copy()`` is only a shallow copy, ``context["ti"]`` in the copy is the *same*
        object as the parent's. If ``IndexedTaskUnmapper.unmap`` rendered the unmapped indexed operator against a
        context still carrying the parent's `ti`, the parent's `ti.task` would end up pointing at
        whichever indexed task was unmapped last, corrupting anything the runner reads off `ti.task`
        after `execute()` returns (e.g. `do_xcom_push`/`multiple_outputs` in `_push_xcom_if_needed`).
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}, {"arg1": 3}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_no_leak")

            with mock_context(task=iterable_op) as context:
                parent_ti_task = context["ti"].task

                list(iterable_op.execute(context=context))

                assert context["ti"].task is parent_ti_task
                assert context["task"] is iterable_op

    def test_an_indexed_task_reads_back_the_xcom_it_pushed(self):
        """
        ``ti.xcom_push("progress", v)`` in an indexed task lands under ``progress_<index>``; the indexed
        task's own ``ti.xcom_pull(key="progress")`` must read that back, not the parent's unsuffixed key.
        """
        seen: list = []

        class PushThenPull(BaseOperator):
            def __init__(self, arg1=None, **kwargs):
                super().__init__(**kwargs)
                self.arg1 = arg1

            def execute(self, context):
                ti = context["ti"]
                ti.xcom_push(key="progress", value=self.arg1 * 10)
                seen.append(
                    (
                        self.arg1,
                        ti.xcom_pull(key="progress"),
                        ti.xcom_pull(task_ids=ti.task_id, key="progress"),
                    )
                )

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            mapped_op = PushThenPull.partial(task_id="push_pull", dag=dag, task_concurrency=1)._expand(
                expand_input, strict=True, register_with_dag=False
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)
            with mock_context(task=iterable_op) as context:
                iterable_op.execute(context=context)

        assert sorted(seen) == [(1, 10, 10), (2, 20, 20)]

    def test_an_indexed_task_sees_its_own_operator_as_the_contexts_task(self):
        """
        ``context["task"]`` inside an indexed task, and in its callbacks, is the indexed task's unmapped
        operator.

        ``.expand()`` gives each mapped task instance its own operator under ``context["task"]``
        (``context_update_for_unmapped`` sets it next to ``ti.task``); the indexed task's view of the context
        must swap that key as well, not only ``ti``, or user code reads the IterableOperator there.
        """
        seen: list[tuple[str, object, object]] = []

        class MockContextTaskOperator(BaseOperator):
            def execute(self, context):
                seen.append(("execute", context["task"], context["ti"].task))

        def on_success(context):
            seen.append(("on_success_callback", context["task"], context["ti"].task))

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}, {}])
            mapped_op = MockContextTaskOperator.partial(
                task_id="context_task", dag=dag, on_success_callback=on_success
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                iterable_op.execute(context=context)

        assert len(seen) == 4
        for where, task, ti_task in seen:
            assert task is ti_task, where
            assert isinstance(task, MockContextTaskOperator), where
            assert task is not iterable_op, where
        assert context["task"] is iterable_op

    def test_an_indexed_task_reads_the_params_of_its_own_operator(self):
        """
        ``context["params"]`` in an indexed task holds the params its own operator was built with.

        An index whose operator has the parent's params reads the parent's processed params, copied;
        one that iterates ``params`` gets them merged over the partial's and processed again, as a
        mapped task instance does under ``.expand()``.
        """
        from airflow.sdk.definitions.param import process_params

        seen: dict[int, dict[str, Any]] = {}

        class MockParamsOperator(BaseOperator):
            def execute(self, context):
                seen[context["ti"].index] = context["params"]

        with DAG("test_dag", params={"d": "dag"}) as dag:
            expand_input = ListOfDictsExpandInput([{"params": {"p": 2}}, {}])
            mapped_op = MockParamsOperator.partial(task_id="params_task", dag=dag, params={"p": 1})._expand(
                expand_input, strict=True, register_with_dag=False
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                context["params"] = process_params(dag, iterable_op, None, suppress_exception=False)
                iterable_op.execute(context=context)

        assert seen == {0: {"d": "dag", "p": 2}, 1: {"d": "dag", "p": 1}}
        assert seen[1] is not context["params"]
        assert context["params"] == {"d": "dag", "p": 1}

    def test_params_an_operator_sets_in_its_init_are_its_own(self):
        """
        An operator that sets a param in its own ``__init__`` reads that value, as a mapped task
        instance does under ``.expand()``, and the iterated task's params stay as declared.
        """
        from airflow.sdk.definitions.param import process_params

        seen: dict[int, Any] = {}

        class MockOwnParamOperator(BaseOperator):
            def __init__(self, value=None, **kwargs):
                super().__init__(**kwargs)
                self.value = value
                self.params["p"] = "own"

            def execute(self, context):
                seen[context["ti"].index] = context["params"]["p"]

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"value": 1}, {"value": 2}])
            mapped_op = MockOwnParamOperator.partial(task_id="own_params", dag=dag, params={"p": 1})._expand(
                expand_input, strict=True, register_with_dag=False
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                context["params"] = process_params(dag, iterable_op, None, suppress_exception=False)
                iterable_op.execute(context=context)

        assert seen == {0: "own", 1: "own"}
        assert iterable_op.params["p"] == 1
        assert mapped_op.params["p"] == 1

    def test_a_param_an_indexed_task_changes_stays_its_own(self):
        """A write to ``self.params`` in one indexed task's ``execute`` does not reach its siblings."""
        seen: list[Any] = []

        class MockChangingParamOperator(BaseOperator):
            def __init__(self, value=None, **kwargs):
                super().__init__(**kwargs)
                self.value = value

            def execute(self, context):
                seen.append(self.params["p"])
                self.params["p"] = f"changed by {self.value}"

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"value": 1}, {"value": 2}, {"value": 3}])
            mapped_op = MockChangingParamOperator.partial(
                task_id="changing_params", dag=dag, params={"p": 1}, task_concurrency=1
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                iterable_op.execute(context=context)

        assert seen == [1, 1, 1]
        assert iterable_op.params["p"] == 1

    def test_execute_resolves_and_reads_the_expand_input_on_the_running_loop(self):
        """
        Regression test for the frozen IterableOperator: indexed task inputs were pulled from the main
        thread between two runs of the event loop, where a blocking supervisor call deadlocked with
        the ``asend`` of an indexed task parked mid-call. The input must be resolved with ``aresolve``
        and every index read while the loop runs; the synchronous ``resolve`` must stay untouched.
        """
        loop_running: list[bool] = []
        original_aresolve = ListOfDictsExpandInput.aresolve

        async def aresolve(self, context):
            loop_running.append(asyncio.get_running_loop().is_running())
            length, aget = await original_aresolve(self, context)

            async def aget_on_loop(index):
                loop_running.append(asyncio.get_running_loop().is_running())
                return await aget(index)

            return Resolved(length, aget_on_loop)

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}, {"arg1": 3}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_async_input")

            with (
                mock_context(task=iterable_op) as context,
                patch.object(ListOfDictsExpandInput, "aresolve", aresolve),
                patch.object(
                    ListOfDictsExpandInput, "resolve", side_effect=AssertionError("sync resolve used")
                ),
            ):
                materialized = sorted(iterable_op.execute(context=context))

        assert materialized == [(1, None, None), (2, None, None), (3, None, None)]
        assert loop_running == [True, True, True, True]

    def test_execute_pulls_xcom_arg_inputs_through_aresolve(self):
        """An XComArg input is resolved with ``aresolve`` (``ti.axcom_pull``), never with blocking ``resolve``."""
        with DAG("test_dag") as dag:
            xcom_arg = make_xcom_arg([{"arg1": 1}, {"arg1": 2}])
            xcom_arg.resolve = lambda *a, **kw: pytest.fail("synchronous resolve() used on the loop")
            expand_input = ListOfDictsExpandInput(xcom_arg)
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_xcom_arg_input")

            with mock_context(task=iterable_op) as context:
                materialized = sorted(iterable_op.execute(context=context))

        assert materialized == [(1, None, None), (2, None, None)]

    def test_a_map_callable_skipping_one_element_skips_only_that_indexed_task(self):
        """
        A ``.map()`` callable that raises ``AirflowSkipException`` filters out its element, as it
        skips only that mapped task instance under ``.expand()``: the other items run and the task
        succeeds without it.
        """

        def skip_b(value):
            if value == "b":
                raise AirflowSkipException("not this one")
            return {"arg1": value}

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(make_xcom_arg(["a", "b", "c"]).map(skip_b))
            iterable_op = create_iterable_operator(dag, expand_input, task_id="map_skips_one")

            with mock_context(task=iterable_op) as context:
                materialized = sorted(iterable_op.execute(context=context))

        assert materialized == [("a", None, None), ("c", None, None)]

    def test_a_map_callable_failing_on_one_element_fails_only_that_indexed_task(self):
        """
        A ``.map()`` callable that raises for one element fails that indexed task alone: the others run and
        are checkpointed, so a retry replays them and only that element is read again.
        """

        def fail_on_b(value):
            if value == "b":
                raise ValueError("cannot map b")
            return {"arg1": value}

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(make_xcom_arg(["a", "b", "c"]).map(fail_on_b))
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="map_fails_one", task_concurrency=1
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                with pytest.raises(ValueError, match="cannot map b"):
                    iterable_op.execute(context=context)

                assert store.checkpoint(0)["status"] == "success"
                assert store.checkpoint(1) is None
                assert store.checkpoint(2)["status"] == "success"

    def test_a_template_failing_for_one_item_fails_only_that_indexed_task(self):
        """Rendering is part of reading an indexed task's input: a template that fails for one indexed task fails it alone."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [{"arg1": "a"}, {"arg1": "{{ no_such_name }}"}, {"arg1": "c"}]
            )
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="render_fails_one", task_concurrency=1
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                with pytest.raises(jinja2.UndefinedError):
                    iterable_op.execute(context=context)

                assert store.checkpoint(0)["status"] == "success"
                assert store.checkpoint(1) is None
                assert store.checkpoint(2)["status"] == "success"

    def test_execute_refuses_an_unmappable_upstream_value(self):
        """
        An upstream returning a JSON string fails the task instead of being iterated as one item.

        ``.expand()`` gets this from the upstream's push (``UnmappableXComTypePushed`` in
        ``_push_xcom_if_needed``), which only fires for a ``MappedOperator`` dependant, so the
        iterated path checks the resolved value itself.
        """
        executed: list = []

        class RecordingOperator(MockOperator):
            def execute(self, context):
                executed.append(self.arg1)
                return super().execute(context)

        with DAG("test_dag") as dag:
            expand_input = DictOfListsExpandInput({"arg1": make_xcom_arg('{"a": 1}')})
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="exec_unmappable", operator_class=RecordingOperator
            )
            with mock_context(task=iterable_op) as context:
                with pytest.raises(UnmappableXComTypePushed, match="str"):
                    iterable_op.execute(context=context)

        assert executed == []

    def test_a_template_reads_the_indexed_tasks_own_state_store(self):
        """A template field reads the same suffixed store as ``execute`` does, not the parent's key."""
        template = "{{ task_state_store.get('offset') }}"
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"offset": template}, {"offset": template}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="template_store", operator_class=MockStateStoreOperator
            )

            with mock_context(task=iterable_op) as context:
                context["task_state_store"].set("offset", "parents")
                context["task_state_store"].set("offset_1", 7)
                task = run_of(iterable_op, context).unmapper.unmap(1, {"offset": template})

        assert task.task.offset == "7"

    def test_a_template_reads_the_indexed_tasks_own_index(self):
        """
        ``{{ ti.index }}`` in a partial kwarg renders per indexed task, so an operator that finds its remote
        work by labels (a ``KubernetesPodOperator`` that reattaches) can tell the indexed tasks apart.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b"}])
            mapped_op = MockOperator.partial(task_id="indexed_label", dag=dag, arg2="{{ ti.index }}")._expand(
                expand_input, strict=True, register_with_dag=False
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                results = sorted(iterable_op.execute(context=context))

        assert results == [("a", "0", None), ("b", "1", None)]

    def test_execute_renders_template_fields_off_the_loop_thread(self):
        """
        Rendering an indexed task's template fields may call the supervisor synchronously: an XComArg in
        a partial kwarg resolves with ``resolve``, ``{{ var.value.x }}`` with ``Variable.get``. On
        the loop thread that call would hold up every sibling for its round trip, so the rendering
        happens in a worker thread.
        """
        on_running_loop: list[bool] = []

        def resolve(context):
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                on_running_loop.append(False)
            else:
                on_running_loop.append(True)
            return "pulled"

        with DAG("test_dag") as dag:
            xcom_arg = make_xcom_arg(None)
            xcom_arg.resolve = resolve
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            mapped_op = MockOperator.partial(task_id="render_off_loop", dag=dag, arg2=xcom_arg)._expand(
                expand_input, strict=True, register_with_dag=False
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                materialized = sorted(iterable_op.execute(context=context))

        assert materialized == [(1, "pulled", None), (2, "pulled", None)]
        assert on_running_loop == [False, False]

    def test_execute_dict_of_lists(self):
        """Test executing IterableOperator with DictOfListsExpandInput."""
        with DAG("test_dag") as dag:
            expand_input = DictOfListsExpandInput({"arg1": [1, 2, 3]})
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_dict_of_lists")

            with mock_context(task=iterable_op) as context:
                result = iterable_op.execute(context=context)
                materialized = list(result)
                assert materialized == [(1, None, None), (2, None, None), (3, None, None)]

    def test_execute_multiple_key_dict_of_lists(self):
        """Test executing IterableOperator with multiple keys in DictOfListsExpandInput."""
        with DAG("test_dag") as dag:
            expand_input = DictOfListsExpandInput({"arg1": [1, 2], "arg2": [10, 20], "arg3": ["x", "y"]})
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_multi_key")

            with mock_context(task=iterable_op) as context:
                result = iterable_op.execute(context=context)
                materialized = list(result)
                # Cartesian product expected order:
                # (1,10,'x'), (1,10,'y'), (1,20,'x'), (1,20,'y'),
                # (2,10,'x'), (2,10,'y'), (2,20,'x'), (2,20,'y')
                assert materialized == [
                    (1, 10, "x"),
                    (1, 10, "y"),
                    (1, 20, "x"),
                    (1, 20, "y"),
                    (2, 10, "x"),
                    (2, 10, "y"),
                    (2, 20, "x"),
                    (2, 20, "y"),
                ]

    def test_execute_with_task_concurrency_setting(self):
        """Test executing IterableOperator with task_concurrency parameter."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}, {"arg1": 3}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="exec_concurrency", task_concurrency=2
            )

            with mock_context(task=iterable_op) as context:
                result = iterable_op.execute(context=context)
                materialized = list(result)
                assert materialized == [(1, None, None), (2, None, None), (3, None, None)]
                assert iterable_op.max_workers == 2

    def test_execute_all_parameters(self):
        """Test executing IterableOperator with all arg1, arg2, arg3 parameters."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"arg1": 1, "arg2": 10, "arg3": 100},
                    {"arg1": 2, "arg2": 20, "arg3": 200},
                ]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_all_args")

            with mock_context(task=iterable_op) as context:
                result = iterable_op.execute(context=context)
                materialized = list(result)
                assert materialized == [(1, 10, 100), (2, 20, 200)]

    def test_an_operator_that_turns_off_its_xcom_push_returns_no_result(self):
        """
        An operator may set ``do_xcom_push`` in its own ``__init__``, as some providers' do, not only
        through ``partial()``: the iterated task follows the operator as it is built for its indexed tasks.
        """

        class NoXComPush(MockOperator):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.do_xcom_push = False

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}]),
                task_id="operator_without_xcom_push",
                operator_class=NoXComPush,
            )

            with mock_context(task=iterable_op) as context:
                result = iterable_op.execute(context=context)

        assert iterable_op.do_xcom_push is True
        assert result is None

    def test_execute_with_do_xcom_push_false(self):
        """With do_xcom_push=False no return_value_<index> XCom is pushed for any indexed task."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="no_xcom_push", do_xcom_push=False
            )

            with (
                mock_context(task=iterable_op) as context,
                patch.object(
                    IndexedTaskLifecycle,
                    "_push_result",
                    new=AsyncMock(spec=IndexedTaskLifecycle._push_result),
                ) as axcom_push,
            ):
                result = iterable_op.execute(context=context)

                assert result is None
                axcom_push.assert_not_awaited()

    def test_execute_does_not_push_xcom_for_none_results(self):
        """An indexed task returning None has nothing to push, even when do_xcom_push is True."""

        class NoneOperator(BaseOperator):
            def __init__(self, arg1=None, **kwargs):
                super().__init__(**kwargs)
                self.arg1 = arg1

            def execute(self, context):
                return None

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="none_results", operator_class=NoneOperator
            )

            with (
                mock_context(task=iterable_op) as context,
                patch.object(
                    IndexedTaskLifecycle,
                    "_push_result",
                    new=AsyncMock(spec=IndexedTaskLifecycle._push_result),
                ) as axcom_push,
            ):
                iterable_op.execute(context=context)

                axcom_push.assert_not_awaited()

    def test_execute_with_failed_tasks_raises_regardless_of_retries(self):
        """
        Test executing IterableOperator where an indexed task fails.

        This test verifies that:
        1. Tasks with fail_on_first_attempt=True raise an exception on first attempt
        2. IterableOperator no longer retries failed indexed tasks in-process — retries (if any) are
           handled by Airflow retrying the whole IterableOperator task instance
        3. The failing indexed task's own exception is raised, regardless of whether the wrapped operator
           has retries configured (one failure is not wrapped in a group, see _pick_failure_for_runner)
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"arg1": 1, "arg2": 10},
                    {"arg1": 2, "arg2": 20, "fail_on_first_attempt": True},
                    {"arg1": 3, "arg2": 30},
                ]
            )
            iterable_op = create_iterable_operator(
                dag,
                expand_input,
                task_id="exec_with_failures",
                retries=1,
            )

            with mock_context(task=iterable_op) as context:
                with pytest.raises(RuntimeError):
                    iterable_op.execute(context=context)

    def test_execute_all_indexed_tasks_skipped_raises_single_skip_exception(self):
        """When every indexed task raises AirflowSkipException, IterableOperator must re-raise a single
        AirflowSkipException so the runner marks it SKIPPED instead of a retryable failure."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"raise_exception": AirflowSkipException("skip 1")},
                    {"raise_exception": AirflowSkipException("skip 2")},
                ]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="all_skipped")

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowSkipException):
                    iterable_op.execute(context=context)

    def test_execute_over_an_empty_input_skips(self):
        """An empty input skips the task, as ``.expand()`` over nothing does, rather than returning nothing."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([]), task_id="empty_input")

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowSkipException, match="empty"):
                    iterable_op.execute(context=context)

    def test_execute_skip_next_to_a_failure_raises_only_the_failure(self):
        """A skipped indexed task is not a failure, so the group holds what failed and nothing else."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"raise_exception": AirflowSkipException("skip 1")},
                    {"raise_exception": RuntimeError("boom")},
                ]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="partial_skip")

            with mock_context(task=iterable_op) as context:
                with pytest.raises(RuntimeError, match="boom") as raised:
                    iterable_op.execute(context=context)

        # A skipped indexed task is not a failure: the only failure is raised on its own.
        assert raised.value.__cause__ is None

    def test_execute_skip_next_to_successes_succeeds(self):
        """
        An indexed task that skips does not fail the task, as a skipped mapped task instance would not:
        the others' results are pushed, the skipped index is left out, and the run counts as complete.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [{"raise_exception": AirflowSkipException("nothing to do")}, {"arg1": 2}]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="skip_and_success")

            with mock_context(task=iterable_op) as context:
                materialized = list(iterable_op.execute(context=context))
                store = context["task_state_store"]

                assert materialized == [(2, None, None)]
                assert store.checkpoint(0)["status"] == "skipped"
                assert store.checkpoint(1)["status"] == "success"
                assert store["_iterable_completed"]["completed"] is True

    def test_an_iteration_returning_none_keeps_its_position_and_reads_as_none(self):
        """
        Only a skip takes an index out of the sequence. An iteration that returned ``None`` pushed
        nothing, as a mapped task instance would, but is counted and reads as ``None``; ``.expand()``
        would not count it, which the docs page says.
        """

        class ValueOrNothing(BaseOperator):
            def __init__(self, value=None, skip=False, **kwargs):
                super().__init__(**kwargs)
                self.value = value
                self.skip = skip

            def execute(self, context):
                if self.skip:
                    raise AirflowSkipException("nothing to do")
                return self.value

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [{"value": 1}, {"value": None}, {"skip": True}, {"value": 4}]
            )
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="none_and_skip", operator_class=ValueOrNothing
            )

            with mock_context(task=iterable_op) as context:
                result = iterable_op.execute(context=context)
                store = context["task_state_store"]

                assert result.skipped == [2]
                assert len(result) == 3
                assert list(result) == [1, None, 4]
                assert store.checkpoint(1)["status"] == "success"
                assert "result" not in store.checkpoint(1)

    @pytest.mark.parametrize("in_a_loop", [False, True])
    def test_the_result_names_the_loop_pass_it_was_pushed_in(self, in_a_loop):
        """Inside a loop the result records its task instance's region and pass; outside one it names none."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}]), task_id="in_a_loop"
            )

            with mock_context(task=iterable_op) as context:
                region_id = uuid.UUID(int=9)
                if in_a_loop:
                    context["ti"].region_id = region_id
                    context["ti"].region_index = 2
                result = iterable_op.execute(context=context)

        assert (result.region_id, result.region_index) == ((region_id, 2) if in_a_loop else (None, None))

    def test_skipped_iteration_is_left_out_of_the_result(self):
        """As a skipped mapped task instance, a skipped iteration is not among the values downstream reads."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [{"arg1": 1}, {"raise_exception": AirflowSkipException("nothing to do")}, {"arg1": 3}]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="skip_in_the_middle")

            with mock_context(task=iterable_op) as context:
                result = iterable_op.execute(context=context)

                assert result.skipped == [1]
                assert len(result) == 2
                assert [value[0] for value in result] == [1, 3]

    @pytest.mark.parametrize(
        ("trigger_rule", "skipped"),
        [
            ("all_success", True),
            ("none_skipped", True),
            ("all_done_min_one_success", True),
            ("none_failed", False),
            ("all_done", False),
            ("one_success", False),
        ],
    )
    def test_partial_skip_skips_the_downstream_tasks_a_skipped_mapped_instance_would(
        self, trigger_rule, skipped
    ):
        """
        A downstream task whose trigger rule skips it when an upstream task instance skipped is skipped,
        after the result is pushed for the others; any other downstream task is left to run.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [{"arg1": 1}, {"raise_exception": AirflowSkipException("nothing to do")}]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="partial_skip")
            iterable_op >> BaseOperator(task_id="downstream", trigger_rule=trigger_rule)

            with (
                mock_context(task=iterable_op) as context,
                patch("airflow.sdk.definitions.iterableoperator._push_xcom_if_needed", autospec=True) as push,
            ):
                if skipped:
                    with pytest.raises(DownstreamTasksSkipped) as raised:
                        iterable_op.execute(context=context)
                    assert raised.value.tasks == ["downstream"]
                    (pushed, ti, _), _ = push.call_args
                    assert ti is context["ti"]
                    assert [value[0] for value in pushed] == [1]
                else:
                    assert len(iterable_op.execute(context=context)) == 1
                    push.assert_not_called()

    def test_no_downstream_task_is_skipped_without_a_skipped_iteration(self):
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}]), task_id="no_skip"
            )
            iterable_op >> BaseOperator(task_id="downstream")

            with mock_context(task=iterable_op) as context:
                assert len(iterable_op.execute(context=context)) == 2

    def test_clear_after_every_iteration_skipped_runs_every_iteration_again(self):
        """
        Regression test: a task whose iterations all skipped is ``SKIPPED``, a final state. Clearing it
        must run the iterations again, as clearing a skipped mapped task instance does, instead of
        replaying the skips recorded before the clear.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"raise_exception": AirflowSkipException("nothing to do yet")},
                    {"raise_exception": AirflowSkipException("nothing to do yet")},
                ]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="all_skipped_then_cleared")

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                with pytest.raises(AirflowSkipException):
                    iterable_op.execute(context=context)

                # Cleared: the next attempt runs on the same try_number sequence, now with work to do,
                # and the clear raised max_tries.
                context["ti"].try_number = 2
                context["ti"].max_tries = 1
                with patch.object(MockOperator, "execute", autospec=True, return_value="done") as execute:
                    result = iterable_op.execute(context=context)

                assert execute.call_count == 2
                assert len(result) == 2

    @pytest.mark.asyncio
    async def test_run_indexed_task_does_not_rerun_an_indexed_task_skipped_on_a_previous_attempt(self):
        """On a retry a ``SKIPPED`` checkpoint is honoured like a ``SUCCESS`` one: the indexed task stays skipped."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="skipped_before")

            with mock_context(task=iterable_op) as context:
                run = run_of(iterable_op, context, trust_checkpoints=True)
                task = run.unmapper.unmap(0, {"raise_exception": RuntimeError("must not run again")})
                leave_checkpoint(
                    run,
                    task,
                    IndexedTaskState(
                        status=TaskInstanceState.SKIPPED, fingerprint=task.input_fingerprint
                    ).serialize(),
                )

                with AsyncAwareExecutor(loop=asyncio.get_running_loop(), max_workers=1) as executor:
                    outcome = await IndexedTaskLifecycle(run, task, executor).run()

        assert outcome.result is None
        assert isinstance(outcome.exception, AirflowSkipException)

    def test_failed_iteration_checkpoint_records_its_input_and_attempt(self):
        """
        A failed iteration's checkpoint carries the input digest and the attempt, like a successful
        one, so the next attempt does not take a plain retry for a clear or a changed input.
        """
        with DAG("test_dag") as dag:
            failing = {"arg1": 1, "fail_on_first_attempt": True}  # serializable, so it has a digest
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([failing]), task_id="failed_checkpoint"
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 3
                with pytest.raises(RuntimeError):
                    iterable_op.execute(context=context)

                checkpoint = context["task_state_store"].checkpoint(0)

        assert checkpoint["status"] == "up_for_retry"
        assert fingerprint_of(failing) is not None
        assert checkpoint["fingerprint"] == fingerprint_of(failing)
        assert checkpoint["try_number"] == 3

    def test_parent_timeout_runs_the_retry_callback_while_retries_are_left(self):
        """The task is retried for the timeout, so its iteration reports a retry, not a failure."""
        FIRED_CALLBACKS.clear()
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{}]),
                task_id="timed_out",
                retries=3,
                operator_class=MockCallbackTimeoutOperator,
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                context["ti"].max_tries = 3
                with pytest.raises(AirflowTaskTimeout):
                    iterable_op.execute(context=context)

        assert FIRED_CALLBACKS == ["retry"]

    def test_a_sync_indexed_tasks_own_timeout_is_its_failure(self):
        """
        A sync operator may raise ``AirflowTaskTimeout`` itself, as a hook that gave up waiting does.
        In its worker thread the parent's limit never strikes, so that is the indexed task's own failure, as
        it is the mapped task instance's under ``.expand()``: the sibling runs, nothing is killed, and
        the task fails with it as with any failed indexed task.
        """
        killed: list = []

        class GivingUpOperator(MockOperator):
            def on_kill(self):
                killed.append(self.arg1)

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput(
                    [{"arg1": 1, "raise_exception": AirflowTaskTimeout("gave up waiting")}, {"arg1": 2}]
                ),
                task_id="own_timeout",
                task_concurrency=1,
                operator_class=GivingUpOperator,
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                with pytest.raises(AirflowTaskTimeout, match="gave up waiting"):
                    iterable_op.execute(context=context)

                assert store.checkpoint(0)["status"] == "up_for_retry"
                assert store.checkpoint(1)["status"] == "success"
        assert killed == []

    def test_an_own_timeout_next_to_another_failure_reports_a_retry(self):
        """
        An indexed task's own ``AirflowTaskTimeout`` and another indexed task's ``ValueError`` reach the
        runner as a ``BaseExceptionGroup``, which it retries like any failure while retries are left. Both
        indexed tasks' callbacks report that retry, not a failure.
        """
        FIRED_CALLBACKS.clear()

        class RecordingOperator(MockOperator):
            def __init__(self, **kwargs):
                kwargs["on_failure_callback"] = lambda context: FIRED_CALLBACKS.append(("failure", self.arg1))
                kwargs["on_retry_callback"] = lambda context: FIRED_CALLBACKS.append(("retry", self.arg1))
                super().__init__(**kwargs)

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput(
                    [
                        {"arg1": 1, "raise_exception": AirflowTaskTimeout("gave up waiting")},
                        {"arg1": 2, "raise_exception": ValueError("bad item")},
                    ]
                ),
                task_id="timeout_and_failure",
                retries=1,
                operator_class=RecordingOperator,
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                context["ti"].max_tries = 1
                with pytest.raises(BaseExceptionGroup):
                    iterable_op.execute(context=context)

        assert sorted(FIRED_CALLBACKS) == [("retry", 1), ("retry", 2)]

    def test_iteration_cancelled_by_a_sibling_runs_no_callback(self):
        """One iteration stops the task; the sibling cancelled on the way did not fail."""
        FIRED_CALLBACKS.clear()
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "defer"}, {"arg1": "sleeper"}]),
                task_id="cancelled_sibling",
                retries=3,
                task_concurrency=2,
                operator_class=MockCallbackAsyncOperator,
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                context["ti"].max_tries = 3
                with pytest.raises(AirflowFailException, match="attempted to defer"):
                    iterable_op.execute(context=context)

        assert ("failure", "sleeper") not in FIRED_CALLBACKS
        assert ("retry", "sleeper") not in FIRED_CALLBACKS

    def test_failed_publish_keeps_the_success_checkpoint_and_the_retry_only_republishes(self):
        """
        When pushing an indexed task's result fails after its SUCCESS checkpoint was written, the checkpoint
        stays, and the retry replays the result instead of running the operator again, which would
        repeat whatever it did outside Airflow.
        """
        runs = []
        original_execute = MockOperator.execute

        def counting_execute(self, context):
            runs.append(self.arg1)
            return original_execute(self, context)

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}]), task_id="failed_publish"
            )

            with (
                mock_context(task=iterable_op) as context,
                patch.object(MockOperator, "execute", counting_execute),
            ):
                context["ti"].try_number = 1
                with patch.object(
                    IndexedTaskLifecycle, "_push_result", side_effect=RuntimeError("xcom backend down")
                ):
                    with pytest.raises(RuntimeError, match="xcom backend down"):
                        iterable_op.execute(context=context)
                checkpoint_after_failure = context["task_state_store"].checkpoint(0)["status"]

                context["ti"].try_number = 2
                result = iterable_op.execute(context=context)
                pushed = list(result)

        assert checkpoint_after_failure == "success"
        assert runs == [1]
        assert pushed == [(1, None, None)]

    def test_the_success_callback_fires_once_the_indexed_task_finished(self):
        """
        An indexed task's success callback fires once it finished and its SUCCESS outcome is recorded,
        without waiting for its round to be written: the outcome becomes durable with the round.

        A round write that fails keeps its outcomes for the next write, so the attempt still succeeds,
        each callback fired once, and the checkpoints are there for a later retry.
        """
        CALLBACKS.clear()

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b"}]),
                task_id="checkpoint_fails",
                task_concurrency=1,
                operator_class=MockCallbackOperator,
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                original_aset = store.aset
                failed: list[str] = []

                async def aset_failing_once(key, value, **kwargs):
                    if not failed:
                        failed.append(key)
                        raise RuntimeError("state store down")
                    await original_aset(key, value, **kwargs)

                store.aset = aset_failing_once
                iterable_op.execute(context=context)

        assert failed == ["_iterable_batch_0"]
        assert CALLBACKS == [("success", "a"), ("success", "b")]
        assert store.checkpoint(0)["status"] == "success"
        assert store.checkpoint(1)["status"] == "success"

    def test_a_failed_publish_fires_the_success_callback_once(self):
        """
        The checkpoint is written before the result is pushed, so a push that fails leaves work the
        retry replays rather than runs again: the success callback fired with the checkpoint and
        does not fire again on the replay.
        """
        CALLBACKS.clear()
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "a"}]),
                task_id="publish_fails",
                retries=2,
                operator_class=MockCallbackOperator,
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                with patch.object(
                    IndexedTaskLifecycle, "_push_result", side_effect=RuntimeError("xcom backend down")
                ):
                    with pytest.raises(RuntimeError, match="xcom backend down"):
                        iterable_op.execute(context=context)
                fired_after_the_failed_push = list(CALLBACKS)

                context["ti"].try_number = 2
                result = iterable_op.execute(context=context)
                pushed = list(result)

        assert fired_after_the_failed_push == [("success", "a")]
        assert CALLBACKS == [("success", "a")]
        assert pushed == ["a"]

    def test_a_success_callback_that_raises_keeps_the_checkpoint(self):
        """
        The success callback runs after the checkpoint is written and may raise what nothing catches:
        a ``BaseException`` such as ``DeadlockImminentError``. The indexed task's
        work is done, so its SUCCESS checkpoint stays and the retry replays the result instead of
        running the indexed task again; the callback fired once.
        """
        runs: list[str] = []
        fired: list[str] = []

        class CallbackRaisesOperator(BaseAsyncOperator):
            def __init__(self, arg1=None, **kwargs):
                kwargs["on_success_callback"] = self._callback
                super().__init__(**kwargs)
                self.arg1 = arg1

            def _callback(self, context):
                fired.append(self.arg1)
                raise DeadlockImminentError("send() from inside a request")

            async def aexecute(self, context):
                runs.append(self.arg1)
                return self.arg1

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "a"}]),
                task_id="callback_raises",
                retries=2,
                operator_class=CallbackRaisesOperator,
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                context["ti"].try_number = 1
                with pytest.raises(AirflowFailException, match="raised a non-Exception BaseException"):
                    iterable_op.execute(context=context)
                status_after_the_failed_callback = store.checkpoint(0)["status"]

                context["ti"].try_number = 2
                result = iterable_op.execute(context=context)
                pushed = list(result)

        assert status_after_the_failed_callback == "success"
        assert runs == ["a"]
        assert fired == ["a"]
        assert pushed == ["a"]

    def test_a_failed_skip_write_keeps_the_skip_for_the_next_write(self):
        """
        A round write that fails is no indexed task's outcome: the SKIPPED outcome is kept and written with
        the next round, and the indexed task stays skipped.
        """
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput(
                    [{"raise_exception": AirflowSkipException("not this one")}, {"arg1": 1}]
                ),
                task_id="skip_checkpoint_fails",
                task_concurrency=1,
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                original_aset = store.aset

                async def aset_failing_the_skip(key, value, **kwargs):
                    if [entry["status"] for entry in value.values()] == ["skipped"]:
                        raise RuntimeError("state store down")
                    await original_aset(key, value, **kwargs)

                store.aset = aset_failing_the_skip
                result = iterable_op.execute(context=context)

        assert result.skipped == [0]
        assert store.checkpoint(0)["status"] == "skipped"
        assert store.checkpoint(1)["status"] == "success"

    def test_a_skip_callback_that_raises_keeps_the_checkpoint(self):
        """
        The skip callback runs after the SKIPPED checkpoint is written, as the success callback runs
        after SUCCESS: a ``BaseException`` it raises is the indexed task's outcome, which fails the task at
        once as any non-``Exception`` does, and the checkpoint stays for the retry to replay.
        """
        fired: list[str] = []

        class SkipCallbackRaisesOperator(BaseOperator):
            def __init__(self, arg1=None, **kwargs):
                kwargs["on_skipped_callback"] = self._callback
                super().__init__(**kwargs)
                self.arg1 = arg1

            def _callback(self, context):
                fired.append(self.arg1)
                raise DeadlockImminentError("send() from inside a request")

            def execute(self, context):
                raise AirflowSkipException("not this one")

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "a"}]),
                task_id="skip_callback_raises",
                operator_class=SkipCallbackRaisesOperator,
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                with pytest.raises(AirflowFailException, match="raised a non-Exception BaseException"):
                    iterable_op.execute(context=context)

        assert fired == ["a"]
        assert store.checkpoint(0)["status"] == "skipped"

    def test_a_failed_up_for_retry_write_keeps_the_indexed_tasks_own_failure(self):
        """
        An indexed task that failed stays failed when the round holding its UP_FOR_RETRY outcome cannot be
        written: the task fails with the indexed task's own error, not the write's, the next indexed task
        still runs, and the outcome is written with the next round.
        """
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"raise_exception": ValueError("item failed")}, {"arg1": 1}]),
                task_id="retry_checkpoint_fails",
                task_concurrency=1,
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                original_aset = store.aset

                async def aset_failing_the_retry(key, value, **kwargs):
                    if [entry["status"] for entry in value.values()] == ["up_for_retry"]:
                        raise RuntimeError("state store down")
                    await original_aset(key, value, **kwargs)

                store.aset = aset_failing_the_retry
                with pytest.raises(ValueError, match="item failed"):
                    iterable_op.execute(context=context)

        assert store.checkpoint(0)["status"] == "up_for_retry"
        assert store.checkpoint(1)["status"] == "success"

    def test_extra_xcoms_are_checkpointed_once_and_pushed_again_when_a_retry_skips_the_indexed_task(self):
        """
        The runner deletes every XCom before a retry. An indexed task skipped because it already succeeded
        gets its other pushed keys back from its checkpoint, written once with its SUCCESS state.
        """
        MockPushingOperator.executed = []
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b", "fail": True}]),
                task_id="extra_xcoms",
                task_concurrency=1,
                operator_class=MockPushingOperator,
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                context["ti"].try_number = 1
                with pytest.raises(RuntimeError, match="sibling failed"):
                    iterable_op.execute(context=context)
                checkpoint = store.checkpoint(0)

                # The retry: the failing indexed task succeeds now; the finished one must not run again.
                pushed = []

                async def recording_apush(cls, ti, key, value, **kwargs):
                    pushed.append((key, value))

                iterable_op.expand_input = ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b"}])
                context["ti"].try_number = 2
                with patch.object(XCom, "_apush", classmethod(recording_apush)):
                    iterable_op.execute(context=context)

        assert checkpoint["status"] == "success"
        assert checkpoint["xcoms"] == {"foo": "foo-of-a"}
        assert "return_value" not in checkpoint["xcoms"]
        assert MockPushingOperator.executed == ["a", "b", "b"]
        assert ("foo_0", "foo-of-a") in pushed
        assert ("return_value_0", "a") in pushed

    @pytest.mark.parametrize("do_xcom_push", [True, False])
    def test_a_return_value_pushed_by_the_operator_is_pushed_again_when_a_retry_skips_the_indexed_task(
        self, do_xcom_push
    ):
        """
        An operator that pushes ``return_value`` itself and returns None still gets it back on the retry
        that skips it after a sibling failed; do_xcom_push governs only what execute returns.
        """
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b", "fail": True}]),
                task_id="pushes_its_return_value",
                task_concurrency=1,
                operator_class=MockReturnValuePushingOperator,
                do_xcom_push=do_xcom_push,
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                context["ti"].try_number = 1
                with pytest.raises(RuntimeError, match="sibling failed"):
                    iterable_op.execute(context=context)
                checkpoint = store.checkpoint(0)

                pushed = []

                async def recording_apush(cls, ti, key, value, **kwargs):
                    pushed.append((key, value))

                iterable_op.expand_input = ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b"}])
                context["ti"].try_number = 2
                with patch.object(XCom, "_apush", classmethod(recording_apush)):
                    iterable_op.execute(context=context)

        assert checkpoint["status"] == "success"
        assert ("return_value_0", "pushed-a") in pushed

    def test_an_indexed_task_pushing_many_keys_writes_one_checkpoint(self):
        """The extra pushes are kept in memory and written with the one SUCCESS checkpoint, not per push."""

        class ManyKeysOperator(BaseOperator):
            template_fields = ()

            def execute(self, context):
                for key in range(5):
                    context["ti"].xcom_push(key=f"key{key}", value=key)
                return "done"

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{}]), task_id="many_keys", operator_class=ManyKeysOperator
            )

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                writes = []
                original_aset, original_set = store.aset, store.set

                async def counting_aset(key, value, **kwargs):
                    writes.append(key)
                    await original_aset(key, value, **kwargs)

                def counting_set(key, value, **kwargs):
                    writes.append(key)
                    original_set(key, value, **kwargs)

                store.aset, store.set = counting_aset, counting_set
                iterable_op.execute(context=context)

        assert writes == ["_iterable_batch_0", "_iterable_completed"]
        assert store.checkpoint(0)["xcoms"] == {f"key{key}": key for key in range(5)}

    def test_an_async_indexed_tasks_extra_xcoms_are_pushed_again_on_retry(self):
        """The async path pushes through axcom_push; its keys are recorded and replayed the same way."""

        class AsyncPushingOperator(BaseAsyncOperator):
            template_fields = ("arg1",)
            executed: list = []

            def __init__(self, arg1=None, fail=False, **kwargs):
                super().__init__(**kwargs)
                self.arg1 = arg1
                self.fail = fail

            async def aexecute(self, context):
                type(self).executed.append(self.arg1)
                await context["ti"].axcom_push(key="bar", value=f"bar-of-{self.arg1}")
                if self.fail:
                    raise RuntimeError("sibling failed")
                return self.arg1

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b", "fail": True}]),
                task_id="async_extra_xcoms",
                task_concurrency=1,
                operator_class=AsyncPushingOperator,
            )

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                with pytest.raises(RuntimeError, match="sibling failed"):
                    iterable_op.execute(context=context)

                pushed = []

                async def recording_apush(cls, ti, key, value, **kwargs):
                    pushed.append((key, value))

                iterable_op.expand_input = ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b"}])
                context["ti"].try_number = 2
                with patch.object(XCom, "_apush", classmethod(recording_apush)):
                    iterable_op.execute(context=context)

        assert AsyncPushingOperator.executed == ["a", "b", "b"]
        assert ("bar_0", "bar-of-a") in pushed

    def test_execute_failed_attempt_leaves_no_completion_marker_so_retry_resumes(self):
        """
        Regression test: an attempt that fails must not write the completion marker.

        The marker tells a retry apart from a rerun after a manual clear: when it is present, every
        checkpoint is ignored and every index runs again. Raising the collected indexed task failures
        outside the ``Checkpoints`` block would let the block exit cleanly and write the marker on a
        failed attempt, so the retry would re-run the succeeded indices too instead of resuming.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"raise_exception": ValueError("boom")}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_failed_no_marker")

            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]

                with pytest.raises(ValueError, match="boom"):
                    iterable_op.execute(context=context)

                assert "_iterable_completed" not in store
                assert store.checkpoint(0)["status"] == "success"
                assert store.checkpoint(1)["status"] == "up_for_retry"

                # The retry: the failing indexed task succeeds now; index 0 must be replayed, not re-run.
                context["ti"].try_number = 2
                iterable_op.expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
                with patch.object(
                    IndexedTaskRunner, "run", autospec=True, side_effect=IndexedTaskRunner.run
                ) as run:
                    materialized = list(iterable_op.execute(context=context))

                assert materialized == [(1, None, None), (2, None, None)]
                assert run.call_count == 1
                assert run.call_args.args[0].task_index == 1  # only the failed index runs again
                assert store["_iterable_completed"] == {
                    "completed": True,
                    "try_number": 2,
                    "max_tries": 0,
                    "since": 0,
                }

    def test_execute_fail_exception_re_raised_directly_without_retry(self):
        """An indexed task that raises AirflowFailException must be re-raised directly (not wrapped in a
        BaseExceptionGroup) so the IterableOperator fails without being retried."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"arg1": 1},
                    {"raise_exception": AirflowFailException("boom")},
                ]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="fail_exception", retries=3)

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowFailException, match="boom"):
                    iterable_op.execute(context=context)

    @pytest.mark.parametrize(
        "raised_exception",
        [
            DagRunTriggerException(
                trigger_dag_id="triggered_dag",
                dag_run_id="triggered_run",
                conf={},
                reset_dag_run=False,
                skip_when_already_exists=False,
                wait_for_completion=False,
                allowed_states=["success"],
                failed_states=["failed"],
                poke_interval=1,
                deferrable=False,
            ),
            DownstreamTasksSkipped(tasks=["downstream_task"]),
        ],
        ids=["DagRunTriggerException", "DownstreamTasksSkipped"],
    )
    def test_execute_rejects_trigger_and_downstream_skip_exceptions(self, raised_exception):
        """TriggerDagRunOperator (DagRunTriggerException) and downstream-skip operators like
        ShortCircuitOperator (DownstreamTasksSkipped) are not supported inside IterableOperator: an
        indexed task has no DAG run or downstream tasks of its own for the trigger/skip to apply
        to, so this must fail the whole IterableOperator immediately with a clear error."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"raise_exception": raised_exception}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="unsupported_exception")

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowFailException, match="not supported inside IterableOperator"):
                    iterable_op.execute(context=context)

    def test_execute_rejects_reschedule_exception(self):
        """A reschedule-mode sensor (AirflowRescheduleException) is not supported inside
        IterableOperator: an indexed task has no task instance of its own to reschedule, so this
        must fail the whole IterableOperator immediately with a clear error rather than being silently
        aggregated into a retryable BaseExceptionGroup."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [{"raise_exception": AirflowRescheduleException(reschedule_date=None)}]
            )
            iterable_op = create_iterable_operator(dag, expand_input, task_id="reschedule_exception")

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowFailException, match="not supported inside IterableOperator"):
                    iterable_op.execute(context=context)

    @pytest.mark.asyncio
    async def test_run_indexed_task_skips_indexed_task_already_checkpointed_as_succeeded(self):
        """
        When the task_state_store already records an indexed task as succeeded (e.g. because
        Airflow retried the whole IterableOperator after a previous partial failure), ``IndexedTaskLifecycle.run``
        must skip re-executing that indexed task entirely.
        """
        from unittest import mock

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1, "arg2": 10}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="checkpoint_skip")

            with mock_context(task=iterable_op) as context:
                run = run_of(iterable_op, context, trust_checkpoints=True)
                task = run.unmapper.unmap(0, {"arg1": 1, "arg2": 10})
                task.try_number = 2  # checkpoint is only consulted from the second attempt onwards
                leave_checkpoint(
                    run,
                    task,
                    IndexedTaskState(
                        status=TaskInstanceState.SUCCESS, fingerprint=task.input_fingerprint
                    ).serialize(),
                )

                executor = mock.MagicMock()
                outcome = await IndexedTaskLifecycle(run, task, executor).run()

                assert outcome.result is None
                assert outcome.exception is None
                executor.run_sync.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_indexed_task_reruns_and_checkpoints_success_after_up_for_retry_state(self):
        """
        An indexed task whose checkpoint records ``UP_FOR_RETRY`` (left behind by a previous failed or
        crashed attempt) is re-executed rather than skipped — only a ``SUCCESS`` checkpoint causes
        ``IndexedTaskLifecycle.run`` to skip re-execution — and a new ``SUCCESS`` checkpoint recording its outcome.result is
        stored once it completes.
        """
        from airflow.sdk.execution_time.executor import AsyncAwareExecutor

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1, "arg2": 10}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="checkpoint_pending")

            with mock_context(task=iterable_op) as context:
                run = run_of(iterable_op, context, trust_checkpoints=True)
                task = run.unmapper.unmap(0, {"arg1": 1, "arg2": 10})
                task.try_number = 2  # checkpoint is only consulted from the second attempt onwards
                leave_checkpoint(
                    run, task, IndexedTaskState(status=TaskInstanceState.UP_FOR_RETRY).serialize()
                )

                with AsyncAwareExecutor(loop=asyncio.get_running_loop(), max_workers=1) as executor:
                    outcome = await IndexedTaskLifecycle(run, task, executor).run()

                assert outcome.exception is None
                assert outcome.result == (1, 10, None)
                assert (
                    outcome.task.try_number == 2
                )  # try_number is inherited from the parent TI, never mutated
                assert (
                    written_checkpoint(run, task)
                    == IndexedTaskState(
                        status=TaskInstanceState.SUCCESS,
                        result=outcome.result,
                        fingerprint=task.input_fingerprint,
                        try_number=2,
                    ).serialize()
                )

    @pytest.mark.asyncio
    async def test_run_indexed_task_merges_outlet_events_into_shared_context_on_success(self):
        """
        A succeeded indexed task's outlet asset events are merged into the task's
        ``context["outlet_events"]``.

        The task instance reports its outlet events once, from that accessor, when the whole
        iteration finishes, so an indexed task's events only reach the server through it.
        """
        from airflow.sdk.execution_time.executor import AsyncAwareExecutor

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="outlet_merge", operator_class=MockOutletEventOperator
            )

            with mock_context(task=iterable_op) as context:
                run = run_of(iterable_op, context, trust_checkpoints=True)
                task = run.unmapper.unmap(0, {})

                with AsyncAwareExecutor(loop=asyncio.get_running_loop(), max_workers=1) as executor:
                    outcome = await IndexedTaskLifecycle(run, task, executor).run()

                assert outcome.exception is None
                assert outcome.result == "done"
                accessor = context["outlet_events"][Asset(name="a", uri="s3://bucket/a")]
                assert accessor.extra == {"value": "v"}
                checkpoint = IndexedTaskState.deserialize(written_checkpoint(run, task))
                assert checkpoint.outlet_events == [
                    {
                        "kind": "asset",
                        "name": "a",
                        "uri": "s3://bucket/a",
                        "extra": {"value": "v"},
                        "partition_keys": [],
                    }
                ]

    @pytest.mark.asyncio
    async def test_run_indexed_task_replays_outlet_events_when_skipping_already_succeeded_indexed_task(self):
        """An indexed task skipped on retry (because its checkpoint already records SUCCESS) never
        re-executes, so it would otherwise never re-populate the fresh ``outlet_events`` accessor
        created for the new attempt; ``IndexedTaskLifecycle.run`` must replay the events it recorded on its
        earlier successful attempt instead of silently losing them."""
        from unittest import mock

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="outlet_replay", operator_class=MockOutletEventOperator
            )

            with mock_context(task=iterable_op) as context:
                run = run_of(iterable_op, context, trust_checkpoints=True)
                task = run.unmapper.unmap(0, {})
                task.try_number = 2  # checkpoint is only consulted from the second attempt onwards
                leave_checkpoint(
                    run,
                    task,
                    IndexedTaskState(
                        status=TaskInstanceState.SUCCESS,
                        fingerprint=task.input_fingerprint,
                        outlet_events=[
                            {
                                "kind": "asset",
                                "name": "a",
                                "uri": "s3://bucket/a",
                                "extra": {"value": "v"},
                                "partition_keys": [],
                            }
                        ],
                    ).serialize(),
                )

                executor = mock.MagicMock()
                outcome = await IndexedTaskLifecycle(run, task, executor).run()

        assert outcome.exception is None
        assert outcome.result is None
        executor.run_sync.assert_not_called()
        accessor = context["outlet_events"][Asset(name="a", uri="s3://bucket/a")]
        assert accessor.extra == {"value": "v"}

    def test_on_kill_propagates_to_active_indexed_operators(self):
        """IterableOperator.on_kill() (SIGTERM or execution_timeout) must propagate the kill
        signal to every indexed task currently in flight, since the default BaseOperator.on_kill()
        no-op would otherwise leave running indexed tasks completely unaware of the kill."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="on_kill_test", operator_class=MockOnKillOperator
            )

        active_operator = MockOnKillOperator(task_id="active_indexed_task")
        iterable_op._state.register(active_operator)

        iterable_op.on_kill()

        assert wait_until(lambda: active_operator.killed)

    def test_on_kill_reaches_indexed_operators_that_compare_equal_and_kills_each_once(self):
        """
        The indexed operators of one iterated task compare equal (same task_id), so the register is keyed
        by identity; and the runner's second on_kill() after a timeout does not kill them again.
        """
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{}]), task_id="on_kill_equal", operator_class=MockOnKillOperator
            )

        first, second = MockOnKillOperator(task_id="same"), MockOnKillOperator(task_id="same")
        assert first == second
        kills = []
        first.on_kill = lambda: kills.append("first")  # type: ignore[method-assign]
        second.on_kill = lambda: kills.append("second")  # type: ignore[method-assign]
        iterable_op._state.register(first)
        iterable_op._state.register(second)

        iterable_op.on_kill()
        iterable_op.on_kill()

        assert wait_until(lambda: len(kills) == 2)
        time.sleep(0.05)  # a second kill would arrive here
        assert sorted(kills) == ["first", "second"]

    def test_on_kill_reaches_every_indexed_operator_when_one_raises_a_base_exception(self):
        """An indexed operator's on_kill may raise a BaseException such as DeadlockImminentError; the rest are still killed."""

        class RaisingOnKill(MockOnKillOperator):
            def on_kill(self):
                raise DeadlockImminentError("send() from inside a request")

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{}]), task_id="on_kill_raises", operator_class=MockOnKillOperator
            )
        first, second = RaisingOnKill(task_id="first"), MockOnKillOperator(task_id="second")
        iterable_op._state.register(first)
        iterable_op._state.register(second)

        iterable_op.on_kill()

        assert wait_until(lambda: second.killed)

    def test_on_kill_with_the_loop_paused_kills_off_the_main_thread(self):
        """
        Between two ``run_until_complete`` calls the loop is paused and a parked ``asend`` may hold
        the comms lock: an indexed operator's sync SDK call in ``on_kill`` on the main thread would wait
        for it forever, since only the paused loop can release it. The kill runs in a thread.
        """
        seen: dict[str, bool] = {}
        done = threading.Event()
        main = threading.get_ident()

        class Op(MockOnKillOperator):
            def on_kill(self):
                seen["on_main"] = threading.get_ident() == main
                done.set()

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{}]), task_id="on_kill_paused", operator_class=Op
            )
        op = Op(task_id="active")
        iterable_op._state.register(op)

        iterable_op.on_kill()  # no loop running on this thread, as when the loop is paused

        assert done.wait(5)
        assert seen == {"on_main": False}
        assert iterable_op._state.stop_requested()

    def test_on_kill_inside_a_running_loop_kills_off_the_loop_thread(self):
        """
        The runner's SIGTERM handler calls on_kill on the main thread, usually while the loop runs
        there; the indexed operators' on_kill may make a sync SDK call, which must not run on the loop.
        """
        seen: dict[str, bool] = {}
        done = threading.Event()
        main = threading.get_ident()

        class Op(MockOnKillOperator):
            def on_kill(self):
                seen["on_running_loop"] = asyncio._get_running_loop() is not None
                seen["on_main"] = threading.get_ident() == main
                done.set()

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{}]), task_id="on_kill_in_loop", operator_class=Op
            )
        op = Op(task_id="active")
        iterable_op._state.register(op)

        async def kill_from_the_loop():
            iterable_op.on_kill()
            await asyncio.wait_for(asyncio.to_thread(done.wait, 5), 6)

        with event_loop() as loop:
            loop.run_until_complete(kill_from_the_loop())

        assert done.is_set()
        assert seen == {"on_running_loop": False, "on_main": False}

    def test_on_kill_takes_no_lock_of_the_iteration_state(self):
        """
        The runner's SIGTERM handler runs ``on_kill()`` on the main thread, between two bytecodes of
        the loop thread, which may be inside ``register``/``unregister`` and hold the state's lock:
        ``on_kill()`` returns at once while the lock is held and the kill follows once it is free.
        """
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{}]), task_id="on_kill_locked", operator_class=MockOnKillOperator
            )
        active_operator = MockOnKillOperator(task_id="active_indexed_task")
        iterable_op._state.register(active_operator)

        with iterable_op._state._lock:
            iterable_op.on_kill()
            assert iterable_op._state.stop_requested()
            assert not active_operator.killed

        assert wait_until(lambda: active_operator.killed)

    def test_on_kill_is_noop_when_no_indexed_operators_are_active(self):
        """on_kill() must not raise when called with no in-flight indexed tasks (e.g. the
        IterableOperator is killed before any indexed task has started, or after all finished)."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="on_kill_noop", operator_class=MockOnKillOperator
            )

        iterable_op.on_kill()  # should not raise

    def test_the_kill_thread_is_waited_for_before_the_run_concludes(self):
        """
        ``on_kill()`` kills in a thread of its own and returns at once; the run waits for that
        thread once the killed indexed tasks came back, so the task does not end, and the process
        with it, while an indexed operator's ``on_kill`` is still cleaning up.
        """
        executing = threading.Event()
        killed = threading.Event()
        cleanup_may_finish = threading.Event()

        class SlowToKill(BaseOperator):
            def __init__(self, arg1=None, **kwargs):
                super().__init__(**kwargs)
                self.arg1 = arg1

            def execute(self, context):
                executing.set()
                killed.wait(5)
                raise RuntimeError("killed")

            def on_kill(self):
                killed.set()
                cleanup_may_finish.wait(5)

        outcome: list[BaseException] = []
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}]), task_id="joined_kill", operator_class=SlowToKill
            )

            with mock_context(task=iterable_op) as context:

                def run():
                    try:
                        iterable_op.execute(context=context)
                    except BaseException as e:
                        outcome.append(e)

                run_thread = threading.Thread(target=run)
                run_thread.start()
                assert executing.wait(5)
                iterable_op.on_kill()
                assert killed.wait(5)

                run_thread.join(0.5)
                assert run_thread.is_alive(), "the run concluded while on_kill was still cleaning up"
                cleanup_may_finish.set()
                run_thread.join(5)

        assert not run_thread.is_alive()
        assert isinstance(outcome[0], AirflowTaskTerminated)

    def test_the_wait_for_the_kill_thread_is_bounded(self):
        """An indexed operator whose ``on_kill`` never returns holds the run for the shutdown timeout, not forever."""
        executing = threading.Event()
        killed = threading.Event()
        released = threading.Event()

        class StuckKill(BaseOperator):
            def __init__(self, arg1=None, **kwargs):
                super().__init__(**kwargs)
                self.arg1 = arg1

            def execute(self, context):
                executing.set()
                killed.wait(5)
                raise RuntimeError("killed")

            def on_kill(self):
                killed.set()
                released.wait(30)

        outcome: list[BaseException] = []
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}]), task_id="stuck_kill", operator_class=StuckKill
            )

            with (
                mock_context(task=iterable_op) as context,
                patch.object(IterationRun, "_SHUTDOWN_TIMEOUT", 0.2),
            ):

                def run():
                    try:
                        iterable_op.execute(context=context)
                    except BaseException as e:
                        outcome.append(e)

                run_thread = threading.Thread(target=run)
                run_thread.start()
                assert executing.wait(5)
                iterable_op.on_kill()
                run_thread.join(5)

        released.set()
        assert not run_thread.is_alive()
        assert isinstance(outcome[0], AirflowTaskTerminated)

    @pytest.mark.execution_timeout(20)
    def test_the_kill_after_a_failure_that_stops_the_task_is_bounded(self):
        """
        An indexed task whose outcome stops the task at once (here it defers) kills the indexed tasks still
        running, with no ``execution_timeout`` to cap it: one whose ``on_kill`` never returns holds the run
        for the shutdown timeout, as on the SIGTERM path, not forever.
        """
        executing = threading.Event()
        killed = threading.Event()
        released = threading.Event()

        class StuckKillAfterFailure(BaseOperator):
            def __init__(self, arg1=None, **kwargs):
                super().__init__(**kwargs)
                self.arg1 = arg1

            def execute(self, context):
                if self.arg1 == "defer":
                    assert executing.wait(5)
                    raise TaskDeferred(trigger=None, method_name="execute_complete")  # type: ignore[arg-type]
                executing.set()
                killed.wait(5)
                raise RuntimeError("killed")

            def on_kill(self):
                killed.set()
                released.wait(30)

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "defer"}, {"arg1": "running"}]),
                task_id="stuck_kill_after_failure",
                task_concurrency=2,
                operator_class=StuckKillAfterFailure,
            )

            with (
                mock_context(task=iterable_op) as context,
                patch.object(IterationRun, "_SHUTDOWN_TIMEOUT", 0.2),
            ):
                try:
                    with pytest.raises(AirflowFailException, match="attempted to defer"):
                        iterable_op.execute(context=context)
                finally:
                    released.set()

        assert killed.is_set()

    def test_run_indexed_task_tracks_active_indexed_operator_during_execution(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        """``IndexedTaskLifecycle.run`` must register the indexed task's unmapped operator in the iteration state
        only for the duration of its execution, so ``on_kill()`` propagates only to indexed tasks that
        are actually running."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="on_kill_tracking", operator_class=MockOnKillOperator
            )

            with mock_context(task=iterable_op) as context:
                run = run_of(iterable_op, context, trust_checkpoints=False)
                task = run.unmapper.unmap(0, {})

                seen_active_during_run = []

                def tracking_execute(context, ti, log):
                    seen_active_during_run.append(task.task in iterable_op._state)

                monkeypatch.setattr("airflow.sdk.execution_time.task_runner._execute_task", tracking_execute)

                with event_loop() as loop, AsyncAwareExecutor(loop=loop, max_workers=1) as executor:
                    outcome = loop.run_until_complete(IndexedTaskLifecycle(run, task, executor).run())

        assert outcome.exception is None
        assert seen_active_during_run == [True]
        assert task.task not in iterable_op._state

    def test_multiple_outputs_is_ignored(self):
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            mapped_op = create_mapped_operator(dag=dag, expand_input=expand_input, task_id="multi")

            iterable_op = IterableOperator(
                operator=mapped_op, expand_input=expand_input, dag=dag, multiple_outputs=True
            )

            assert iterable_op.multiple_outputs is False

    def test_iterable_execution_timeout_caps_whole_iteration_and_wrapped_operator_retains_it(self):
        """IterableOperator keeps execution_timeout as the wall-clock cap on the whole iteration, which
        the runner enforces on the outer TI; the wrapped operator keeps the value too, as any of its
        own parameters, without a limit of its own per indexed task."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            execution_timeout = timedelta(seconds=7)
            mapped_op = create_mapped_operator(
                dag, expand_input, task_id="timeout_task", execution_timeout=execution_timeout
            )

            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            assert iterable_op._operator.execution_timeout == execution_timeout
            assert iterable_op.execution_timeout == execution_timeout

    @pytest.mark.parametrize(
        "base_exception",
        [
            SystemExit(1),
            KeyboardInterrupt(),
        ],
        ids=["SystemExit", "KeyboardInterrupt"],
    )
    def test_base_exception_not_retried_raises_airflow_fail_exception(self, base_exception):
        """
        BaseException subclasses (e.g., SystemExit, KeyboardInterrupt) must never
        be retried—they signal conditions where continuing iteration is meaningless.
        They should raise AirflowFailException immediately.
        """
        with DAG("test_dag") as dag:
            # Create a mapped operator that raises a BaseException
            expand_input = ListOfDictsExpandInput([{"raise_exception": base_exception}])
            mapped_op = MockOperator.partial(
                task_id="base_exception_task",
                dag=dag,
                retries=3,  # Has retries available, but should NOT use them
            )._expand(
                expand_input,
                strict=True,
                register_with_dag=False,
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowFailException):
                    iterable_op.execute(context=context)

    def test_parent_timeout_landing_in_an_indexed_task_stays_a_timeout(self):
        """
        The parent's ``execution_timeout`` is raised by a signal handler on the main thread, so it
        can surface inside whichever async indexed task is running there. It is not that indexed task's
        outcome: it has to reach the runner as ``AirflowTaskTimeout``, which retries the task, and
        not be turned into a non-retryable ``AirflowFailException`` blamed on the indexed task.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag,
                expand_input,
                task_id="timeout_task",
                retries=3,
                operator_class=MockCallbackTimeoutOperator,
            )

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowTaskTimeout):
                    iterable_op.execute(context=context)

                assert context["task_state_store"].checkpoint(0) is None

    @pytest.mark.asyncio
    async def test_run_indexed_task_lets_a_cancellation_through(self):
        """A cancelled indexed task stays cancelled: no checkpoint write, no result handed back."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="cancelled_task")

            with mock_context(task=iterable_op) as context:
                run = run_of(iterable_op, context)
                task = run.unmapper.unmap(0, {"raise_exception": asyncio.CancelledError()})

                with AsyncAwareExecutor(loop=asyncio.get_running_loop(), max_workers=1) as executor:
                    with pytest.raises(asyncio.CancelledError):
                        await IndexedTaskLifecycle(run, task, executor).run()

                assert written_checkpoint(run, task) is None

    def test_deadlock_imminent_error_fails_the_task_without_retry(self):
        """
        An indexed task that raises DeadlockImminentError (a sync SDK call made on a thread already in
        the middle of a request) ends the task at once, with retries left, as any BaseException
        that is no Exception does.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [{"raise_exception": DeadlockImminentError("send() from inside a request")}]
            )
            mapped_op = MockOperator.partial(
                task_id="deadlock_task",
                dag=dag,
                retries=3,  # Has retries available, but should NOT use them
            )._expand(
                expand_input,
                strict=True,
                register_with_dag=False,
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                with pytest.raises(
                    AirflowFailException, match="raised a non-Exception BaseException: DeadlockImminentError"
                ):
                    iterable_op.execute(context=context)

    def test_deferred_operator_raises_airflow_fail_exception(self):
        """An indexed task that raises TaskDeferred must cause IterableOperator to raise AirflowFailException."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}, {}])
            mapped_op = MockDeferredOperator.partial(task_id="deferred_task", dag=dag)._expand(
                expand_input,
                strict=True,
                register_with_dag=False,
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

        with mock_context(task=iterable_op) as context:
            with pytest.raises(AirflowFailException, match="attempted to defer"):
                iterable_op.execute(context=context)

    def test_reschedule_mode_sensor_fails_the_task_without_retry(self):
        """
        An indexed task that raises ``AirflowRescheduleException`` fails the whole task without a retry:
        an indexed task has no task instance of its own to reschedule, so
        ``IndexedTaskOutcome.fail_fast`` turns it into an ``AirflowFailException`` that says so.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}, {}])
            mapped_op = MockRescheduleSensor.partial(task_id="reschedule_sensor", dag=dag)._expand(
                expand_input,
                strict=True,
                register_with_dag=False,
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

        with mock_context(task=iterable_op) as context:
            with pytest.raises(AirflowFailException, match="attempted to reschedule"):
                iterable_op.execute(context=context)


class TestIterableOperatorContextIsolation:
    """
    Verify that each indexed task run by IterableOperator sees its own indexed
    context via get_current_context(), not the parent's.
    """

    def test_indexed_task_sees_its_own_context(self):
        """Each indexed task's get_current_context() must return its own indexed ti, not the parent's."""
        captured: dict[int, object] = {}

        class ContextCapturingOperator(BaseOperator):
            def __init__(self, index: int, **kwargs):
                super().__init__(**kwargs)
                self.index = index

            def execute(self, context):
                ctx = get_current_context()
                captured[self.index] = ctx["ti"]
                return self.index

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"index": 0}, {"index": 1}, {"index": 2}])
            mapped_op = ContextCapturingOperator.partial(task_id="ctx_task", dag=dag)._expand(
                expand_input, strict=True, register_with_dag=False
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

        with mock_context(task=iterable_op) as context:
            iterable_op.execute(context=context)

            parent_ti = context["ti"]
            for idx, indexed_ti in captured.items():
                # Each indexed task must have seen its own IndexedTaskInstance, not the parent TI.
                assert indexed_ti is not parent_ti, f"Indexed task {idx} observed the parent context"
                assert indexed_ti.index == idx, f"Indexed task {idx} observed wrong index {indexed_ti.index}"


class MockCallbackSyncOperator(BaseOperator):
    """Sync twin of MockCallbackAsyncOperator: defers for ``arg1="defer"``, sleeps otherwise."""

    template_fields = ("arg1",)

    def __init__(self, arg1=None, **kwargs):
        kwargs["on_success_callback"] = lambda context: FIRED_CALLBACKS.append(("success", self.arg1))
        kwargs["on_failure_callback"] = lambda context: FIRED_CALLBACKS.append(("failure", self.arg1))
        kwargs["on_retry_callback"] = lambda context: FIRED_CALLBACKS.append(("retry", self.arg1))
        super().__init__(**kwargs)
        self.arg1 = arg1

    def execute(self, context):
        if self.arg1 == "defer":
            raise TaskDeferred(trigger=None, method_name="execute_complete")  # type: ignore[arg-type]
        time.sleep(0.5)
        return self.arg1


class TestItemThreads:
    """
    Which thread an indexed task's code and callbacks run on.

    A sync indexed task runs in a worker thread, enter and exit included, so a sync SDK call from its
    ``execute`` or its callbacks waits for the comms lock without holding up the loop; on the loop
    thread the same call would hold up every sibling for its round trip. An async indexed task runs on the
    loop and should make async calls.
    """

    @staticmethod
    def _run(operator_class, is_async):
        seen: list[tuple[str, bool, bool]] = []
        main = threading.get_ident()

        def note(where):
            seen.append((where, threading.get_ident() != main, asyncio._get_running_loop() is not None))

        if is_async:

            class Op(operator_class):
                def __init__(self, **kwargs):
                    kwargs["on_success_callback"] = lambda context: note("on_success_callback")
                    kwargs["on_execute_callback"] = lambda context: note("on_execute_callback")
                    super().__init__(**kwargs)

                async def aexecute(self, context):
                    note("aexecute")

        else:

            class Op(operator_class):
                def __init__(self, **kwargs):
                    kwargs["on_success_callback"] = lambda context: note("on_success_callback")
                    kwargs["on_execute_callback"] = lambda context: note("on_execute_callback")
                    super().__init__(**kwargs)

                def execute(self, context):
                    note("execute")

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}, {}])
            mapped_op = Op.partial(task_id="threads", dag=dag, task_concurrency=2)._expand(
                expand_input, strict=True, register_with_dag=False
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)
            with mock_context(task=iterable_op) as context:
                iterable_op.execute(context=context)
        assert len(seen) == 6
        return seen

    def test_a_sync_indexed_tasks_code_and_callbacks_run_in_its_worker_thread(self):
        for where, off_main, on_running_loop in self._run(BaseOperator, is_async=False):
            assert off_main, where
            assert not on_running_loop, where

    def test_an_async_indexed_tasks_code_and_callbacks_run_on_the_loop_thread(self):
        for where, off_main, on_running_loop in self._run(BaseAsyncOperator, is_async=True):
            assert not off_main, where
            assert on_running_loop, where

    def test_a_sync_indexed_task_cancelled_by_a_sibling_reports_nothing_when_its_thread_finishes(self):
        """
        The coroutine waiting for a sync indexed task is cancelled while its thread goes on; the indexed task
        gets no checkpoint, so its exit must fire no callback: the next attempt runs it and reports then.
        """
        FIRED_CALLBACKS.clear()
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "defer"}, {"arg1": "sleeper"}]),
                task_id="cancelled_sync_sibling",
                retries=3,
                task_concurrency=2,
                operator_class=MockCallbackSyncOperator,
            )
            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                context["ti"].max_tries = 3
                with pytest.raises(AirflowFailException, match="attempted to defer"):
                    iterable_op.execute(context=context)

        assert [fired for fired in FIRED_CALLBACKS if fired[1] == "sleeper"] == []


class TestIterationState:
    """What one run of an iterated task remembers, apart from the operator's configuration."""

    def test_registered_operators_are_in_flight_until_unregistered(self):
        state = IterationState(Mock())
        op = MockOperator(task_id="op")
        assert op not in state
        state.register(op)
        assert op in state
        state.unregister(op)
        assert op not in state
        state.unregister(op)  # a second time is harmless

    def test_in_flight_is_keyed_by_identity(self):
        """Indexed operators of one iterated task compare equal; each is registered on its own."""
        state = IterationState(Mock())
        first, second = MockOperator(task_id="same"), MockOperator(task_id="same")
        assert first == second
        state.register(first)
        state.register(second)
        assert first in state
        assert second in state
        state.unregister(first)
        assert first not in state
        assert second in state

    def test_take_in_flight_hands_each_operator_out_once(self):
        """The runner's second on_kill() after a timeout must not kill the same operator again."""
        state = IterationState(Mock())
        first, second, third = (MockOperator(task_id=f"op{i}") for i in range(3))
        state.register(first)
        state.register(second)
        assert state.take_in_flight() == [first, second]
        state.register(third)
        assert state.take_in_flight() == [third]
        assert state.take_in_flight() == []
        # Still in flight as far as the register goes: a kill does not unregister.
        assert first in state

    def test_stop_is_requested_once_asked(self):
        state = IterationState(Mock())
        assert not state.stop_requested()
        state.request_stop()
        assert state.stop_requested()
        state.request_stop()
        assert state.stop_requested()

    def test_the_calls_a_signal_handler_makes_take_no_lock(self):
        """``request_stop`` and ``start_kill`` run in the SIGTERM handler, on the thread that may hold the lock."""
        state = IterationState(Mock())
        op = MockOperator(task_id="op")
        state.register(op)
        taken: list[BaseOperator] = []

        with state._lock:
            state.request_stop()
            state.start_kill(lambda: taken.extend(state.take_in_flight()))
            assert state.stop_requested()
            assert taken == []

        state.await_kill(5)
        assert taken == [op]

    def test_a_copy_is_a_fresh_state(self):
        import copy

        state = IterationState(Mock())
        op = MockOperator(task_id="op")
        state.register(op)
        state.request_stop()

        copied = copy.deepcopy(state)

        assert copied is not state
        assert op not in copied
        assert not copied.stop_requested()
        assert op in state
        assert state.stop_requested()

    def test_a_fresh_state_remembers_nothing(self):
        state = IterationState(Mock())
        assert not state.stop_requested()
        assert state.take_in_flight() == []

    def test_kill_reaches_every_operator_whatever_one_raises(self):
        """One indexed operator's on_kill raising, a BaseException included, does not keep the kill from the others."""
        log = Mock()
        state = IterationState(log)
        first, second = MockOperator(task_id="first"), MockOperator(task_id="second")
        first.on_kill = Mock(side_effect=KeyboardInterrupt)
        second.on_kill = Mock()

        state.kill([first, second])

        second.on_kill.assert_called_once_with()
        log.exception.assert_called_once_with("Error calling on_kill() for indexed task operator %s", "first")


class TestIterationRun:
    """One run of an iterated task: what its indexed tasks share while it goes."""

    def test_length_is_unknown_until_the_input_is_resolved(self):
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([{"arg1": 1}]))
        run = run_of(iterable_op, MagicMock())
        assert run.resolved is None
        assert run.length is None

        async def aget(index):
            return {}

        run.resolved = Resolved(3, aget)
        assert run.length == 3


class TestIndexedTaskUnmapper:
    """The indexed task of each index of a run, built from its mapped kwargs."""

    def test_a_run_pushes_until_an_indexed_task_says_otherwise(self):
        """Whether the run's result is pushed follows the operator built for an indexed task, True until one is."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}]), do_xcom_push=False
            )
            with mock_context(task=iterable_op) as context:
                run = run_of(iterable_op, context)
                assert run.unmapper.do_xcom_push is True
                run.unmapper.unmap(0, {"arg1": 1})
        assert run.unmapper.do_xcom_push is False


class TestIndexedTaskOutcomes:
    """What the indexed tasks ended with, turned into the task's own outcome and its callbacks."""

    @staticmethod
    def _outcomes(retries: int = DEFAULT_RETRIES, state: IterationState | None = None) -> IndexedTaskOutcomes:
        with DAG("test_dag") as dag:
            op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": "a"}]), task_id="it", retries=retries
            )
        context = {"ti": MagicMock(try_number=1, max_tries=3)}
        return IndexedTaskOutcomes(op, state or IterationState(Mock()), context)  # type: ignore[arg-type]

    @staticmethod
    def _task(index: int) -> IndexedTaskInstance:
        return MagicMock(spec=IndexedTaskInstance, task_id="it", index=index)

    def test_nothing_raised_is_counted_and_records_nothing_else(self):
        outcomes = self._outcomes()
        outcomes.record(IndexedTaskOutcome(self._task(0)))
        assert outcomes.total == 1
        assert outcomes.exceptions == []
        assert outcomes.skipped == {}

    def test_a_skip_is_noted_under_its_index(self):
        outcomes = self._outcomes()
        skip = AirflowSkipException("odd")
        outcomes.record(IndexedTaskOutcome(self._task(2), exception=skip))
        assert outcomes.skipped == {2: skip}
        assert outcomes.exceptions == []

    def test_a_failure_is_collected(self):
        outcomes = self._outcomes()
        raised = ValueError("boom")
        outcomes.record(IndexedTaskOutcome(self._task(1), exception=raised))
        assert outcomes.exceptions == [raised]

    @pytest.mark.parametrize(
        ("raised", "match"),
        [
            (TaskDeferred(trigger=MagicMock(), method_name="x"), "attempted to defer"),
            (AirflowRescheduleException(datetime.now(UTC)), "attempted to reschedule"),
            (DownstreamTasksSkipped(tasks=["a"]), "raised DownstreamTasksSkipped"),
            (DeadlockImminentError("send() from inside a request"), "raised a non-Exception BaseException"),
            (KeyboardInterrupt(), "raised a non-Exception BaseException"),
        ],
    )
    def test_an_outcome_the_iteration_cannot_carry_ends_the_task_at_once(self, raised, match):
        """Every later indexed task would end the same way, so nothing is collected: it is raised."""
        outcomes = self._outcomes()
        with pytest.raises(AirflowFailException, match=rf"Indexed task it\[3\] {match}") as info:
            outcomes.record(IndexedTaskOutcome(self._task(3), exception=raised))
        assert info.value.__cause__ is raised
        assert outcomes.exceptions == []

    def test_a_kill_concludes_as_terminated_whatever_else_happened(self):
        state = IterationState(Mock())
        outcomes = self._outcomes(state=state)
        outcomes.record(IndexedTaskOutcome(self._task(0), exception=ValueError("a")))
        outcomes.record(IndexedTaskOutcome(self._task(1)))
        state.request_stop()
        with pytest.raises(AirflowTaskTerminated, match="2 of 5 indexed tasks ran") as info:
            outcomes.conclude(5)
        assert isinstance(info.value.__cause__, BaseExceptionGroup)
        assert info.value.__cause__.exceptions == (outcomes.exceptions[0],)

    def test_an_indexed_task_not_started_after_the_kill_is_counted_apart(self):
        """It ran no code: not a failure to collect or log, not an indexed task that ran, named in the message."""
        state = IterationState(Mock())
        outcomes = self._outcomes(state=state)
        outcomes.record(IndexedTaskOutcome(self._task(0)))
        outcomes.record(
            IndexedTaskOutcome(
                self._task(1), exception=IndexedTaskInstanceNotStarted("pulled before the kill")
            )
        )
        assert outcomes.total == 1
        assert outcomes.not_started == 1
        assert outcomes.exceptions == []
        state.request_stop()
        with pytest.raises(
            AirflowTaskTerminated,
            match=r"1 of 3 indexed tasks ran, 1 pulled but never started, 1 never pulled\.$",
        ):
            outcomes.conclude(3)

    def test_a_kill_after_every_indexed_task_was_pulled_claims_no_remainder(self):
        """Every indexed task pulled and run, the killed one back as a failure: the message ends with the count."""
        state = IterationState(Mock())
        outcomes = self._outcomes(state=state)
        outcomes.record(IndexedTaskOutcome(self._task(0), exception=ValueError("killed")))
        outcomes.record(IndexedTaskOutcome(self._task(1)))
        state.request_stop()
        with pytest.raises(AirflowTaskTerminated, match=r"2 of 2 indexed tasks ran\.$"):
            outcomes.conclude(2)

    def test_a_kill_while_resolving_is_not_an_empty_input(self):
        state = IterationState(Mock())
        state.request_stop()
        with pytest.raises(AirflowTaskTerminated, match="before its input was resolved") as info:
            self._outcomes(state=state).conclude(None)
        assert info.value.__cause__ is None

    def test_failures_conclude_on_the_exception_handed_to_the_runner(self):
        outcomes = self._outcomes()
        raised = ValueError("a")
        outcomes.record(IndexedTaskOutcome(self._task(0), exception=raised))
        outcomes.record(IndexedTaskOutcome(self._task(1)))
        with pytest.raises(ValueError, match="a") as info:
            outcomes.conclude(2)
        assert info.value is raised

    def test_an_empty_input_concludes_as_a_skip(self):
        with pytest.raises(AirflowSkipException, match="empty"):
            self._outcomes().conclude(0)

    def test_every_indexed_task_skipped_skips_the_task(self):
        outcomes = self._outcomes()
        first = AirflowSkipException("first")
        outcomes.record(IndexedTaskOutcome(self._task(0), exception=first))
        outcomes.record(IndexedTaskOutcome(self._task(1), exception=AirflowSkipException("second")))
        with pytest.raises(AirflowSkipException) as info:
            outcomes.conclude(2)
        assert info.value is first

    def test_a_partial_skip_concludes_with_the_skipped_indices_in_order(self):
        outcomes = self._outcomes()
        outcomes.record(IndexedTaskOutcome(self._task(2), exception=AirflowSkipException("c")))
        outcomes.record(IndexedTaskOutcome(self._task(0), exception=AirflowSkipException("a")))
        outcomes.record(IndexedTaskOutcome(self._task(1)))
        assert outcomes.conclude(3) == [0, 2]

    def test_failed_runners_are_kept_in_the_order_they_failed(self):
        outcomes = self._outcomes()
        first, second = object(), object()
        outcomes.note_failed(first)  # type: ignore[arg-type]
        outcomes.note_failed(second)  # type: ignore[arg-type]
        assert outcomes.failed_runners == (first, second)

    def test_the_failed_runners_report_the_tasks_fate_when_an_exception_leaves_the_block(self):
        """A retry while attempts are left, a final failure for a fail-fast exception, in failure order."""
        for raised, will_retry in [(ValueError("x"), True), (AirflowFailException("x"), False)]:
            outcomes = self._outcomes(retries=2)
            first, second = MagicMock(), MagicMock()
            first.task_instance.is_eligible_to_retry = True
            outcomes.note_failed(first)
            outcomes.note_failed(second)
            manager = Mock()
            manager.attach_mock(first, "first")
            manager.attach_mock(second, "second")

            with pytest.raises(type(raised)), outcomes:
                raise raised

            assert manager.mock_calls == [
                call.first.report_failure(task_will_retry=will_retry),
                call.second.report_failure(task_will_retry=will_retry),
            ]

    def test_nothing_is_reported_when_the_block_ends_normally(self):
        """No failure left the block, so no indexed task's callback is waiting on it."""
        outcomes = self._outcomes()
        runner = MagicMock()
        outcomes.note_failed(runner)
        with outcomes:
            pass
        runner.report_failure.assert_not_called()


KILL_TARGET: list = []


class MockKillingOperator(BaseOperator):
    """Operator whose ``arg1="kill"`` indexed task kills the iterated task from inside, as SIGTERM would."""

    template_fields = ("arg1",)

    def __init__(self, arg1=None, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1

    def execute(self, context):
        if self.arg1 == "kill":
            KILL_TARGET[0].on_kill()
        return self.arg1


class MockEchoAsyncOperator(BaseAsyncOperator):
    """Async operator that returns its ``arg1``, for kills that come from elsewhere than the indexed task."""

    template_fields = ("arg1",)

    def __init__(self, arg1=None, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1

    async def aexecute(self, context):
        return self.arg1


class TestAKillSticks:
    """on_kill() stops the iteration: nothing new starts, and the task fails without a retry."""

    def test_a_kill_stops_the_iteration_and_fails_the_task(self):
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": "kill"}, {"arg1": 1}, {"arg1": 2}, {"arg1": 3}]),
                task_id="killed",
                task_concurrency=1,
                operator_class=MockKillingOperator,
            )
            KILL_TARGET[:] = [iterable_op]
            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                with pytest.raises(AirflowTaskTerminated, match="1 of 4 indexed tasks ran"):
                    iterable_op.execute(context=context)

        assert "_iterable_completed" not in store
        assert store.checkpoint(0)["status"] == "success"
        assert all(store.checkpoint(index) is None for index in (1, 2, 3))

    def test_an_indexed_task_pulled_before_the_kill_but_not_started_does_not_run(self):
        """
        on_kill() reaches what has started; an indexed task whose input is still being read has not.

        Every indexed task reads and renders its input before its runner starts, in a worker thread. A
        kill that lands meanwhile (here from the read itself, as a SIGTERM would land on the main thread)
        finds the indexed task unregistered: it must not run afterwards, gets no checkpoint and fires no
        callback, as the indexed tasks never pulled do not, and the message counts it apart.
        """
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": 0}, {"arg1": 1}, {"arg1": 2}]),
                task_id="killed",
                task_concurrency=3,
                operator_class=MockEchoAsyncOperator,
            )
            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                item_0_done = threading.Event()
                killed = threading.Event()
                unmap = IndexedTaskUnmapper.unmap
                record = Checkpoints.record

                async def record_and_signal(checkpoints, task, state):
                    await record(checkpoints, task, state)
                    if task.index == 0:
                        item_0_done.set()

                def unmap_and_kill(unmapper, index, mapped_kwargs):
                    # Indexed tasks reach this read in the order their worker threads run it, so the test
                    # sets the order: indexed task 0 runs to its outcome, indexed task 1's read is where
                    # the kill lands, and indexed task 2's read comes after it.
                    if index == 1:
                        assert item_0_done.wait(timeout=5)
                        iterable_op.on_kill()
                        killed.set()
                    elif index == 2:
                        assert killed.wait(timeout=5)
                    return unmap(unmapper, index, mapped_kwargs)

                with (
                    patch.object(IndexedTaskUnmapper, "unmap", unmap_and_kill),
                    patch.object(Checkpoints, "record", record_and_signal),
                    pytest.raises(
                        AirflowTaskTerminated, match="1 of 3 indexed tasks ran, 2 pulled but never started"
                    ),
                ):
                    iterable_op.execute(context=context)

        assert "_iterable_completed" not in store
        assert store.checkpoint(0)["status"] == "success"
        assert all(store.checkpoint(index) is None for index in (1, 2))

    def test_a_kill_before_the_run_started_still_stops_it(self):
        """SIGTERM can land between the operator's creation and its run: the stop must survive."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}]), task_id="killed_early"
            )
            iterable_op.on_kill()
            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                with pytest.raises(AirflowTaskTerminated, match="before its input was resolved"):
                    iterable_op.execute(context=context)

        assert "_iterable_completed" not in store
        assert store.checkpoint(0) is None

    def test_a_rerun_in_the_same_process_does_not_see_the_earlier_kill(self):
        """dag.test() and the tests run one operator object several times; the state is per run."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag, ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}]), task_id="killed_then_rerun"
            )
            iterable_op.on_kill()
            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowTaskTerminated):
                    iterable_op.execute(context=context)

                result = iterable_op.execute(context=context)

        assert len(result) == 2
        assert "_iterable_completed" in context["task_state_store"]

    def test_a_kill_before_any_indexed_task_started_is_not_an_empty_input(self):
        """A kill while the input is resolved leaves nothing to run; that is a kill, not a skip."""
        xcom_arg = make_xcom_arg(None)

        async def aresolve(*a, **kw):
            KILL_TARGET[0].on_kill()
            return [{"arg1": 1}, {"arg1": 2}]

        xcom_arg.aresolve = aresolve
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput(xcom_arg),
                task_id="killed_early",
                operator_class=MockKillingOperator,
            )
            KILL_TARGET[:] = [iterable_op]
            with mock_context(task=iterable_op) as context:
                store = context["task_state_store"]
                with pytest.raises(AirflowTaskTerminated, match="0 of 2 indexed tasks ran"):
                    iterable_op.execute(context=context)

        assert "_iterable_completed" not in store


KILLED_ON_TIMEOUT: list = []


class MockSlowSyncOperator(BaseOperator):
    """Sync operator that waits up to 3 s for on_kill(), as an operator stopping an external job would."""

    template_fields = ("arg1",)

    def __init__(self, arg1=None, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.stop = threading.Event()

    def execute(self, context):
        self.stop.wait(3)

    def on_kill(self):
        KILLED_ON_TIMEOUT.append(("sync", self.arg1))
        self.stop.set()


class MockSlowAsyncKillableOperator(BaseAsyncOperator):
    template_fields = ("arg1",)

    def __init__(self, arg1=None, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1

    async def aexecute(self, context):
        await asyncio.sleep(3)

    def on_kill(self):
        KILLED_ON_TIMEOUT.append(("async", self.arg1))


class TestExecutionTimeoutKillsInFlightIndexedTasks:
    """
    The parent's execution_timeout reaches every indexed task in flight, once, through on_kill().

    Runs the operator through the runner's own ``_run_execute_callable`` with a real timeout, as the
    task runner does, so the order in which the executor cancels and the runner calls on_kill() is
    the real one.
    """

    @pytest.mark.parametrize(
        ("operator_class", "kind"),
        [(MockSlowSyncOperator, "sync"), (MockSlowAsyncKillableOperator, "async")],
        ids=["sync", "async"],
    )
    def test_every_indexed_task_in_flight_is_killed_once(self, operator_class, kind, mock_supervisor_comms):
        from airflow.sdk.execution_time.task_runner import _run_execute_callable

        KILLED_ON_TIMEOUT.clear()
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            mapped_op = create_mapped_operator(
                dag,
                expand_input,
                task_id="timed_out",
                task_concurrency=2,
                execution_timeout=timedelta(milliseconds=300),
                operator_class=operator_class,
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowTaskTimeout):
                    _run_execute_callable(context, iterable_op.execute, iterable_op)

        assert sorted(KILLED_ON_TIMEOUT) == [(kind, 1), (kind, 2)]

    def test_the_parents_timeout_is_started_on_the_supervisor_once(self, mock_supervisor_comms):
        """
        Sync indexed tasks with an execution_timeout run in worker threads, where TimeoutPosix cannot fire,
        under the parent's limit. Starting the supervisor's timeout again per indexed task would move its
        hard-kill deadline to the last indexed task started, up to one full timeout late.
        """
        from airflow.sdk.execution_time.comms import StartExecutionTimeout
        from airflow.sdk.execution_time.task_runner import _run_execute_callable

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}, {"arg1": 3}])
            mapped_op = create_mapped_operator(
                dag,
                expand_input,
                task_id="timed_items",
                task_concurrency=2,
                execution_timeout=timedelta(seconds=30),
            )
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                _run_execute_callable(context, iterable_op.execute, iterable_op)

        sent = [
            call.kwargs.get("msg", call.args[0] if call.args else None)
            for call in mock_supervisor_comms.send.call_args_list
        ]
        assert [msg for msg in sent if isinstance(msg, StartExecutionTimeout)] == [StartExecutionTimeout()]

    def test_the_struck_async_indexed_tasks_on_kill_runs_off_the_loop_thread(self):
        """
        The parent's timeout can land inside the async indexed task running on the loop thread. Its on_kill
        may make a sync SDK call (cancelling a remote job through a sync hook), which on that thread,
        interrupted mid-request, raises DeadlockImminentError; killed there, the timeout would turn
        into a failure without a retry and the job would go on. The struck indexed task stays registered and
        the parent
        kills it off the loop with the others. The strike is simulated by the indexed task raising the
        timeout itself, as the signal handler would inside its coroutine.
        """

        class SyncHookOnKill(MockSlowAsyncKillableOperator):
            async def aexecute(self, context):
                if self.arg1 == 1:
                    raise AirflowTaskTimeout("the parent ran out of time")
                await asyncio.sleep(3)

            def on_kill(self):
                if asyncio._get_running_loop() is not None:
                    raise DeadlockImminentError("send() from inside a request")
                super().on_kill()

        KILLED_ON_TIMEOUT.clear()
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}]),
                task_id="timed_out_sync_hook",
                task_concurrency=2,
                operator_class=SyncHookOnKill,
            )
            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowTaskTimeout):
                    iterable_op.execute(context=context)

        assert sorted(KILLED_ON_TIMEOUT) == [("async", 1), ("async", 2)]


class TestFailureHandedToTheRunner:
    """
    The runner classifies the outcome by exception type, so an indexed task's own exception reaches it
    whenever one decides: a fail-fast one, the only one, or the one the retry policy decides on.
    """

    @staticmethod
    def _execute(items, retry_policy=None):
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(items)
            mapped_op = MockOperator.partial(
                task_id="failing", dag=dag, retries=2, retry_policy=retry_policy, task_concurrency=1
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 1
                context["ti"].max_tries = 2
                try:
                    iterable_op.execute(context=context)
                except BaseException as exc:
                    return exc
        raise AssertionError("execute() did not raise")

    def test_sensor_timeout_is_raised_on_its_own_next_to_other_failures(self):
        from airflow.sdk.exceptions import AirflowSensorTimeout

        raised = self._execute(
            [{"raise_exception": ValueError("boom")}, {"raise_exception": AirflowSensorTimeout("poked out")}]
        )

        assert isinstance(raised, AirflowSensorTimeout)
        assert isinstance(raised.__cause__, BaseExceptionGroup)
        assert sorted(type(exc).__name__ for exc in raised.__cause__.exceptions) == [
            "AirflowSensorTimeout",
            "ValueError",
        ]

    def test_retry_policy_decides_on_the_indexed_tasks_own_exception(self):
        from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy, RetryAction, RetryRule

        policy = ExceptionRetryPolicy(rules=[RetryRule(exception=PermissionError, action=RetryAction.FAIL)])

        raised = self._execute(
            [
                {"arg1": 1},
                {"raise_exception": ValueError("boom")},
                {"raise_exception": PermissionError("no")},
            ],
            retry_policy=policy,
        )

        assert isinstance(raised, PermissionError)
        assert isinstance(raised.__cause__, BaseExceptionGroup)
        # What the runner evaluates next: the rule matches the indexed task's exception, never the group.
        assert policy.evaluate(exception=raised, try_number=1, max_tries=2).action == RetryAction.FAIL
        assert (
            policy.evaluate(exception=raised.__cause__, try_number=1, max_tries=2).action
            == RetryAction.DEFAULT
        )

    def test_several_failures_no_policy_decides_on_stay_a_group(self):
        raised = self._execute([{"raise_exception": ValueError("one")}, {"raise_exception": KeyError("two")}])

        assert isinstance(raised, BaseExceptionGroup)
        assert sorted(type(exc).__name__ for exc in raised.exceptions) == ["KeyError", "ValueError"]


class TestFingerprintCoversPartialInputsFromUpstream:
    """
    A checkpoint is honoured only for the input it was written for, and that input includes the
    ``.partial()`` kwargs an upstream task provides: clearing the upstream can change them.
    """

    @staticmethod
    def _run_twice(
        partial_value_on_retry, arg2_first="from-upstream", arg2_is_upstream=True, shape=lambda value: value
    ):
        """
        Attempt 1: indexed task 2 fails. Attempt 2 (retry or clear): indexed task 2 succeeds. Returns runs and
        results.

        ``shape`` places the upstream value in the partial kwarg: as it is, or deep in a list or mapping.
        """
        upstream_value = [arg2_first]
        runs = []
        original_execute = MockOperator.execute

        def counting_execute(self, context):
            runs.append((context["ti"].try_number, self.arg1))
            return original_execute(self, context)

        with DAG("test_dag") as dag:
            if arg2_is_upstream:
                upstream = make_xcom_arg(None)
                upstream.resolve = lambda *args, **kwargs: upstream_value[0]
                arg2 = shape(upstream)
            else:
                arg2 = arg2_first
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2, "fail_on_first_attempt": True}])
            mapped_op = MockOperator.partial(
                task_id="partial_input", dag=dag, arg2=arg2, task_concurrency=1
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with (
                mock_context(task=iterable_op) as context,
                patch.object(MockOperator, "execute", counting_execute),
            ):
                context["ti"].try_number = 1
                with pytest.raises(RuntimeError):
                    iterable_op.execute(context=context)
                upstream_value[0] = partial_value_on_retry
                iterable_op.expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
                context["ti"].try_number = 2
                results = list(iterable_op.execute(context=context))
        return runs, results

    # Where the upstream value sits in the partial kwarg, and how to read it back once rendered.
    SHAPES = {
        "top level": (lambda value: value, lambda rendered: rendered),
        "in a list": (lambda value: [value, "literal"], lambda rendered: rendered[0]),
        "two levels into a mapping": (
            lambda value: {"cfg": {"x": value, "when": "{{ ti.try_number }}"}},
            lambda rendered: rendered["cfg"]["x"],
        ),
        "in a list in a mapping": (
            lambda value: {"paths": ["literal", value]},
            lambda rendered: rendered["paths"][1],
        ),
    }

    @pytest.mark.parametrize(("shape", "read"), SHAPES.values(), ids=SHAPES.keys())
    def test_a_changed_upstream_value_runs_the_indexed_task_again(self, shape, read):
        runs, results = self._run_twice("changed-upstream", shape=shape)

        assert (2, 1) in runs
        assert [read(value[1]) for value in results] == ["changed-upstream", "changed-upstream"]

    @pytest.mark.parametrize(("shape", "read"), SHAPES.values(), ids=SHAPES.keys())
    def test_an_unchanged_upstream_value_keeps_the_checkpoint(self, shape, read):
        """A templated neighbour of the upstream value, rendered anew each attempt, does not count."""
        runs, results = self._run_twice("from-upstream", shape=shape)

        assert (2, 1) not in runs
        assert [read(value[1]) for value in results] == ["from-upstream", "from-upstream"]

    def test_a_templated_partial_value_does_not_make_checkpoints_stale(self):
        """Only upstream values count: a value rendered anew every attempt must not force a rerun."""
        runs, results = self._run_twice(None, arg2_first="{{ ti.try_number }}", arg2_is_upstream=False)

        assert (2, 1) not in runs
        assert [value[0] for value in results] == [1, 2]

    def test_partial_inputs_from_upstream_are_read_from_the_rendered_operator(self):
        upstream = make_xcom_arg(None)
        rendered = SimpleNamespace(
            arg2="rendered-arg2",
            op_kwargs={"y": "rendered-y", "z": 1, "cfg": {"x": "rendered-x", "when": 3}},
            op_args=["rendered-0", 1, ("rendered-2-0",)],
            retries=2,
        )
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([{"arg1": "a"}]))
        iterable_op.partial_kwargs = {
            "arg2": upstream,
            "op_kwargs": {"y": upstream, "z": 1, "cfg": {"x": upstream, "when": "{{ ti.try_number }}"}},
            "op_args": [upstream, 1, (upstream,)],
            "retries": 2,
        }

        inputs = fingerprint_of_a_run(iterable_op).upstream_inputs.collect(rendered)

        assert inputs == {
            "arg2": "rendered-arg2",
            "op_kwargs.y": "rendered-y",
            "op_kwargs.cfg.x": "rendered-x",
            "op_args[0]": "rendered-0",
            "op_args[2][0]": "rendered-2-0",
        }

    @pytest.mark.parametrize(
        ("retry_items", "runs_again"),
        [([1, 3], True), ([1, 2], False)],
        ids=["same-length-other-items", "same-items"],
    )
    def test_an_iterated_upstream_counts_by_its_items(self, retry_items, runs_again):
        """
        An iterated upstream hands its values as an ``XComIterable``, whose serialized form holds no item:
        cleared and run again with other items of the same length, it must still make the checkpoint stale.
        """
        # The same upstream task instance in both attempts, so the same handle: only the items differ.
        first = XComIterable(task_id="upstream", dag_id="test_dag", run_id="run", length=2)
        retry = XComIterable(task_id="upstream", dag_id="test_dag", run_id="run", length=2)
        items = {id(first): [1, 2], id(retry): retry_items}
        original_getitem = XComIterable.__getitem__

        def upstream_item(self, index):
            return items[id(self)][index] if id(self) in items else original_getitem(self, index)

        with patch.object(XComIterable, "__getitem__", upstream_item):
            runs, _ = self._run_twice(retry, arg2_first=first)

        assert ((2, 1) in runs) is runs_again

    @pytest.mark.parametrize(
        "lazy",
        [
            pytest.param(XComIterable(task_id="upstream", dag_id="d", run_id="r", length=2), id="iterated"),
            pytest.param(LazyXComSequence(xcom_arg=Mock(), ti=Mock()), id="mapped"),
        ],
    )
    def test_a_lazy_upstream_value_is_read_once_per_run_and_digested_by_its_items(self, lazy):
        """Every index of a run gets the digest the first one took; another run reads the upstream again."""
        reads = []
        items = [[1, 2]]

        def read_item(self, index):
            if index == 0:
                reads.append(1)
            return items[0][index]

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([{"arg1": "a"}]))
        iterable_op.partial_kwargs = {"arg2": make_xcom_arg(None)}
        rendered = SimpleNamespace(arg2=lazy)

        with (
            patch.object(type(lazy), "__getitem__", read_item),
            patch.object(type(lazy), "__len__", lambda self: len(items[0])),
        ):
            fingerprint = fingerprint_of_a_run(iterable_op)
            first_run = [fingerprint.upstream_inputs.collect(rendered)["arg2"] for _ in range(3)]
            items[0] = [1, 3]
            second_run = fingerprint_of_a_run(iterable_op).upstream_inputs.collect(rendered)["arg2"]

        assert len(reads) == 2
        assert len(set(first_run)) == 1
        assert isinstance(first_run[0], str)
        assert second_run != first_run[0]

    @staticmethod
    def _recorded_for(rendered_value):
        """What the fingerprint records for a partial kwarg from upstream that rendered to ``rendered_value``."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([{"arg1": "a"}]))
        iterable_op.partial_kwargs = {"arg2": make_xcom_arg(None)}
        return fingerprint_of_a_run(iterable_op).upstream_inputs.collect(
            SimpleNamespace(arg2=rendered_value)
        )["arg2"]

    COMPOSITIONS = {
        "map": lambda upstream: _MapResult(upstream, [lambda item: item * 10]),
        "zip": lambda upstream: _ZipResult([upstream, ["x", "y"]]),
        "concat": lambda upstream: _ConcatResult([upstream, ["x"]]),
    }

    @pytest.mark.parametrize("compose", COMPOSITIONS.values(), ids=COMPOSITIONS.keys())
    def test_an_upstream_behind_map_zip_or_concat_counts_by_its_items(self, compose):
        """
        ``.map()``, ``.zip()`` and ``.concat()`` render to a sequence around the iterated upstream's
        ``XComIterable``: its items make the digest, not its handle, which stays the same after a clear.
        """
        first, cleared, unchanged = (
            XComIterable(task_id="upstream", dag_id="test_dag", run_id="run", length=2) for _ in range(3)
        )
        items = {id(first): [1, 2], id(cleared): [1, 3], id(unchanged): [1, 2]}

        def upstream_item(self, index):
            return items[id(self)][index]

        with patch.object(XComIterable, "__getitem__", upstream_item):
            digests = [self._recorded_for(compose(upstream)) for upstream in (first, cleared, unchanged)]

        assert all(isinstance(digest, str) for digest in digests)
        assert digests[0] != digests[1]
        assert digests[0] == digests[2]

    @pytest.mark.parametrize(
        ("killed_while_reading", "expected_reads"),
        [(None, []), (1, [0, 1])],
        ids=["killed-before-the-digest", "killed-while-reading-an-item"],
    )
    def test_a_kill_ends_the_read_of_a_lazy_upstream_before_the_next_item(
        self, killed_while_reading, expected_reads
    ):
        """A kill stops the digest of a large upstream at the next item, and the value gets no digest."""
        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(dag, ListOfDictsExpandInput([{"arg1": "a"}]))
        iterable_op.partial_kwargs = {"arg2": make_xcom_arg(None)}
        upstream = XComIterable(task_id="upstream", dag_id="test_dag", run_id="run", length=100)
        reads = []

        def read_item(self, index):
            reads.append(index)
            if index == killed_while_reading:
                iterable_op._state.request_stop()
            return index

        if killed_while_reading is None:
            iterable_op._state.request_stop()
        with patch.object(XComIterable, "__getitem__", read_item):
            recorded = fingerprint_of_a_run(iterable_op).upstream_inputs.collect(
                SimpleNamespace(arg2=upstream)
            )

        assert recorded["arg2"] is None
        assert reads == expected_reads

    def test_a_map_callable_raising_for_one_item_still_counts_the_other_items(self):
        """
        The indexed tasks may never read that item, so its failure is not theirs: it counts as unreadable,
        and the other items still decide the digest, with a warning naming the upstream.
        """

        def refuse_two(item):
            if item == 2:
                raise ValueError("cannot map 2")
            return item

        log = Mock()
        with patch.object(IterableOperator, "log", new_callable=PropertyMock, return_value=log):
            digest = self._recorded_for(_MapResult([1, 2], [refuse_two]))

        assert isinstance(digest, str)
        assert self._recorded_for(_MapResult([1, 2], [refuse_two])) == digest
        assert self._recorded_for(_MapResult([3, 2], [refuse_two])) != digest
        log.warning.assert_called_once()
        assert log.warning.call_args.args[1:] == (1, 2, "arg2", 1)


CALLBACKS: list = []


class MockCallbackOperator(BaseOperator):
    """Operator that records which callback each of its indexed tasks gets."""

    template_fields = ("arg1",)
    arg1: Any

    def __init__(self, arg1=None, raise_exception: BaseException | None = None, **kwargs):
        kwargs["on_success_callback"] = lambda context: CALLBACKS.append(("success", self.arg1))
        kwargs["on_failure_callback"] = lambda context: CALLBACKS.append(("failure", self.arg1))
        kwargs["on_retry_callback"] = lambda context: CALLBACKS.append(("retry", self.arg1))
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.raise_exception = raise_exception

    def execute(self, context):
        if self.raise_exception is not None:
            raise self.raise_exception
        return self.arg1


class TestCallbacksFollowTheTasksFate:
    """
    A failed indexed task's failure or retry callback waits until every indexed task has run, and then says
    what happens to the task: retried, or failed for good. Success callbacks fire right away.
    """

    @staticmethod
    def _run(items, try_number=1, max_tries=2, retry_policy=None):
        CALLBACKS.clear()
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(items)
            mapped_op = MockCallbackOperator.partial(
                task_id="callbacks", dag=dag, retries=2, retry_policy=retry_policy, task_concurrency=1
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = try_number
                context["ti"].max_tries = max_tries
                try:
                    iterable_op.execute(context=context)
                except BaseException as exc:
                    return exc, list(CALLBACKS)
        return None, list(CALLBACKS)

    @pytest.mark.parametrize(
        ("try_number", "callback"), [(1, "on_retry_callback"), (3, "on_failure_callback")]
    )
    def test_a_failed_indexed_tasks_callback_gets_its_own_exception(self, try_number, callback):
        """Each indexed task's failure or retry callback finds that indexed task's error under ``exception``."""
        seen: list = []

        class RecordsException(BaseOperator):
            def __init__(self, error=None, **kwargs):
                kwargs[callback] = lambda context: seen.append(context.get("exception"))
                super().__init__(**kwargs)
                self.error = error

            def execute(self, context):
                raise self.error

        first, second = ValueError("first"), KeyError("second")
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"error": first}, {"error": second}])
            mapped_op = RecordsException.partial(
                task_id="records_exception", dag=dag, retries=2, task_concurrency=1
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = try_number
                context["ti"].max_tries = 2
                with pytest.raises(BaseExceptionGroup):
                    iterable_op.execute(context=context)

        assert seen == [first, second]

    def test_a_siblings_fail_exception_turns_every_failure_into_a_final_one(self):
        """
        A failed indexed task's retry callback waits until no sibling rules the retry out.

        A ``ValueError`` indexed task next to an ``AirflowFailException`` indexed task is reported as a
        failure: the task fails without a retry, so announcing a retry for the first indexed task would be
        wrong.
        """
        raised, fired = self._run(
            [
                {"arg1": "ok"},
                {"arg1": "value_error", "raise_exception": ValueError("x")},
                {"arg1": "fail", "raise_exception": AirflowFailException("stop")},
            ]
        )

        assert isinstance(raised, AirflowFailException)
        assert fired == [("success", "ok"), ("failure", "value_error"), ("failure", "fail")]

    def test_failures_the_task_is_retried_for_all_get_the_retry_callback(self):
        raised, fired = self._run(
            [
                {"arg1": "a", "raise_exception": ValueError("a")},
                {"arg1": "b", "raise_exception": KeyError("b")},
            ]
        )

        assert isinstance(raised, BaseExceptionGroup)
        assert fired == [("retry", "a"), ("retry", "b")]

    def test_on_the_last_attempt_every_failure_is_final(self):
        _, fired = self._run(
            [
                {"arg1": "a", "raise_exception": ValueError("a")},
                {"arg1": "b", "raise_exception": KeyError("b")},
            ],
            try_number=3,
            max_tries=2,
        )

        assert fired == [("failure", "a"), ("failure", "b")]

    def test_the_retry_policy_is_evaluated_once_per_failed_indexed_task(self):
        """
        Choosing the exception for the runner costs one evaluation per failed indexed task, and no more.

        The callbacks reuse the decision taken for the exception handed over instead of evaluating
        the policy again: a policy that calls a model (common.ai's ``LLMRetryPolicy``) pays per call
        and may answer differently each time.
        """
        from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy, RetryDecision

        class CountingPolicy(ExceptionRetryPolicy):
            calls: list = []

            def evaluate(self, exception, try_number, max_tries, context=None):
                self.calls.append(exception)
                return RetryDecision.retry()

        _, fired = self._run(
            [
                {"arg1": "a", "raise_exception": ValueError("a")},
                {"arg1": "b", "raise_exception": KeyError("b")},
                {"arg1": "c", "raise_exception": OSError("c")},
            ],
            retry_policy=CountingPolicy(rules=[]),
        )

        assert len(CountingPolicy.calls) == 3
        assert fired == [("retry", "a"), ("retry", "b"), ("retry", "c")]

    def test_an_undecided_group_is_not_evaluated_again_for_the_callbacks(self):
        """
        Failures whose decisions are all the default are handed over as a group, decided once.

        Choosing costs one evaluation per failed indexed task; the group then carries the default, so the
        callbacks follow it without evaluating the policy once more, on an exception no indexed task raised.
        """
        from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy, RetryDecision

        class DefaultPolicy(ExceptionRetryPolicy):
            calls: list = []

            def evaluate(self, exception, try_number, max_tries, context=None):
                self.calls.append(exception)
                return RetryDecision.default()

        raised, fired = self._run(
            [
                {"arg1": "a", "raise_exception": ValueError("a")},
                {"arg1": "b", "raise_exception": KeyError("b")},
                {"arg1": "c", "raise_exception": OSError("c")},
            ],
            retry_policy=DefaultPolicy(rules=[]),
        )

        assert isinstance(raised, BaseExceptionGroup)
        assert len(DefaultPolicy.calls) == 3
        assert fired == [("retry", "a"), ("retry", "b"), ("retry", "c")]

    def test_the_callbacks_follow_the_decision_taken_for_the_exception_handed_over(self):
        """
        A policy that answers differently per call cannot make the callbacks disagree with the task.

        The second indexed task's exception wins with FAIL and is handed to the runner; evaluating the policy
        once more for the callbacks would get RETRY and announce a retry the runner does not take.
        """
        from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy, RetryDecision

        class AlternatingPolicy(ExceptionRetryPolicy):
            answers = iter([RetryDecision.retry(), RetryDecision.fail("no"), RetryDecision.retry()])

            def evaluate(self, exception, try_number, max_tries, context=None):
                return next(self.answers)

        raised, fired = self._run(
            [
                {"arg1": "a", "raise_exception": ValueError("a")},
                {"arg1": "b", "raise_exception": KeyError("b")},
            ],
            retry_policy=AlternatingPolicy(rules=[]),
        )

        assert isinstance(raised, KeyError)
        assert fired == [("failure", "a"), ("failure", "b")]

    def test_a_retry_policy_that_fails_the_task_makes_every_failure_final(self):
        from airflow.sdk.definitions.retry_policy import ExceptionRetryPolicy, RetryAction, RetryRule

        policy = ExceptionRetryPolicy(rules=[RetryRule(exception=PermissionError, action=RetryAction.FAIL)])

        _, fired = self._run(
            [
                {"arg1": "value_error", "raise_exception": ValueError("x")},
                {"arg1": "denied", "raise_exception": PermissionError("no")},
            ],
            retry_policy=policy,
        )

        assert fired == [("failure", "value_error"), ("failure", "denied")]

    def test_an_item_the_operator_rejects_fails_the_task_for_every_indexed_task(self):
        """A downstream skip from an indexed task is rejected with AirflowFailException, so nothing is retried."""
        raised, fired = self._run(
            [
                {"arg1": "value_error", "raise_exception": ValueError("x")},
                {"arg1": "skipper", "raise_exception": DownstreamTasksSkipped(tasks=["downstream"])},
            ]
        )

        assert isinstance(raised, AirflowFailException)
        assert fired == [("failure", "value_error"), ("failure", "skipper")]

    def test_a_failed_indexed_tasks_callback_waits_for_the_indexed_tasks_after_it(self):
        _, fired = self._run([{"arg1": "first", "raise_exception": ValueError("x")}, {"arg1": "second"}])

        assert fired == [("success", "second"), ("retry", "first")]


class TestIterableOperatorCopy:
    """An iterated task can be deep-copied, as dag.partial_subset() does for every task it keeps."""

    @staticmethod
    def _dag():
        from airflow.sdk import task

        with DAG("copy_dag") as dag:

            @task
            def up():
                return [1, 2]

            @task
            def f(x):
                return x

            @task
            def down(values):
                return values

            down(f.iterate(x=up()))
        return dag

    def test_deepcopy_gets_its_own_state_with_no_indexed_tasks_in_flight(self):
        import copy

        iterable_op = self._dag().task_dict["f"]
        in_flight = MockOperator(task_id="in_flight")
        iterable_op._state.register(in_flight)
        iterable_op._state.request_stop()

        copied = copy.deepcopy(iterable_op)

        assert isinstance(copied, IterableOperator)
        assert copied.task_id == "f"
        assert copied._state is not iterable_op._state
        assert in_flight not in copied._state
        assert not copied._state.stop_requested()
        # The original is untouched.
        assert in_flight in iterable_op._state
        assert iterable_op._state.stop_requested()

    def test_the_copy_prepared_for_execution_gets_its_own_state(self):
        """
        ``prepare_for_execution`` copies with ``copy.copy``. A kill in one attempt must not stop the
        next one, which ``dag.test()`` prepares from the same Dag operator.
        """
        iterable_op = self._dag().task_dict["f"]

        first = iterable_op.prepare_for_execution()
        first.on_kill()
        second = iterable_op.prepare_for_execution()

        assert first._state is not iterable_op._state
        assert first._state.stop_requested()
        assert not iterable_op._state.stop_requested()
        assert second._state is not first._state
        assert not second._state.stop_requested()
        assert second._lock_for_execution

    def test_partial_subset_keeps_the_iterated_task(self):
        dag = self._dag()

        subset = dag.partial_subset("f", include_upstream=True, include_downstream=True)

        assert sorted(subset.task_dict) == ["down", "f", "up"]
        assert isinstance(subset.task_dict["f"], IterableOperator)
        assert subset.task_dict["f"] is not dag.task_dict["f"]


class TestCheckpoints:
    @staticmethod
    def _context(try_number: int, marker: dict | None = None, max_tries: int = 3):
        from airflow.sdk.execution_time.context import TaskStateStoreAccessor

        store = create_autospec(TaskStateStoreAccessor, instance=True)
        store.get.side_effect = lambda key, default=None: (
            marker if key == Checkpoints.COMPLETION_KEY else default
        )
        ti = SimpleNamespace(task_id="my_task", try_number=try_number, max_tries=max_tries)
        return {"task_state_store": store, "ti": ti}, store

    @staticmethod
    def _checkpoints(context) -> Checkpoints:
        return Checkpoints(context, round_size=1, log=Mock())

    @staticmethod
    def _completed(try_number: int, max_tries: int = 3, since: int = 0) -> dict:
        return {"completed": True, "try_number": try_number, "max_tries": max_tries, "since": since}

    def test_first_attempt_never_reads_and_marks_completed_on_exit(self):
        context, store = self._context(try_number=1)

        with self._checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is False

        store.get.assert_not_called()
        store.delete.assert_not_called()
        store.set.assert_called_once_with("_iterable_completed", self._completed(1))

    def test_retry_without_marker_trusts_checkpoints(self):
        context, store = self._context(try_number=2, marker=None)

        with self._checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is True

        # The batches first, until the first one missing, then the marker.
        assert store.get.call_args_list == [call("_iterable_batch_0"), call("_iterable_completed")]
        store.delete.assert_not_called()
        store.set.assert_called_once_with("_iterable_completed", self._completed(2))

    def test_failed_attempt_leaves_no_marker(self):
        context, store = self._context(try_number=1)

        with pytest.raises(RuntimeError, match="indexed task failures"):
            with self._checkpoints(context):
                raise RuntimeError("indexed task failures")

        store.set.assert_not_called()

    def test_attempt_where_every_iteration_skipped_marks_completed(self):
        """The task ends ``SKIPPED``; a clear of it must run every iteration again."""
        context, store = self._context(try_number=1)

        with pytest.raises(AirflowSkipException):
            with self._checkpoints(context):
                raise AirflowSkipException("every iteration skipped")

        store.set.assert_called_once_with("_iterable_completed", self._completed(1))

    @pytest.mark.parametrize(
        "marker",
        [
            pytest.param(
                {"completed": True, "try_number": 1, "max_tries": 3, "since": 0}, id="max-tries-raised"
            ),
            pytest.param(
                {"completed": True, "try_number": 1, "max_tries": 5, "since": 0}, id="max-tries-lowered"
            ),
            pytest.param({"completed": True, "try_number": 1}, id="marker-without-max-tries"),
        ],
    )
    def test_rerun_after_clear_records_where_it_starts_and_ignores_checkpoints(self, marker):
        """A clear changes ``max_tries`` (``try_number + retries``), up or down when the retries changed."""
        context, store = self._context(try_number=2, marker=marker, max_tries=4)

        with self._checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is False
            store.set.assert_called_once_with("_iterable_completed", {"completed": False, "since": 2})

        store.set.assert_called_with("_iterable_completed", self._completed(2, max_tries=4, since=2))

    @pytest.mark.parametrize("since", [0, 2])
    def test_retry_after_a_completed_attempt_failed_late_resumes_from_the_checkpoints(self, since):
        """
        Every indexed task ran and the marker was written, but the task failed afterwards (the worker died,
        ``post_execute`` or the result push failed): the retry keeps ``max_tries``, so it is no clear,
        and it resumes from the checkpoints the completed attempt could trust.
        """
        marker = self._completed(3, max_tries=3, since=since)
        context, store = self._context(try_number=4, marker=marker, max_tries=3)

        with self._checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is True
            assert checkpoints.since == since
            store.set.assert_not_called()

        store.set.assert_called_once_with("_iterable_completed", self._completed(4, max_tries=3, since=since))

    def test_failed_rerun_leaves_where_it_started(self):
        context, store = self._context(try_number=2, marker={"completed": True, "try_number": 1})

        with pytest.raises(RuntimeError):
            with self._checkpoints(context):
                raise RuntimeError("indexed task failed")

        store.set.assert_called_once_with("_iterable_completed", {"completed": False, "since": 2})

    def test_attempt_after_a_failed_rerun_trusts_the_checkpoints_written_since(self):
        context, store = self._context(try_number=3, marker={"completed": False, "since": 2})

        with self._checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is True
            assert checkpoints.since == 2
            store.set.assert_not_called()

        store.set.assert_called_once_with("_iterable_completed", self._completed(3, since=2))


class TestCheckpointBatches:
    """The indexed tasks' outcomes are written a round of ``task_concurrency`` at a time, one key per round."""

    @staticmethod
    def _batches(store, round_size: int) -> CheckpointBatches:
        return CheckpointBatches(store, round_size=round_size, log=Mock())

    @staticmethod
    def _state(status: str = "success", try_number: int = 1) -> IndexedTaskState:
        return IndexedTaskState(status=TaskInstanceState(status), fingerprint="digest", try_number=try_number)

    @pytest.mark.parametrize(
        ("length", "round_size", "writes"), [(10, 3, 4), (9, 3, 3), (5, 1, 5), (2, 4, 1)]
    )
    def test_outcomes_are_written_a_round_at_a_time_each_once(self, length, round_size, writes):
        """One write per round of concurrency and one for the rest, instead of one per indexed task."""
        store = MockTaskStateStoreAccessor()
        batches = self._batches(store, round_size)

        async def finish_all():
            await asyncio.gather(*(batches.add(index, self._state()) for index in range(length)))

        asyncio.run(finish_all())
        batches.flush()

        assert sorted(store._data) == sorted(f"_iterable_batch_{n}" for n in range(writes))
        assert sorted(int(index) for batch in store._data.values() for index in batch) == list(range(length))

    def test_a_round_is_written_once_when_several_indexed_tasks_fill_it_at_once(self):
        """The indexed tasks that finish while the round is written wait for the lock and add to the next one."""
        writes = []

        class SlowStore(MockTaskStateStoreAccessor):
            async def aset(self, key, value, **kwargs):
                writes.append(sorted(value))
                await asyncio.sleep(0)
                self.set(key, value)

        batches = self._batches(SlowStore(), 2)

        async def finish_all():
            await asyncio.gather(*(batches.add(index, self._state()) for index in range(4)))

        asyncio.run(finish_all())
        batches.flush()

        # Two rounds, two writes: the indexed task that finds the round written meanwhile writes nothing.
        assert writes == [["0", "1"], ["2", "3"]]

    def test_a_failed_write_keeps_its_outcomes_for_the_next_one(self):
        class FailingOnce(MockTaskStateStoreAccessor):
            failed = False

            async def aset(self, key, value, **kwargs):
                if not self.failed:
                    self.failed = True
                    raise ConnectionError("API server busy")
                self.set(key, value)

        store = FailingOnce()
        batches = self._batches(store, 2)

        async def finish_one_by_one():
            for index in range(4):
                await batches.add(index, self._state())

        asyncio.run(finish_one_by_one())
        batches.flush()

        assert list(store._data) == ["_iterable_batch_0"]
        assert sorted(int(index) for index in store._data["_iterable_batch_0"]) == [0, 1, 2, 3]
        batches._log.warning.assert_called_once()

    def test_a_failed_final_write_is_logged_and_does_not_fail_the_task(self):
        class Unavailable(MockTaskStateStoreAccessor):
            def set(self, key, value, **kwargs):
                raise ConnectionError("API server gone")

        batches = self._batches(Unavailable(), 4)
        asyncio.run(batches.add(0, self._state()))

        batches.flush()

        batches._log.exception.assert_called_once()

    def test_a_write_interrupted_by_a_base_exception_keeps_its_outcomes(self):
        """The parent's timeout or a cancellation goes on, and the round is still written at the end."""

        class TimedOut(MockTaskStateStoreAccessor):
            async def aset(self, key, value, **kwargs):
                raise AirflowTaskTimeout("the parent's execution_timeout")

        store = TimedOut()
        batches = self._batches(store, 1)

        with pytest.raises(AirflowTaskTimeout):
            asyncio.run(batches.add(0, self._state()))
        batches.flush()

        assert list(store._data["_iterable_batch_0"]) == ["0"]

    def test_reading_back_takes_the_latest_entry_and_stops_at_the_first_missing_batch(self):
        store = MockTaskStateStoreAccessor()
        store._data.update(
            {
                "_iterable_batch_0": {
                    "0": self._state("up_for_retry").serialize(),
                    "1": self._state().serialize(),
                },
                "_iterable_batch_1": {"0": self._state(try_number=2).serialize()},
                # After a gap: never read, and the next write goes to _iterable_batch_2.
                "_iterable_batch_3": {"5": self._state().serialize()},
            }
        )
        earlier = EarlierCheckpoints()
        batches = self._batches(store, 4)

        earlier.read(store)
        batches.start_at(earlier.count)
        asyncio.run(batches.add(7, self._state()))
        batches.flush()

        assert earlier.count == 2
        assert earlier.state_of(0).try_number == 2
        assert earlier.state_of(1) == self._state()
        assert earlier.state_of(5) is None
        assert list(store._data["_iterable_batch_2"]) == ["7"]

    def test_a_value_that_is_no_batch_ends_the_read(self):
        store = MockTaskStateStoreAccessor()
        store._data.update({"_iterable_batch_0": ["not", "a", "batch"], "_iterable_batch_1": {"0": {}}})
        earlier = EarlierCheckpoints()

        earlier.read(store)

        assert earlier.count == 0

    def test_the_rest_of_the_round_is_written_before_the_completion_marker(self):
        """The marker says every outcome is durable, so it comes after the last batch."""
        written = []

        class Recording(MockTaskStateStoreAccessor):
            def set(self, key, value, **kwargs):
                written.append(key)
                super().set(key, value, **kwargs)

        context = {
            "task_state_store": Recording(),
            "ti": SimpleNamespace(task_id="my_task", try_number=1, max_tries=0),
        }
        with Checkpoints(context, round_size=4, log=Mock()) as checkpoints:
            asyncio.run(checkpoints.record(SimpleNamespace(index=0), self._state()))

        assert written == ["_iterable_batch_0", "_iterable_completed"]

    def test_a_retry_writes_a_round_at_a_time_and_runs_only_what_failed(self):
        """
        Five items, two at a time: the first attempt writes its outcomes as three batches instead of five
        keys, and the retry runs only the item that failed, replays the others from the batches and writes
        its own batch after them.
        """
        ran = []

        def execute(self, context):
            ran.append(self.arg1)
            if self.arg1 == 3 and context["ti"].try_number == 1:
                raise RuntimeError("item 3 failed")
            return self.arg1

        with DAG("test_dag") as dag:
            iterable_op = create_iterable_operator(
                dag,
                ListOfDictsExpandInput([{"arg1": index} for index in range(5)]),
                task_id="rounds",
                task_concurrency=2,
                retries=1,
            )

            with mock_context(task=iterable_op) as context, patch.object(MockOperator, "execute", execute):
                store = context["task_state_store"]
                context["ti"].try_number = 1
                with pytest.raises(RuntimeError, match="item 3 failed"):
                    iterable_op.execute(context=context)
                first_attempt_keys = sorted(store._data)

                ran.clear()
                context["ti"].try_number = 2
                iterable_op.execute(context=context)

        assert first_attempt_keys == ["_iterable_batch_0", "_iterable_batch_1", "_iterable_batch_2"]
        assert ran == [3]
        assert store._data["_iterable_batch_3"]["3"]["status"] == "success"
        assert store._data["_iterable_completed"]["completed"] is True


def test_deserialization_allowed_values_are_those_of_the_iterated_operator():
    """Every indexed task pushes what the iterated operator declares, so the iterated task reports the same."""

    class DeclaringOperator(BaseOperator):
        deserialization_allowed_class_fields = ("output_type",)

        def __init__(self, *, output_type=None, value=None, **kwargs):
            super().__init__(**kwargs)
            self.output_type = output_type
            self.value = value

    with DAG("declaring_dag"):
        iterated = DeclaringOperator.partial(task_id="iterated", output_type=int).iterate(value=[1, 2])

    assert isinstance(iterated, IterableOperator)
    assert iterated.deserialization_allowed_values() == [int]


class TestKeepsFailureToItsItem:
    """The rule an indexed task's steps share: what a step raises is that indexed task's outcome."""

    class Steps:
        """Stands in for IndexedTaskLifecycle: the decorator only needs the ``task`` its steps are of."""

        def __init__(self, task):
            self.task = task

        @keeps_failure_to_its_indexed_task
        async def step(self, exception=None, *, result="done"):
            """Raise ``exception`` when given, else return the outcome."""
            if exception is not None:
                raise exception
            return IndexedTaskOutcome(self.task, result)

    @pytest.mark.parametrize(
        "exception",
        [RuntimeError("store down"), AirflowSkipException("skip"), DeadlockImminentError("re-entry")],
        ids=["exception", "airflow-exception", "base-exception"],
    )
    def test_what_the_step_raises_is_returned_as_the_outcome(self, exception):
        task = object()

        assert asyncio.run(self.Steps(task).step(exception)) == IndexedTaskOutcome(task, exception=exception)

    @pytest.mark.parametrize(
        "exception",
        [asyncio.CancelledError(), AirflowTaskTimeout("parent timeout")],
        ids=["cancelled", "parent-timeout"],
    )
    def test_a_cancellation_or_the_parents_timeout_goes_on(self, exception):
        with pytest.raises(type(exception)):
            asyncio.run(self.Steps(object()).step(exception))

    def test_the_outcome_of_a_step_that_succeeds_is_kept(self):
        task = object()

        assert asyncio.run(self.Steps(task).step(result="pushed")) == IndexedTaskOutcome(task, "pushed")

    def test_the_step_keeps_its_name_and_docstring(self):
        assert self.Steps.step.__name__ == "step"
        assert self.Steps.step.__doc__ == "Raise ``exception`` when given, else return the outcome."


class TestIterateOverALoopTask:
    """An iterated task over the output of a task in a loop, in the same loop: outside it, it is refused."""

    @staticmethod
    def _input_of(iterate_inside_the_loop: bool):
        """The iterated task and the XComArg of its input, over ``body.produce`` of a two-pass loop."""
        from airflow.sdk import task, task_group
        from airflow.sdk.definitions._internal.loop import create_loop

        @task
        def consume(value):
            return value

        @task_group
        def body():
            @task
            def produce():
                return [1, 2]

            produced = produce()
            if iterate_inside_the_loop:
                consume.iterate(value=produced)

        with DAG("iterate_loop_output", schedule=None) as dag:
            create_loop(body, max_iterations=2)
            if not iterate_inside_the_loop:
                consume.iterate(value=dag.get_task("body.produce").output)

        iterated = dag.get_task("body.consume" if iterate_inside_the_loop else "consume")
        assert isinstance(iterated, IterableOperator)
        return iterated, iterated.expand_input.value["value"]

    def test_inside_the_loop_it_reads_the_pass_it_runs_in(self):
        """The producer's value of the same pass, so each index is one of that pass's items."""
        iterated, xcom_arg = self._input_of(iterate_inside_the_loop=True)

        assert xcom_arg._resolve_map_indexes(SimpleNamespace(task=iterated)) is None

    def test_it_expands_over_the_upstream_of_its_input_without_being_mapped_over_it(self):
        """
        The loop check sees the upstream it iterates over; the scheduler does not count its items,
        so it is no mapped dependency of the iterated task and gets no mapped dependant.
        """
        iterated, _ = self._input_of(iterate_inside_the_loop=True)
        produce = iterated.dag.get_task("body.produce")

        assert list(iterated.iter_expansion_sources()) == [produce]
        assert not hasattr(iterated, "iter_mapped_dependencies")
        assert list(produce.iter_mapped_dependants()) == []

    def test_each_pass_checkpoints_under_its_own_task_instance(
        self, create_runtime_ti, mock_supervisor_comms
    ):
        """
        A pass of a loop is a task instance of its own, and an indexed task checkpoints under the one
        it runs in: the API server keys the task state by that task instance's pass, so the next pass
        finds no checkpoint of this one and runs every index again.
        """
        from airflow.sdk.execution_time.comms import SetTaskStateStore

        iterated, _ = self._input_of(iterate_inside_the_loop=True)
        written = []

        def send(message):
            if isinstance(message, SetTaskStateStore):
                written.append(message.ti_id)

        mock_supervisor_comms.send.side_effect = send
        passes = [create_runtime_ti(task=iterated) for _ in range(2)]
        for ti in passes:
            run = run_of(iterated, ti.get_template_context())
            indexed = run.unmapper.unmap(0, {"op_kwargs": {"value": 1}})
            asyncio.run(run.checkpoints.record(indexed, IndexedTaskState(status=TaskInstanceState.SUCCESS)))
            # The round is not full: what is left is written once the run's loop closed.
            run.checkpoints.batches.flush()

        assert written == [ti.id for ti in passes]
        assert written[0] != written[1]
