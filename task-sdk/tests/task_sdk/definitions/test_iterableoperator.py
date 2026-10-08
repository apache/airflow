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
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import timedelta
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, create_autospec, patch

try:
    # Python 3.11+
    BaseExceptionGroup
except NameError:
    from exceptiongroup import BaseExceptionGroup

import pytest
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
from airflow.sdk.bases.operator import event_loop
from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_RETRIES
from airflow.sdk.definitions._internal.expandinput import (
    DictOfListsExpandInput,
    ExpandInput,
    ListOfDictsExpandInput,
    Resolved,
)
from airflow.sdk.definitions.context import clone_context
from airflow.sdk.definitions.iterableoperator import Checkpoints, IterableOperator, _fingerprint
from airflow.sdk.exceptions import (
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSkipException,
    AirflowTaskTimeout,
    DagRunTriggerException,
    DownstreamTasksSkipped,
    TaskDeferred,
)
from airflow.sdk.execution_time.comms import DeadlockImminentError
from airflow.sdk.execution_time.context import InletEventsAccessors, OutletEventAccessors
from airflow.sdk.execution_time.executor import AsyncAwareExecutor
from airflow.sdk.execution_time.task_runner import (
    IndexedTaskRunner,
    IndexedTaskState,
    RuntimeTaskInstance,
)
from airflow.sdk.execution_time.xcom import XCom

from tests_common.test_utils.mock_context import mock_context as _mock_context_base

if TYPE_CHECKING:
    from airflow.sdk.definitions._internal.expandinput import ExpandInput
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    from tests_common.test_utils.compat import Context


class MockTaskStateStoreAccessor:
    """Minimal in-memory stand-in for ``TaskStateStoreAccessor``, exposing only the async
    ``aget``/``aset`` methods used by ``IterableOperator`` to checkpoint per-index sub-task
    progress (see ``IterableOperator._run_task``), plus the sync ``get``/``set``/``delete`` used for
    the completion marker (see ``IterableOperator._run_tasks``)."""

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

    with (
        patch.object(XCom, "set", classmethod(_set)),
        patch.object(XCom, "aset", classmethod(_aset)),
        patch.object(XCom, "get_one", classmethod(_get_one)),
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
    IterableOperator merges/replays per-sub-task outlet events (see ``_run_task``)."""

    template_fields = ()

    def __init__(self, extra_value: str = "v", **kwargs):
        super().__init__(**kwargs)
        self.extra_value = extra_value

    def execute(self, context):
        context["outlet_events"][Asset(name="a", uri="s3://bucket/a")].extra["value"] = self.extra_value
        return "done"


class MockOnKillOperator(BaseOperator):
    """Operator that records whether ``on_kill()`` was called on it, used to test that
    IterableOperator.on_kill() propagates to currently in-flight sub-tasks (see ``on_kill``)."""

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


class MockSlowAsyncOperator(BaseAsyncOperator):
    """Async operator that sleeps longer than any reasonable execution_timeout."""

    template_fields = ()

    async def aexecute(self, context):
        await asyncio.sleep(60)
        return "should_not_reach"


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


class MockAttemptOperator(BaseOperator):
    """
    Operator whose result tells which attempt produced it.

    For the values in ``times_out_on`` the parent's execution timeout strikes instead, which ends
    the whole iteration there and leaves the items after it unreached, as a crash would.
    """

    template_fields = ("arg1",)
    times_out_on: set = set()

    def __init__(self, arg1=None, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1

    def execute(self, context):
        if self.arg1 in self.times_out_on:
            raise AirflowTaskTimeout("the iteration ran out of time")
        return f"{self.arg1}@attempt{context['ti'].try_number}"


FIRED_CALLBACKS: list = []


class MockCallbackTimeoutOperator(BaseOperator):
    """Operator in which the parent's execution timeout strikes; records which callback fired."""

    template_fields = ()

    def __init__(self, **kwargs):
        kwargs["on_failure_callback"] = lambda context: FIRED_CALLBACKS.append("failure")
        kwargs["on_retry_callback"] = lambda context: FIRED_CALLBACKS.append("retry")
        super().__init__(**kwargs)

    def execute(self, context):
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


class MockClearingStateStoreOperator(BaseOperator):
    template_fields = ()

    def execute(self, context):
        context["task_state_store"].clear()


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
            ({"a": 1}, [{"a": 1}]),
            ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}]),
            ({"a": "hello"}, [{"a": "hello"}]),
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
        wrapped operator onto its own DAG node, not just onto the generated sub-tasks."""
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
        ``_get_unmap_kwargs``) to apply the same flags to each generated sub-task."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            mapped_op = create_mapped_operator(dag, expand_input)
            # unmap() only ever applies these three flags to sub-tasks by reading them off
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
        """_xcom_push awaits task.axcom_push with the default XCom return key."""
        from unittest import mock

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(dag, expand_input)

        task = mock.MagicMock()
        task.axcom_push = mock.AsyncMock()
        task.task_id = "my_task"
        task.index = 0

        asyncio.run(iterable_op.axcom_push(task=task, value="result_value"))

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
        assert all(f"_iterable_{index}" in store for index in range(3))

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
        Once every sub-task index has succeeded, ``_run_tasks`` writes a single completion marker
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
                assert store["_iterable_0"]["status"] == "success"
                assert store["_iterable_1"]["status"] == "success"

    def test_execute_reruns_every_index_after_a_clear_that_follows_success(self):
        """
        A manual clear does not reset the parent TI's ``try_number`` (see ``clear_task_instances``),
        so the next attempt looks like a retry. The completion marker left by the previous fully
        successful run tells ``_run_tasks`` to ignore the stale ``SUCCESS`` checkpoints and run every
        index again, recording first where the rerun starts so a crash mid-rerun resumes from the
        new checkpoints only.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="exec_rerun")

            with mock_context(task=iterable_op) as context:
                context["ti"].try_number = 2
                store = context["task_state_store"]
                store._data["_iterable_completed"] = {"completed": True, "try_number": 1}
                for index in (0, 1):
                    store._data[f"_iterable_{index}"] = IndexedTaskState(
                        status=TaskInstanceState.SUCCESS, result="stale"
                    ).serialize()

                materialized = list(iterable_op.execute(context=context))

                assert materialized == [(1, None, None), (2, None, None)]
                assert IndexedTaskState.deserialize(store["_iterable_0"]).result == (1, None, None)
                assert store["_iterable_completed"] == {"completed": True, "try_number": 2}

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

                # Cleared; the rerun finishes the first item and stops at the second.
                context["ti"].try_number = 2
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
                store._data["_iterable_0"] = IndexedTaskState(
                    status=TaskInstanceState.SUCCESS,
                    result="from_checkpoint",
                    fingerprint=_fingerprint({"arg1": 1}),
                ).serialize()
                store._data["_iterable_1"] = IndexedTaskState(
                    status=TaskInstanceState.UP_FOR_RETRY
                ).serialize()

                materialized = list(iterable_op.execute(context=context))

                assert materialized == ["from_checkpoint", (2, None, None)]
                assert store["_iterable_1"]["status"] == "success"
                assert store["_iterable_completed"]["completed"] is True

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
                store._data["_iterable_0"] = IndexedTaskState(
                    status=TaskInstanceState.SUCCESS,
                    result="from_the_old_item",
                    fingerprint=_fingerprint({"arg1": "old"}),
                ).serialize()
                store._data["_iterable_1"] = IndexedTaskState(
                    status=TaskInstanceState.SUCCESS,
                    result="from_checkpoint",
                    fingerprint=_fingerprint({"arg1": 2}),
                ).serialize()

                materialized = list(iterable_op.execute(context=context))

                assert materialized == [("new", None, None), "from_checkpoint"]
                assert store["_iterable_0"]["fingerprint"] == _fingerprint({"arg1": "new"})

    def test_fingerprint(self):
        """The digest follows the content, not the key order, and gives up on what serde cannot serialize."""
        assert _fingerprint({"a": 1, "b": [1, 2]}) == _fingerprint({"b": [1, 2], "a": 1})
        assert _fingerprint({"a": 1}) != _fingerprint({"a": 2})
        assert _fingerprint({"a": object()}) is None

    def test_execute_does_not_leak_unmapped_operator_into_parent_context(self):
        """
        Regression test: unmapping a sub-task must not mutate the parent context's own `ti`.

        ``context_update_for_unmapped`` sets ``context["ti"].task = task`` in place. Since
        ``context.copy()`` is only a shallow copy, ``context["ti"]`` in the copy is the *same*
        object as the parent's. If ``_create_task`` rendered the unmapped sub-operator against a
        context still carrying the parent's `ti`, the parent's `ti.task` would end up pointing at
        whichever sub-task was unmapped last, corrupting anything the runner reads off `ti.task`
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

    def test_execute_resolves_and_reads_the_expand_input_on_the_running_loop(self):
        """
        Regression test for the frozen IterableOperator: sub-task inputs were pulled from the main
        thread between two runs of the event loop, where a blocking supervisor call deadlocked with
        the ``asend`` of a sub-task parked mid-call. The input must be resolved with ``aresolve``
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

    def test_execute_renders_template_fields_off_the_loop_thread(self):
        """
        Rendering a sub-task's template fields may call the supervisor synchronously: an XComArg in
        a partial kwarg resolves with ``resolve``, ``{{ var.value.x }}`` with ``Variable.get``. On
        the loop thread that call raises ``DeadlockImminentError`` whenever a sibling's ``asend`` is
        in flight, so the rendering has to happen in a worker thread.
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

    def test_execute_with_do_xcom_push_false(self):
        """With do_xcom_push=False no return_value_<index> XCom is pushed for any sub-task."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="no_xcom_push", do_xcom_push=False
            )

            with (
                mock_context(task=iterable_op) as context,
                patch.object(
                    IterableOperator, "axcom_push", new=AsyncMock(spec=IterableOperator.axcom_push)
                ) as axcom_push,
            ):
                result = iterable_op.execute(context=context)

                assert result is None
                axcom_push.assert_not_awaited()

    def test_execute_does_not_push_xcom_for_none_results(self):
        """A sub-task returning None has nothing to push, even when do_xcom_push is True."""

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
                    IterableOperator, "axcom_push", new=AsyncMock(spec=IterableOperator.axcom_push)
                ) as axcom_push,
            ):
                iterable_op.execute(context=context)

                axcom_push.assert_not_awaited()

    def test_execute_with_failed_tasks_raises_regardless_of_retries(self):
        """
        Test executing IterableOperator where a sub-task fails.

        This test verifies that:
        1. Tasks with fail_on_first_attempt=True raise an exception on first attempt
        2. IterableOperator no longer retries failed sub-tasks in-process — retries (if any) are
           handled by Airflow retrying the whole IterableOperator task instance
        3. The failing sub-task's own exception is raised, regardless of whether the wrapped operator
           has retries configured (one failure is not wrapped in a group, see _failure_for_the_runner)
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

    def test_execute_all_sub_tasks_skipped_raises_single_skip_exception(self):
        """When every sub-task raises AirflowSkipException, IterableOperator must re-raise a single
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
        """A skipped sub-task is not a failure, so the group holds what failed and nothing else."""
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

        # A skipped sub-task is not a failure: the only failure is raised on its own.
        assert raised.value.__cause__ is None

    def test_execute_skip_next_to_successes_succeeds(self):
        """
        A sub-task that skips does not fail the task, as a skipped mapped task instance would not:
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
                assert store["_iterable_0"]["status"] == "skipped"
                assert store["_iterable_1"]["status"] == "success"
                assert store["_iterable_completed"]["completed"] is True

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

                # Cleared: the next attempt runs on the same try_number sequence, now with work to do.
                context["ti"].try_number = 2
                with patch.object(MockOperator, "execute", autospec=True, return_value="done") as execute:
                    result = iterable_op.execute(context=context)

                assert execute.call_count == 2
                assert len(result) == 2

    @pytest.mark.asyncio
    async def test_run_task_does_not_rerun_a_sub_task_skipped_on_a_previous_attempt(self):
        """On a retry a ``SKIPPED`` checkpoint is honoured like a ``SUCCESS`` one: the sub-task stays skipped."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="skipped_before")

            with mock_context(task=iterable_op) as context:
                task = iterable_op._create_task(
                    context=context,
                    index=0,
                    mapped_kwargs={"raise_exception": RuntimeError("must not run again")},
                    jinja_env=iterable_op.get_template_env(dag=dag),
                )
                await context["task_state_store"].aset(
                    task.state_key,
                    IndexedTaskState(
                        status=TaskInstanceState.SKIPPED, fingerprint=task.input_fingerprint
                    ).serialize(),
                )

                with AsyncAwareExecutor(loop=asyncio.get_running_loop(), max_workers=1) as executor:
                    _, result, raised = await iterable_op._run_task(
                        executor, context, task, trust_checkpoints=True
                    )

        assert result is None
        assert isinstance(raised, AirflowSkipException)

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

                checkpoint = context["task_state_store"]["_iterable_0"]

        assert checkpoint["status"] == "up_for_retry"
        assert _fingerprint(failing) is not None
        assert checkpoint["fingerprint"] == _fingerprint(failing)
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
        When pushing an item's result fails after its SUCCESS checkpoint was written, the checkpoint
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
                    IterableOperator, "axcom_push", side_effect=RuntimeError("xcom backend down")
                ):
                    with pytest.raises(RuntimeError, match="xcom backend down"):
                        iterable_op.execute(context=context)
                checkpoint_after_failure = context["task_state_store"]["_iterable_0"]["status"]

                context["ti"].try_number = 2
                result = iterable_op.execute(context=context)
                pushed = list(result)

        assert checkpoint_after_failure == "success"
        assert runs == [1]
        assert pushed == [(1, None, None)]

    def test_extra_xcoms_are_checkpointed_once_and_pushed_again_when_a_retry_skips_the_item(self):
        """
        The runner deletes every XCom before a retry. An item skipped because it already succeeded
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
                checkpoint = store["_iterable_0"]

                # The retry: the failing item succeeds now; the finished one must not run again.
                pushed = []

                async def recording_aset(cls, key, value, **kwargs):
                    pushed.append((key, value))

                iterable_op.expand_input = ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b"}])
                context["ti"].try_number = 2
                with patch.object(XCom, "aset", classmethod(recording_aset)):
                    iterable_op.execute(context=context)

        assert checkpoint["status"] == "success"
        assert checkpoint["xcoms"] == {"foo": "foo-of-a"}
        assert "return_value" not in checkpoint["xcoms"]
        assert MockPushingOperator.executed == ["a", "b", "b"]
        assert ("foo_0", "foo-of-a") in pushed
        assert ("return_value_0", "a") in pushed

    def test_an_item_pushing_many_keys_writes_one_checkpoint(self):
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
                original_aset = store.aset

                async def counting_aset(key, value, **kwargs):
                    writes.append(key)
                    await original_aset(key, value, **kwargs)

                store.aset = counting_aset
                iterable_op.execute(context=context)

        assert writes == ["_iterable_0"]
        assert store["_iterable_0"]["xcoms"] == {f"key{key}": key for key in range(5)}

    def test_an_async_items_extra_xcoms_are_pushed_again_on_retry(self):
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

                async def recording_aset(cls, key, value, **kwargs):
                    pushed.append((key, value))

                iterable_op.expand_input = ListOfDictsExpandInput([{"arg1": "a"}, {"arg1": "b"}])
                context["ti"].try_number = 2
                with patch.object(XCom, "aset", classmethod(recording_aset)):
                    iterable_op.execute(context=context)

        assert AsyncPushingOperator.executed == ["a", "b", "b"]
        assert ("bar_0", "bar-of-a") in pushed

    def test_execute_failed_attempt_leaves_no_completion_marker_so_retry_resumes(self):
        """
        Regression test: an attempt that fails must not write the completion marker.

        The marker tells a retry apart from a rerun after a manual clear: when it is present, every
        checkpoint is ignored and every index runs again. Raising the collected sub-task failures
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
                assert store["_iterable_0"]["status"] == "success"
                assert store["_iterable_1"]["status"] == "up_for_retry"

                # The retry: the failing item succeeds now; index 0 must be replayed, not re-run.
                context["ti"].try_number = 2
                iterable_op.expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
                with patch.object(
                    IndexedTaskRunner, "run", autospec=True, side_effect=IndexedTaskRunner.run
                ) as run:
                    materialized = list(iterable_op.execute(context=context))

                assert materialized == [(1, None, None), (2, None, None)]
                assert run.call_count == 1
                assert run.call_args.args[0].task_index == 1  # only the failed index runs again
                assert store["_iterable_completed"] == {"completed": True, "try_number": 2}

    def test_execute_fail_exception_re_raised_directly_without_retry(self):
        """A sub-task that raises AirflowFailException must be re-raised directly (not wrapped in a
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
        ShortCircuitOperator (DownstreamTasksSkipped) are not supported inside IterableOperator: a
        sub-task index has no DAG run or downstream tasks of its own for the trigger/skip to apply
        to, so this must fail the whole IterableOperator immediately with a clear error."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"raise_exception": raised_exception}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="unsupported_exception")

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowFailException, match="not supported inside IterableOperator"):
                    iterable_op.execute(context=context)

    def test_execute_rejects_reschedule_exception(self):
        """A reschedule-mode sensor (AirflowRescheduleException) is not supported inside
        IterableOperator: the sub-task's index has no task instance of its own to reschedule, so this
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
    async def test_run_task_skips_sub_task_already_checkpointed_as_succeeded(self):
        """
        When the task_state_store already records a sub-task index as succeeded (e.g. because
        Airflow retried the whole IterableOperator after a previous partial failure), ``_run_task``
        must skip re-executing that sub-task entirely.
        """
        from unittest import mock

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1, "arg2": 10}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="checkpoint_skip")

            with mock_context(task=iterable_op) as context:
                jinja_env = iterable_op.get_template_env(dag=dag)
                task = iterable_op._create_task(
                    context=context, index=0, mapped_kwargs={"arg1": 1, "arg2": 10}, jinja_env=jinja_env
                )
                task.try_number = 2  # checkpoint is only consulted from the second attempt onwards
                await context["task_state_store"].aset(
                    task.state_key,
                    IndexedTaskState(
                        status=TaskInstanceState.SUCCESS, fingerprint=task.input_fingerprint
                    ).serialize(),
                )

                executor = mock.MagicMock()
                _, result, raised = await iterable_op._run_task(
                    executor, context, task, trust_checkpoints=True
                )

                assert result is None
                assert raised is None
                executor.run_sync.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_task_reruns_and_checkpoints_success_after_up_for_retry_state(self):
        """
        A sub-task whose checkpoint records ``UP_FOR_RETRY`` (left behind by a previous failed or
        crashed attempt) is re-executed rather than skipped — only a ``SUCCESS`` checkpoint causes
        ``_run_task`` to skip re-execution — and a new ``SUCCESS`` checkpoint recording its result is
        stored once it completes.
        """
        from airflow.sdk.execution_time.executor import AsyncAwareExecutor

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1, "arg2": 10}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="checkpoint_pending")

            with mock_context(task=iterable_op) as context:
                jinja_env = iterable_op.get_template_env(dag=dag)
                task = iterable_op._create_task(
                    context=context, index=0, mapped_kwargs={"arg1": 1, "arg2": 10}, jinja_env=jinja_env
                )
                task.try_number = 2  # checkpoint is only consulted from the second attempt onwards
                await context["task_state_store"].aset(
                    task.state_key,
                    IndexedTaskState(status=TaskInstanceState.UP_FOR_RETRY).serialize(),
                )

                with AsyncAwareExecutor(loop=asyncio.get_running_loop(), max_workers=1) as executor:
                    result_task, result, raised = await iterable_op._run_task(
                        executor, context, task, trust_checkpoints=True
                    )

                assert raised is None
                assert result == (1, 10, None)
                assert (
                    result_task.try_number == 2
                )  # try_number is inherited from the parent TI, never mutated
                store = context["task_state_store"]
                assert (
                    store[task.state_key]
                    == IndexedTaskState(
                        status=TaskInstanceState.SUCCESS,
                        result=result,
                        fingerprint=task.input_fingerprint,
                        try_number=2,
                    ).serialize()
                )

    @pytest.mark.asyncio
    async def test_run_task_merges_outlet_events_into_shared_context_on_success(self):
        """A sub-task's outlet asset events must be visible in the IterableOperator's own
        ``context["outlet_events"]`` once it succeeds, so they get serialized along with the
        parent task's own outlet events when the whole IterableOperator finishes (see kaxil's
        comment on outlet-event handling in ``_run_task``)."""
        from airflow.sdk.execution_time.executor import AsyncAwareExecutor

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="outlet_merge", operator_class=MockOutletEventOperator
            )

            with mock_context(task=iterable_op) as context:
                jinja_env = iterable_op.get_template_env(dag=dag)
                task = iterable_op._create_task(
                    context=context, index=0, mapped_kwargs={}, jinja_env=jinja_env
                )

                with AsyncAwareExecutor(loop=asyncio.get_running_loop(), max_workers=1) as executor:
                    _, result, raised = await iterable_op._run_task(
                        executor, context, task, trust_checkpoints=True
                    )

                assert raised is None
                assert result == "done"
                accessor = context["outlet_events"][Asset(name="a", uri="s3://bucket/a")]
                assert accessor.extra == {"value": "v"}
                store = context["task_state_store"]
                checkpoint = IndexedTaskState.deserialize(store[task.state_key])
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
    async def test_run_task_replays_outlet_events_when_skipping_already_succeeded_sub_task(self):
        """A sub-task skipped on retry (because its checkpoint already records SUCCESS) never
        re-executes, so it would otherwise never re-populate the fresh ``outlet_events`` accessor
        created for the new attempt; ``_run_task`` must replay the events it recorded on its
        earlier successful attempt instead of silently losing them."""
        from unittest import mock

        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="outlet_replay", operator_class=MockOutletEventOperator
            )

            with mock_context(task=iterable_op) as context:
                jinja_env = iterable_op.get_template_env(dag=dag)
                task = iterable_op._create_task(
                    context=context, index=0, mapped_kwargs={}, jinja_env=jinja_env
                )
                task.try_number = 2  # checkpoint is only consulted from the second attempt onwards
                await context["task_state_store"].aset(
                    task.state_key,
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
                _, result, raised = await iterable_op._run_task(
                    executor, context, task, trust_checkpoints=True
                )

        assert raised is None
        assert result is None
        executor.run_sync.assert_not_called()
        accessor = context["outlet_events"][Asset(name="a", uri="s3://bucket/a")]
        assert accessor.extra == {"value": "v"}

    def test_on_kill_propagates_to_active_sub_operators(self):
        """IterableOperator.on_kill() (SIGTERM or execution_timeout) must propagate the kill
        signal to every sub-task currently in flight, since the default BaseOperator.on_kill()
        no-op would otherwise leave running sub-tasks completely unaware of the kill."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="on_kill_test", operator_class=MockOnKillOperator
            )

        active_operator = MockOnKillOperator(task_id="active_sub_task")
        iterable_op._active_sub_operators[id(active_operator)] = active_operator

        iterable_op.on_kill()

        assert active_operator.killed is True

    def test_on_kill_reaches_sub_operators_that_compare_equal_and_kills_each_once(self):
        """
        The sub-operators of one iterated task compare equal (same task_id), so the register is keyed
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
        iterable_op._active_sub_operators.update({id(first): first, id(second): second})

        iterable_op.on_kill()
        iterable_op.on_kill()

        assert sorted(kills) == ["first", "second"]

    def test_on_kill_is_noop_when_no_sub_operators_are_active(self):
        """on_kill() must not raise when called with no in-flight sub-tasks (e.g. the
        IterableOperator is killed before any sub-task has started, or after all finished)."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="on_kill_noop", operator_class=MockOnKillOperator
            )

        iterable_op.on_kill()  # should not raise

    def test_run_task_tracks_active_sub_operator_during_execution(self, monkeypatch: pytest.MonkeyPatch):
        """``_run_task`` must register the sub-task's unmapped operator in
        ``_active_sub_operators`` only for the duration of its execution, so ``on_kill()``
        propagates only to sub-tasks that are actually running."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            iterable_op = create_iterable_operator(
                dag, expand_input, task_id="on_kill_tracking", operator_class=MockOnKillOperator
            )

            with mock_context(task=iterable_op) as context:
                jinja_env = iterable_op.get_template_env(dag=dag)
                task = iterable_op._create_task(
                    context=context, index=0, mapped_kwargs={}, jinja_env=jinja_env
                )

                seen_active_during_run = []

                def tracking_execute(context, ti, log):
                    seen_active_during_run.append(id(task.task) in iterable_op._active_sub_operators)

                monkeypatch.setattr("airflow.sdk.execution_time.task_runner._execute_task", tracking_execute)

                with event_loop() as loop, AsyncAwareExecutor(loop=loop, max_workers=1) as executor:
                    _, _, raised = loop.run_until_complete(
                        iterable_op._run_task(executor, context, task, trust_checkpoints=False)
                    )

        assert raised is None
        assert seen_active_during_run == [True]
        assert id(task.task) not in iterable_op._active_sub_operators

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
        the runner enforces on the outer TI; the wrapped operator retains its own execution_timeout for
        per-sub-task enforcement. A UserWarning is emitted when the wrapped operator is sync, since
        TimeoutPosix won't fire in worker threads."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            execution_timeout = timedelta(seconds=7)
            mapped_op = create_mapped_operator(
                dag, expand_input, task_id="timeout_task", execution_timeout=execution_timeout
            )

            with pytest.warns(UserWarning, match="execution_timeout"):
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

    def test_parent_timeout_landing_in_a_sub_task_stays_a_timeout(self):
        """
        The parent's ``execution_timeout`` is raised by a signal handler on the main thread, so it
        can surface inside whichever sub-task is running there. It is not that sub-task's outcome:
        it has to reach the runner as ``AirflowTaskTimeout``, which retries the task, and not be
        turned into a non-retryable ``AirflowFailException`` blamed on the sub-task.
        """
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"raise_exception": AirflowTaskTimeout("timed out")}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="timeout_task", retries=3)

            with mock_context(task=iterable_op) as context:
                with pytest.raises(AirflowTaskTimeout):
                    iterable_op.execute(context=context)

                assert "_iterable_0" not in context["task_state_store"]

    @pytest.mark.asyncio
    async def test_run_task_lets_a_cancellation_through(self):
        """A cancelled sub-task stays cancelled: no checkpoint write, no result handed back."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            iterable_op = create_iterable_operator(dag, expand_input, task_id="cancelled_task")

            with mock_context(task=iterable_op) as context:
                task = iterable_op._create_task(
                    context=context,
                    index=0,
                    mapped_kwargs={"raise_exception": asyncio.CancelledError()},
                    jinja_env=iterable_op.get_template_env(dag=dag),
                )

                with AsyncAwareExecutor(loop=asyncio.get_running_loop(), max_workers=1) as executor:
                    with pytest.raises(asyncio.CancelledError):
                        await iterable_op._run_task(executor, context, task, trust_checkpoints=False)

                assert task.state_key not in context["task_state_store"]

    def test_deadlock_imminent_error_raises_actionable_airflow_fail_exception(self):
        """A sub-task that raises DeadlockImminentError (a sync SDK call made from an async
        sub-task) must never be retried and must surface an actionable error pointing at async-safe
        SDK alternatives, rather than the generic non-Exception BaseException message."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput(
                [{"raise_exception": DeadlockImminentError("simulated sync SDK call")}]
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
                with pytest.raises(AirflowFailException, match="synchronous SDK call"):
                    iterable_op.execute(context=context)

    def test_deferred_operator_raises_airflow_fail_exception(self):
        """A sub-task that raises TaskDeferred must cause IterableOperator to raise AirflowFailException."""
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

    def test_reschedule_mode_sensor_raises_base_exception_group(self):
        """A sub-task that raises AirflowRescheduleException is no longer special-cased: it is treated
        like any other sub-task failure and surfaces via BaseExceptionGroup. The requested
        reschedule_date is not honored inside IterableOperator — Airflow's standard retry mechanism
        (via the IterableOperator's own retries/retry_delay) takes over instead."""
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
    Verify that each sub-task run by IterableOperator sees its own indexed
    context via get_current_context(), not the parent's.
    """

    def test_subtask_sees_its_own_context(self):
        """Each sub-task's get_current_context() must return its own indexed ti, not the parent's."""
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
            for idx, sub_ti in captured.items():
                # Each sub-task must have seen its own IndexedTaskInstance, not the parent TI.
                assert sub_ti is not parent_ti, f"Sub-task {idx} observed the parent context"
                assert sub_ti.index == idx, f"Sub-task {idx} observed wrong index {sub_ti.index}"

    def test_async_subtask_execution_timeout_is_enforced(self):
        """execution_timeout is enforced for async sub-tasks via asyncio.wait_for."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{}])
            mapped_op = MockSlowAsyncOperator.partial(
                task_id="slow_async_task",
                dag=dag,
                execution_timeout=timedelta(milliseconds=50),
            )._expand(expand_input, strict=True, register_with_dag=False)
            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

        with mock_context(task=iterable_op) as context:
            # asyncio.TimeoutError is only the built-in TimeoutError from Python 3.11 on.
            with pytest.raises((TimeoutError, asyncio.TimeoutError)):
                iterable_op.execute(context=context)

    def test_sync_subtask_with_execution_timeout_emits_warning(self):
        """A sync operator with execution_timeout warns that it is not enforced per sub-task."""
        with DAG("test_dag") as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            mapped_op = create_mapped_operator(
                dag, expand_input, task_id="sync_timeout_task", execution_timeout=timedelta(seconds=5)
            )
            with pytest.warns(UserWarning, match="TimeoutPosix") as warning_list:
                IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

        assert len(warning_list) == 1
        assert "sync" in str(warning_list[0].message).lower()
        assert "caps the whole iteration" in str(warning_list[0].message)


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


class TestExecutionTimeoutKillsInFlightSubTasks:
    """
    The parent's execution_timeout reaches every sub-task in flight, once, through on_kill().

    Runs the operator through the runner's own ``_run_execute_callable`` with a real timeout, as the
    task runner does, so the order in which the executor cancels and the runner calls on_kill() is
    the real one.
    """

    @pytest.mark.parametrize(
        ("operator_class", "kind"),
        [(MockSlowSyncOperator, "sync"), (MockSlowAsyncKillableOperator, "async")],
        ids=["sync", "async"],
    )
    def test_every_sub_task_in_flight_is_killed_once(self, operator_class, kind, mock_supervisor_comms):
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


class TestFailureHandedToTheRunner:
    """
    The runner classifies the outcome by exception type, so an item's own exception reaches it
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

    def test_retry_policy_decides_on_the_items_own_exception(self):
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
        # What the runner evaluates next: the rule matches the item's exception, never the group.
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
    def _run_twice(partial_value_on_retry, arg2_first="from-upstream", arg2_is_upstream=True):
        """Attempt 1: item 2 fails. Attempt 2 (retry or clear): item 2 succeeds. Returns runs and results."""
        upstream_value = [arg2_first]
        runs = []
        original_execute = MockOperator.execute

        def counting_execute(self, context):
            runs.append((context["ti"].try_number, self.arg1))
            return original_execute(self, context)

        with DAG("test_dag") as dag:
            if arg2_is_upstream:
                arg2 = make_xcom_arg(None)
                arg2.resolve = lambda *args, **kwargs: upstream_value[0]
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

    def test_a_changed_upstream_value_runs_the_item_again(self):
        runs, results = self._run_twice("changed-upstream")

        assert (2, 1) in runs
        assert results == [(1, "changed-upstream", None), (2, "changed-upstream", None)]

    def test_an_unchanged_upstream_value_keeps_the_checkpoint(self):
        runs, results = self._run_twice("from-upstream")

        assert (2, 1) not in runs
        assert results == [(1, "from-upstream", None), (2, "from-upstream", None)]

    def test_a_templated_partial_value_does_not_make_checkpoints_stale(self):
        """Only upstream values count: a value rendered anew every attempt must not force a rerun."""
        runs, results = self._run_twice(None, arg2_first="{{ ti.try_number }}", arg2_is_upstream=False)

        assert (2, 1) not in runs
        assert [value[0] for value in results] == [1, 2]

    def test_partial_inputs_from_upstream_are_read_from_the_rendered_operator(self):
        from airflow.sdk.definitions.iterableoperator import _partial_inputs_from_upstream

        upstream = make_xcom_arg(None)
        rendered = SimpleNamespace(arg2="rendered-arg2", op_kwargs={"y": "rendered-y", "z": 1}, retries=2)

        inputs = _partial_inputs_from_upstream(
            {"arg2": upstream, "op_kwargs": {"y": upstream, "z": 1}, "retries": 2}, rendered
        )

        assert inputs == {"arg2": "rendered-arg2", "op_kwargs.y": "rendered-y"}


CALLBACKS: list = []


class MockCallbackOperator(BaseOperator):
    """Operator that records which callback each of its items gets."""

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
    A failed item's failure or retry callback waits until every item has run, and then says what
    happens to the task: retried, or failed for good. Success callbacks fire right away.
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

    def test_a_siblings_fail_exception_turns_every_failure_into_a_final_one(self):
        """Kaxil's case: without the wait the ValueError item announced a retry that never came."""
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

    def test_an_item_the_operator_rejects_fails_the_task_for_every_item(self):
        """A downstream skip from an item is rejected with AirflowFailException, so nothing is retried."""
        raised, fired = self._run(
            [
                {"arg1": "value_error", "raise_exception": ValueError("x")},
                {"arg1": "skipper", "raise_exception": DownstreamTasksSkipped(tasks=["downstream"])},
            ]
        )

        assert isinstance(raised, AirflowFailException)
        assert fired == [("failure", "value_error"), ("failure", "skipper")]

    def test_a_failed_items_callback_waits_for_the_items_after_it(self):
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

    def test_deepcopy_gets_its_own_lock_and_no_sub_tasks_in_flight(self):
        import copy
        import threading

        iterable_op = self._dag().task_dict["f"]
        in_flight = MockOperator(task_id="in_flight")
        iterable_op._active_sub_operators[id(in_flight)] = in_flight

        copied = copy.deepcopy(iterable_op)

        assert isinstance(copied, IterableOperator)
        assert copied.task_id == "f"
        assert copied._active_sub_operators == {}
        assert copied._active_sub_operators_lock is not iterable_op._active_sub_operators_lock
        assert isinstance(copied._active_sub_operators_lock, type(threading.Lock()))
        with copied._active_sub_operators_lock:
            pass
        # The original is untouched.
        assert iterable_op._active_sub_operators == {id(in_flight): in_flight}

    def test_partial_subset_keeps_the_iterated_task(self):
        dag = self._dag()

        subset = dag.partial_subset("f", include_upstream=True, include_downstream=True)

        assert sorted(subset.task_dict) == ["down", "f", "up"]
        assert isinstance(subset.task_dict["f"], IterableOperator)
        assert subset.task_dict["f"] is not dag.task_dict["f"]


class TestCheckpoints:
    @staticmethod
    def _context(try_number: int, marker: dict | None = None):
        from airflow.sdk.execution_time.context import TaskStateStoreAccessor

        store = create_autospec(TaskStateStoreAccessor, instance=True)
        store.get.return_value = marker
        ti = SimpleNamespace(task_id="my_task", try_number=try_number)
        return {"task_state_store": store, "ti": ti}, store

    def test_first_attempt_never_reads_and_marks_completed_on_exit(self):
        context, store = self._context(try_number=1)

        with Checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is False

        store.get.assert_not_called()
        store.delete.assert_not_called()
        store.set.assert_called_once_with("_iterable_completed", {"completed": True, "try_number": 1})

    def test_retry_without_marker_trusts_checkpoints(self):
        context, store = self._context(try_number=2, marker=None)

        with Checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is True

        store.get.assert_called_once_with("_iterable_completed")
        store.delete.assert_not_called()
        store.set.assert_called_once_with("_iterable_completed", {"completed": True, "try_number": 2})

    def test_failed_attempt_leaves_no_marker(self):
        context, store = self._context(try_number=1)

        with pytest.raises(RuntimeError, match="sub-task failures"):
            with Checkpoints(context):
                raise RuntimeError("sub-task failures")

        store.set.assert_not_called()

    def test_attempt_where_every_iteration_skipped_marks_completed(self):
        """The task ends ``SKIPPED``; a clear of it must run every iteration again."""
        context, store = self._context(try_number=1)

        with pytest.raises(AirflowSkipException):
            with Checkpoints(context):
                raise AirflowSkipException("every iteration skipped")

        store.set.assert_called_once_with("_iterable_completed", {"completed": True, "try_number": 1})

    def test_rerun_after_clear_records_where_it_starts_and_ignores_checkpoints(self):
        context, store = self._context(try_number=2, marker={"completed": True, "try_number": 1})

        with Checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is False
            store.set.assert_called_once_with("_iterable_completed", {"completed": False, "since": 2})

        store.set.assert_called_with("_iterable_completed", {"completed": True, "try_number": 2})

    def test_failed_rerun_leaves_where_it_started(self):
        context, store = self._context(try_number=2, marker={"completed": True, "try_number": 1})

        with pytest.raises(RuntimeError):
            with Checkpoints(context):
                raise RuntimeError("sub-task failed")

        store.set.assert_called_once_with("_iterable_completed", {"completed": False, "since": 2})

    def test_attempt_after_a_failed_rerun_trusts_the_checkpoints_written_since(self):
        context, store = self._context(try_number=3, marker={"completed": False, "since": 2})

        with Checkpoints(context) as checkpoints:
            assert checkpoints.trust_checkpoints is True
            assert checkpoints.since == 2
            store.set.assert_not_called()

        store.set.assert_called_once_with("_iterable_completed", {"completed": True, "try_number": 3})
