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
from datetime import timedelta
from typing import TYPE_CHECKING

try:
    # Python 3.11+
    BaseExceptionGroup
except NameError:
    from exceptiongroup import BaseExceptionGroup

import pytest

from airflow.sdk import DAG, BaseOperator, BaseXCom
from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_RETRIES
from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput, ListOfDictsExpandInput
from airflow.sdk.definitions.iterableoperator import IterableOperator
from airflow.sdk.execution_time.xcom import XCom

from tests_common.test_utils.mock_context import mock_context

if TYPE_CHECKING:
    from airflow.sdk.definitions._internal.expandinput import ExpandInput
    from airflow.sdk.definitions.mappedoperator import MappedOperator

    from tests_common.test_utils.compat import Context


class MockOperator(BaseOperator):
    """Mock operator for testing IterableOperator expansion."""

    def __init__(self, arg1=None, arg2=None, arg3=None, fail_on_first_attempt=False, **kwargs):
        super().__init__(**kwargs)
        self.arg1 = arg1
        self.arg2 = arg2
        self.arg3 = arg3
        self.fail_on_first_attempt = fail_on_first_attempt

    def execute(self, context):
        """Execute the operator and return passed arguments as tuple if do_xcom_push is True."""
        expected = copy.deepcopy(context)

        if self.fail_on_first_attempt:
            self.fail_on_first_attempt = False
            raise RuntimeError
        if not self.do_xcom_push:
            return None
        result = self.arg1, self.arg2, self.arg3

        assert context == expected, "Context was unexpectedly mutated during task execution"
        return result


@pytest.fixture
def mock_xcom_get_one(monkeypatch: pytest.MonkeyPatch):
    """
    Fixture that mocks XCom.get_one using monkeypatch for proper cleanup.

    Captures values pushed via context["ti"].xcom_push in the order they arrive,
    then serves them back by index when XComIterable calls
    XCom.get_one(key=f"{task_id}_{idx}", ...).
    """

    def _mock_xcom(context: Context):
        pushed_values: list = []

        original_push = context["ti"].xcom_push

        def capturing_push(key: str, value, **kwargs) -> None:
            pushed_values.append(value)
            original_push(key=key, value=value, **kwargs)

        monkeypatch.setattr(context["ti"], "xcom_push", capturing_push)

        task_id = context["ti"].task_id

        def mock_get_one(**kwargs):
            key = kwargs.get("key", "")
            prefix = f"{task_id}_"
            if key.startswith(prefix):
                try:
                    idx = int(key[len(prefix) :])
                    if 0 <= idx < len(pushed_values):
                        return pushed_values[idx]
                except (ValueError, TypeError):
                    pass
            return None

        monkeypatch.setattr(XCom, "get_one", mock_get_one)

    return _mock_xcom


class TestIterableOperator:
    @classmethod
    def create_mapped_operator(
        cls,
        dag: DAG,
        expand_input: ExpandInput,
        task_id: str = "my_task",
        retries: int = DEFAULT_RETRIES,
        do_xcom_push: bool = True,
        task_concurrency: int | None = None,
        execution_timeout: timedelta | None = None,
    ) -> MappedOperator:
        """
        Create a MappedOperator and assign it to a DAG.

        :param expand_input: The input to expand
        :param dag: The DAG to assign the operator to
        :param task_id: Task ID for the operator
        :param do_xcom_push: Whether to push XCom (default True)
        """
        return MockOperator.partial(
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

    @classmethod
    def create_iterable_operator(
        cls,
        dag: DAG,
        expand_input: ExpandInput,
        task_id: str = "my_task",
        task_concurrency: int | None = None,
        retries: int = DEFAULT_RETRIES,
        do_xcom_push: bool = True,
    ) -> IterableOperator:
        """Create an IterableOperator with a MappedOperator and ExpandInput."""
        mapped_op = cls.create_mapped_operator(
            dag=dag,
            expand_input=expand_input,
            task_id=task_id,
            retries=retries,
            do_xcom_push=do_xcom_push,
            task_concurrency=task_concurrency,
        )
        return IterableOperator(
            operator=mapped_op,
            expand_input=expand_input,
            dag=dag,
        )

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}]),
            ([{"a": 1, "b": 2}], [{"a": 1, "b": 2}]),
            ([], []),
        ],
    )
    def test_list_of_dicts_expand_input_iter_values(self, dag_maker, session, actual, expected):
        """Test IterableOperator with ListOfDictsExpandInput expand_input."""
        if not actual:
            pytest.skip("Empty list case tested separately")

        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput(actual)
            iterable_op = self.create_iterable_operator(dag, expand_input)

            result = list(iterable_op.expand_input.iter_values({}))
            assert result == expected

    @pytest.mark.db_test
    def test_list_of_dicts_empty(self, dag_maker, session):
        """Test IterableOperator with empty list."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([])
            iterable_op = self.create_iterable_operator(dag, expand_input)

            result = list(iterable_op.expand_input.iter_values({}))
            assert result == []

    @pytest.mark.db_test
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
        ],
    )
    def test_dict_of_lists_expand_input_iter_values(self, dag_maker, session, actual, expected):
        """Test IterableOperator with DictOfListsExpandInput expand_input."""
        with dag_maker(session=session) as dag:
            expand_input = DictOfListsExpandInput(actual)
            iterable_op = self.create_iterable_operator(dag, expand_input)

            result = list(iterable_op.expand_input.iter_values({}))
            assert result == expected

    @pytest.mark.db_test
    def test_task_type(self, dag_maker, session):
        """Test that IterableOperator correctly reports task_type."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = self.create_iterable_operator(dag, expand_input)

            assert isinstance(iterable_op, IterableOperator)
            assert iterable_op.task_type == "MappedOperator"

    @pytest.mark.db_test
    def test_task_id(self, dag_maker, session):
        """Test that IterableOperator inherits task_id from operator."""
        with dag_maker(session=session) as dag:
            task_id = "my_task"
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = self.create_iterable_operator(dag, expand_input, task_id=task_id)

            assert iterable_op.task_id == task_id

    @pytest.mark.db_test
    def test_with_task_concurrency(self, dag_maker, session):
        """Test that IterableOperator respects task_concurrency parameter."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = self.create_iterable_operator(dag, expand_input, task_concurrency=4)

            assert iterable_op.max_workers == 4

    @pytest.mark.db_test
    def test_expand_input_stored(self, dag_maker, session):
        """Test that IterableOperator stores expand_input correctly."""
        with dag_maker(session=session) as dag:
            expand_input_data = ListOfDictsExpandInput([{"a": 1}, {"a": 2}])
            iterable_op = self.create_iterable_operator(dag, expand_input_data)

            assert iterable_op.expand_input is expand_input_data
            assert isinstance(iterable_op.expand_input, (ListOfDictsExpandInput, DictOfListsExpandInput))

    @pytest.mark.db_test
    def test_partial_kwargs_stored(self, dag_maker, session):
        """Test that IterableOperator stores partial_kwargs from operator."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            iterable_op = self.create_iterable_operator(dag, expand_input)

            assert hasattr(iterable_op, "partial_kwargs")
            assert isinstance(iterable_op.partial_kwargs, dict)

    @pytest.mark.db_test
    def test_xcom_push_delegates_to_task_when_not_pushed(self, dag_maker, session):
        """_xcom_push delegates to task.xcom_push only when xcom_pushed is False."""
        from unittest import mock

        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            iterable_op = self.create_iterable_operator(dag, expand_input)

        task = mock.MagicMock()
        task.xcom_pushed = False
        task.task_id = "my_task"
        task.index = 0

        iterable_op._xcom_push(task=task, value="result_value")

        task.xcom_push.assert_called_once_with(key=BaseXCom.XCOM_RETURN_KEY, value="result_value")

    @pytest.mark.db_test
    def test_xcom_push_skips_when_already_pushed(self, dag_maker, session):
        """_xcom_push skips pushing when xcom_pushed is already True."""
        from unittest import mock

        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}])
            iterable_op = self.create_iterable_operator(dag, expand_input)

        task = mock.MagicMock()
        task.xcom_pushed = True
        task.task_id = "my_task"
        task.index = 0

        iterable_op._xcom_push(task=task, value="result_value")

        task.xcom_push.assert_not_called()

    @pytest.mark.db_test
    def test_execute_list_of_dicts(self, dag_maker, session, mock_xcom_get_one):
        """Test executing IterableOperator with ListOfDictsExpandInput."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = self.create_iterable_operator(dag, expand_input, task_id="exec_list_of_dicts")

            context = mock_context(task=iterable_op)
            mock_xcom_get_one(context)
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, None, None), (2, None, None)]

    @pytest.mark.db_test
    def test_execute_dict_of_lists(self, dag_maker, session, mock_xcom_get_one):
        """Test executing IterableOperator with DictOfListsExpandInput."""
        with dag_maker(session=session) as dag:
            expand_input = DictOfListsExpandInput({"arg1": [1, 2, 3]})
            iterable_op = self.create_iterable_operator(dag, expand_input, task_id="exec_dict_of_lists")

            context = mock_context(task=iterable_op)
            mock_xcom_get_one(context)
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, None, None), (2, None, None), (3, None, None)]

    @pytest.mark.db_test
    def test_execute_empty_list_of_dicts(self, dag_maker, session, mock_xcom_get_one):
        """Test executing IterableOperator with empty ListOfDictsExpandInput."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([])
            iterable_op = self.create_iterable_operator(dag, expand_input, task_id="exec_empty")

            context = mock_context(task=iterable_op)
            mock_xcom_get_one(context)
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == []

    @pytest.mark.db_test
    def test_execute_multiple_key_dict_of_lists(self, dag_maker, session, mock_xcom_get_one):
        """Test executing IterableOperator with multiple keys in DictOfListsExpandInput."""
        with dag_maker(session=session) as dag:
            expand_input = DictOfListsExpandInput({"arg1": [1, 2], "arg2": [10, 20], "arg3": ["x", "y"]})
            iterable_op = self.create_iterable_operator(dag, expand_input, task_id="exec_multi_key")

            context = mock_context(task=iterable_op)
            mock_xcom_get_one(context)
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

    @pytest.mark.db_test
    def test_execute_with_task_concurrency_setting(self, dag_maker, session, mock_xcom_get_one):
        """Test executing IterableOperator with task_concurrency parameter."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}, {"arg1": 3}])
            iterable_op = self.create_iterable_operator(
                dag, expand_input, task_id="exec_concurrency", task_concurrency=2
            )

            context = mock_context(task=iterable_op)
            mock_xcom_get_one(context)
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, None, None), (2, None, None), (3, None, None)]
            assert iterable_op.max_workers == 2

    @pytest.mark.db_test
    def test_execute_all_parameters(self, dag_maker, session, mock_xcom_get_one):
        """Test executing IterableOperator with all arg1, arg2, arg3 parameters."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"arg1": 1, "arg2": 10, "arg3": 100},
                    {"arg1": 2, "arg2": 20, "arg3": 200},
                ]
            )
            iterable_op = self.create_iterable_operator(dag, expand_input, task_id="exec_all_args")

            context = mock_context(task=iterable_op)
            mock_xcom_get_one(context)
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, 10, 100), (2, 20, 200)]

    @pytest.mark.db_test
    def test_execute_with_do_xcom_push_false(self, dag_maker, session):
        """Test executing IterableOperator when do_xcom_push is False."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
            iterable_op = self.create_iterable_operator(
                dag, expand_input, task_id="no_xcom_push", do_xcom_push=False
            )

            context = mock_context(task=iterable_op)
            result = iterable_op.execute(context=context)

            assert result is None

    @pytest.mark.db_test
    def test_execute_with_failed_tasks_but_no_retries(self, dag_maker, session, mock_xcom_get_one):
        """
        Test executing IterableOperator where tasks fail but no retries are available.

        This test verifies that:
        1. Tasks with fail_on_first_attempt=True raise an exception on first attempt
        2. When no retries are configured (retries=0), the exception propagates and is not retried
        3. The BaseExceptionGroup is raised containing the task failure
        """
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"arg1": 1, "arg2": 10},
                    {"arg1": 2, "arg2": 20, "fail_on_first_attempt": True},
                    {"arg1": 3, "arg2": 30},
                ]
            )
            iterable_op = self.create_iterable_operator(
                dag,
                expand_input,
                task_id="exec_with_failures",
                retries=0,
            )

            context = mock_context(task=iterable_op)
            mock_xcom_get_one(context)
            with pytest.raises(BaseExceptionGroup):
                iterable_op.execute(context=context)

    @pytest.mark.db_test
    def test_execute_with_failed_tasks_and_expired_reschedule_date(
        self, dag_maker, session, mock_xcom_get_one
    ):
        """
        Test executing IterableOperator where certain map_index tasks fail on first attempt and are retried.

        This test verifies that:
        1. Tasks with fail_on_first_attempt=True raise an exception on first attempt (try_number == 0)
        2. Failed tasks are retried immediately without deferring (since reschedule_date is expired)
        3. Retried tasks succeed on subsequent attempts (try_number > 0) and produce the expected output
        """
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput(
                [
                    {"arg1": 1, "arg2": 10},
                    {"arg1": 2, "arg2": 20, "fail_on_first_attempt": True},
                    {"arg1": 3, "arg2": 30},
                ]
            )
            iterable_op = self.create_iterable_operator(
                dag,
                expand_input,
                task_id="exec_with_failures",
                retries=1,
            )

            context = mock_context(task=iterable_op)
            mock_xcom_get_one(context)
            result = iterable_op.execute(context=context)
            materialized = list(result)

            assert len(materialized) == 3
            assert materialized == [(1, 10, None), (2, 20, None), (3, 30, None)]

    @pytest.mark.db_test
    def test_timeout_reads_mapped_operator_and_iterable_execution_timeout_is_none(self, dag_maker, session):
        """Ensure IterableOperator.timeout reads the wrapped operator.execution_timeout
        and the IterableOperator.execution_timeout remains None (not propagated to parent)."""
        with dag_maker(session=session) as dag:
            expand_input = ListOfDictsExpandInput([{"a": 1}])
            execution_timeout = timedelta(seconds=7)
            mapped_op = self.create_mapped_operator(
                dag, expand_input, task_id="timeout_task", execution_timeout=execution_timeout
            )

            iterable_op = IterableOperator(operator=mapped_op, expand_input=expand_input, dag=dag)

            assert iterable_op._operator.execution_timeout == execution_timeout
            assert iterable_op.execution_timeout is None
            assert iterable_op.timeout == 7.0
