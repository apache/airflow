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

from typing import TYPE_CHECKING
from unittest import mock

try:
    # Python 3.11+
    BaseExceptionGroup
except NameError:
    from exceptiongroup import BaseExceptionGroup

import pytest

from airflow.sdk import BaseOperator, DAG, timezone
from airflow.sdk.definitions._internal.abstractoperator import DEFAULT_RETRIES
from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput, ListOfDictsExpandInput
from airflow.sdk.definitions.iterableoperator import IterableOperator
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.execution_time.xcom import XCom
from tests_common.test_utils.mock_context import mock_context

if TYPE_CHECKING:
    from airflow.sdk.definitions._internal.expandinput import ExpandInput


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
        if self.fail_on_first_attempt:
            self.fail_on_first_attempt = False
            raise RuntimeError
        if not self.do_xcom_push:
            return None
        return self.arg1, self.arg2, self.arg3


class TestIterableOperator:
    def _get_dag(self):
        """Create a fresh DAG for each test."""
        return DAG(dag_id="test_dag", start_date=timezone.utcnow())

    def _mock_xcom_get_one(self, context):
        """Create a context manager that mocks XCom.get_one to retrieve from context values."""
        def mock_get_one(**kwargs):
            kwargs["task_ids"] = kwargs.pop("task_id")
            return context["ti"].xcom_pull(**kwargs)

        return mock.patch.object(XCom, "get_one", side_effect=mock_get_one)

    def _create_mapped_operator(
        self, expand_input: ExpandInput, task_id: str = "my_task", retries: int = DEFAULT_RETRIES, do_xcom_push: bool = True
    ) -> MappedOperator:
        """
        Create a MappedOperator without adding it to a DAG.

        :param expand_input: The input to expand
        :param task_id: Task ID for the operator
        :param do_xcom_push: Whether to push XCom (default True)
        """
        return MockOperator.partial(task_id=task_id, dag=None, retries=retries, do_xcom_push=do_xcom_push)._expand(
            expand_input, strict=True, apply_upstream_relationship=False,
        )

    def _create_iterable_operator(
        self,
        dag: DAG,
        expand_input: ExpandInput,
        task_id: str = "my_task",
        task_concurrency: int | None = None,
        retries: int = DEFAULT_RETRIES,
        do_xcom_push: bool = True
    ) -> IterableOperator:
        """Create an IterableOperator with a MappedOperator and ExpandInput."""
        mapped_op = self._create_mapped_operator(expand_input, task_id=task_id, retries=retries, do_xcom_push=do_xcom_push)
        return IterableOperator(
            operator=mapped_op,
            expand_input=expand_input,
            task_concurrency=task_concurrency,
            dag=dag,
        )

    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ([{"a": 1}, {"a": 2}], [{"a": 1}, {"a": 2}]),
            ([{"a": 1, "b": 2}], [{"a": 1, "b": 2}]),
            ([], []),
        ],
    )
    def test_list_of_dicts_expand_input_iter_values(self, actual, expected):
        """Test IterableOperator with ListOfDictsExpandInput expand_input."""
        if not actual:
            pytest.skip("Empty list case tested separately")

        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput(actual)
        iterable_op = self._create_iterable_operator(dag, expand_input)

        result = list(iterable_op.expand_input.iter_values({}))
        assert result == expected

    def test_list_of_dicts_empty(self):
        """Test IterableOperator with empty list."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput([])
        iterable_op = self._create_iterable_operator(dag, expand_input)

        result = list(iterable_op.expand_input.iter_values({}))
        assert result == []

    @pytest.mark.parametrize(
        ("actual", "expected"),
        [
            ({"a": 1}, [{"a": 1}]),
            ({"a": [1, 2, 3]}, [{"a": 1}, {"a": 2}, {"a": 3}]),
            ({"a": "hello"}, [{"a": "hello"}]),
            ({"a": [1, 2], "b": [10, 20]}, [{"a": 1, "b": 10}, {"a": 2, "b": 20}]),
            ({"a": [1, 2]}, [{"a": 1}, {"a": 2}]),  # Convert generator to list for testing
        ],
    )
    def test_dict_of_lists_expand_input_iter_values(self, actual, expected):
        """Test IterableOperator with DictOfListsExpandInput expand_input."""
        dag = self._get_dag()
        expand_input = DictOfListsExpandInput(actual)
        iterable_op = self._create_iterable_operator(dag, expand_input)

        result = list(iterable_op.expand_input.iter_values({}))
        assert result == expected

    def test_task_type(self):
        """Test that IterableOperator correctly reports task_type."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput([{"a": 1}])
        iterable_op = self._create_iterable_operator(dag, expand_input)

        assert isinstance(iterable_op, IterableOperator)
        assert iterable_op.task_type == "MappedOperator"

    def test_task_id(self):
        """Test that IterableOperator inherits task_id from operator."""
        dag = self._get_dag()
        task_id = "my_task"
        expand_input = ListOfDictsExpandInput([{"a": 1}])
        iterable_op = self._create_iterable_operator(dag, expand_input, task_id=task_id)

        assert iterable_op.task_id == task_id

    def test_with_task_concurrency(self):
        """Test that IterableOperator respects task_concurrency parameter."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput([{"a": 1}])
        iterable_op = self._create_iterable_operator(dag, expand_input, task_concurrency=4)

        assert iterable_op.max_workers == 4

    def test_expand_input_stored(self):
        """Test that IterableOperator stores expand_input correctly."""
        dag = self._get_dag()
        expand_input_data = ListOfDictsExpandInput([{"a": 1}, {"a": 2}])
        iterable_op = self._create_iterable_operator(dag, expand_input_data)

        assert iterable_op.expand_input is expand_input_data
        assert isinstance(iterable_op.expand_input, (ListOfDictsExpandInput, DictOfListsExpandInput))

    def test_partial_kwargs_stored(self):
        """Test that IterableOperator stores partial_kwargs from operator."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput([{"a": 1}])
        iterable_op = self._create_iterable_operator(dag, expand_input)

        assert hasattr(iterable_op, "partial_kwargs")
        assert isinstance(iterable_op.partial_kwargs, dict)

    def test_execute_list_of_dicts(self):
        """Test executing IterableOperator with ListOfDictsExpandInput."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
        iterable_op = self._create_iterable_operator(dag, expand_input, task_id="exec_list_of_dicts")

        context = mock_context(task=iterable_op)
        with self._mock_xcom_get_one(context):
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, None, None), (2, None, None)]

    def test_execute_dict_of_lists(self):
        """Test executing IterableOperator with DictOfListsExpandInput."""
        dag = self._get_dag()
        expand_input = DictOfListsExpandInput({"arg1": [1, 2, 3]})
        iterable_op = self._create_iterable_operator(dag, expand_input, task_id="exec_dict_of_lists")

        context = mock_context(task=iterable_op)
        with self._mock_xcom_get_one(context):
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, None, None), (2, None, None), (3, None, None)]

    def test_execute_empty_list_of_dicts(self):
        """Test executing IterableOperator with empty ListOfDictsExpandInput."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput([])
        iterable_op = self._create_iterable_operator(dag, expand_input, task_id="exec_empty")

        context = mock_context(task=iterable_op)
        with self._mock_xcom_get_one(context):
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == []

    def test_execute_multiple_key_dict_of_lists(self):
        """Test executing IterableOperator with multiple keys in DictOfListsExpandInput."""
        dag = self._get_dag()
        expand_input = DictOfListsExpandInput({"arg1": [1, 2], "arg2": [10, 20], "arg3": ["x", "y"]})
        iterable_op = self._create_iterable_operator(dag, expand_input, task_id="exec_multi_key")

        context = mock_context(task=iterable_op)
        with self._mock_xcom_get_one(context):
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, 10, "x"), (2, 20, "y")]

    def test_execute_with_task_concurrency_setting(self):
        """Test executing IterableOperator with task_concurrency parameter."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}, {"arg1": 3}])
        iterable_op = self._create_iterable_operator(
            dag, expand_input, task_id="exec_concurrency", task_concurrency=2
        )

        context = mock_context(task=iterable_op)
        with self._mock_xcom_get_one(context):
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, None, None), (2, None, None), (3, None, None)]
            assert iterable_op.max_workers == 2

    def test_execute_all_parameters(self):
        """Test executing IterableOperator with all arg1, arg2, arg3 parameters."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput(
            [
                {"arg1": 1, "arg2": 10, "arg3": 100},
                {"arg1": 2, "arg2": 20, "arg3": 200},
            ]
        )
        iterable_op = self._create_iterable_operator(dag, expand_input, task_id="exec_all_args")

        context = mock_context(task=iterable_op)
        with self._mock_xcom_get_one(context):
            result = iterable_op.execute(context=context)
            materialized = list(result)
            assert materialized == [(1, 10, 100), (2, 20, 200)]

    def test_execute_with_do_xcom_push_false(self):
        """Test executing IterableOperator when do_xcom_push is False."""
        dag = self._get_dag()
        expand_input = ListOfDictsExpandInput([{"arg1": 1}, {"arg1": 2}])
        iterable_op = self._create_iterable_operator(
            dag, expand_input, task_id="no_xcom_push", do_xcom_push=False
        )

        context = mock_context(task=iterable_op)
        result = iterable_op.execute(context=context)

        assert result is None

    def test_execute_with_failed_tasks_but_no_retries(self):
        """Test executing IterableOperator where tasks fail but no retries are available.

        This test verifies that:
        1. Tasks with fail_on_first_attempt=True raise an exception on first attempt
        2. When no retries are configured (retries=0), the exception propagates and is not retried
        3. The BaseExceptionGroup is raised containing the task failure
        """
        dag = self._get_dag()
        # Create expand_input where arg1=2 will fail on first attempt
        expand_input = ListOfDictsExpandInput([
            {"arg1": 1, "arg2": 10},
            {"arg1": 2, "arg2": 20, "fail_on_first_attempt": True},
            {"arg1": 3, "arg2": 30},
        ])
        iterable_op = self._create_iterable_operator(
            dag, expand_input, task_id="exec_with_failures", retries=0
        )

        context = mock_context(task=iterable_op)
        with self._mock_xcom_get_one(context):
            # Expect an exception to be raised since no retries are available
            with pytest.raises(BaseExceptionGroup):
                iterable_op.execute(context=context)

    def test_execute_with_failed_tasks_and_expired_reschedule_date(self):
        """Test executing IterableOperator where certain map_index tasks fail on first attempt and are retried.

        This test verifies that:
        1. Tasks with fail_on_first_attempt=True raise an exception on first attempt (try_number == 0)
        2. Failed tasks are retried immediately without deferring (since reschedule_date is expired)
        3. Retried tasks succeed on subsequent attempts (try_number > 0) and produce the expected output
        """
        dag = self._get_dag()
        # Create expand_input where arg1=2 will fail on first attempt
        expand_input = ListOfDictsExpandInput([
            {"arg1": 1, "arg2": 10},
            {"arg1": 2, "arg2": 20, "fail_on_first_attempt": True},
            {"arg1": 3, "arg2": 30},
        ])
        iterable_op = self._create_iterable_operator(
            dag, expand_input, task_id="exec_with_failures", retries=1,
        )

        context = mock_context(task=iterable_op)
        with self._mock_xcom_get_one(context):
            result = iterable_op.execute(context=context)
            materialized = list(result)

            # Verify all tasks completed successfully despite arg1=2 failing on first attempt
            assert len(materialized) == 3
            assert materialized == [(1, 10, None), (2, 20, None), (3, 30, None)]
