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

import pytest

from airflow.sdk import BaseOperator, DAG, timezone
from airflow.sdk.definitions._internal.expandinput import DictOfListsExpandInput, ListOfDictsExpandInput
from airflow.sdk.definitions.iterableoperator import IterableOperator
from airflow.sdk.definitions.mappedoperator import MappedOperator

if TYPE_CHECKING:
    from airflow.sdk.definitions._internal.expandinput import ExpandInput


class MockOperator(BaseOperator):
    """Mock operator for testing IterableOperator expansion."""

    def __init__(self, arg1=None, arg2=None, arg3=None, **kwargs):
        self.arg1 = arg1
        self.arg2 = arg2
        self.arg3 = arg3
        self.kwargs = kwargs
        super().__init__(**kwargs)


class TestIterableOperator:
    def _get_dag(self):
        """Create a fresh DAG for each test."""
        return DAG(dag_id="test_dag", start_date=timezone.utcnow())

    def _create_mapped_operator(
        self, expand_input: dict | list, task_id: str = "my_task"
    ) -> MappedOperator:
        """Create a MappedOperator without adding it to a DAG."""
        return MockOperator.partial(task_id=task_id, dag=None).expand(arg2=expand_input)

    def _create_iterable_operator(
        self, dag: DAG, expand_input: ExpandInput, task_id: str = "my_task", task_concurrency: int | None = None
    ) -> IterableOperator:
        """Create an IterableOperator with a MappedOperator and ExpandInput."""
        # Extract the actual value from expand_input for creating MappedOperator
        expand_value = expand_input.value
        # Convert generators to lists since they can only be iterated once
        if hasattr(expand_value, "__next__"):  # It's a generator
            expand_value = list(expand_value)

        mapped_op = self._create_mapped_operator(expand_value, task_id=task_id)
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
