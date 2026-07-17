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

from unittest.mock import MagicMock

from airflow.sdk import DAG, BaseOperator
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.sdk.execution_time.comms import TICount
from airflow.sdk.execution_time.task_mapping import (
    _find_common_ancestor_mapped_group,
    _is_further_mapped_inside,
    get_relevant_map_indexes,
    get_ti_count_for_task,
)


class TestFindCommonAncestorMappedGroup:
    """Tests for _find_common_ancestor_mapped_group function."""

    def test_no_common_group_different_dags(self):
        """Tasks in different DAGs should return None."""
        with DAG("dag1"):
            op1 = BaseOperator(task_id="op1")

        with DAG("dag2"):
            op2 = BaseOperator(task_id="op2")

        result = _find_common_ancestor_mapped_group(op1, op2)
        assert result is None

    def test_no_common_group_no_mapped_groups(self):
        """Tasks not in any mapped group should return None."""
        with DAG("dag1"):
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")

        result = _find_common_ancestor_mapped_group(op1, op2)
        assert result is None

    def test_no_dag_returns_none(self):
        """Tasks without DAG should return None."""
        op1 = BaseOperator(task_id="op1")
        op2 = BaseOperator(task_id="op2")

        # Function should handle operators not assigned to a DAG gracefully
        result = _find_common_ancestor_mapped_group(op1, op2)
        assert result is None


class TestIsFurtherMappedInside:
    """Tests for _is_further_mapped_inside function."""

    def test_mapped_operator_returns_true(self):
        """A mapped operator should return True."""
        with DAG("dag1"):
            with TaskGroup("tg") as tg:
                op = BaseOperator(task_id="op")

        # Simulate a mapped operator
        op._is_mapped = True

        result = _is_further_mapped_inside(op, tg)
        assert result is True

    def test_non_mapped_operator_returns_false(self):
        """A non-mapped operator with no mapped parent groups should return False."""
        with DAG("dag1"):
            with TaskGroup("tg") as tg:
                op = BaseOperator(task_id="op")

        result = _is_further_mapped_inside(op, tg)
        assert result is False


class TestGetTiCountForTask:
    """Tests for get_ti_count_for_task function."""

    def test_queries_supervisor(self, mock_supervisor_comms):
        """Should send GetTICount message to supervisor with task_ids."""
        from airflow.sdk.execution_time.comms import TICount

        mock_supervisor_comms.send.return_value = TICount(count=3)

        result = get_ti_count_for_task("task_id", "dag_id", "run_id")

        assert result == 3
        mock_supervisor_comms.send.assert_called_once()
        call_args = mock_supervisor_comms.send.call_args[0][0]
        assert call_args.dag_id == "dag_id"
        assert call_args.task_ids == ["task_id"]
        assert call_args.run_ids == ["run_id"]


class TestGetRelevantMapIndexes:
    """Tests for get_relevant_map_indexes function."""

    def test_returns_none_when_no_ti_count(self):
        """Should return None when ti_count is 0 or None."""
        with DAG("dag1"):
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")

        result = get_relevant_map_indexes(
            task=op1,
            run_id="run_id",
            map_index=0,
            ti_count=0,
            relative=op2,
            dag_id="dag1",
        )
        assert result is None

    def test_returns_none_when_no_common_ancestor(self):
        """Should return None when tasks have no common mapped ancestor."""
        with DAG("dag1"):
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")

        result = get_relevant_map_indexes(
            task=op1,
            run_id="run_id",
            map_index=0,
            ti_count=3,
            relative=op2,
            dag_id="dag1",
        )
        assert result is None

    def test_same_mapped_group_returns_single_index(self, mock_supervisor_comms):
        """Tasks in same mapped group should get single index matching their map_index."""
        with DAG("dag1"):
            with TaskGroup("tg"):
                op1 = BaseOperator(task_id="op1")
                op2 = BaseOperator(task_id="op2")
                op1 >> op2

        # Mock iter_mapped_task_groups to simulate a mapped task group
        mock_mapped_tg = MagicMock(spec=TaskGroup)
        mock_mapped_tg.group_id = "tg"
        op1.iter_mapped_task_groups = MagicMock(spec=TaskGroup, return_value=iter([mock_mapped_tg]))
        op2.iter_mapped_task_groups = MagicMock(spec=TaskGroup, return_value=iter([mock_mapped_tg]))

        # Mock: op2 has 3 TIs (mapped by 3)
        mock_supervisor_comms.send.return_value = TICount(count=3)

        # For map_index=1 with ti_count=3, should return 1 (same index)
        result = get_relevant_map_indexes(
            task=op2,
            run_id="run_id",
            map_index=1,
            ti_count=3,
            relative=op1,
            dag_id="dag1",
        )
        assert result == 1

    def test_unmapped_task_pulling_from_mapped_returns_none(self):
        """Unmapped task pulling from mapped upstream should return None (pull all)."""
        with DAG("dag1"):
            op1 = BaseOperator(task_id="op1")
            op2 = BaseOperator(task_id="op2")
            op1 >> op2

        # op2 is not in a mapped group, so there's no common ancestor
        result = get_relevant_map_indexes(
            task=op2,
            run_id="run_id",
            map_index=0,
            ti_count=1,
            relative=op1,
            dag_id="dag1",
        )
        assert result is None
