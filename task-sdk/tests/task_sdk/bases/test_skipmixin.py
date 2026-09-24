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

from unittest.mock import MagicMock, Mock

import pytest

from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.skipmixin import (
    XCOM_SKIPMIXIN_FOLLOWED,
    XCOM_SKIPMIXIN_KEY,
    XCOM_SKIPMIXIN_SKIPPED,
    SkipMixin,
    _ensure_tasks,
)
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.exceptions import AirflowException, DownstreamTasksSkipped
from airflow.sdk.types import RuntimeTaskInstanceProtocol


class TestEnsureTasks:
    def test_filters_non_operators(self):
        """Only BaseOperator and MappedOperator instances should be returned."""
        op = MagicMock(spec=BaseOperator)
        mapped = MagicMock(spec=MappedOperator)
        other = MagicMock()  # not an operator

        result = _ensure_tasks([op, mapped, other])
        assert result == [op, mapped]

    def test_empty_input(self):
        assert _ensure_tasks([]) == []


class TestSkipMixin:
    def test_skip_pushes_xcom_and_raises(self):
        """skip() should push skipped task IDs to XCom and raise DownstreamTasksSkipped."""
        mixin = SkipMixin()
        mixin.task_id = "branch_task"

        task1 = MagicMock(spec=BaseOperator, task_id="task1", wait_for_past_depends_before_skipping=False)
        mock_task = MagicMock(spec=BaseOperator)
        mock_task.get_direct_relative_ids.return_value = {"task1"}
        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            mixin.skip(ti=ti, tasks=[task1])

        ti.xcom_push.assert_called_once_with(
            key=XCOM_SKIPMIXIN_KEY,
            value={XCOM_SKIPMIXIN_SKIPPED: ["task1"]},
        )
        assert exc_info.value.tasks == ["task1"]

    def test_skip_none_tasks(self):
        """skip() should return None when no tasks are provided."""
        ti = Mock(spec=RuntimeTaskInstanceProtocol)
        assert SkipMixin().skip(ti=ti, tasks=[]) is None

    def test_skip_mapped_task_does_not_raise(self):
        """skip() should not raise when map_index != -1 (mapped tasks)."""
        mixin = SkipMixin()
        mixin.task_id = "branch_task"

        task1 = MagicMock(spec=BaseOperator, task_id="task1")
        mock_task = MagicMock(spec=BaseOperator)
        mock_task.get_direct_relative_ids.return_value = {"task1"}
        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=2, task=mock_task)

        # Should not raise — mapped tasks are handled by NotPreviouslySkippedDep
        mixin.skip(ti=ti, tasks=[task1])

    def test_skip_excludes_wait_for_past_depends_task(self):
        """skip() must not force-skip a DIRECT downstream task with
        wait_for_past_depends_before_skipping=True.

        Regression test for https://github.com/apache/airflow/issues/55146: force-skipping
        such a task here bypasses NotPreviouslySkippedDep entirely, so the flag never gets a
        chance to hold the task back when its own past depends are not met.
        """
        mixin = SkipMixin()
        mixin.task_id = "branch_task"

        task1 = MagicMock(spec=BaseOperator, task_id="task1", wait_for_past_depends_before_skipping=False)
        task2 = MagicMock(spec=BaseOperator, task_id="task2", wait_for_past_depends_before_skipping=True)
        mock_task = MagicMock(spec=BaseOperator)
        mock_task.get_direct_relative_ids.return_value = {"task1", "task2"}
        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            mixin.skip(ti=ti, tasks=[task1, task2])

        # Both tasks are still recorded in the SkipMixin XCom, so NotPreviouslySkippedDep can
        # apply the wait_for_past_depends_before_skipping check once it evaluates task2.
        ti.xcom_push.assert_called_once_with(
            key=XCOM_SKIPMIXIN_KEY,
            value={XCOM_SKIPMIXIN_SKIPPED: ["task1", "task2"]},
        )
        # Only task1 is force-skipped immediately; task2 is deferred to the dependency check.
        assert exc_info.value.tasks == ["task1"]

    def test_skip_indirect_wait_for_past_depends_task_is_force_skipped(self):
        """skip() must still force-skip a wait_for_past_depends_before_skipping=True task that
        is reached through an intermediate hop, not just a direct downstream relative.

        Regression test for the review on https://github.com/apache/airflow/pull/73470:
        NotPreviouslySkippedDep only consults a task's DIRECT upstream relatives, so deferring
        the skip only works one hop away from the SkipMixin task. ``gate >> middle >> target``,
        where only ``target`` sets the flag, is exactly ShortCircuitOperator's default
        ignore_downstream_trigger_rules=True mode, which flattens the whole subtree into a
        single ``skip()`` call. Deferring ``target`` there would leave it unskipped forever,
        since ``middle`` never runs SkipMixin.skip() itself and so never records anything in
        XCom for NotPreviouslySkippedDep to find. Preserve the pre-flag default instead: skip
        the whole subtree immediately.
        """
        mixin = SkipMixin()
        mixin.task_id = "gate"

        middle = MagicMock(spec=BaseOperator, task_id="middle", wait_for_past_depends_before_skipping=False)
        target = MagicMock(spec=BaseOperator, task_id="target", wait_for_past_depends_before_skipping=True)
        mock_task = MagicMock(spec=BaseOperator)
        # Only "middle" is a direct downstream relative of "gate"; "target" is one hop further.
        mock_task.get_direct_relative_ids.return_value = {"middle"}
        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            mixin.skip(ti=ti, tasks=[middle, target])

        ti.xcom_push.assert_called_once_with(
            key=XCOM_SKIPMIXIN_KEY,
            value={XCOM_SKIPMIXIN_SKIPPED: ["middle", "target"]},
        )
        # Both are force-skipped: "target" has the flag but isn't a direct downstream relative,
        # so NotPreviouslySkippedDep could never reach it through "middle".
        assert exc_info.value.tasks == ["middle", "target"]

    def test_skip_without_task_id_does_not_push_xcom(self):
        """skip() should not push XCom when the mixin has no task_id."""
        mixin = SkipMixin()
        # No task_id attribute set

        task1 = MagicMock(spec=BaseOperator, task_id="task1", wait_for_past_depends_before_skipping=False)
        mock_task = MagicMock(spec=BaseOperator)
        mock_task.get_direct_relative_ids.return_value = {"task1"}
        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)

        with pytest.raises(DownstreamTasksSkipped):
            mixin.skip(ti=ti, tasks=[task1])

        ti.xcom_push.assert_not_called()

    def test_skip_all_except_with_none_skips_all(self):
        """skip_all_except(None) should skip all downstream tasks."""
        mixin = SkipMixin()

        downstream1 = MagicMock(
            spec=BaseOperator, task_id="down1", wait_for_past_depends_before_skipping=False
        )
        downstream2 = MagicMock(
            spec=BaseOperator, task_id="down2", wait_for_past_depends_before_skipping=False
        )

        mock_task = MagicMock(spec=BaseOperator)
        mock_task.downstream_list = [downstream1, downstream2]
        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_ids = ["task1", "down1", "down2"]
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            mixin.skip_all_except(ti=ti, branch_task_ids=None)

        assert set(exc_info.value.tasks) == {("down1", -1), ("down2", -1)}

    def test_skip_all_except_with_string_branch(self):
        """skip_all_except('down1') should skip down2 but not down1."""
        mixin = SkipMixin()

        downstream1 = MagicMock(
            spec=BaseOperator, task_id="down1", wait_for_past_depends_before_skipping=False
        )
        downstream1.get_flat_relative_ids.return_value = set()
        downstream2 = MagicMock(
            spec=BaseOperator, task_id="down2", wait_for_past_depends_before_skipping=False
        )

        mock_task = MagicMock(spec=BaseOperator)
        mock_task.downstream_list = [downstream1, downstream2]
        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_ids = ["task1", "down1", "down2"]
        mock_dag.get_task.return_value = downstream1
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            mixin.skip_all_except(ti=ti, branch_task_ids="down1")

        assert exc_info.value.tasks == [("down2", -1)]
        ti.xcom_push.assert_called_once_with(
            key=XCOM_SKIPMIXIN_KEY,
            value={XCOM_SKIPMIXIN_FOLLOWED: ["down1"]},
        )

    def test_skip_all_except_invalid_type_raises(self):
        """skip_all_except() should raise when branch_task_ids is an invalid type."""
        mixin = SkipMixin()
        mock_task = MagicMock(spec=BaseOperator)
        mock_task.dag = MagicMock(spec=DAG)
        ti = Mock(spec=RuntimeTaskInstanceProtocol, task=mock_task)

        with pytest.raises(AirflowException, match="must be either None, a task ID, or an Iterable"):
            mixin.skip_all_except(ti=ti, branch_task_ids=42)

    def test_skip_all_except_invalid_iterable_element_raises(self):
        """skip_all_except() should raise when branch_task_ids contains non-string elements."""
        mixin = SkipMixin()
        mock_task = MagicMock(spec=BaseOperator)
        mock_task.dag = MagicMock(spec=DAG)
        ti = Mock(spec=RuntimeTaskInstanceProtocol, task=mock_task)

        with pytest.raises(AirflowException, match="invalid 'branch_task_ids'"):
            mixin.skip_all_except(ti=ti, branch_task_ids=["task1", 42])

    def test_skip_all_except_invalid_task_id_raises(self):
        """skip_all_except() should raise when branch_task_ids contains non-existent task IDs."""
        mixin = SkipMixin()

        mock_task = MagicMock(spec=BaseOperator)
        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_ids = ["task1", "down1"]
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, task=mock_task)

        with pytest.raises(AirflowException, match="must contain only valid task_ids"):
            mixin.skip_all_except(ti=ti, branch_task_ids="nonexistent")

    def test_skip_all_except_excludes_wait_for_past_depends_task(self):
        """skip_all_except() must not force-skip a task with wait_for_past_depends_before_skipping=True.

        Regression test for https://github.com/apache/airflow/issues/55146, the BranchPythonOperator
        counterpart of the ShortCircuitOperator case: force-skipping the not-taken branch bypasses
        NotPreviouslySkippedDep, so the flag never gets to hold the task back.
        """
        mixin = SkipMixin()

        downstream1 = MagicMock(spec=BaseOperator, task_id="down1")
        downstream1.get_flat_relative_ids.return_value = set()
        downstream2 = MagicMock(
            spec=BaseOperator, task_id="down2", wait_for_past_depends_before_skipping=True
        )

        mock_task = MagicMock(spec=BaseOperator)
        mock_task.downstream_list = [downstream1, downstream2]
        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_ids = ["task1", "down1", "down2"]
        mock_dag.get_task.return_value = downstream1
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)

        # down1 is the branch taken, so nothing is force-skipped: down2 (not taken) is the
        # only candidate, and it must be deferred rather than force-skipped.
        mixin.skip_all_except(ti=ti, branch_task_ids="down1")

        ti.xcom_push.assert_called_once_with(
            key=XCOM_SKIPMIXIN_KEY,
            value={XCOM_SKIPMIXIN_FOLLOWED: ["down1"]},
        )

    def test_skip_all_except_generator_branch_task_ids(self):
        """skip_all_except() should handle generator branch_task_ids."""
        mixin = SkipMixin()

        downstream1 = MagicMock(
            spec=BaseOperator, task_id="down1", wait_for_past_depends_before_skipping=False
        )
        downstream1.get_flat_relative_ids.return_value = set()
        downstream2 = MagicMock(
            spec=BaseOperator, task_id="down2", wait_for_past_depends_before_skipping=False
        )

        mock_task = MagicMock(spec=BaseOperator)
        mock_task.downstream_list = [downstream1, downstream2]
        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_ids = ["task1", "down1", "down2"]
        mock_dag.get_task.return_value = downstream1
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)

        def gen():
            yield "down1"

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            mixin.skip_all_except(ti=ti, branch_task_ids=gen())

        assert exc_info.value.tasks == [("down2", -1)]
