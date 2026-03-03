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

from airflow.sdk.bases.branch import BaseBranchOperator, BranchMixIn
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.taskgroup import TaskGroup
from airflow.sdk.exceptions import DownstreamTasksSkipped
from airflow.sdk.types import RuntimeTaskInstanceProtocol


class TestBranchMixIn:
    def test_do_branch_with_none_skips_all(self):
        """do_branch(context, None) should skip all downstream tasks."""
        mixin = BranchMixIn()

        downstream1 = MagicMock(spec=BaseOperator, task_id="down1")
        downstream2 = MagicMock(spec=BaseOperator, task_id="down2")

        mock_task = MagicMock(spec=BaseOperator)
        mock_task.downstream_list = [downstream1, downstream2]
        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_ids = ["branch", "down1", "down2"]
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)
        context = {"ti": ti}

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            mixin.do_branch(context, None)

        assert set(exc_info.value.tasks) == {("down1", -1), ("down2", -1)}

    def test_do_branch_with_string(self):
        """do_branch(context, 'down1') should follow down1 and skip others."""
        mixin = BranchMixIn()

        downstream1 = MagicMock(spec=BaseOperator, task_id="down1")
        downstream1.get_flat_relative_ids.return_value = set()
        downstream2 = MagicMock(spec=BaseOperator, task_id="down2")

        mock_task = MagicMock(spec=BaseOperator)
        mock_task.downstream_list = [downstream1, downstream2]
        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_ids = ["branch", "down1", "down2"]
        mock_dag.get_task.return_value = downstream1
        mock_dag.task_group_dict = {}
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, map_index=-1, task=mock_task)
        context = {"ti": ti}

        with pytest.raises(DownstreamTasksSkipped) as exc_info:
            mixin.do_branch(context, "down1")

        assert exc_info.value.tasks == [("down2", -1)]

    def test_expand_task_group_roots(self):
        """_expand_task_group_roots should expand task group into root task IDs."""
        mixin = BranchMixIn()

        mock_tg = MagicMock(spec=TaskGroup)
        mock_root1 = MagicMock(spec=BaseOperator, task_id="tg.root1")
        mock_root2 = MagicMock(spec=BaseOperator, task_id="tg.root2")
        mock_tg.roots = [mock_root1, mock_root2]
        mock_tg.group_id = "tg"

        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_group_dict = {"tg": mock_tg}
        mock_task = MagicMock(spec=BaseOperator)
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, task=mock_task)

        result = list(mixin._expand_task_group_roots(ti, ["tg"]))
        assert result == ["tg.root1", "tg.root2"]

    def test_expand_task_group_roots_passthrough(self):
        """_expand_task_group_roots should pass through regular task IDs."""
        mixin = BranchMixIn()

        mock_dag = MagicMock(spec=DAG)
        mock_dag.task_group_dict = {}
        mock_task = MagicMock(spec=BaseOperator)
        mock_task.dag = mock_dag

        ti = Mock(spec=RuntimeTaskInstanceProtocol, task=mock_task)

        result = list(mixin._expand_task_group_roots(ti, "regular_task"))
        assert result == ["regular_task"]


class TestBaseBranchOperator:
    def test_choose_branch_not_implemented(self):
        """BaseBranchOperator.choose_branch should raise NotImplementedError."""
        op = BaseBranchOperator.__new__(BaseBranchOperator)
        with pytest.raises(NotImplementedError):
            op.choose_branch({})

    def test_inherits_from_skipmixin_flag(self):
        assert BaseBranchOperator.inherits_from_skipmixin is True
