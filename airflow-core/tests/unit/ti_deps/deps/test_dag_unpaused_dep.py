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

from unittest import mock

import pytest

from airflow.models import TaskInstance
from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.ti_deps.deps.dag_unpaused_dep import DagUnpausedDep

pytestmark = pytest.mark.db_test


@mock.patch.object(DagUnpausedDep, "_is_dag_paused")
class TestDagUnpausedDep:
    def test_concurrency_reached(self, mock_is_dag_paused):
        """
        Test paused DAG should fail dependency
        """
        mock_is_dag_paused.return_value = True
        task = SerializedBaseOperator(task_id="op")
        ti = TaskInstance(task=task, dag_version_id=mock.MagicMock())

        assert not DagUnpausedDep().is_met(ti=ti)

    def test_all_conditions_met(self, mock_is_dag_paused):
        """
        Test all conditions met should pass dep
        """
        mock_is_dag_paused.return_value = False
        task = SerializedBaseOperator(task_id="op")
        ti = TaskInstance(task=task, dag_version_id=mock.MagicMock())

        assert DagUnpausedDep().is_met(ti=ti)
