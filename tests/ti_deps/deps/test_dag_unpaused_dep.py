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

from unittest.mock import ANY, Mock, call, patch

import pytest

from airflow.models import TaskInstance
from airflow.ti_deps.deps.dag_unpaused_dep import DagUnpausedDep

pytestmark = pytest.mark.db_test


class TestDagUnpausedDep:
    @patch.object(DagUnpausedDep, "_get_is_paused", return_value=True)
    def test_concurrency_reached(self, mock_get_is_paused):
        """
        Test paused DAG should fail dependency
        """
        ti = TaskInstance(task=Mock(dag_id="mock_dag_id"))
        assert not DagUnpausedDep().is_met(ti=ti)
        assert mock_get_is_paused.mock_calls == [call(dag_id="mock_dag_id", session=ANY)]

    @patch.object(DagUnpausedDep, "_get_is_paused", return_value=False)
    def test_all_conditions_met(self, mock_get_is_paused):
        """
        Test all conditions met should pass dep
        """
        ti = TaskInstance(task=Mock(dag_id="mock_dag_id"))
        assert DagUnpausedDep().is_met(ti=ti)
        assert mock_get_is_paused.mock_calls == [call(dag_id="mock_dag_id", session=ANY)]
