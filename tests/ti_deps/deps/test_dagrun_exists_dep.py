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

from unittest.mock import Mock, patch

from airflow.models import DAG, DagRun
from airflow.ti_deps.deps.dagrun_exists_dep import DagrunRunningDep
from airflow.utils.state import State


class TestDagrunRunningDep:
    @patch("airflow.models.DagRun.find", return_value=())
    def test_dagrun_doesnt_exist(self, mock_dagrun_find):
        """
        Task instances without dagruns should fail this dep
        """
        dag = DAG("test_dag", max_active_runs=2)
        dagrun = DagRun(state=State.QUEUED)
        ti = Mock(task=Mock(dag=dag), get_dagrun=Mock(return_value=dagrun))
        assert not DagrunRunningDep().is_met(ti=ti)

    def test_dagrun_exists(self):
        """
        Task instances with a dagrun should pass this dep
        """
        dagrun = DagRun(state=State.RUNNING)
        ti = Mock(get_dagrun=Mock(return_value=dagrun))
        assert DagrunRunningDep().is_met(ti=ti)
