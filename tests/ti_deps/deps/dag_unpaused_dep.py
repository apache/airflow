# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest

from airflow.ti_deps.deps.dag_unpaused_dep import DagUnpausedDep
from fake_models import FakeDag, FakeTask, FakeTI


class DagUnpausedDepTest(unittest.TestCase):

    def test_concurrency_reached(self):
        """
        Test paused DAG should fail dependency
        """
        dag = FakeDag(is_paused=True)
        task = FakeTask(dag=dag)
        ti = FakeTI(task=task, dag_id="fake_dag")

        self.assertFalse(DagUnpausedDep().is_met(ti=ti, dep_context=None))

    def test_all_conditions_met(self):
        """
        Test all conditions met should pass dep
        """
        dag = FakeDag(is_paused=False)
        task = FakeTask(dag=dag)
        ti = FakeTI(task=task, dag_id="fake_dag")

        self.assertTrue(DagUnpausedDep().is_met(ti=ti, dep_context=None))
