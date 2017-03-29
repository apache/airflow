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

from airflow.ti_deps.deps.dagrun_exists_dep import DagrunRunningDep
from fake_models import FakeDag, FakeTask, FakeTI


class DagrunRunningDepTest(unittest.TestCase):

    def test_dagrun_doesnt_exist(self):
        """
        Task instances without dagruns should fail this dep
        """
        dag = FakeDag(running_dagruns=[], max_active_runs=1)
        task = FakeTask(dag=dag)
        ti = FakeTI(dagrun=None, task=task, dag_id="fake_dag")

        self.assertFalse(DagrunRunningDep().is_met(ti=ti, dep_context=None))

    def test_dagrun_exists(self):
        """
        Task instances with a dagrun should pass this dep
        """
        dag = FakeDag(running_dagruns=[], max_active_runs=1)
        task = FakeTask(dag=dag)
        ti = FakeTI(dagrun="Fake Dagrun", task=task, dag_id="fake_dag")

        self.assertTrue(DagrunRunningDep().is_met(ti=ti, dep_context=None))
