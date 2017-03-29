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

from airflow.ti_deps.deps.dag_ti_slots_available_dep import DagTISlotsAvailableDep
from fake_models import FakeDag, FakeTask, FakeTI


class DagTISlotsAvailableDepTest(unittest.TestCase):

    def test_concurrency_reached(self):
        """
        Test concurrency reached should fail dep
        """
        dag = FakeDag(concurrency=1, concurrency_reached=True)
        task = FakeTask(dag=dag)
        ti = FakeTI(task=task, dag_id="fake_dag")

        self.assertFalse(DagTISlotsAvailableDep().is_met(ti=ti, dep_context=None))

    def test_all_conditions_met(self):
        """
        Test all conditions met should pass dep
        """
        dag = FakeDag(concurrency=1, concurrency_reached=False)
        task = FakeTask(dag=dag)
        ti = FakeTI(task=task, dag_id="fake_dag")

        self.assertTrue(DagTISlotsAvailableDep().is_met(ti=ti, dep_context=None))
