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
from datetime import datetime, timedelta

from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.utils.state import State
from fake_models import FakeDag, FakeTask, FakeTI


class NotInRetryPeriodDepTest(unittest.TestCase):

    def test_still_in_retry_period(self):
        """
        Task instances that are in their retry period should fail this dep
        """
        dag = FakeDag()
        task = FakeTask(dag=dag, retry_delay=timedelta(minutes=1))
        ti = FakeTI(
            task=task,
            state=State.UP_FOR_RETRY,
            end_date=datetime(2016, 1, 1),
            is_premature=True)

        self.assertFalse(NotInRetryPeriodDep().is_met(ti=ti, dep_context=None))

    def test_retry_period_finished(self):
        """
        Task instance's that have had their retry period elapse should pass this dep
        """
        dag = FakeDag()
        task = FakeTask(dag=dag, retry_delay=timedelta(minutes=1))
        ti = FakeTI(
            task=task,
            state=State.UP_FOR_RETRY,
            end_date=datetime(2016, 1, 1),
            is_premature=False)

        self.assertTrue(NotInRetryPeriodDep().is_met(ti=ti, dep_context=None))

    def test_not_in_retry_period(self):
        """
        Task instance's that are not up for retry can not be in their retry period
        """
        dag = FakeDag()
        task = FakeTask(dag=dag)
        ti = FakeTI(task=task, state=State.SUCCESS)

        self.assertTrue(NotInRetryPeriodDep().is_met(ti=ti, dep_context=None))
