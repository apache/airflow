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
from freezegun import freeze_time
from mock import Mock

from airflow.models import TaskInstance
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.utils.state import State


class NotInRetryPeriodDepTest(unittest.TestCase):

    def _get_task_instance(self, state, end_date=None,
                           retry_delay=timedelta(minutes=15)):
        task = Mock(retry_delay=retry_delay, retry_exponential_backoff=False)
        ti = TaskInstance(task=task, state=state, execution_date=None)
        ti.end_date = end_date
        return ti

    @freeze_time('2016-01-01 15:44')
    def test_still_in_retry_period(self):
        """
        Task instances that are in their retry period should fail this dep
        """
        ti = self._get_task_instance(State.UP_FOR_RETRY,
                                     end_date=datetime(2016, 1, 1, 15, 30))
        self.assertTrue(ti.is_premature)
        self.assertFalse(NotInRetryPeriodDep().is_met(ti=ti))

    @freeze_time('2016-01-01 15:46')
    def test_retry_period_finished(self):
        """
        Task instance's that have had their retry period elapse should pass this dep
        """
        ti = self._get_task_instance(State.UP_FOR_RETRY,
                                     end_date=datetime(2016, 1, 1))
        self.assertFalse(ti.is_premature)
        self.assertTrue(NotInRetryPeriodDep().is_met(ti=ti))

    def test_not_in_retry_period(self):
        """
        Task instance's that are not up for retry can not be in their retry period
        """
        ti = self._get_task_instance(State.SUCCESS)
        self.assertTrue(NotInRetryPeriodDep().is_met(ti=ti))
