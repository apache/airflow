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

from datetime import timedelta
from unittest import mock

import pytest
import time_machine

from airflow._shared.timezones.timezone import datetime
from airflow.models import TaskInstance
from airflow.serialization.definitions.baseoperator import SerializedBaseOperator
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.utils.state import State

pytestmark = pytest.mark.db_test


class TestNotInRetryPeriodDep:
    def _get_task_instance(self, state, end_date=None, retry_delay=timedelta(minutes=15)):
        task = SerializedBaseOperator(task_id="fake")
        task.retry_delay = retry_delay
        task.retry_exponential_backoff = 0
        ti = TaskInstance(task=task, state=state, dag_version_id=mock.MagicMock())
        ti.end_date = end_date
        return ti

    @time_machine.travel("2016-01-01 15:44")
    def test_still_in_retry_period(self):
        """
        Task instances that are in their retry period should fail this dep
        """
        ti = self._get_task_instance(State.UP_FOR_RETRY, end_date=datetime(2016, 1, 1, 15, 30))
        assert ti.is_premature
        assert not NotInRetryPeriodDep().is_met(ti=ti)

    @time_machine.travel("2016-01-01 15:46")
    def test_retry_period_finished(self):
        """
        Task instance's that have had their retry period elapse should pass this dep
        """
        ti = self._get_task_instance(State.UP_FOR_RETRY, end_date=datetime(2016, 1, 1))
        assert not ti.is_premature
        assert NotInRetryPeriodDep().is_met(ti=ti)

    def test_not_in_retry_period(self):
        """
        Task instance's that are not up for retry can not be in their retry period
        """
        ti = self._get_task_instance(State.SUCCESS)
        assert NotInRetryPeriodDep().is_met(ti=ti)
