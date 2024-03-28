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

from datetime import datetime
from unittest.mock import Mock

import pytest

from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.utils.session import provide_session
from airflow.utils.state import State

pytestmark = pytest.mark.db_test


class DummyDep(BaseTIDep):
    """
    A Dep class for testing, whose TI_SCHEDULE_DECISION is True
    i.e. this Dep will be used by the DagRun to make decision if
    a TI should be scheduled or not.
    """

    NAME = "Dep for Testing"
    IGNORABLE = False
    TI_SCHEDULE_DECISION = True

    @provide_session
    def _get_dep_statuses(self, ti, session, dep_context=None):
        yield self._failing_status(reason="Always fail, for testing purpose")


class TestDepWithTIScheduleDecision:
    def test_cxt_ti_schedule_decision_false(self):
        """
        Check the DummyDep (whose TI_SCHEDULE_DECISION is True)
        with default DepContext (ti_schedule_decision=False)

        It should always pass.
        """
        ti = Mock(state=State.QUEUED, end_date=datetime(2016, 1, 1))
        assert DummyDep().is_met(ti=ti)

    def test_cxt_ti_schedule_decision_true(self):
        """
        Check the DummyDep (whose TI_SCHEDULE_DECISION is True)
        with DepContext(ti_schedule_decision=True)

        _get_dep_statuses() in the dep should be invoked.
        """
        ti = Mock(state=State.QUEUED, end_date=datetime(2016, 1, 1))
        assert not DummyDep().is_met(ti=ti, dep_context=DepContext(ti_schedule_decision=True))
