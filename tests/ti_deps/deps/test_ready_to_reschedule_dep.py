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
from unittest.mock import patch

import pytest
import time_machine
from slugify import slugify

from airflow.models.taskreschedule import TaskReschedule
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.deps.ready_to_reschedule import ReadyToRescheduleDep
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.test_utils import db

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2016, 1, 1)


@pytest.fixture
def not_expected_tr_db_call():
    def side_effect(*args, **kwargs):
        raise SystemError("Not expected DB call to `TaskReschedule` statement.")

    with patch("airflow.models.taskreschedule.TaskReschedule.stmt_for_task_instance") as m:
        m.side_effect = side_effect
        yield m


class TestNotInReschedulePeriodDep:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, request, create_task_instance):
        db.clear_db_runs()
        db.clear_rendered_ti_fields()

        self.dag_id = f"dag_{slugify(request.cls.__name__)}"
        self.task_id = f"task_{slugify(request.node.name, max_length=40)}"
        self.run_id = f"run_{slugify(request.node.name, max_length=40)}"
        self.ti_maker = create_task_instance

        with time_machine.travel(DEFAULT_DATE, tick=False):
            yield
        db.clear_rendered_ti_fields()
        db.clear_db_runs()

    def _get_task_instance(self, state, *, map_index=-1):
        """Helper which create fake task_instance"""
        ti = self.ti_maker(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            execution_date=DEFAULT_DATE,
            map_index=map_index,
            state=state,
        )
        ti.task.reschedule = True
        return ti

    def _create_task_reschedule(self, ti, minutes: int | list[int]):
        """Helper which create fake task_reschedule(s) from task_instance."""
        if isinstance(minutes, int):
            minutes = [minutes]
        trs = []
        for minutes_timedelta in minutes:
            dt = ti.execution_date + timedelta(minutes=minutes_timedelta)
            trs.append(
                TaskReschedule(
                    task=ti.task,
                    run_id=ti.run_id,
                    try_number=ti.try_number,
                    map_index=ti.map_index,
                    start_date=dt,
                    end_date=dt,
                    reschedule_date=dt,
                )
            )
        with create_session() as session:
            session.add_all(trs)
            session.commit()

    def test_should_pass_if_ignore_in_reschedule_period_is_set(self, not_expected_tr_db_call):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        dep_context = DepContext(ignore_in_reschedule_period=True)
        assert ReadyToRescheduleDep().is_met(ti=ti, dep_context=dep_context)

    def test_should_pass_if_not_reschedule_mode(self, not_expected_tr_db_call):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        del ti.task.reschedule
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_should_pass_if_not_in_none_state(self, not_expected_tr_db_call):
        ti = self._get_task_instance(State.UP_FOR_RETRY)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_should_pass_if_no_reschedule_record_exists(self):
        ti = self._get_task_instance(State.NONE)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_should_pass_after_reschedule_date_one(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        self._create_task_reschedule(ti, -1)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_should_pass_after_reschedule_date_multiple(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        self._create_task_reschedule(ti, [-21, -11, -1])
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_should_fail_before_reschedule_date_one(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        self._create_task_reschedule(ti, 1)
        assert not ReadyToRescheduleDep().is_met(ti=ti)

    def test_should_fail_before_reschedule_date_multiple(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE)
        self._create_task_reschedule(ti, [-19, -9, 1])
        # Last TaskReschedule doesn't meet requirements
        assert not ReadyToRescheduleDep().is_met(ti=ti)

    def test_mapped_task_should_pass_if_ignore_in_reschedule_period_is_set(self, not_expected_tr_db_call):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE, map_index=42)
        dep_context = DepContext(ignore_in_reschedule_period=True)
        assert ReadyToRescheduleDep().is_met(ti=ti, dep_context=dep_context)

    def test_mapped_task_should_pass_if_not_reschedule_mode(self, not_expected_tr_db_call):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE, map_index=42)
        del ti.task.reschedule
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_mapped_task_should_pass_if_not_in_none_state(self, not_expected_tr_db_call):
        ti = self._get_task_instance(State.UP_FOR_RETRY, map_index=42)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_mapped_should_pass_if_no_reschedule_record_exists(self):
        ti = self._get_task_instance(State.NONE, map_index=42)
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_mapped_should_pass_after_reschedule_date_one(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE, map_index=42)
        self._create_task_reschedule(ti, [-1])
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_mapped_task_should_pass_after_reschedule_date_multiple(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE, map_index=42)
        self._create_task_reschedule(ti, [-21, -11, -1])
        assert ReadyToRescheduleDep().is_met(ti=ti)

    def test_mapped_task_should_fail_before_reschedule_date_one(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE, map_index=42)
        self._create_task_reschedule(ti, 1)
        assert not ReadyToRescheduleDep().is_met(ti=ti)

    def test_mapped_task_should_fail_before_reschedule_date_multiple(self):
        ti = self._get_task_instance(State.UP_FOR_RESCHEDULE, map_index=42)
        self._create_task_reschedule(ti, [-19, -9, 1])
        # Last TaskReschedule doesn't meet requirements
        assert not ReadyToRescheduleDep().is_met(ti=ti)
