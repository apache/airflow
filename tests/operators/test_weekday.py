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

import datetime
import unittest

import pytest
from freezegun import freeze_time
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.operators.dummy import DummyOperator
from airflow.operators.weekday import BranchDayOfWeekOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.weekday import WeekDay

DEFAULT_DATE = timezone.datetime(2020, 2, 5)  # Wednesday
INTERVAL = datetime.timedelta(hours=12)


class TestBranchDayOfWeekOperator(unittest.TestCase):
    """
    Tests for BranchDayOfWeekOperator
    """

    @classmethod
    def setUpClass(cls):

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        self.dag = DAG(
            "branch_day_of_week_operator_test",
            start_date=DEFAULT_DATE,
            schedule_interval=INTERVAL,
        )
        self.branch_1 = DummyOperator(task_id="branch_1", dag=self.dag)
        self.branch_2 = DummyOperator(task_id="branch_2", dag=self.dag)
        self.branch_3 = None

    def tearDown(self):

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def _assert_task_ids_match_states(self, dr, task_ids_to_states):
        """Helper that asserts task instances with a given id are in a given state"""
        tis = dr.get_task_instances()
        for ti in tis:
            try:
                expected_state = task_ids_to_states[ti.task_id]
            except KeyError:
                raise ValueError(f'Invalid task id {ti.task_id} found!')
            else:
                self.assertEqual(
                    ti.state,
                    expected_state,
                    f"Task {ti.task_id} has state {ti.state} instead of expected {expected_state}",
                )

    @parameterized.expand(
        [
            ("with-string", "Monday"),
            ("with-enum", WeekDay.MONDAY),
            ("with-enum-set", {WeekDay.MONDAY}),
            ("with-enum-set-2-items", {WeekDay.MONDAY, WeekDay.FRIDAY}),
            ("with-string-set", {"Monday"}),
            ("with-string-set-2-items", {"Monday", "Friday"}),
        ]
    )
    @freeze_time("2021-01-25")  # Monday
    def test_branch_follow_true(self, _, weekday):
        """Checks if BranchDayOfWeekOperator follows true branch"""
        print(datetime.datetime.now())
        branch_op = BranchDayOfWeekOperator(
            task_id="make_choice",
            follow_task_ids_if_true=["branch_1", "branch_2"],
            follow_task_ids_if_false="branch_3",
            week_day=weekday,
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.branch_3 = DummyOperator(task_id="branch_3", dag=self.dag)
        self.branch_3.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self._assert_task_ids_match_states(
            dr,
            {
                'make_choice': State.SUCCESS,
                'branch_1': State.NONE,
                'branch_2': State.NONE,
                'branch_3': State.SKIPPED,
            },
        )

    @freeze_time("2021-01-25")  # Monday
    def test_branch_follow_true_with_execution_date(self):
        """Checks if BranchDayOfWeekOperator follows true branch when set use_task_execution_day """

        branch_op = BranchDayOfWeekOperator(
            task_id="make_choice",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            week_day="Wednesday",
            use_task_execution_day=True,  # We compare to DEFAULT_DATE which is Wednesday
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self._assert_task_ids_match_states(
            dr,
            {
                'make_choice': State.SUCCESS,
                'branch_1': State.NONE,
                'branch_2': State.SKIPPED,
            },
        )

    @freeze_time("2021-01-25")  # Monday
    def test_branch_follow_false(self):
        """Checks if BranchDayOfWeekOperator follow false branch"""

        branch_op = BranchDayOfWeekOperator(
            task_id="make_choice",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            week_day="Sunday",
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        self._assert_task_ids_match_states(
            dr,
            {
                'make_choice': State.SUCCESS,
                'branch_1': State.SKIPPED,
                'branch_2': State.NONE,
            },
        )

    def test_branch_with_no_weekday(self):
        """Check if BranchDayOfWeekOperator raises exception on missing weekday"""
        with self.assertRaises(AirflowException):
            BranchDayOfWeekOperator(  # pylint: disable=missing-kwoa
                task_id="make_choice",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                dag=self.dag,
            )

    def test_branch_with_invalid_type(self):
        """Check if BranchDayOfWeekOperator raises exception on unsupported weekday type"""
        invalid_week_day = ['Monday']
        with pytest.raises(
            TypeError,
            match='Unsupported Type for week_day parameter:'
            ' {}. It should be one of str, set or '
            'Weekday enum type'.format(type(invalid_week_day)),
        ):
            BranchDayOfWeekOperator(
                task_id="make_choice",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                week_day=invalid_week_day,
                dag=self.dag,
            )

    def test_weekday_branch_invalid_weekday_number(self):
        """Check if BranchDayOfWeekOperator raises exception on wrong value of weekday"""
        invalid_week_day = 'Thsday'
        with pytest.raises(AttributeError, match=f'Invalid Week Day passed: "{invalid_week_day}"'):
            BranchDayOfWeekOperator(
                task_id="make_choice",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                week_day=invalid_week_day,
                dag=self.dag,
            )

    @freeze_time("2021-01-25")  # Monday
    def test_branch_xcom_push_true_branch(self):
        """Check if BranchDayOfWeekOperator push to xcom value of follow_task_ids_if_true"""
        branch_op = BranchDayOfWeekOperator(
            task_id="make_choice",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            week_day="Monday",
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                assert ti.xcom_pull(task_ids='make_choice') == 'branch_1'
