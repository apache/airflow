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

import datetime

import pytest
import time_machine

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.weekday import BranchDayOfWeekOperator
from airflow.providers.standard.utils.skipmixin import XCOM_SKIPMIXIN_FOLLOWED, XCOM_SKIPMIXIN_KEY
from airflow.providers.standard.utils.weekday import WeekDay
from airflow.timetables.base import DataInterval
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_1, AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.models.xcom import XComModel as XCom
else:
    from airflow.models.xcom import XCom  # type: ignore[no-redef]

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2020, 2, 5)  # Wednesday
INTERVAL = datetime.timedelta(hours=12)
TEST_CASE_BRANCH_FOLLOW_TRUE = {
    "with-string": "Monday",
    "with-enum": WeekDay.MONDAY,
    "with-enum-set": {WeekDay.MONDAY},
    "with-enum-list": [WeekDay.MONDAY],
    "with-enum-dict": {WeekDay.MONDAY: "some_value"},
    "with-enum-set-2-items": {WeekDay.MONDAY, WeekDay.FRIDAY},
    "with-enum-list-2-items": [WeekDay.MONDAY, WeekDay.FRIDAY],
    "with-enum-dict-2-items": {WeekDay.MONDAY: "some_value", WeekDay.FRIDAY: "some_value_2"},
    "with-string-set": {"Monday"},
    "with-string-set-2-items": {"Monday", "Friday"},
    "with-set-mix-types": {"Monday", WeekDay.FRIDAY},
    "with-list-mix-types": ["Monday", WeekDay.FRIDAY],
    "with-dict-mix-types": {"Monday": "some_value", WeekDay.FRIDAY: "some_value_2"},
}


class TestBranchDayOfWeekOperator:
    """
    Tests for BranchDayOfWeekOperator
    """

    @classmethod
    def setup_class(cls):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()
            session.query(XCom).delete()

    def _assert_task_ids_match_states(self, dr, task_ids_to_states):
        """Helper that asserts task instances with a given id are in a given state"""
        tis = dr.get_task_instances()
        for ti in tis:
            try:
                expected_state = task_ids_to_states[ti.task_id]
            except KeyError:
                raise ValueError(f"Invalid task id {ti.task_id} found!")
            else:
                assert_msg = f"Task {ti.task_id} has state {ti.state} instead of expected {expected_state}"
                assert ti.state == expected_state, assert_msg

    @pytest.mark.parametrize(
        "weekday", TEST_CASE_BRANCH_FOLLOW_TRUE.values(), ids=TEST_CASE_BRANCH_FOLLOW_TRUE.keys()
    )
    @time_machine.travel("2021-01-25")  # Monday
    def test_branch_follow_true(self, weekday, dag_maker):
        """Checks if BranchDayOfWeekOperator follows true branch"""
        with dag_maker(
            "branch_day_of_week_operator_test", start_date=DEFAULT_DATE, schedule=INTERVAL, serialized=True
        ):
            branch_op = BranchDayOfWeekOperator(
                task_id="make_choice",
                follow_task_ids_if_true=["branch_1", "branch_2"],
                follow_task_ids_if_false="branch_3",
                week_day=weekday,
            )
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_3 = EmptyOperator(task_id="branch_3")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)
            branch_3.set_upstream(branch_op)

        dr = dag_maker.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            logical_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
        )

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                dag_maker.run_ti("make_choice", dr)

            assert exc_info.value.tasks == [("branch_3", -1)]
        else:
            dag_maker.run_ti("make_choice", dr)

            self._assert_task_ids_match_states(
                dr,
                {
                    "make_choice": State.SUCCESS,
                    "branch_1": State.NONE,
                    "branch_2": State.NONE,
                    "branch_3": State.SKIPPED,
                },
            )

    @time_machine.travel("2021-01-25")  # Monday
    def test_branch_follow_true_with_logical_date(self, dag_maker):
        """Checks if BranchDayOfWeekOperator follows true branch when set use_task_logical_date"""
        with dag_maker(
            "branch_day_of_week_operator_test", start_date=DEFAULT_DATE, schedule=INTERVAL, serialized=True
        ):
            branch_op = BranchDayOfWeekOperator(
                task_id="make_choice",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                week_day="Wednesday",
                use_task_logical_date=True,  # We compare to DEFAULT_DATE which is Wednesday
            )
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)

        dr = dag_maker.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            logical_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
        )

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                dag_maker.run_ti("make_choice", dr)
            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            dag_maker.run_ti("make_choice", dr)

            self._assert_task_ids_match_states(
                dr,
                {
                    "make_choice": State.SUCCESS,
                    "branch_1": State.NONE,
                    "branch_2": State.SKIPPED,
                },
            )

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Skip on Airflow < 3.0")
    @time_machine.travel("2021-01-25")  # Monday
    def test_choose_branch_should_use_run_after_when_logical_date_none(self, dag_maker):
        with dag_maker(
            "branch_day_of_week_operator_test", start_date=DEFAULT_DATE, schedule=INTERVAL, serialized=True
        ):
            branch_op = BranchDayOfWeekOperator(
                task_id="make_choice",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                week_day="Wednesday",
                use_task_logical_date=True,  # We compare to DEFAULT_DATE which is Wednesday
            )
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)

        dr = dag_maker.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            **{"run_after": DEFAULT_DATE},
        )
        assert branch_op.choose_branch(context={"dag_run": dr}) == "branch_1"

    @time_machine.travel("2021-01-25")  # Monday
    def test_branch_follow_false(self, dag_maker):
        """Checks if BranchDayOfWeekOperator follow false branch"""
        with dag_maker(
            "branch_day_of_week_operator_test", start_date=DEFAULT_DATE, schedule=INTERVAL, serialized=True
        ):
            branch_op = BranchDayOfWeekOperator(
                task_id="make_choice",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                week_day="Sunday",
            )
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)

        dr = dag_maker.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            logical_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
        )

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                dag_maker.run_ti("make_choice", dr)

            assert exc_info.value.tasks == [("branch_1", -1)]
        else:
            dag_maker.run_ti("make_choice", dr)

            self._assert_task_ids_match_states(
                dr,
                {
                    "make_choice": State.SUCCESS,
                    "branch_1": State.SKIPPED,
                    "branch_2": State.NONE,
                },
            )

    def test_branch_with_no_weekday(self, dag_maker):
        """Check if BranchDayOfWeekOperator raises exception on missing weekday"""
        with pytest.raises((TypeError, AirflowException), match="missing keyword argument 'week_day'"):
            with dag_maker(
                "branch_day_of_week_operator_test",
                start_date=DEFAULT_DATE,
                schedule=INTERVAL,
                serialized=True,
            ):
                BranchDayOfWeekOperator(
                    task_id="make_choice",
                    follow_task_ids_if_true="branch_1",
                    follow_task_ids_if_false="branch_2",
                )

    def test_branch_with_invalid_type(self, dag_maker):
        """Check if BranchDayOfWeekOperator raises exception on unsupported weekday type"""
        invalid_week_day = 5
        with pytest.raises(
            TypeError,
            match=f"Unsupported Type for week_day parameter: {type(invalid_week_day)}."
            "Input should be iterable type:"
            "str, set, list, dict or Weekday enum type",
        ):
            with dag_maker(
                "branch_day_of_week_operator_test",
                start_date=DEFAULT_DATE,
                schedule=INTERVAL,
                serialized=True,
            ):
                BranchDayOfWeekOperator(
                    task_id="make_choice",
                    follow_task_ids_if_true="branch_1",
                    follow_task_ids_if_false="branch_2",
                    week_day=invalid_week_day,
                )

    @pytest.mark.parametrize(
        ("_", "week_day", "fail_msg"),
        [
            ("string", "Thsday", "Thsday"),
            ("list", ["Monday", "Thsday"], "Thsday"),
            ("set", {WeekDay.MONDAY, "Thsday"}, "Thsday"),
        ],
    )
    def test_weekday_branch_invalid_weekday_value(self, _, week_day, fail_msg, dag_maker):
        """Check if BranchDayOfWeekOperator raises exception on wrong value of weekday"""
        with pytest.raises(AttributeError, match=f'Invalid Week Day passed: "{fail_msg}"'):
            with dag_maker(
                "branch_day_of_week_operator_test",
                start_date=DEFAULT_DATE,
                schedule=INTERVAL,
                serialized=True,
            ):
                BranchDayOfWeekOperator(
                    task_id="make_choice",
                    follow_task_ids_if_true="branch_1",
                    follow_task_ids_if_false="branch_2",
                    week_day=week_day,
                )

    @time_machine.travel("2021-01-25")  # Monday
    def test_branch_xcom_push_true_branch(self, dag_maker):
        """Check if BranchDayOfWeekOperator push to xcom value of follow_task_ids_if_true"""
        with dag_maker(
            "branch_day_of_week_operator_test", start_date=DEFAULT_DATE, schedule=INTERVAL, serialized=True
        ):
            branch_op = BranchDayOfWeekOperator(
                task_id="make_choice",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                week_day="Monday",
            )
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)

        dr = dag_maker.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            logical_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
        )
        branch_op_ti = dr.get_task_instance(branch_op.task_id)

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                branch_op_ti.run()

            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            dag_maker.run_ti("make_choice", dr)

            assert branch_op_ti.xcom_pull(task_ids="make_choice", key=XCOM_SKIPMIXIN_KEY) == {
                XCOM_SKIPMIXIN_FOLLOWED: ["branch_1"]
            }
