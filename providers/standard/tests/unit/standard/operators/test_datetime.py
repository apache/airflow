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
from sqlalchemy import delete

from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.standard.operators.datetime import BranchDateTimeOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.base import DataInterval
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_1, AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

if AIRFLOW_V_3_1_PLUS:
    from airflow.sdk import timezone
else:
    from airflow.utils import timezone  # type: ignore[attr-defined,no-redef]

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = datetime.timedelta(hours=12)


class TestBranchDateTimeOperator:
    @classmethod
    def setup_class(cls):
        with create_session() as session:
            session.execute(delete(DagRun))
            session.execute(delete(TI))

    targets = [
        (datetime.datetime(2020, 7, 7, 10, 0, 0), datetime.datetime(2020, 7, 7, 11, 0, 0)),
        (datetime.time(10, 0, 0), datetime.time(11, 0, 0)),
        (datetime.datetime(2020, 7, 7, 10, 0, 0), datetime.time(11, 0, 0)),
        (datetime.time(10, 0, 0), datetime.datetime(2020, 7, 7, 11, 0, 0)),
    ]

    @pytest.fixture(autouse=True)
    def base_tests_setup(self, dag_maker):
        self.dag_maker = dag_maker  # Store dag_maker for use in test methods
        with dag_maker(
            "branch_datetime_operator_test",
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
            serialized=True,
        ) as dag:
            self.dag = dag
            self.branch_1 = EmptyOperator(task_id="branch_1")
            self.branch_2 = EmptyOperator(task_id="branch_2")

            self.branch_op = BranchDateTimeOperator(
                task_id="datetime_branch",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                target_upper=datetime.datetime(2020, 7, 7, 11, 0, 0),
                target_lower=datetime.datetime(2020, 7, 7, 10, 0, 0),
            )

            self.branch_1.set_upstream(self.branch_op)
            self.branch_2.set_upstream(self.branch_op)

        self.dr = dag_maker.create_dagrun(
            run_id="manual__",
            start_date=DEFAULT_DATE,
            logical_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
        )

    def teardown_method(self):
        with create_session() as session:
            session.execute(delete(DagRun))
            session.execute(delete(TI))

    def _assert_task_ids_match_states(self, task_ids_to_states):
        """Helper that asserts task instances with a given id are in a given state"""
        tis = self.dr.get_task_instances()
        for ti in tis:
            try:
                expected_state = task_ids_to_states[ti.task_id]
            except KeyError:
                raise ValueError(f"Invalid task id {ti.task_id} found!")
            else:
                assert ti.state == (expected_state), (
                    f"Task {ti.task_id} has state {ti.state} instead of expected {expected_state}"
                )

    def test_no_target_time(self):
        """Check if BranchDateTimeOperator raises exception on missing target"""
        with pytest.raises(AirflowException):
            BranchDateTimeOperator(
                task_id="datetime_branch",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                target_upper=None,
                target_lower=None,
                dag=self.dag_maker.dag,
            )

    @pytest.mark.parametrize(
        ("target_lower", "target_upper"),
        targets,
    )
    @time_machine.travel("2020-07-07 10:54:05")
    def test_branch_datetime_operator_falls_within_range(self, target_lower, target_upper):
        """Check BranchDateTimeOperator branch operation"""
        self.branch_op.target_lower = target_lower
        self.branch_op.target_upper = target_upper
        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                self.dag_maker.run_ti("datetime_branch", self.dr)
            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            self.dag_maker.run_ti("datetime_branch", self.dr)

            self._assert_task_ids_match_states(
                {
                    "datetime_branch": State.SUCCESS,
                    "branch_1": State.NONE,
                    "branch_2": State.SKIPPED,
                }
            )

    @pytest.mark.parametrize(
        ("target_lower", "target_upper"),
        targets,
    )
    @pytest.mark.parametrize(
        "date",
        (
            datetime.datetime(2020, 7, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 6, 7, 9, 0, 0, tzinfo=datetime.timezone.utc),
        ),
    )
    def test_branch_datetime_operator_falls_outside_range(self, date, target_lower, target_upper):
        """Check BranchDateTimeOperator branch operation"""
        self.branch_op.target_lower = target_lower
        self.branch_op.target_upper = target_upper

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info, time_machine.travel(date):
                self.dag_maker.run_ti("datetime_branch", self.dr)

            assert exc_info.value.tasks == [("branch_1", -1)]
        else:
            with time_machine.travel(date):
                self.dag_maker.run_ti("datetime_branch", self.dr)

                self._assert_task_ids_match_states(
                    {
                        "datetime_branch": State.SUCCESS,
                        "branch_1": State.SKIPPED,
                        "branch_2": State.NONE,
                    }
                )

    @pytest.mark.parametrize("target_upper", [target_upper for (_, target_upper) in targets])
    @time_machine.travel("2020-07-07 10:54:05")
    def test_branch_datetime_operator_upper_comparison_within_range(self, target_upper):
        """Check BranchDateTimeOperator branch operation"""
        self.branch_op.target_upper = target_upper
        self.branch_op.target_lower = None

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                self.dag_maker.run_ti("datetime_branch", self.dr)

            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            self.dag_maker.run_ti("datetime_branch", self.dr)

            self._assert_task_ids_match_states(
                {
                    "datetime_branch": State.SUCCESS,
                    "branch_1": State.NONE,
                    "branch_2": State.SKIPPED,
                }
            )

    @pytest.mark.parametrize("target_lower", [target_lower for (target_lower, _) in targets])
    @time_machine.travel("2020-07-07 10:54:05")
    def test_branch_datetime_operator_lower_comparison_within_range(self, target_lower):
        """Check BranchDateTimeOperator branch operation"""
        self.branch_op.target_lower = target_lower
        self.branch_op.target_upper = None

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                self.dag_maker.run_ti("datetime_branch", self.dr)

            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            self.dag_maker.run_ti("datetime_branch", self.dr)

            self._assert_task_ids_match_states(
                {
                    "datetime_branch": State.SUCCESS,
                    "branch_1": State.NONE,
                    "branch_2": State.SKIPPED,
                }
            )

    @pytest.mark.parametrize("target_upper", [target_upper for (_, target_upper) in targets])
    @time_machine.travel("2020-07-07 12:00:00")
    def test_branch_datetime_operator_upper_comparison_outside_range(self, target_upper):
        """Check BranchDateTimeOperator branch operation"""
        self.branch_op.target_upper = target_upper
        self.branch_op.target_lower = None

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                self.dag_maker.run_ti("datetime_branch", self.dr)

            assert exc_info.value.tasks == [("branch_1", -1)]
        else:
            self.dag_maker.run_ti("datetime_branch", self.dr)

            self._assert_task_ids_match_states(
                {
                    "datetime_branch": State.SUCCESS,
                    "branch_1": State.SKIPPED,
                    "branch_2": State.NONE,
                }
            )

    @pytest.mark.parametrize("target_lower", [target_lower for (target_lower, _) in targets])
    @time_machine.travel("2020-07-07 09:00:00")
    def test_branch_datetime_operator_lower_comparison_outside_range(self, target_lower):
        """Check BranchDateTimeOperator branch operation"""
        self.branch_op.target_lower = target_lower
        self.branch_op.target_upper = None

        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                self.dag_maker.run_ti("datetime_branch", self.dr)

            assert exc_info.value.tasks == [("branch_1", -1)]
        else:
            self.dag_maker.run_ti("datetime_branch", self.dr)

            self._assert_task_ids_match_states(
                {
                    "datetime_branch": State.SUCCESS,
                    "branch_1": State.SKIPPED,
                    "branch_2": State.NONE,
                }
            )

    @pytest.mark.parametrize(
        ("target_lower", "target_upper"),
        targets,
    )
    @time_machine.travel("2020-12-01 09:00:00")
    def test_branch_datetime_operator_use_task_logical_date(self, dag_maker, target_lower, target_upper):
        """Check if BranchDateTimeOperator uses task logical date"""
        in_between_date = timezone.datetime(2020, 7, 7, 10, 30, 0)
        self.branch_op.use_task_logical_date = True
        self.dr = dag_maker.create_dagrun(
            run_id="manual_exec_date__",
            start_date=in_between_date,
            logical_date=in_between_date,
            state=State.RUNNING,
            data_interval=DataInterval(in_between_date, in_between_date),
        )

        self.branch_op.target_lower = target_lower
        self.branch_op.target_upper = target_upper
        if AIRFLOW_V_3_0_1:
            from airflow.providers.common.compat.sdk import DownstreamTasksSkipped

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                self.dag_maker.run_ti("datetime_branch", self.dr)

            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            self.dag_maker.run_ti("datetime_branch", self.dr)

            self._assert_task_ids_match_states(
                {
                    "datetime_branch": State.SUCCESS,
                    "branch_1": State.NONE,
                    "branch_2": State.SKIPPED,
                }
            )

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Skip on Airflow < 3.0")
    @time_machine.travel("2020-12-01 09:00:00")
    def test_choose_branch_should_use_run_after_when_logical_date_none(self, dag_maker):
        with dag_maker(
            "branch_datetime_operator_uses_run_after",
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
            serialized=True,
        ):
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")

            branch_op = BranchDateTimeOperator(
                task_id="datetime_branch",
                follow_task_ids_if_true="branch_1",
                follow_task_ids_if_false="branch_2",
                target_upper=datetime.datetime(2020, 9, 7, 11, 0, 0),
                target_lower=datetime.datetime(2020, 6, 7, 10, 0, 0),
                use_task_logical_date=True,
            )
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)

        dr = dag_maker.create_dagrun(
            run_id="manual__run_after",
            start_date=DEFAULT_DATE,
            state=State.RUNNING,
            **{"run_after": timezone.datetime(2020, 8, 7)},
        )
        assert branch_op.choose_branch(context={"dag_run": dr}) == "branch_1"
