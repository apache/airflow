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

from airflow.models.taskinstance import TaskInstance as TI
from airflow.providers.standard.operators.branch import BaseBranchOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.utils.skipmixin import XCOM_SKIPMIXIN_FOLLOWED, XCOM_SKIPMIXIN_KEY
from airflow.timetables.base import DataInterval
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

try:
    from airflow.sdk.definitions.taskgroup import TaskGroup
except ImportError:
    # Fallback for Airflow < 3.1
    from airflow.utils.task_group import TaskGroup  # type: ignore[no-redef]

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_1, AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.providers.common.compat.sdk import DownstreamTasksSkipped
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = datetime.timedelta(hours=12)


class ChooseBranchOne(BaseBranchOperator):
    def choose_branch(self, context):
        return "branch_1"


class ChooseBranchOneTwo(BaseBranchOperator):
    def choose_branch(self, context):
        return ["branch_1", "branch_2"]


class ChooseBranchThree(BaseBranchOperator):
    def choose_branch(self, context):
        return ["branch_3"]


class TestBranchOperator:
    def test_without_dag_run(self, dag_maker):
        """This checks the defensive against non-existent tasks in a dag run"""
        dag_id = "branch_operator_test"
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        with dag_maker(
            dag_id,
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
            serialized=True,
        ):
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_op = ChooseBranchOne(task_id="make_choice")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)
        dr = dag_maker.create_dagrun(**triggered_by_kwargs)

        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                dag_maker.run_ti("make_choice", dr)

            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            dag_maker.run_ti("make_choice", dr)

            ti_date = TI.logical_date if AIRFLOW_V_3_0_PLUS else TI.execution_date

            for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, ti_date == DEFAULT_DATE):
                if ti.task_id == "make_choice":
                    assert ti.state == State.SUCCESS
                elif ti.task_id == "branch_1":
                    assert ti.state == State.NONE
                elif ti.task_id == "branch_2":
                    assert ti.state == State.SKIPPED
                else:
                    raise Exception

    def test_branch_list_without_dag_run(self, dag_maker):
        """This checks if the BranchOperator supports branching off to a list of tasks."""
        dag_id = "branch_operator_test"
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        with dag_maker(
            dag_id,
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
            serialized=True,
        ):
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_3 = EmptyOperator(task_id="branch_3")
            branch_op = ChooseBranchOneTwo(task_id="make_choice")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)
            branch_3.set_upstream(branch_op)
        dr = dag_maker.create_dagrun(**triggered_by_kwargs)

        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                dag_maker.run_ti("make_choice", dr)

            assert exc_info.value.tasks == [("branch_3", -1)]
        else:
            dag_maker.run_ti("make_choice", dr)

            expected = {
                "make_choice": State.SUCCESS,
                "branch_1": State.NONE,
                "branch_2": State.NONE,
                "branch_3": State.SKIPPED,
            }

            ti_date = TI.logical_date if AIRFLOW_V_3_0_PLUS else TI.execution_date

            for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, ti_date == DEFAULT_DATE):
                if ti.task_id in expected:
                    assert ti.state == expected[ti.task_id]
                else:
                    raise Exception

    def test_with_dag_run(self, dag_maker):
        dag_id = "branch_operator_test"
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        with dag_maker(
            dag_id,
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
            serialized=True,
        ):
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_op = ChooseBranchOne(task_id="make_choice")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)
        if AIRFLOW_V_3_0_1:
            dr = dag_maker.create_dagrun(
                run_type=DagRunType.MANUAL,
                start_date=timezone.utcnow(),
                logical_date=DEFAULT_DATE,
                state=State.RUNNING,
                data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
                **triggered_by_kwargs,
            )

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                dag_maker.run_ti("make_choice", dr)

            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            dr = dag_maker.create_dagrun(
                run_type=DagRunType.MANUAL,
                start_date=timezone.utcnow(),
                execution_date=DEFAULT_DATE,
                state=State.RUNNING,
                data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
                **triggered_by_kwargs,
            )

            dag_maker.run_ti("make_choice", dr)

            expected = {
                "make_choice": State.SUCCESS,
                "branch_1": State.NONE,
                "branch_2": State.SKIPPED,
            }

            ti_date = TI.logical_date if AIRFLOW_V_3_0_PLUS else TI.execution_date

            for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, ti_date == DEFAULT_DATE):
                if ti.task_id in expected:
                    assert ti.state == expected[ti.task_id]
                else:
                    raise Exception

    def test_with_skip_in_branch_downstream_dependencies(self, dag_maker):
        dag_id = "branch_operator_test"
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        with dag_maker(
            dag_id,
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
            serialized=True,
        ):
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_op = ChooseBranchOne(task_id="make_choice")
            branch_op >> branch_1 >> branch_2
            branch_op >> branch_2

        if AIRFLOW_V_3_0_PLUS:
            dr = dag_maker.create_dagrun(
                run_type=DagRunType.MANUAL,
                start_date=timezone.utcnow(),
                logical_date=DEFAULT_DATE,
                state=State.RUNNING,
                data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
                **triggered_by_kwargs,
            )
        else:
            dr = dag_maker.create_dagrun(
                run_type=DagRunType.MANUAL,
                start_date=timezone.utcnow(),
                execution_date=DEFAULT_DATE,
                state=State.RUNNING,
                data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
                **triggered_by_kwargs,
            )

        dag_maker.run_ti("make_choice", dr)

        expected = {
            "make_choice": State.SUCCESS,
            "branch_1": State.NONE,
            "branch_2": State.NONE,
        }

        ti_date = TI.logical_date if AIRFLOW_V_3_0_PLUS else TI.execution_date

        for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, ti_date == DEFAULT_DATE):
            if ti.task_id in expected:
                assert ti.state == expected[ti.task_id]
            else:
                raise Exception

    def test_xcom_push(self, dag_maker):
        dag_id = "branch_operator_test"

        triggered_by_kwargs = (
            {
                "triggered_by": DagRunTriggeredByType.TEST,
                "logical_date": DEFAULT_DATE,
            }
            if AIRFLOW_V_3_0_PLUS
            else {"execution_date": DEFAULT_DATE}
        )
        with dag_maker(
            dag_id,
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
            serialized=True,
        ):
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_op = ChooseBranchOne(task_id="make_choice")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)

        dr = dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        if AIRFLOW_V_3_0_1:
            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                dag_maker.run_ti("make_choice", dr)

            assert exc_info.value.tasks == [("branch_2", -1)]
        else:
            dag_maker.run_ti("make_choice", dr)
            branch_op_ti = dr.get_task_instance("make_choice")
            assert branch_op_ti.xcom_pull(task_ids="make_choice", key=XCOM_SKIPMIXIN_KEY) == {
                XCOM_SKIPMIXIN_FOLLOWED: ["branch_1"]
            }

    def test_with_dag_run_task_groups(self, dag_maker):
        dag_id = "branch_operator_test"
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        with dag_maker(
            dag_id,
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule=INTERVAL,
            serialized=True,
        ):
            branch_1 = EmptyOperator(task_id="branch_1")
            branch_2 = EmptyOperator(task_id="branch_2")
            branch_3 = TaskGroup("branch_3")
            EmptyOperator(task_id="task_1", task_group=branch_3)
            EmptyOperator(task_id="task_2", task_group=branch_3)
            branch_op = ChooseBranchThree(task_id="make_choice")
            branch_1.set_upstream(branch_op)
            branch_2.set_upstream(branch_op)
            branch_3.set_upstream(branch_op)

        if AIRFLOW_V_3_0_1:
            dr = dag_maker.create_dagrun(
                run_type=DagRunType.MANUAL,
                start_date=timezone.utcnow(),
                logical_date=DEFAULT_DATE,
                state=State.RUNNING,
                data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
                **triggered_by_kwargs,
            )

            with pytest.raises(DownstreamTasksSkipped) as exc_info:
                dag_maker.run_ti("make_choice", dr)

            assert set(exc_info.value.tasks) == {("branch_1", -1), ("branch_2", -1)}
        else:
            dr = dag_maker.create_dagrun(
                run_type=DagRunType.MANUAL,
                start_date=timezone.utcnow(),
                execution_date=DEFAULT_DATE,
                state=State.RUNNING,
                data_interval=DataInterval(DEFAULT_DATE, DEFAULT_DATE),
                **triggered_by_kwargs,
            )
            dag_maker.run_ti("make_choice", dr)

            ti_date = TI.logical_date if AIRFLOW_V_3_0_PLUS else TI.execution_date

            for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, ti_date == DEFAULT_DATE):
                if ti.task_id == "make_choice":
                    assert ti.state == State.SUCCESS
                elif ti.task_id == "branch_1":
                    assert ti.state == State.SKIPPED
                elif ti.task_id == "branch_2":
                    assert ti.state == State.SKIPPED
                elif ti.task_id == "branch_3.task_1":
                    assert ti.state == State.NONE
                elif ti.task_id == "branch_3.task_2":
                    assert ti.state == State.NONE
                else:
                    raise Exception
