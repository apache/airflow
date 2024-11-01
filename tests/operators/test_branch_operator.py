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
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.task_group import TaskGroup
from airflow.utils.types import DagRunType

from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
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
        """This checks the defensive against non existent tasks in a dag run"""
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
        dag_maker.create_dagrun(**triggered_by_kwargs)

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, TI.execution_date == DEFAULT_DATE):
            if ti.task_id == "make_choice":
                assert ti.state == State.SUCCESS
            elif ti.task_id == "branch_1":
                # should exist with state None
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
        dag_maker.create_dagrun(**triggered_by_kwargs)

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        expected = {
            "make_choice": State.SUCCESS,
            "branch_1": State.NONE,
            "branch_2": State.NONE,
            "branch_3": State.SKIPPED,
        }

        for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, TI.execution_date == DEFAULT_DATE):
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
        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        expected = {
            "make_choice": State.SUCCESS,
            "branch_1": State.NONE,
            "branch_2": State.SKIPPED,
        }

        for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, TI.execution_date == DEFAULT_DATE):
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

        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        expected = {
            "make_choice": State.SUCCESS,
            "branch_1": State.NONE,
            "branch_2": State.NONE,
        }

        for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, TI.execution_date == DEFAULT_DATE):
            if ti.task_id in expected:
                assert ti.state == expected[ti.task_id]
            else:
                raise Exception

    def test_xcom_push(self, dag_maker):
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

        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, TI.execution_date == DEFAULT_DATE):
            if ti.task_id == "make_choice":
                assert ti.xcom_pull(task_ids="make_choice") == "branch_1"

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

        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            **triggered_by_kwargs,
        )

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        for ti in dag_maker.session.query(TI).filter(TI.dag_id == dag_id, TI.execution_date == DEFAULT_DATE):
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
