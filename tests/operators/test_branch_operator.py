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

from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.operators.branch import BaseBranchOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = datetime.timedelta(hours=12)


class ChooseBranchOne(BaseBranchOperator):
    def choose_branch(self, context):
        return 'branch_1'


class ChooseBranchOneTwo(BaseBranchOperator):
    def choose_branch(self, context):
        return ['branch_1', 'branch_2']


class TestBranchOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        self.dag = DAG(
            'branch_operator_test',
            default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )

        self.branch_1 = DummyOperator(task_id='branch_1', dag=self.dag)
        self.branch_2 = DummyOperator(task_id='branch_2', dag=self.dag)
        self.branch_3 = None
        self.branch_op = None

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_without_dag_run(self):
        """This checks the defensive against non existent tasks in a dag run"""
        self.branch_op = ChooseBranchOne(task_id="make_choice", dag=self.dag)
        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.dag.clear()

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(TI.dag_id == self.dag.dag_id, TI.execution_date == DEFAULT_DATE)

            for ti in tis:
                if ti.task_id == 'make_choice':
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == 'branch_1':
                    # should exist with state None
                    self.assertEqual(ti.state, State.NONE)
                elif ti.task_id == 'branch_2':
                    self.assertEqual(ti.state, State.SKIPPED)
                else:
                    raise Exception

    def test_branch_list_without_dag_run(self):
        """This checks if the BranchOperator supports branching off to a list of tasks."""
        self.branch_op = ChooseBranchOneTwo(task_id='make_choice', dag=self.dag)
        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.branch_3 = DummyOperator(task_id='branch_3', dag=self.dag)
        self.branch_3.set_upstream(self.branch_op)
        self.dag.clear()

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        with create_session() as session:
            tis = session.query(TI).filter(TI.dag_id == self.dag.dag_id, TI.execution_date == DEFAULT_DATE)

            expected = {
                "make_choice": State.SUCCESS,
                "branch_1": State.NONE,
                "branch_2": State.NONE,
                "branch_3": State.SKIPPED,
            }

            for ti in tis:
                if ti.task_id in expected:
                    self.assertEqual(ti.state, expected[ti.task_id])
                else:
                    raise Exception

    def test_with_dag_run(self):
        self.branch_op = ChooseBranchOne(task_id="make_choice", dag=self.dag)
        self.branch_1.set_upstream(self.branch_op)
        self.branch_2.set_upstream(self.branch_op)
        self.dag.clear()

        dagrun = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dagrun.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise Exception

    def test_with_skip_in_branch_downstream_dependencies(self):
        self.branch_op = ChooseBranchOne(task_id="make_choice", dag=self.dag)
        self.branch_op >> self.branch_1 >> self.branch_2
        self.branch_op >> self.branch_2
        self.dag.clear()

        dagrun = self.dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        self.branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dagrun.get_task_instances()
        for ti in tis:
            if ti.task_id == 'make_choice':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise Exception
