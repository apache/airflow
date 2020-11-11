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
from unittest import mock

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.operators.datetime_branch_operator import DateTimeBranchOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = datetime.timedelta(hours=12)


class TestDateTimeBranchOperator(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        self.dag = DAG(
            'datetime_branch_operator_test',
            default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )

        self.branch_1 = DummyOperator(task_id='branch_1', dag=self.dag)
        self.branch_2 = DummyOperator(task_id='branch_2', dag=self.dag)

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_no_target_time(self):
        """Check if DateTimeBranchOperator raises exception on missing target"""
        with self.assertRaises(AirflowException):
            DateTimeBranchOperator(
                task_id='datetime_branch',
                follow_task_ids_if_true='branch_1',
                follow_task_ids_if_false='branch_2',
                target_upper=None,
                target_lower=None,
                dag=self.dag,
            )

    @mock.patch('airflow.operators.datetime_branch_operator.timezone.utcnow')
    def test_datetime_branch_operator_falls_within_range(self, mock_timezone):
        """Check DateTimeBranchOperator branch operation"""
        branch_op = DateTimeBranchOperator(
            task_id='datetime_branch',
            follow_task_ids_if_true='branch_1',
            follow_task_ids_if_false='branch_2',
            target_upper=datetime.datetime(2020, 7, 7, 11, 0, 0),
            target_lower=datetime.datetime(2020, 7, 7, 10, 0, 0),
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id='manual__', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.RUNNING
        )

        mock_timezone.return_value = datetime.datetime(2020, 7, 7, 10, 54, 5, tzinfo=datetime.timezone.utc)
        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'datetime_branch':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    @mock.patch('airflow.operators.datetime_branch_operator.timezone.utcnow')
    def test_datetime_branch_operator_falls_outside_range(self, mock_timezone):
        """Check DateTimeBranchOperator branch operation"""
        branch_op = DateTimeBranchOperator(
            task_id='datetime_branch',
            follow_task_ids_if_true='branch_1',
            follow_task_ids_if_false='branch_2',
            target_upper=datetime.datetime(2020, 7, 7, 11, 0, 0),
            target_lower=datetime.datetime(2020, 7, 7, 10, 0, 0),
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id='manual__', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.RUNNING
        )

        dates = [
            datetime.datetime(2020, 7, 7, 12, 0, 0, tzinfo=datetime.timezone.utc),
            datetime.datetime(2020, 6, 7, tzinfo=datetime.timezone.utc),
        ]

        for date in dates:
            mock_timezone.return_value = date
            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == 'datetime_branch':
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == 'branch_1':
                    self.assertEqual(ti.state, State.SKIPPED)
                elif ti.task_id == 'branch_2':
                    self.assertEqual(ti.state, State.NONE)
                else:
                    raise ValueError(f'Invalid task id {ti.task_id} found!')

    @mock.patch('airflow.operators.datetime_branch_operator.timezone.utcnow')
    def test_datetime_branch_operator_upper_comparison_within_range(self, mock_timezone):
        """Check DateTimeBranchOperator branch operation"""
        branch_op = DateTimeBranchOperator(
            task_id='datetime_branch',
            follow_task_ids_if_true='branch_1',
            follow_task_ids_if_false='branch_2',
            target_upper=datetime.datetime(2020, 7, 7, 11, 0, 0),
            target_lower=None,
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id='manual__', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.RUNNING
        )

        mock_timezone.return_value = datetime.datetime(2020, 7, 7, 10, 54, 5, tzinfo=datetime.timezone.utc)
        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'datetime_branch':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    @mock.patch('airflow.operators.datetime_branch_operator.timezone.utcnow')
    def test_datetime_branch_operator_lower_comparison_within_range(self, mock_timezone):
        """Check DateTimeBranchOperator branch operation"""
        branch_op = DateTimeBranchOperator(
            task_id='datetime_branch',
            follow_task_ids_if_true='branch_1',
            follow_task_ids_if_false='branch_2',
            target_upper=None,
            target_lower=datetime.datetime(2020, 7, 7, 10, 0, 0),
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id='manual__', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.RUNNING
        )

        mock_timezone.return_value = datetime.datetime(2020, 7, 7, 10, 54, 5, tzinfo=datetime.timezone.utc)
        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'datetime_branch':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    @mock.patch('airflow.operators.datetime_branch_operator.timezone.utcnow')
    def test_datetime_branch_operator_upper_comparison_outside_range(self, mock_timezone):
        """Check DateTimeBranchOperator branch operation"""
        branch_op = DateTimeBranchOperator(
            task_id='datetime_branch',
            follow_task_ids_if_true='branch_1',
            follow_task_ids_if_false='branch_2',
            target_upper=datetime.datetime(2020, 7, 7, 11, 0, 0),
            target_lower=None,
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id='manual__', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.RUNNING
        )

        mock_timezone.return_value = datetime.datetime(2020, 7, 7, 12, 0, 0, tzinfo=datetime.timezone.utc)
        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'datetime_branch':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.SKIPPED)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')

    @mock.patch('airflow.operators.datetime_branch_operator.timezone.utcnow')
    def test_datetime_branch_operator_lower_comparison_outside_range(self, mock_timezone):
        """Check DateTimeBranchOperator branch operation"""
        branch_op = DateTimeBranchOperator(
            task_id='datetime_branch',
            follow_task_ids_if_true='branch_1',
            follow_task_ids_if_false='branch_2',
            target_upper=None,
            target_lower=datetime.datetime(2020, 7, 7, 10, 0, 0),
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id='manual__', start_date=DEFAULT_DATE, execution_date=DEFAULT_DATE, state=State.RUNNING
        )

        mock_timezone.return_value = datetime.datetime(2020, 7, 7, 9, 0, 0, tzinfo=datetime.timezone.utc)
        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == 'datetime_branch':
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == 'branch_1':
                self.assertEqual(ti.state, State.SKIPPED)
            elif ti.task_id == 'branch_2':
                self.assertEqual(ti.state, State.NONE)
            else:
                raise ValueError(f'Invalid task id {ti.task_id} found!')
