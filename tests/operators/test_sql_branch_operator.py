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

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql_branch_operator import BranchSqlOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from tests.providers.apache.hive import TestHiveEnvironment

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
INTERVAL = datetime.timedelta(hours=12)

SUPPORTED_TRUE_VALUES = [
    ["True"],
    ["true"],
    ["1"],
    ["on"],
    [1],
    True,
    "true",
    "1",
    "on",
    1,
]
SUPPORTED_FALSE_VALUES = [
    ["False"],
    ["false"],
    ["0"],
    ["off"],
    [0],
    False,
    "false",
    "0",
    "off",
    0,
]


class TestSqlBranch(TestHiveEnvironment, unittest.TestCase):
    """
    Test for SQL Branch Operator
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            "sql_branch_operator_test",
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )
        self.branch_1 = DummyOperator(task_id="branch_1", dag=self.dag)
        self.branch_2 = DummyOperator(task_id="branch_2", dag=self.dag)
        self.branch_3 = None

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_unsupported_conn_type(self):
        """ Check if BranchSqlOperator throws an exception for unsupported connection type """
        op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="redis_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with self.assertRaises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_conn(self):
        """ Check if BranchSqlOperator throws an exception for invalid connection """
        op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with self.assertRaises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_follow_task_true(self):
        """ Check if BranchSqlOperator throws an exception for invalid connection """
        op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true=None,
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with self.assertRaises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_follow_task_false(self):
        """ Check if BranchSqlOperator throws an exception for invalid connection """
        op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false=None,
            dag=self.dag,
        )

        with self.assertRaises(AirflowException):
            op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.backend("mysql")
    def test_sql_branch_operator_mysql(self):
        """ Check if BranchSqlOperator works with backend """
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )
        branch_op.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True
        )

    @pytest.mark.backend("postgres")
    def test_sql_branch_operator_postgres(self):
        """ Check if BranchSqlOperator works with backend """
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="postgres_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )
        branch_op.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, ignore_ti_state=True
        )

    @mock.patch("airflow.operators.sql_branch_operator.BaseHook")
    def test_branch_single_value_with_dag_run(self, mock_hook):
        """ Check BranchSqlOperator branch operation """
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
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

        mock_hook.get_connection("mysql_default").conn_type = "mysql"
        mock_get_records = (
            mock_hook.get_connection.return_value.get_hook.return_value.get_first
        )

        mock_get_records.return_value = 1

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == "branch_1":
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == "branch_2":
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.operators.sql_branch_operator.BaseHook")
    def test_branch_true_with_dag_run(self, mock_hook):
        """ Check BranchSqlOperator branch operation """
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
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

        mock_hook.get_connection("mysql_default").conn_type = "mysql"
        mock_get_records = (
            mock_hook.get_connection.return_value.get_hook.return_value.get_first
        )

        for true_value in SUPPORTED_TRUE_VALUES:
            mock_get_records.return_value = true_value

            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == "make_choice":
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == "branch_1":
                    self.assertEqual(ti.state, State.NONE)
                elif ti.task_id == "branch_2":
                    self.assertEqual(ti.state, State.SKIPPED)
                else:
                    raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.operators.sql_branch_operator.BaseHook")
    def test_branch_false_with_dag_run(self, mock_hook):
        """ Check BranchSqlOperator branch operation """
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
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

        mock_hook.get_connection("mysql_default").conn_type = "mysql"
        mock_get_records = (
            mock_hook.get_connection.return_value.get_hook.return_value.get_first
        )

        for false_value in SUPPORTED_FALSE_VALUES:
            mock_get_records.return_value = false_value

            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == "make_choice":
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == "branch_1":
                    self.assertEqual(ti.state, State.SKIPPED)
                elif ti.task_id == "branch_2":
                    self.assertEqual(ti.state, State.NONE)
                else:
                    raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.operators.sql_branch_operator.BaseHook")
    def test_branch_list_with_dag_run(self, mock_hook):
        """ Checks if the BranchSqlOperator supports branching off to a list of tasks."""
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true=["branch_1", "branch_2"],
            follow_task_ids_if_false="branch_3",
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

        mock_hook.get_connection("mysql_default").conn_type = "mysql"
        mock_get_records = (
            mock_hook.get_connection.return_value.get_hook.return_value.get_first
        )
        mock_get_records.return_value = [["1"]]

        branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == "make_choice":
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == "branch_1":
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == "branch_2":
                self.assertEqual(ti.state, State.NONE)
            elif ti.task_id == "branch_3":
                self.assertEqual(ti.state, State.SKIPPED)
            else:
                raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.operators.sql_branch_operator.BaseHook")
    def test_invalid_query_result_with_dag_run(self, mock_hook):
        """ Check BranchSqlOperator branch operation """
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        self.branch_1.set_upstream(branch_op)
        self.branch_2.set_upstream(branch_op)
        self.dag.clear()

        self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        mock_hook.get_connection("mysql_default").conn_type = "mysql"
        mock_get_records = (
            mock_hook.get_connection.return_value.get_hook.return_value.get_first
        )

        mock_get_records.return_value = ["Invalid Value"]

        with self.assertRaises(AirflowException):
            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    @mock.patch("airflow.operators.sql_branch_operator.BaseHook")
    def test_with_skip_in_branch_downstream_dependencies(self, mock_hook):
        """ Test SQL Branch with skipping all downstream dependencies """
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        branch_op >> self.branch_1 >> self.branch_2
        branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        mock_hook.get_connection("mysql_default").conn_type = "mysql"
        mock_get_records = (
            mock_hook.get_connection.return_value.get_hook.return_value.get_first
        )

        for true_value in SUPPORTED_TRUE_VALUES:
            mock_get_records.return_value = [true_value]

            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == "make_choice":
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == "branch_1":
                    self.assertEqual(ti.state, State.NONE)
                elif ti.task_id == "branch_2":
                    self.assertEqual(ti.state, State.NONE)
                else:
                    raise ValueError(f"Invalid task id {ti.task_id} found!")

    @mock.patch("airflow.operators.sql_branch_operator.BaseHook")
    def test_with_skip_in_branch_downstream_dependencies2(self, mock_hook):
        """ Test skipping downstream dependency for false condition"""
        branch_op = BranchSqlOperator(
            task_id="make_choice",
            conn_id="mysql_default",
            sql="SELECT 1",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        branch_op >> self.branch_1 >> self.branch_2
        branch_op >> self.branch_2
        self.dag.clear()

        dr = self.dag.create_dagrun(
            run_id="manual__",
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        mock_hook.get_connection("mysql_default").conn_type = "mysql"
        mock_get_records = (
            mock_hook.get_connection.return_value.get_hook.return_value.get_first
        )

        for false_value in SUPPORTED_FALSE_VALUES:
            mock_get_records.return_value = [false_value]

            branch_op.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

            tis = dr.get_task_instances()
            for ti in tis:
                if ti.task_id == "make_choice":
                    self.assertEqual(ti.state, State.SUCCESS)
                elif ti.task_id == "branch_1":
                    self.assertEqual(ti.state, State.SKIPPED)
                elif ti.task_id == "branch_2":
                    self.assertEqual(ti.state, State.NONE)
                else:
                    raise ValueError(f"Invalid task id {ti.task_id} found!")
