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

import six

from tests.compat import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.operators.check_operator import (
    CheckOperator, IntervalCheckOperator, ThresholdCheckOperator, ValueCheckOperator,
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import BranchSQLOperator
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.utils.state import State
from tests.hooks.test_hive_hook import TestHiveEnvironment

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


class TestCheckOperator(unittest.TestCase):
    @mock.patch.object(CheckOperator, "get_db_hook")
    def test_execute_no_records(self, mock_get_db_hook):
        mock_get_db_hook.return_value.get_first.return_value = []

        with self.assertRaises(AirflowException):
            CheckOperator(sql="sql").execute()

    @mock.patch.object(CheckOperator, "get_db_hook")
    def test_execute_not_all_records_are_true(self, mock_get_db_hook):
        mock_get_db_hook.return_value.get_first.return_value = ["data", ""]

        with self.assertRaises(AirflowException):
            CheckOperator(sql="sql").execute()


class TestValueCheckOperator(unittest.TestCase):
    def setUp(self):
        self.task_id = "test_task"
        self.conn_id = "default_conn"

    def _construct_operator(self, sql, pass_value, tolerance=None):
        dag = DAG("test_dag", start_date=datetime.datetime(2017, 1, 1))

        return ValueCheckOperator(
            dag=dag,
            task_id=self.task_id,
            conn_id=self.conn_id,
            sql=sql,
            pass_value=pass_value,
            tolerance=tolerance,
        )

    def test_pass_value_template_string(self):
        pass_value_str = "2018-03-22"
        operator = self._construct_operator(
            "select date from tab1;", "{{ ds }}")

        operator.render_template_fields({"ds": pass_value_str})

        self.assertEqual(operator.task_id, self.task_id)
        self.assertEqual(operator.pass_value, pass_value_str)

    def test_pass_value_template_string_float(self):
        pass_value_float = 4.0
        operator = self._construct_operator(
            "select date from tab1;", pass_value_float)

        operator.render_template_fields({})

        self.assertEqual(operator.task_id, self.task_id)
        self.assertEqual(operator.pass_value, str(pass_value_float))

    @mock.patch.object(ValueCheckOperator, "get_db_hook")
    def test_execute_pass(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [10]
        mock_get_db_hook.return_value = mock_hook
        sql = "select value from tab1 limit 1;"
        operator = self._construct_operator(sql, 5, 1)

        operator.execute(None)

        mock_hook.get_first.assert_called_once_with(sql)

    @mock.patch.object(ValueCheckOperator, "get_db_hook")
    def test_execute_fail(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [11]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            "select value from tab1 limit 1;", 5, 1)

        with six.assertRaisesRegex(self, AirflowException, "Tolerance:100.0%"):
            operator.execute()


class TestIntervalCheckOperator(unittest.TestCase):
    def _construct_operator(self, table, metric_thresholds, ratio_formula, ignore_zero):
        return IntervalCheckOperator(
            task_id="test_task",
            table=table,
            metrics_thresholds=metric_thresholds,
            ratio_formula=ratio_formula,
            ignore_zero=ignore_zero,
        )

    def test_invalid_ratio_formula(self):
        with six.assertRaisesRegex(self, AirflowException, "Invalid diff_method"):
            self._construct_operator(
                table="test_table",
                metric_thresholds={"f1": 1, },
                ratio_formula="abs",
                ignore_zero=False,
            )

    @mock.patch.object(IntervalCheckOperator, "get_db_hook")
    def test_execute_not_ignore_zero(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [0]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table="test_table",
            metric_thresholds={"f1": 1, },
            ratio_formula="max_over_min",
            ignore_zero=False,
        )

        with self.assertRaises(AirflowException):
            operator.execute()

    @mock.patch.object(IntervalCheckOperator, "get_db_hook")
    def test_execute_ignore_zero(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [0]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table="test_table",
            metric_thresholds={"f1": 1, },
            ratio_formula="max_over_min",
            ignore_zero=True,
        )

        operator.execute()

    @mock.patch.object(IntervalCheckOperator, "get_db_hook")
    def test_execute_min_max(self, mock_get_db_hook):
        mock_hook = mock.Mock()

        def returned_row():
            rows = [
                [2, 2, 2, 2],  # reference
                [1, 1, 1, 1],  # current
            ]
            return rows

        mock_hook.get_first.side_effect = returned_row()
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table="test_table",
            metric_thresholds={"f0": 1.0, "f1": 1.5, "f2": 2.0, "f3": 2.5, },
            ratio_formula="max_over_min",
            ignore_zero=True,
        )

        with six.assertRaisesRegex(self, AirflowException, "f0, f1, f2"):
            operator.execute()

    @mock.patch.object(IntervalCheckOperator, "get_db_hook")
    def test_execute_diff(self, mock_get_db_hook):
        mock_hook = mock.Mock()

        def returned_row():
            rows = [
                [3, 3, 3, 3],  # reference
                [1, 1, 1, 1],  # current
            ]

            return rows

        mock_hook.get_first.side_effect = returned_row()
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            table="test_table",
            metric_thresholds={"f0": 0.5, "f1": 0.6, "f2": 0.7, "f3": 0.8, },
            ratio_formula="relative_diff",
            ignore_zero=True,
        )

        with six.assertRaisesRegex(self, AirflowException, "f0, f1"):
            operator.execute()


class TestThresholdCheckOperator(unittest.TestCase):
    def _construct_operator(self, sql, min_threshold, max_threshold):
        dag = DAG("test_dag", start_date=datetime.datetime(2017, 1, 1))

        return ThresholdCheckOperator(
            task_id="test_task",
            sql=sql,
            min_threshold=min_threshold,
            max_threshold=max_threshold,
            dag=dag,
        )

    @mock.patch.object(ThresholdCheckOperator, "get_db_hook")
    def test_pass_min_value_max_value(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [(10,)]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            "Select avg(val) from table1 limit 1", 1, 100
        )

        operator.execute()

    @mock.patch.object(ThresholdCheckOperator, "get_db_hook")
    def test_fail_min_value_max_value(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.return_value = [(10,)]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            "Select avg(val) from table1 limit 1", 20, 100
        )

        with six.assertRaisesRegex(self, AirflowException, "10.*20.0.*100.0"):
            operator.execute()

    @mock.patch.object(ThresholdCheckOperator, "get_db_hook")
    def test_pass_min_sql_max_sql(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.side_effect = lambda x: [(int(x.split()[1]),)]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            "Select 10", "Select 1", "Select 100")

        operator.execute()

    @mock.patch.object(ThresholdCheckOperator, "get_db_hook")
    def test_fail_min_sql_max_sql(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.side_effect = lambda x: [(int(x.split()[1]),)]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator(
            "Select 10", "Select 20", "Select 100")

        with six.assertRaisesRegex(self, AirflowException, "10.*20.*100"):
            operator.execute()

    @mock.patch.object(ThresholdCheckOperator, "get_db_hook")
    def test_pass_min_value_max_sql(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.side_effect = lambda x: [(int(x.split()[1]),)]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("Select 75", 45, "Select 100")

        operator.execute()

    @mock.patch.object(ThresholdCheckOperator, "get_db_hook")
    def test_fail_min_sql_max_value(self, mock_get_db_hook):
        mock_hook = mock.Mock()
        mock_hook.get_first.side_effect = lambda x: [(int(x.split()[1]),)]
        mock_get_db_hook.return_value = mock_hook

        operator = self._construct_operator("Select 155", "Select 45", 100)

        with six.assertRaisesRegex(self, AirflowException, "155.*45.*100.0"):
            operator.execute()


class TestSqlBranch(TestHiveEnvironment, unittest.TestCase):
    """
    Test for SQL Branch Operator
    """

    @classmethod
    def setUpClass(cls):
        super(TestSqlBranch, cls).setUpClass()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def setUp(self):
        super(TestSqlBranch, self).setUp()
        self.dag = DAG(
            "sql_branch_operator_test",
            default_args={"owner": "airflow", "start_date": DEFAULT_DATE},
            schedule_interval=INTERVAL,
        )
        self.branch_1 = DummyOperator(task_id="branch_1", dag=self.dag)
        self.branch_2 = DummyOperator(task_id="branch_2", dag=self.dag)
        self.branch_3 = None

    def tearDown(self):
        super(TestSqlBranch, self).tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_unsupported_conn_type(self):
        """ Check if BranchSQLOperator throws an exception for unsupported connection type """
        op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="redis_default",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with self.assertRaises(AirflowException):
            op.run(start_date=DEFAULT_DATE,
                   end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_conn(self):
        """ Check if BranchSQLOperator throws an exception for invalid connection """
        op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with self.assertRaises(AirflowException):
            op.run(start_date=DEFAULT_DATE,
                   end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_follow_task_true(self):
        """ Check if BranchSQLOperator throws an exception for invalid connection """
        op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true=None,
            follow_task_ids_if_false="branch_2",
            dag=self.dag,
        )

        with self.assertRaises(AirflowException):
            op.run(start_date=DEFAULT_DATE,
                   end_date=DEFAULT_DATE, ignore_ti_state=True)

    def test_invalid_follow_task_false(self):
        """ Check if BranchSQLOperator throws an exception for invalid connection """
        op = BranchSQLOperator(
            task_id="make_choice",
            conn_id="invalid_connection",
            sql="SELECT count(1) FROM INFORMATION_SCHEMA.TABLES",
            follow_task_ids_if_true="branch_1",
            follow_task_ids_if_false=None,
            dag=self.dag,
        )

        with self.assertRaises(AirflowException):
            op.run(start_date=DEFAULT_DATE,
                   end_date=DEFAULT_DATE, ignore_ti_state=True)

    @pytest.mark.backend("mysql")
    def test_sql_branch_operator_mysql(self):
        """ Check if BranchSQLOperator works with backend """
        branch_op = BranchSQLOperator(
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
        """ Check if BranchSQLOperator works with backend """
        branch_op = BranchSQLOperator(
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

    @mock.patch("airflow.operators.sql.BaseHook")
    def test_branch_single_value_with_dag_run(self, mock_hook):
        """ Check BranchSQLOperator branch operation """
        branch_op = BranchSQLOperator(
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
                raise ValueError("Invalid task id {task_id} found!".format(task_id=ti.task_id))

    @mock.patch("airflow.operators.sql.BaseHook")
    def test_branch_true_with_dag_run(self, mock_hook):
        """ Check BranchSQLOperator branch operation """
        branch_op = BranchSQLOperator(
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
                    raise ValueError("Invalid task id {task_id} found!".format(task_id=ti.task_id))

    @mock.patch("airflow.operators.sql.BaseHook")
    def test_branch_false_with_dag_run(self, mock_hook):
        """ Check BranchSQLOperator branch operation """
        branch_op = BranchSQLOperator(
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
                    raise ValueError("Invalid task id {task_id} found!".format(task_id=ti.task_id))

    @mock.patch("airflow.operators.sql.BaseHook")
    def test_branch_list_with_dag_run(self, mock_hook):
        """ Checks if the BranchSQLOperator supports branching off to a list of tasks."""
        branch_op = BranchSQLOperator(
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
                raise ValueError("Invalid task id {task_id} found!".format(task_id=ti.task_id))

    @mock.patch("airflow.operators.sql.BaseHook")
    def test_invalid_query_result_with_dag_run(self, mock_hook):
        """ Check BranchSQLOperator branch operation """
        branch_op = BranchSQLOperator(
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

    @mock.patch("airflow.operators.sql.BaseHook")
    def test_with_skip_in_branch_downstream_dependencies(self, mock_hook):
        """ Test SQL Branch with skipping all downstream dependencies """
        branch_op = BranchSQLOperator(
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
                    raise ValueError("Invalid task id {task_id} found!".format(task_id=ti.task_id))

    @mock.patch("airflow.operators.sql.BaseHook")
    def test_with_skip_in_branch_downstream_dependencies2(self, mock_hook):
        """ Test skipping downstream dependency for false condition"""
        branch_op = BranchSQLOperator(
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
                    raise ValueError("Invalid task id {task_id} found!".format(task_id=ti.task_id))
