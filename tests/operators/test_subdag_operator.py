# -*- coding: utf-8 -*-
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

import unittest
from unittest.mock import Mock

import airflow
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from airflow.utils.db import create_session
from tests.test_utils.db import clear_db_runs


DEFAULT_DATE = datetime(2016, 1, 1)

default_args = dict(
    owner='airflow',
    start_date=DEFAULT_DATE,
)


class SubDagOperatorTests(unittest.TestCase):

    def setUp(self):
        clear_db_runs()
        self.dag_run_running = DagRun()
        self.dag_run_running.state = State.RUNNING
        self.dag_run_success = DagRun()
        self.dag_run_success.state = State.SUCCESS
        self.dag_run_failed = DagRun()
        self.dag_run_failed.state = State.FAILED

    def test_subdag_name(self):
        """
        Subdag names must be {parent_dag}.{subdag task}
        """
        dag = DAG('parent', default_args=default_args)
        subdag_good = DAG('parent.test', default_args=default_args)
        subdag_bad1 = DAG('parent.bad', default_args=default_args)
        subdag_bad2 = DAG('bad.test', default_args=default_args)
        subdag_bad3 = DAG('bad.bad', default_args=default_args)

        SubDagOperator(task_id='test', dag=dag, subdag=subdag_good)
        self.assertRaises(
            AirflowException,
            SubDagOperator, task_id='test', dag=dag, subdag=subdag_bad1)
        self.assertRaises(
            AirflowException,
            SubDagOperator, task_id='test', dag=dag, subdag=subdag_bad2)
        self.assertRaises(
            AirflowException,
            SubDagOperator, task_id='test', dag=dag, subdag=subdag_bad3)

    def test_subdag_in_context_manager(self):
        """
        Creating a sub DAG within a main DAG's context manager
        """
        with DAG('parent', default_args=default_args) as dag:
            subdag = DAG('parent.test', default_args=default_args)
            op = SubDagOperator(task_id='test', subdag=subdag)

            self.assertEqual(op.dag, dag)
            self.assertEqual(op.subdag, subdag)

    def test_subdag_pools(self):
        """
        Subdags and subdag tasks can't both have a pool with 1 slot
        """
        dag = DAG('parent', default_args=default_args)
        subdag = DAG('parent.child', default_args=default_args)

        session = airflow.settings.Session()
        pool_1 = airflow.models.Pool(pool='test_pool_1', slots=1)
        pool_10 = airflow.models.Pool(pool='test_pool_10', slots=10)
        session.add(pool_1)
        session.add(pool_10)
        session.commit()

        DummyOperator(task_id='dummy', dag=subdag, pool='test_pool_1')

        self.assertRaises(
            AirflowException,
            SubDagOperator,
            task_id='child', dag=dag, subdag=subdag, pool='test_pool_1')

        # recreate dag because failed subdagoperator was already added
        dag = DAG('parent', default_args=default_args)
        SubDagOperator(
            task_id='child', dag=dag, subdag=subdag, pool='test_pool_10')

        session.delete(pool_1)
        session.delete(pool_10)
        session.commit()

    def test_subdag_pools_no_possible_conflict(self):
        """
        Subdags and subdag tasks with no pool overlap, should not to query
        pools
        """
        dag = DAG('parent', default_args=default_args)
        subdag = DAG('parent.child', default_args=default_args)

        session = airflow.settings.Session()
        pool_1 = airflow.models.Pool(pool='test_pool_1', slots=1)
        pool_10 = airflow.models.Pool(pool='test_pool_10', slots=10)
        session.add(pool_1)
        session.add(pool_10)
        session.commit()

        DummyOperator(task_id='dummy', dag=subdag, pool='test_pool_10')

        mock_session = Mock()
        SubDagOperator(
            task_id='child', dag=dag, subdag=subdag, pool='test_pool_1',
            session=mock_session)
        self.assertFalse(mock_session.query.called)

        session.delete(pool_1)
        session.delete(pool_10)
        session.commit()

    def test_execute_create_dagrun_wait_until_success(self):
        """
        When SubDagOperator executes, it creates a DagRun if there is no existing one
        and wait until the DagRun succeeds.
        """
        dag = DAG('parent', default_args=default_args)
        subdag = DAG('parent.test', default_args=default_args)
        subdag_task = SubDagOperator(task_id='test', subdag=subdag, dag=dag, poke_interval=1)

        subdag.create_dagrun = Mock()
        subdag.create_dagrun.return_value = self.dag_run_running

        subdag_task._get_dagrun = Mock()
        subdag_task._get_dagrun.side_effect = [None, self.dag_run_success, self.dag_run_success]

        subdag_task.pre_execute(context={'execution_date': DEFAULT_DATE})
        subdag_task.execute(context={'execution_date': DEFAULT_DATE})
        subdag_task.post_execute(context={'execution_date': DEFAULT_DATE})

        subdag.create_dagrun.assert_called_once_with(
            run_id="scheduled__{}".format(DEFAULT_DATE.isoformat()),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
            external_trigger=True,
        )

        self.assertEqual(3, len(subdag_task._get_dagrun.mock_calls))

    def test_execute_dagrun_failed(self):
        """
        When the DagRun failed during the execution, it raises an Airflow Exception.
        """
        dag = DAG('parent', default_args=default_args)
        subdag = DAG('parent.test', default_args=default_args)
        subdag_task = SubDagOperator(task_id='test', subdag=subdag, dag=dag, poke_interval=1)

        subdag.create_dagrun = Mock()
        subdag.create_dagrun.return_value = self.dag_run_running

        subdag_task._get_dagrun = Mock()
        subdag_task._get_dagrun.side_effect = [None, self.dag_run_failed, self.dag_run_failed]

        with self.assertRaises(AirflowException):
            subdag_task.pre_execute(context={'execution_date': DEFAULT_DATE})
            subdag_task.execute(context={'execution_date': DEFAULT_DATE})
            subdag_task.post_execute(context={'execution_date': DEFAULT_DATE})

    def test_execute_skip_if_dagrun_success(self):
        """
        When there is an existing DagRun in SUCCESS state, skip the execution.
        """
        dag = DAG('parent', default_args=default_args)
        subdag = DAG('parent.test', default_args=default_args)

        subdag.create_dagrun = Mock()
        subdag_task = SubDagOperator(task_id='test', subdag=subdag, dag=dag, poke_interval=1)
        subdag_task._get_dagrun = Mock()
        subdag_task._get_dagrun.return_value = self.dag_run_success

        subdag_task.pre_execute(context={'execution_date': DEFAULT_DATE})
        subdag_task.execute(context={'execution_date': DEFAULT_DATE})
        subdag_task.post_execute(context={'execution_date': DEFAULT_DATE})

        subdag.create_dagrun.assert_not_called()
        self.assertEqual(3, len(subdag_task._get_dagrun.mock_calls))

    def test_rerun_failed_subdag(self):
        """
        When there is an existing DagRun with failed state, reset the DagRun and the
        corresponding TaskInstances
        """
        dag = DAG('parent', default_args=default_args)
        subdag = DAG('parent.test', default_args=default_args)
        subdag_task = SubDagOperator(task_id='test', subdag=subdag, dag=dag, poke_interval=1)
        dummy_task = DummyOperator(task_id='dummy', dag=subdag)

        with create_session() as session:
            dummy_task_instance = TaskInstance(
                task=dummy_task,
                execution_date=DEFAULT_DATE,
                state=State.FAILED,
            )
            session.add(dummy_task_instance)
            session.commit()

        sub_dagrun = subdag.create_dagrun(
            run_id="scheduled__{}".format(DEFAULT_DATE.isoformat()),
            execution_date=DEFAULT_DATE,
            state=State.FAILED,
            external_trigger=True,
        )

        subdag_task._reset_dag_run_and_task_instances(sub_dagrun, execution_date=DEFAULT_DATE)

        dummy_task_instance.refresh_from_db()
        self.assertEqual(dummy_task_instance.state, State.NONE)

        sub_dagrun.refresh_from_db()
        self.assertEqual(sub_dagrun.state, State.RUNNING)
