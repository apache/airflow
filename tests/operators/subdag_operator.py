# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import os
import unittest

from mock import Mock

import airflow
from airflow.models import DAG, DagBag
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.jobs import BackfillJob
from airflow.exceptions import AirflowException

DEFAULT_DATE = datetime.datetime(2016, 1, 1)

default_args = dict(
    owner='airflow',
    start_date=DEFAULT_DATE,
)

class SubDagOperatorTests(unittest.TestCase):

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

        dummy_1 = DummyOperator(task_id='dummy', dag=subdag, pool='test_pool_1')

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

        dummy_1 = DummyOperator(
            task_id='dummy', dag=subdag, pool='test_pool_10')

        mock_session = Mock()
        SubDagOperator(
            task_id='child', dag=dag, subdag=subdag, pool='test_pool_1',
            session=mock_session)
        self.assertFalse(mock_session.query.called)

        session.delete(pool_1)
        session.delete(pool_10)
        session.commit()

    def test_subdag_deadlock(self):
        dagbag = DagBag()
        dag = dagbag.get_dag('test_subdag_deadlock')
        dag.clear()
        subdag = dagbag.get_dag('test_subdag_deadlock.subdag')
        subdag.clear()

        # first make sure subdag has failed
        self.assertRaises(AirflowException, subdag.run, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        # now make sure dag picks up the subdag error
        self.assertRaises(AirflowException, dag.run, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
