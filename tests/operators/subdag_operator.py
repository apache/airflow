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

import unittest
from datetime import datetime

import airflow
from airflow import DAG
from airflow.operators import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.exceptions import AirflowException

default_args = dict(
    owner='airflow',
    start_date=datetime(2016, 1, 1),
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

    def test_subdag_pools(self):
        """
        Subdags and subdag tasks can't both have a pool with 1 slot
        """
        dag = DAG('parent', default_args=default_args)
        subdag = DAG('parent.test', default_args=default_args)

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
            task_id='test', dag=dag, subdag=subdag, pool='test_pool_1')

        # recreate dag because failed subdagoperator was already added
        dag = DAG('parent', default_args=default_args)
        SubDagOperator(
            task_id='test', dag=dag, subdag=subdag, pool='test_pool_10')

        session.delete(pool_1)
        session.delete(pool_10)
        session.commit()


if __name__ == "__main__":
    unittest.main()
