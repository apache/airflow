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

from airflow import DAG
from airflow.exceptions import AirflowDagCycleException
from airflow.operators.dummy import DummyOperator
from airflow.utils.dag_cycle_tester import test_cycle as _test_cycle  # to avoid being a test function
from tests.models import DEFAULT_DATE


class TestCycleTester(unittest.TestCase):
    def test_cycle_empty(self):
        # test empty
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        self.assertFalse(_test_cycle(dag))

    def test_cycle_single_task(self):
        # test single task
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        with dag:
            DummyOperator(task_id='A')

        self.assertFalse(_test_cycle(dag))

    def test_semi_complex(self):
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        # A -> B -> C
        #      B -> D
        # E -> F
        with dag:
            create_cluster = DummyOperator(task_id="c")
            pod_task = DummyOperator(task_id="p")
            pod_task_xcom = DummyOperator(task_id="x")
            delete_cluster = DummyOperator(task_id="d")
            pod_task_xcom_result = DummyOperator(task_id="r")
            create_cluster >> pod_task >> delete_cluster
            create_cluster >> pod_task_xcom >> delete_cluster
            pod_task_xcom >> pod_task_xcom_result

    def test_cycle_no_cycle(self):
        # test no cycle
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        # A -> B -> C
        #      B -> D
        # E -> F
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op5 = DummyOperator(task_id='E')
            op6 = DummyOperator(task_id='F')
            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op2.set_downstream(op4)
            op5.set_downstream(op6)

        self.assertFalse(_test_cycle(dag))

    def test_cycle_loop(self):
        # test self loop
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        # A -> A
        with dag:
            op1 = DummyOperator(task_id='A')
            op1.set_downstream(op1)

        with self.assertRaises(AirflowDagCycleException):
            self.assertFalse(_test_cycle(dag))

    def test_cycle_downstream_loop(self):
        # test downstream self loop
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        # A -> B -> C -> D -> E -> E
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='D')
            op5 = DummyOperator(task_id='E')
            op1.set_downstream(op2)
            op2.set_downstream(op3)
            op3.set_downstream(op4)
            op4.set_downstream(op5)
            op5.set_downstream(op5)

        with self.assertRaises(AirflowDagCycleException):
            self.assertFalse(_test_cycle(dag))

    def test_cycle_large_loop(self):
        # large loop
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        # A -> B -> C -> D -> E -> A
        with dag:
            start = DummyOperator(task_id='start')
            current = start

            for i in range(10000):
                next_task = DummyOperator(task_id=f'task_{i}')
                current.set_downstream(next_task)
                current = next_task

            current.set_downstream(start)
        with self.assertRaises(AirflowDagCycleException):
            self.assertFalse(_test_cycle(dag))

    def test_cycle_arbitrary_loop(self):
        # test arbitrary loop
        dag = DAG('dag', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        # E-> A -> B -> F -> A
        #       -> C -> F
        with dag:
            op1 = DummyOperator(task_id='A')
            op2 = DummyOperator(task_id='B')
            op3 = DummyOperator(task_id='C')
            op4 = DummyOperator(task_id='E')
            op5 = DummyOperator(task_id='F')
            op1.set_downstream(op2)
            op1.set_downstream(op3)
            op4.set_downstream(op1)
            op3.set_downstream(op5)
            op2.set_downstream(op5)
            op5.set_downstream(op1)

        with self.assertRaises(AirflowDagCycleException):
            self.assertFalse(_test_cycle(dag))
