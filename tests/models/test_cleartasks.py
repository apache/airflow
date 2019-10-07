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

import datetime
import os
import unittest

from airflow import settings
from airflow.configuration import conf
from airflow.models import DAG, TaskInstance as TI, XCom, clear_task_instances
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.state import State
from tests.models import DEFAULT_DATE


class TestClearTasks(unittest.TestCase):

    def test_clear_task_instances(self):
        dag = DAG('test_clear_task_instances', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)

        ti0.run()
        ti1.run()
        session = settings.Session()
        qry = session.query(TI).filter(
            TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session, dag=dag)
        session.commit()
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 3)

    def test_clear_task_instances_without_task(self):
        dag = DAG('test_clear_task_instances_without_task', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='task0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='task1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        ti0.run()
        ti1.run()

        # Remove the task from dag.
        dag.task_dict = {}
        self.assertFalse(dag.has_task(task0.task_id))
        self.assertFalse(dag.has_task(task1.task_id))

        session = settings.Session()
        qry = session.query(TI).filter(
            TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.commit()
        # When dag is None, max_tries will be maximum of original max_tries or try_number.
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 2)

    def test_clear_task_instances_without_dag(self):
        dag = DAG('test_clear_task_instances_without_dag', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='task_0', owner='test', dag=dag)
        task1 = DummyOperator(task_id='task_1', owner='test', dag=dag, retries=2)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        ti0.run()
        ti1.run()

        session = settings.Session()
        qry = session.query(TI).filter(
            TI.dag_id == dag.dag_id).all()
        clear_task_instances(qry, session)
        session.commit()
        # When dag is None, max_tries will be maximum of original max_tries or try_number.
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # Next try to run will be try 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)
        self.assertEqual(ti1.try_number, 2)
        self.assertEqual(ti1.max_tries, 2)

    def test_dag_clear(self):
        dag = DAG('test_dag_clear', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task0 = DummyOperator(task_id='test_dag_clear_task_0', owner='test', dag=dag)
        ti0 = TI(task=task0, execution_date=DEFAULT_DATE)
        # Next try to run will be try 1
        self.assertEqual(ti0.try_number, 1)
        ti0.run()
        self.assertEqual(ti0.try_number, 2)
        dag.clear()
        ti0.refresh_from_db()
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.state, State.NONE)
        self.assertEqual(ti0.max_tries, 1)

        task1 = DummyOperator(task_id='test_dag_clear_task_1', owner='test',
                              dag=dag, retries=2)
        ti1 = TI(task=task1, execution_date=DEFAULT_DATE)
        self.assertEqual(ti1.max_tries, 2)
        ti1.try_number = 1
        # Next try will be 2
        ti1.run()
        self.assertEqual(ti1.try_number, 3)
        self.assertEqual(ti1.max_tries, 2)

        dag.clear()
        ti0.refresh_from_db()
        ti1.refresh_from_db()
        # after clear dag, ti2 should show attempt 3 of 5
        self.assertEqual(ti1.max_tries, 4)
        self.assertEqual(ti1.try_number, 3)
        # after clear dag, ti1 should show attempt 2 of 2
        self.assertEqual(ti0.try_number, 2)
        self.assertEqual(ti0.max_tries, 1)

    def test_dags_clear(self):
        # setup
        session = settings.Session()
        dags, tis = [], []
        num_of_dags = 5
        for i in range(num_of_dags):
            dag = DAG('test_dag_clear_' + str(i), start_date=DEFAULT_DATE,
                      end_date=DEFAULT_DATE + datetime.timedelta(days=10))
            ti = TI(task=DummyOperator(task_id='test_task_clear_' + str(i), owner='test',
                                       dag=dag),
                    execution_date=DEFAULT_DATE)
            dags.append(dag)
            tis.append(ti)

        # test clear all dags
        for i in range(num_of_dags):
            tis[i].run()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 2)
            self.assertEqual(tis[i].max_tries, 0)

        DAG.clear_dags(dags)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            self.assertEqual(tis[i].state, State.NONE)
            self.assertEqual(tis[i].try_number, 2)
            self.assertEqual(tis[i].max_tries, 1)

        # test dry_run
        for i in range(num_of_dags):
            tis[i].run()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 3)
            self.assertEqual(tis[i].max_tries, 1)

        DAG.clear_dags(dags, dry_run=True)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            self.assertEqual(tis[i].state, State.SUCCESS)
            self.assertEqual(tis[i].try_number, 3)
            self.assertEqual(tis[i].max_tries, 1)

        # test only_failed
        from random import randint
        failed_dag_idx = randint(0, len(tis) - 1)
        tis[failed_dag_idx].state = State.FAILED
        session.merge(tis[failed_dag_idx])
        session.commit()

        DAG.clear_dags(dags, only_failed=True)

        for i in range(num_of_dags):
            tis[i].refresh_from_db()
            if i != failed_dag_idx:
                self.assertEqual(tis[i].state, State.SUCCESS)
                self.assertEqual(tis[i].try_number, 3)
                self.assertEqual(tis[i].max_tries, 1)
            else:
                self.assertEqual(tis[i].state, State.NONE)
                self.assertEqual(tis[i].try_number, 3)
                self.assertEqual(tis[i].max_tries, 2)

    def test_operator_clear(self):
        dag = DAG('test_operator_clear', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        t1 = DummyOperator(task_id='bash_op', owner='test', dag=dag)
        t2 = DummyOperator(task_id='dummy_op', owner='test', dag=dag, retries=1)

        t2.set_upstream(t1)

        ti1 = TI(task=t1, execution_date=DEFAULT_DATE)
        ti2 = TI(task=t2, execution_date=DEFAULT_DATE)
        ti2.run()
        # Dependency not met
        self.assertEqual(ti2.try_number, 1)
        self.assertEqual(ti2.max_tries, 1)

        t2.clear(upstream=True)
        ti1.run()
        ti2.run()
        self.assertEqual(ti1.try_number, 2)
        # max_tries is 0 because there is no task instance in db for ti1
        # so clear won't change the max_tries.
        self.assertEqual(ti1.max_tries, 0)
        self.assertEqual(ti2.try_number, 2)
        # try_number (0) + retries(1)
        self.assertEqual(ti2.max_tries, 1)

    def test_xcom_disable_pickle_type(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test1"
        dag_id = "test_dag1"
        task_id = "test_task1"

        conf.set("core", "enable_xcom_pickling", "False")

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id,
                 task_id=task_id,
                 execution_date=execution_date)

        ret_value = XCom.get_one(key=key,
                                 dag_id=dag_id,
                                 task_id=task_id,
                                 execution_date=execution_date)

        self.assertEqual(ret_value, json_obj)

        session = settings.Session()
        ret_value = session.query(XCom).filter(XCom.key == key, XCom.dag_id == dag_id,
                                               XCom.task_id == task_id,
                                               XCom.execution_date == execution_date
                                               ).first().value

        self.assertEqual(ret_value, json_obj)

    def test_xcom_enable_pickle_type(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test2"
        dag_id = "test_dag2"
        task_id = "test_task2"

        conf.set("core", "enable_xcom_pickling", "True")

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id,
                 task_id=task_id,
                 execution_date=execution_date)

        ret_value = XCom.get_one(key=key,
                                 dag_id=dag_id,
                                 task_id=task_id,
                                 execution_date=execution_date)

        self.assertEqual(ret_value, json_obj)

        session = settings.Session()
        ret_value = session.query(XCom).filter(XCom.key == key, XCom.dag_id == dag_id,
                                               XCom.task_id == task_id,
                                               XCom.execution_date == execution_date
                                               ).first().value

        self.assertEqual(ret_value, json_obj)

    def test_xcom_disable_pickle_type_fail_on_non_json(self):
        class PickleRce:
            def __reduce__(self):
                return os.system, ("ls -alt",)

        conf.set("core", "xcom_enable_pickling", "False")

        self.assertRaises(TypeError, XCom.set,
                          key="xcom_test3",
                          value=PickleRce(),
                          dag_id="test_dag3",
                          task_id="test_task3",
                          execution_date=timezone.utcnow())

    def test_xcom_get_many(self):
        json_obj = {"key": "value"}
        execution_date = timezone.utcnow()
        key = "xcom_test4"
        dag_id1 = "test_dag4"
        task_id1 = "test_task4"
        dag_id2 = "test_dag5"
        task_id2 = "test_task5"

        conf.set("core", "xcom_enable_pickling", "True")

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id1,
                 task_id=task_id1,
                 execution_date=execution_date)

        XCom.set(key=key,
                 value=json_obj,
                 dag_id=dag_id2,
                 task_id=task_id2,
                 execution_date=execution_date)

        results = XCom.get_many(key=key,
                                execution_date=execution_date)

        for result in results:
            self.assertEqual(result.value, json_obj)
