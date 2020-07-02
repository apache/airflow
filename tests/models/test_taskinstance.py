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
import time
import unittest
import urllib
from typing import Union, List
import pendulum
import pytest
from freezegun import freeze_time
from mock import patch, mock_open
from parameterized import parameterized, param
from sqlalchemy.orm.session import Session
from airflow import models, settings
from airflow.configuration import conf
from airflow.contrib.sensors.python_sensor import PythonSensor
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.models import (
    DAG, DagRun, Pool, RenderedTaskInstanceFields, TaskFail, TaskInstance as TI, TaskReschedule, Variable,
)
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.ti_deps.dep_context import REQUEUEABLE_DEPS, RUNNABLE_STATES, RUNNING_DEPS
from airflow.ti_deps.deps.base_ti_dep import TIDepStatus
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import timezone
from airflow.utils.db import create_session, provide_session
from airflow.utils.state import State
from tests.models import DEFAULT_DATE
from tests.test_utils import db


class TaskInstanceTest(unittest.TestCase):

    def setUp(self):
        db.clear_db_pools()
        db.clear_rendered_ti_fields()
        with create_session() as session:
            test_pool = Pool(pool='test_pool', slots=1)
            session.add(test_pool)
            session.commit()

    def tearDown(self):
        db.clear_db_pools()
        db.clear_rendered_ti_fields()
        with create_session() as session:
            session.query(TaskFail).delete()
            session.query(TaskReschedule).delete()
            session.query(models.TaskInstance).delete()
            session.query(models.DagRun).delete()

    def test_set_task_dates(self):
        """
        Test that tasks properly take start/end dates from DAGs
        """
        dag = DAG('dag', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))

        op1 = DummyOperator(task_id='op_1', owner='test')

        self.assertTrue(op1.start_date is None and op1.end_date is None)

        # dag should assign its dates to op1 because op1 has no dates
        dag.add_task(op1)
        self.assertTrue(
            op1.start_date == dag.start_date and op1.end_date == dag.end_date)

        op2 = DummyOperator(
            task_id='op_2',
            owner='test',
            start_date=DEFAULT_DATE - datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=11))

        # dag should assign its dates to op2 because they are more restrictive
        dag.add_task(op2)
        self.assertTrue(
            op2.start_date == dag.start_date and op2.end_date == dag.end_date)

        op3 = DummyOperator(
            task_id='op_3',
            owner='test',
            start_date=DEFAULT_DATE + datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=9))
        # op3 should keep its dates because they are more restrictive
        dag.add_task(op3)
        self.assertTrue(
            op3.start_date == DEFAULT_DATE + datetime.timedelta(days=1))
        self.assertTrue(
            op3.end_date == DEFAULT_DATE + datetime.timedelta(days=9))

    def test_timezone_awareness(self):
        NAIVE_DATETIME = DEFAULT_DATE.replace(tzinfo=None)

        # check ti without dag (just for bw compat)
        op_no_dag = DummyOperator(task_id='op_no_dag')
        ti = TI(task=op_no_dag, execution_date=NAIVE_DATETIME)

        self.assertEqual(ti.execution_date, DEFAULT_DATE)

        # check with dag without localized execution_date
        dag = DAG('dag', start_date=DEFAULT_DATE)
        op1 = DummyOperator(task_id='op_1')
        dag.add_task(op1)
        ti = TI(task=op1, execution_date=NAIVE_DATETIME)

        self.assertEqual(ti.execution_date, DEFAULT_DATE)

        # with dag and localized execution_date
        tz = pendulum.timezone("Europe/Amsterdam")
        execution_date = timezone.datetime(2016, 1, 1, 1, 0, 0, tzinfo=tz)
        utc_date = timezone.convert_to_utc(execution_date)
        ti = TI(task=op1, execution_date=execution_date)
        self.assertEqual(ti.execution_date, utc_date)

    def test_task_naive_datetime(self):
        NAIVE_DATETIME = DEFAULT_DATE.replace(tzinfo=None)

        op_no_dag = DummyOperator(task_id='test_task_naive_datetime',
                                  start_date=NAIVE_DATETIME,
                                  end_date=NAIVE_DATETIME)

        self.assertTrue(op_no_dag.start_date.tzinfo)
        self.assertTrue(op_no_dag.end_date.tzinfo)

    def test_set_dag(self):
        """
        Test assigning Operators to Dags, including deferred assignment
        """
        dag = DAG('dag', start_date=DEFAULT_DATE)
        dag2 = DAG('dag2', start_date=DEFAULT_DATE)
        op = DummyOperator(task_id='op_1', owner='test')

        # no dag assigned
        self.assertFalse(op.has_dag())
        self.assertRaises(AirflowException, getattr, op, 'dag')

        # no improper assignment
        with self.assertRaises(TypeError):
            op.dag = 1

        op.dag = dag

        # no reassignment
        with self.assertRaises(AirflowException):
            op.dag = dag2

        # but assigning the same dag is ok
        op.dag = dag

        self.assertIs(op.dag, dag)
        self.assertIn(op, dag.tasks)

    def test_infer_dag(self):
        dag = DAG('dag', start_date=DEFAULT_DATE)
        dag2 = DAG('dag2', start_date=DEFAULT_DATE)

        op1 = DummyOperator(task_id='test_op_1', owner='test')
        op2 = DummyOperator(task_id='test_op_2', owner='test')
        op3 = DummyOperator(task_id='test_op_3', owner='test', dag=dag)
        op4 = DummyOperator(task_id='test_op_4', owner='test', dag=dag2)

        # double check dags
        self.assertEqual(
            [i.has_dag() for i in [op1, op2, op3, op4]],
            [False, False, True, True])

        # can't combine operators with no dags
        self.assertRaises(AirflowException, op1.set_downstream, op2)

        # op2 should infer dag from op1
        op1.dag = dag
        op1.set_downstream(op2)
        self.assertIs(op2.dag, dag)

        # can't assign across multiple DAGs
        self.assertRaises(AirflowException, op1.set_downstream, op4)
        self.assertRaises(AirflowException, op1.set_downstream, [op3, op4])

    def test_bitshift_compose_operators(self):
        dag = DAG('dag', start_date=DEFAULT_DATE)
        op1 = DummyOperator(task_id='test_op_1', owner='test')
        op2 = DummyOperator(task_id='test_op_2', owner='test')
        op3 = DummyOperator(task_id='test_op_3', owner='test')
        op4 = DummyOperator(task_id='test_op_4', owner='test')
        op5 = DummyOperator(task_id='test_op_5', owner='test')

        # can't compose operators without dags
        with self.assertRaises(AirflowException):
            op1 >> op2

        dag >> op1 >> op2 << op3

        # make sure dag assignment carries through
        # using __rrshift__
        self.assertIs(op1.dag, dag)
        self.assertIs(op2.dag, dag)
        self.assertIs(op3.dag, dag)

        # op2 should be downstream of both
        self.assertIn(op2, op1.downstream_list)
        self.assertIn(op2, op3.downstream_list)

        # test dag assignment with __rlshift__
        dag << op4
        self.assertIs(op4.dag, dag)

        # dag assignment with __rrshift__
        dag >> op5
        self.assertIs(op5.dag, dag)

    @patch.object(DAG, 'concurrency_reached')
    def test_requeue_over_dag_concurrency(self, mock_concurrency_reached):
        mock_concurrency_reached.return_value = True

        dag = DAG(dag_id='test_requeue_over_dag_concurrency', start_date=DEFAULT_DATE,
                  max_active_runs=1, concurrency=2)
        task = DummyOperator(task_id='test_requeue_over_dag_concurrency_op', dag=dag)

        ti = TI(task=task, execution_date=timezone.utcnow(), state=State.QUEUED)
        # TI.run() will sync from DB before validating deps.
        with create_session() as session:
            session.add(ti)
            session.commit()
        ti.run()
        self.assertEqual(ti.state, State.NONE)

    def test_requeue_over_task_concurrency(self):
        dag = DAG(dag_id='test_requeue_over_task_concurrency', start_date=DEFAULT_DATE,
                  max_active_runs=1, concurrency=2)
        task = DummyOperator(task_id='test_requeue_over_task_concurrency_op', dag=dag,
                             task_concurrency=0)

        ti = TI(task=task, execution_date=timezone.utcnow(), state=State.QUEUED)
        # TI.run() will sync from DB before validating deps.
        with create_session() as session:
            session.add(ti)
            session.commit()
        ti.run()
        self.assertEqual(ti.state, State.NONE)

    def test_requeue_over_pool_concurrency(self):
        dag = DAG(dag_id='test_requeue_over_pool_concurrency', start_date=DEFAULT_DATE,
                  max_active_runs=1, concurrency=2)
        task = DummyOperator(task_id='test_requeue_over_pool_concurrency_op', dag=dag,
                             task_concurrency=0)

        ti = TI(task=task, execution_date=timezone.utcnow(), state=State.QUEUED)
        # TI.run() will sync from DB before validating deps.
        with create_session() as session:
            pool = session.query(Pool).filter(Pool.pool == 'test_pool').one()
            pool.slots = 0
            session.add(ti)
            session.commit()
        ti.run()
        self.assertEqual(ti.state, State.NONE)

    def test_not_requeue_non_requeueable_task_instance(self):
        dag = models.DAG(dag_id='test_not_requeue_non_requeueable_task_instance')
        # Use BaseSensorOperator because sensor got
        # one additional DEP in BaseSensorOperator().deps
        task = BaseSensorOperator(
            task_id='test_not_requeue_non_requeueable_task_instance_op',
            dag=dag,
            pool='test_pool',
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow(), state=State.QUEUED)
        with create_session() as session:
            session.add(ti)
            session.commit()

        all_deps = RUNNING_DEPS | task.deps
        all_non_requeueable_deps = all_deps - REQUEUEABLE_DEPS
        patch_dict = {}
        for dep in all_non_requeueable_deps:
            class_name = dep.__class__.__name__
            dep_patch = patch('%s.%s.%s' % (dep.__module__, class_name,
                                            dep._get_dep_statuses.__name__))
            method_patch = dep_patch.start()
            method_patch.return_value = iter([TIDepStatus('mock_' + class_name, True,
                                                          'mock')])
            patch_dict[class_name] = (dep_patch, method_patch)

        for class_name, (dep_patch, method_patch) in patch_dict.items():
            method_patch.return_value = iter(
                [TIDepStatus('mock_' + class_name, False, 'mock')])
            ti.run()
            self.assertEqual(ti.state, State.QUEUED)
            dep_patch.return_value = TIDepStatus('mock_' + class_name, True, 'mock')

        for (dep_patch, method_patch) in patch_dict.values():
            dep_patch.stop()

    def test_mark_non_runnable_task_as_success(self):
        """
        test that running task with mark_success param update task state
        as SUCCESS without running task despite it fails dependency checks.
        """
        non_runnable_state = (
            set(State.task_states) - RUNNABLE_STATES - set(State.SUCCESS)).pop()
        dag = models.DAG(dag_id='test_mark_non_runnable_task_as_success')
        task = DummyOperator(
            task_id='test_mark_non_runnable_task_as_success_op',
            dag=dag,
            pool='test_pool',
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow(), state=non_runnable_state)
        # TI.run() will sync from DB before validating deps.
        with create_session() as session:
            session.add(ti)
            session.commit()
        ti.run(mark_success=True)
        self.assertEqual(ti.state, State.SUCCESS)

    def test_run_pooling_task(self):
        """
        test that running a task in an existing pool update task state as SUCCESS.
        """
        dag = models.DAG(dag_id='test_run_pooling_task')
        task = DummyOperator(task_id='test_run_pooling_task_op', dag=dag,
                             pool='test_pool', owner='airflow',
                             start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow())
        ti.run()

        db.clear_db_pools()
        self.assertEqual(ti.state, State.SUCCESS)

    def test_pool_slots_property(self):
        """
        test that try to create a task with pool_slots less than 1
        """
        def create_task_instance():
            dag = models.DAG(dag_id='test_run_pooling_task')
            task = DummyOperator(task_id='test_run_pooling_task_op', dag=dag,
                                 pool='test_pool', pool_slots=0, owner='airflow',
                                 start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
            return TI(task=task, execution_date=timezone.utcnow())

        self.assertRaises(AirflowException, create_task_instance)

    @provide_session
    def test_ti_updates_with_task(self, session=None):
        """
        test that updating the executor_config propogates to the TaskInstance DB
        """
        dag = models.DAG(dag_id='test_run_pooling_task')
        task = DummyOperator(task_id='test_run_pooling_task_op', dag=dag, owner='airflow',
                             executor_config={'foo': 'bar'},
                             start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow())

        ti.run(session=session)
        tis = dag.get_task_instances()
        self.assertEqual({'foo': 'bar'}, tis[0].executor_config)

        task2 = DummyOperator(task_id='test_run_pooling_task_op', dag=dag, owner='airflow',
                              executor_config={'bar': 'baz'},
                              start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))

        ti = TI(
            task=task2, execution_date=timezone.utcnow())
        ti.run(session=session)
        tis = dag.get_task_instances()
        self.assertEqual({'bar': 'baz'}, tis[1].executor_config)

    def test_run_pooling_task_with_mark_success(self):
        """
        test that running task in an existing pool with mark_success param
        update task state as SUCCESS without running task
        despite it fails dependency checks.
        """
        dag = models.DAG(dag_id='test_run_pooling_task_with_mark_success')
        task = DummyOperator(
            task_id='test_run_pooling_task_with_mark_success_op',
            dag=dag,
            pool='test_pool',
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow())
        ti.run(mark_success=True)
        self.assertEqual(ti.state, State.SUCCESS)

    def test_run_pooling_task_with_skip(self):
        """
        test that running task which returns AirflowSkipOperator will end
        up in a SKIPPED state.
        """

        def raise_skip_exception():
            raise AirflowSkipException

        dag = models.DAG(dag_id='test_run_pooling_task_with_skip')
        task = PythonOperator(
            task_id='test_run_pooling_task_with_skip',
            dag=dag,
            python_callable=raise_skip_exception,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=timezone.utcnow())
        ti.run()
        self.assertEqual(State.SKIPPED, ti.state)

    def test_retry_delay(self):
        """
        Test that retry delays are respected
        """
        dag = models.DAG(dag_id='test_retry_handling')
        task = BashOperator(
            task_id='test_retry_handling_op',
            bash_command='exit 1',
            retries=1,
            retry_delay=datetime.timedelta(seconds=3),
            dag=dag,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))

        def run_with_error(ti):
            try:
                ti.run()
            except AirflowException:
                pass

        ti = TI(
            task=task, execution_date=timezone.utcnow())

        self.assertEqual(ti.try_number, 1)
        # first run -- up for retry
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 2)

        # second run -- still up for retry because retry_delay hasn't expired
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)

        # third run -- failed
        time.sleep(3)
        run_with_error(ti)
        self.assertEqual(ti.state, State.FAILED)

    @parameterized.expand([
        (False, None,),
        (True, {'env': None, 'bash_command': 'echo test_retry_handling; exit 1'},),
    ])
    def test_retry_handling(self, dag_serialization, expected_rendered_ti_fields):
        """
        Test that task retries are handled properly
        """
        with patch("airflow.models.taskinstance.STORE_SERIALIZED_DAGS", dag_serialization):
            dag = models.DAG(dag_id='test_retry_handling')
            task = BashOperator(
                task_id='test_retry_handling_op',
                bash_command='echo {{dag.dag_id}}; exit 1',
                retries=1,
                retry_delay=datetime.timedelta(seconds=0),
                dag=dag,
                owner='test_pool',
                start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))

            def run_with_error(ti):
                try:
                    ti.run()
                except AirflowException:
                    pass

            ti = TI(
                task=task, execution_date=timezone.utcnow())
            self.assertEqual(ti.try_number, 1)

            # first run -- up for retry
            run_with_error(ti)
            self.assertEqual(ti.state, State.UP_FOR_RETRY)
            self.assertEqual(ti._try_number, 1)
            self.assertEqual(ti.try_number, 2)

            # second run -- fail
            run_with_error(ti)
            self.assertEqual(ti.state, State.FAILED)
            self.assertEqual(ti._try_number, 2)
            self.assertEqual(ti.try_number, 3)

            # Clear the TI state since you can't run a task with a FAILED state without
            # clearing it first
            dag.clear()

            # third run -- up for retry
            run_with_error(ti)
            self.assertEqual(ti.state, State.UP_FOR_RETRY)
            self.assertEqual(ti._try_number, 3)
            self.assertEqual(ti.try_number, 4)

            # fourth run -- fail
            run_with_error(ti)
            ti.refresh_from_db()
            self.assertEqual(ti.state, State.FAILED)
            self.assertEqual(ti._try_number, 4)
            self.assertEqual(ti.try_number, 5)
            self.assertEqual(RenderedTaskInstanceFields.get_templated_fields(ti), expected_rendered_ti_fields)

    def test_next_retry_datetime(self):
        delay = datetime.timedelta(seconds=30)
        max_delay = datetime.timedelta(minutes=60)

        dag = models.DAG(dag_id='fail_dag')
        task = BashOperator(
            task_id='task_with_exp_backoff_and_max_delay',
            bash_command='exit 1',
            retries=3,
            retry_delay=delay,
            retry_exponential_backoff=True,
            max_retry_delay=max_delay,
            dag=dag,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=DEFAULT_DATE)
        ti.end_date = pendulum.instance(timezone.utcnow())

        dt = ti.next_retry_datetime()
        # between 30 * 2^0.5 and 30 * 2^1 (15 and 30)
        period = ti.end_date.add(seconds=30) - ti.end_date.add(seconds=15)
        self.assertTrue(dt in period)

        ti.try_number = 3
        dt = ti.next_retry_datetime()
        # between 30 * 2^2 and 30 * 2^3 (120 and 240)
        period = ti.end_date.add(seconds=240) - ti.end_date.add(seconds=120)
        self.assertTrue(dt in period)

        ti.try_number = 5
        dt = ti.next_retry_datetime()
        # between 30 * 2^4 and 30 * 2^5 (480 and 960)
        period = ti.end_date.add(seconds=960) - ti.end_date.add(seconds=480)
        self.assertTrue(dt in period)

        ti.try_number = 9
        dt = ti.next_retry_datetime()
        self.assertEqual(dt, ti.end_date + max_delay)

        ti.try_number = 50
        dt = ti.next_retry_datetime()
        self.assertEqual(dt, ti.end_date + max_delay)

    def test_next_retry_datetime_short_intervals(self):
        delay = datetime.timedelta(seconds=1)
        max_delay = datetime.timedelta(minutes=60)

        dag = models.DAG(dag_id='fail_dag')
        task = BashOperator(
            task_id='task_with_exp_backoff_and_short_time_interval',
            bash_command='exit 1',
            retries=3,
            retry_delay=delay,
            retry_exponential_backoff=True,
            max_retry_delay=max_delay,
            dag=dag,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))
        ti = TI(
            task=task, execution_date=DEFAULT_DATE)
        ti.end_date = pendulum.instance(timezone.utcnow())

        dt = ti.next_retry_datetime()
        # between 1 * 2^0.5 and 1 * 2^1 (15 and 30)
        period = ti.end_date.add(seconds=1) - ti.end_date.add(seconds=15)
        self.assertTrue(dt in period)

    @patch.object(TI, 'pool_full')
    def test_reschedule_handling(self, mock_pool_full):
        """
        Test that task reschedules are handled properly
        """
        # Return values of the python sensor callable, modified during tests
        done = False
        fail = False

        def callable():
            if fail:
                raise AirflowException()
            return done

        dag = models.DAG(dag_id='test_reschedule_handling')
        task = PythonSensor(
            task_id='test_reschedule_handling_sensor',
            poke_interval=0,
            mode='reschedule',
            python_callable=callable,
            retries=1,
            retry_delay=datetime.timedelta(seconds=0),
            dag=dag,
            owner='airflow',
            pool='test_pool',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))

        ti = TI(task=task, execution_date=timezone.utcnow())
        self.assertEqual(ti._try_number, 0)
        self.assertEqual(ti.try_number, 1)

        def run_ti_and_assert(run_date, expected_start_date, expected_end_date,
                              expected_duration,
                              expected_state, expected_try_number,
                              expected_task_reschedule_count):
            with freeze_time(run_date):
                try:
                    ti.run()
                except AirflowException:
                    if not fail:
                        raise
            ti.refresh_from_db()
            self.assertEqual(ti.state, expected_state)
            self.assertEqual(ti._try_number, expected_try_number)
            self.assertEqual(ti.try_number, expected_try_number + 1)
            self.assertEqual(ti.start_date, expected_start_date)
            self.assertEqual(ti.end_date, expected_end_date)
            self.assertEqual(ti.duration, expected_duration)
            trs = TaskReschedule.find_for_task_instance(ti)
            self.assertEqual(len(trs), expected_task_reschedule_count)

        date1 = timezone.utcnow()
        date2 = date1 + datetime.timedelta(minutes=1)
        date3 = date2 + datetime.timedelta(minutes=1)
        date4 = date3 + datetime.timedelta(minutes=1)

        # Run with multiple reschedules.
        # During reschedule the try number remains the same, but each reschedule is recorded.
        # The start date is expected to remain the initial date, hence the duration increases.
        # When finished the try number is incremented and there is no reschedule expected
        # for this try.

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 0, 1)

        done, fail = False, False
        run_ti_and_assert(date2, date1, date2, 60, State.UP_FOR_RESCHEDULE, 0, 2)

        done, fail = False, False
        run_ti_and_assert(date3, date1, date3, 120, State.UP_FOR_RESCHEDULE, 0, 3)

        done, fail = True, False
        run_ti_and_assert(date4, date1, date4, 180, State.SUCCESS, 1, 0)

        # Clear the task instance.
        dag.clear()
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.NONE)
        self.assertEqual(ti._try_number, 1)

        # Run again after clearing with reschedules and a retry.
        # The retry increments the try number, and for that try no reschedule is expected.
        # After the retry the start date is reset, hence the duration is also reset.

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 1, 1)

        done, fail = False, True
        run_ti_and_assert(date2, date1, date2, 60, State.UP_FOR_RETRY, 2, 0)

        done, fail = False, False
        run_ti_and_assert(date3, date3, date3, 0, State.UP_FOR_RESCHEDULE, 2, 1)

        done, fail = True, False
        run_ti_and_assert(date4, date3, date4, 60, State.SUCCESS, 3, 0)

    @patch.object(TI, 'pool_full')
    def test_reschedule_handling_clear_reschedules(self, mock_pool_full):
        """
        Test that task reschedules clearing are handled properly
        """
        # Return values of the python sensor callable, modified during tests
        done = False
        fail = False

        def callable():
            if fail:
                raise AirflowException()
            return done

        dag = models.DAG(dag_id='test_reschedule_handling')
        task = PythonSensor(
            task_id='test_reschedule_handling_sensor',
            poke_interval=0,
            mode='reschedule',
            python_callable=callable,
            retries=1,
            retry_delay=datetime.timedelta(seconds=0),
            dag=dag,
            owner='airflow',
            pool='test_pool',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0))

        ti = TI(task=task, execution_date=timezone.utcnow())
        self.assertEqual(ti._try_number, 0)
        self.assertEqual(ti.try_number, 1)

        def run_ti_and_assert(run_date, expected_start_date, expected_end_date,
                              expected_duration,
                              expected_state, expected_try_number,
                              expected_task_reschedule_count):
            with freeze_time(run_date):
                try:
                    ti.run()
                except AirflowException:
                    if not fail:
                        raise
            ti.refresh_from_db()
            self.assertEqual(ti.state, expected_state)
            self.assertEqual(ti._try_number, expected_try_number)
            self.assertEqual(ti.try_number, expected_try_number + 1)
            self.assertEqual(ti.start_date, expected_start_date)
            self.assertEqual(ti.end_date, expected_end_date)
            self.assertEqual(ti.duration, expected_duration)
            trs = TaskReschedule.find_for_task_instance(ti)
            self.assertEqual(len(trs), expected_task_reschedule_count)

        date1 = timezone.utcnow()

        done, fail = False, False
        run_ti_and_assert(date1, date1, date1, 0, State.UP_FOR_RESCHEDULE, 0, 1)

        # Clear the task instance.
        dag.clear()
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.NONE)
        self.assertEqual(ti._try_number, 0)
        # Check that reschedules for ti have also been cleared.
        trs = TaskReschedule.find_for_task_instance(ti)
        self.assertFalse(trs)

    def test_depends_on_past(self):
        dag = DAG(
            dag_id='test_depends_on_past',
            start_date=DEFAULT_DATE
        )

        task = DummyOperator(
            task_id='test_dop_task',
            dag=dag,
            depends_on_past=True,
        )
        dag.clear()

        run_date = task.start_date + datetime.timedelta(days=5)

        ti = TI(task, run_date)

        # depends_on_past prevents the run
        task.run(start_date=run_date, end_date=run_date)
        ti.refresh_from_db()
        self.assertIs(ti.state, None)

        # ignore first depends_on_past to allow the run
        task.run(
            start_date=run_date,
            end_date=run_date,
            ignore_first_depends_on_past=True)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    # Parameterized tests to check for the correct firing
    # of the trigger_rule under various circumstances
    # Numeric fields are in order:
    #   successes, skipped, failed, upstream_failed, done
    @parameterized.expand([

        #
        # Tests for all_success
        #
        ['all_success', 5, 0, 0, 0, 0, True, None, True],
        ['all_success', 2, 0, 0, 0, 0, True, None, False],
        ['all_success', 2, 0, 1, 0, 0, True, State.UPSTREAM_FAILED, False],
        ['all_success', 2, 1, 0, 0, 0, True, State.SKIPPED, False],
        #
        # Tests for one_success
        #
        ['one_success', 5, 0, 0, 0, 5, True, None, True],
        ['one_success', 2, 0, 0, 0, 2, True, None, True],
        ['one_success', 2, 0, 1, 0, 3, True, None, True],
        ['one_success', 2, 1, 0, 0, 3, True, None, True],
        #
        # Tests for all_failed
        #
        ['all_failed', 5, 0, 0, 0, 5, True, State.SKIPPED, False],
        ['all_failed', 0, 0, 5, 0, 5, True, None, True],
        ['all_failed', 2, 0, 0, 0, 2, True, State.SKIPPED, False],
        ['all_failed', 2, 0, 1, 0, 3, True, State.SKIPPED, False],
        ['all_failed', 2, 1, 0, 0, 3, True, State.SKIPPED, False],
        #
        # Tests for one_failed
        #
        ['one_failed', 5, 0, 0, 0, 0, True, None, False],
        ['one_failed', 2, 0, 0, 0, 0, True, None, False],
        ['one_failed', 2, 0, 1, 0, 0, True, None, True],
        ['one_failed', 2, 1, 0, 0, 3, True, None, False],
        ['one_failed', 2, 3, 0, 0, 5, True, State.SKIPPED, False],
        #
        # Tests for done
        #
        ['all_done', 5, 0, 0, 0, 5, True, None, True],
        ['all_done', 2, 0, 0, 0, 2, True, None, False],
        ['all_done', 2, 0, 1, 0, 3, True, None, False],
        ['all_done', 2, 1, 0, 0, 3, True, None, False]
    ])
    def test_check_task_dependencies(self, trigger_rule, successes, skipped,
                                     failed, upstream_failed, done,
                                     flag_upstream_failed,
                                     expect_state, expect_completed):
        start_date = timezone.datetime(2016, 2, 1, 0, 0, 0)
        dag = models.DAG('test-dag', start_date=start_date)
        downstream = DummyOperator(task_id='downstream',
                                   dag=dag, owner='airflow',
                                   trigger_rule=trigger_rule)
        for i in range(5):
            task = DummyOperator(task_id='runme_{}'.format(i),
                                 dag=dag, owner='airflow')
            task.set_downstream(downstream)
        run_date = task.start_date + datetime.timedelta(days=5)

        ti = TI(downstream, run_date)
        dep_results = TriggerRuleDep()._evaluate_trigger_rule(
            ti=ti,
            successes=successes,
            skipped=skipped,
            failed=failed,
            upstream_failed=upstream_failed,
            done=done,
            flag_upstream_failed=flag_upstream_failed)
        completed = all([dep.passed for dep in dep_results])

        self.assertEqual(completed, expect_completed)
        self.assertEqual(ti.state, expect_state)

    def test_xcom_pull(self):
        """
        Test xcom_pull, using different filtering methods.
        """
        dag = models.DAG(
            dag_id='test_xcom', schedule_interval='@monthly',
            start_date=timezone.datetime(2016, 6, 1, 0, 0, 0))

        exec_date = timezone.utcnow()

        # Push a value
        task1 = DummyOperator(task_id='test_xcom_1', dag=dag, owner='airflow')
        ti1 = TI(task=task1, execution_date=exec_date)
        ti1.xcom_push(key='foo', value='bar')

        # Push another value with the same key (but by a different task)
        task2 = DummyOperator(task_id='test_xcom_2', dag=dag, owner='airflow')
        ti2 = TI(task=task2, execution_date=exec_date)
        ti2.xcom_push(key='foo', value='baz')

        # Pull with no arguments
        result = ti1.xcom_pull()
        self.assertEqual(result, None)
        # Pull the value pushed most recently by any task.
        result = ti1.xcom_pull(key='foo')
        self.assertIn(result, 'baz')
        # Pull the value pushed by the first task
        result = ti1.xcom_pull(task_ids='test_xcom_1', key='foo')
        self.assertEqual(result, 'bar')
        # Pull the value pushed by the second task
        result = ti1.xcom_pull(task_ids='test_xcom_2', key='foo')
        self.assertEqual(result, 'baz')
        # Pull the values pushed by both tasks
        result = ti1.xcom_pull(
            task_ids=['test_xcom_1', 'test_xcom_2'], key='foo')
        self.assertEqual(result, ('bar', 'baz'))

    def test_xcom_pull_after_success(self):
        """
        tests xcom set/clear relative to a task in a 'success' rerun scenario
        """
        key = 'xcom_key'
        value = 'xcom_value'

        dag = models.DAG(dag_id='test_xcom', schedule_interval='@monthly')
        task = DummyOperator(
            task_id='test_xcom',
            dag=dag,
            pool='test_xcom',
            owner='airflow',
            start_date=timezone.datetime(2016, 6, 2, 0, 0, 0))
        exec_date = timezone.utcnow()
        ti = TI(
            task=task, execution_date=exec_date)
        ti.run(mark_success=True)
        ti.xcom_push(key=key, value=value)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), value)
        ti.run()
        # The second run and assert is to handle AIRFLOW-131 (don't clear on
        # prior success)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), value)

        # Test AIRFLOW-703: Xcom shouldn't be cleared if the task doesn't
        # execute, even if dependencies are ignored
        ti.run(ignore_all_deps=True, mark_success=True)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), value)
        # Xcom IS finally cleared once task has executed
        ti.run(ignore_all_deps=True)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), None)

    def test_xcom_pull_different_execution_date(self):
        """
        tests xcom fetch behavior with different execution dates, using
        both xcom_pull with "include_prior_dates" and without
        """
        key = 'xcom_key'
        value = 'xcom_value'

        dag = models.DAG(dag_id='test_xcom', schedule_interval='@monthly')
        task = DummyOperator(
            task_id='test_xcom',
            dag=dag,
            pool='test_xcom',
            owner='airflow',
            start_date=timezone.datetime(2016, 6, 2, 0, 0, 0))
        exec_date = timezone.utcnow()
        ti = TI(
            task=task, execution_date=exec_date)
        ti.run(mark_success=True)
        ti.xcom_push(key=key, value=value)
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), value)
        ti.run()
        exec_date += datetime.timedelta(days=1)
        ti = TI(
            task=task, execution_date=exec_date)
        ti.run()
        # We have set a new execution date (and did not pass in
        # 'include_prior_dates'which means this task should now have a cleared
        # xcom value
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom', key=key), None)
        # We *should* get a value using 'include_prior_dates'
        self.assertEqual(ti.xcom_pull(task_ids='test_xcom',
                                      key=key,
                                      include_prior_dates=True),
                         value)

    def test_xcom_push_flag(self):
        """
        Tests the option for Operators to push XComs
        """
        value = 'hello'
        task_id = 'test_no_xcom_push'
        dag = models.DAG(dag_id='test_xcom')

        # nothing saved to XCom
        task = PythonOperator(
            task_id=task_id,
            dag=dag,
            python_callable=lambda: value,
            do_xcom_push=False,
            owner='airflow',
            start_date=datetime.datetime(2017, 1, 1)
        )
        ti = TI(task=task, execution_date=datetime.datetime(2017, 1, 1))
        ti.run()
        self.assertEqual(
            ti.xcom_pull(
                task_ids=task_id, key=models.XCOM_RETURN_KEY
            ),
            None
        )

    def test_post_execute_hook(self):
        """
        Test that post_execute hook is called with the Operator's result.
        The result ('error') will cause an error to be raised and trapped.
        """

        class TestError(Exception):
            pass

        class TestOperator(PythonOperator):
            def post_execute(self, context, result):
                if result == 'error':
                    raise TestError('expected error.')

        dag = models.DAG(dag_id='test_post_execute_dag')
        task = TestOperator(
            task_id='test_operator',
            dag=dag,
            python_callable=lambda: 'error',
            owner='airflow',
            start_date=timezone.datetime(2017, 2, 1))
        ti = TI(task=task, execution_date=timezone.utcnow())

        with self.assertRaises(TestError):
            ti.run()

    def test_check_and_change_state_before_execution(self):
        dag = models.DAG(dag_id='test_check_and_change_state_before_execution')
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)
        ti = TI(
            task=task, execution_date=timezone.utcnow())
        self.assertEqual(ti._try_number, 0)
        self.assertTrue(ti._check_and_change_state_before_execution())
        # State should be running, and try_number column should be incremented
        self.assertEqual(ti.state, State.RUNNING)
        self.assertEqual(ti._try_number, 1)

    def test_check_and_change_state_before_execution_dep_not_met(self):
        dag = models.DAG(dag_id='test_check_and_change_state_before_execution')
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)
        task2 = DummyOperator(task_id='task2', dag=dag, start_date=DEFAULT_DATE)
        task >> task2
        ti = TI(
            task=task2, execution_date=timezone.utcnow())
        self.assertFalse(ti._check_and_change_state_before_execution())

    def test_try_number(self):
        """
        Test the try_number accessor behaves in various running states
        """
        dag = models.DAG(dag_id='test_check_and_change_state_before_execution')
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)
        ti = TI(task=task, execution_date=timezone.utcnow())
        self.assertEqual(1, ti.try_number)
        ti.try_number = 2
        ti.state = State.RUNNING
        self.assertEqual(2, ti.try_number)
        ti.state = State.SUCCESS
        self.assertEqual(3, ti.try_number)

    def test_get_num_running_task_instances(self):
        session = settings.Session()

        dag = models.DAG(dag_id='test_get_num_running_task_instances')
        dag2 = models.DAG(dag_id='test_get_num_running_task_instances_dummy')
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)
        task2 = DummyOperator(task_id='task', dag=dag2, start_date=DEFAULT_DATE)

        ti1 = TI(task=task, execution_date=DEFAULT_DATE)
        ti2 = TI(task=task, execution_date=DEFAULT_DATE + datetime.timedelta(days=1))
        ti3 = TI(task=task2, execution_date=DEFAULT_DATE)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        ti3.state = State.RUNNING
        session.add(ti1)
        session.add(ti2)
        session.add(ti3)
        session.commit()

        self.assertEqual(1, ti1.get_num_running_task_instances(session=session))
        self.assertEqual(1, ti2.get_num_running_task_instances(session=session))
        self.assertEqual(1, ti3.get_num_running_task_instances(session=session))

    def test_log_url(self):
        dag = DAG('dag', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='op', dag=dag)
        ti = TI(task=task, execution_date=datetime.datetime(2018, 1, 1))

        expected_url = (
            'http://localhost:8080/admin/airflow/log?'
            'execution_date=2018-01-01T00%3A00%3A00%2B00%3A00'
            '&task_id=op'
            '&dag_id=dag'
        )
        self.assertEqual(ti.log_url, expected_url)

    def test_log_url_rbac(self):
        dag = DAG('dag', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='op', dag=dag)
        ti = TI(task=task, execution_date=datetime.datetime(2018, 1, 1))
        conf.set('webserver', 'rbac', 'True')

        expected_url = (
            'http://localhost:8080/log?'
            'execution_date=2018-01-01T00%3A00%3A00%2B00%3A00'
            '&task_id=op'
            '&dag_id=dag'
        )
        self.assertEqual(ti.log_url, expected_url)

    def test_mark_success_url(self):
        now = pendulum.now('Europe/Brussels')
        dag = DAG('dag', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='op', dag=dag)
        ti = TI(task=task, execution_date=now)
        d = urllib.parse.parse_qs(
            urllib.parse.urlparse(ti.mark_success_url).query,
            keep_blank_values=True, strict_parsing=True)
        self.assertEqual(d['dag_id'][0], 'dag')
        self.assertEqual(d['task_id'][0], 'op')
        self.assertEqual(pendulum.parse(d['execution_date'][0]), now)

    def test_overwrite_params_with_dag_run_conf(self):
        task = DummyOperator(task_id='op')
        ti = TI(task=task, execution_date=datetime.datetime.now())
        dag_run = DagRun()
        dag_run.conf = {"override": True}
        params = {"override": False}

        ti.overwrite_params_with_dag_run_conf(params, dag_run)

        self.assertEqual(True, params["override"])

    def test_overwrite_params_with_dag_run_none(self):
        task = DummyOperator(task_id='op')
        ti = TI(task=task, execution_date=datetime.datetime.now())
        params = {"override": False}

        ti.overwrite_params_with_dag_run_conf(params, None)

        self.assertEqual(False, params["override"])

    def test_overwrite_params_with_dag_run_conf_none(self):
        task = DummyOperator(task_id='op')
        ti = TI(task=task, execution_date=datetime.datetime.now())
        params = {"override": False}
        dag_run = DagRun()

        ti.overwrite_params_with_dag_run_conf(params, dag_run)

        self.assertEqual(False, params["override"])

    @patch('airflow.models.taskinstance.send_email')
    def test_email_alert(self, mock_send_email):
        dag = models.DAG(dag_id='test_failure_email')
        task = BashOperator(
            task_id='test_email_alert',
            dag=dag,
            bash_command='exit 1',
            start_date=DEFAULT_DATE,
            email='to')

        ti = TI(task=task, execution_date=datetime.datetime.now())

        try:
            ti.run()
        except AirflowException:
            pass

        (email, title, body), _ = mock_send_email.call_args
        self.assertEqual(email, 'to')
        self.assertIn('test_email_alert', title)
        self.assertIn('test_email_alert', body)
        self.assertIn('Try 1', body)

    @patch('airflow.models.taskinstance.send_email')
    def test_email_alert_with_config(self, mock_send_email):
        dag = models.DAG(dag_id='test_failure_email')
        task = BashOperator(
            task_id='test_email_alert_with_config',
            dag=dag,
            bash_command='exit 1',
            start_date=DEFAULT_DATE,
            email='to')

        ti = TI(
            task=task, execution_date=datetime.datetime.now())

        conf.set('email', 'subject_template', '/subject/path')
        conf.set('email', 'html_content_template', '/html_content/path')

        opener = mock_open(read_data='template: {{ti.task_id}}')
        with patch('airflow.models.taskinstance.open', opener, create=True):
            try:
                ti.run()
            except AirflowException:
                pass

        (email, title, body), _ = mock_send_email.call_args
        self.assertEqual(email, 'to')
        self.assertEqual('template: test_email_alert_with_config', title)
        self.assertEqual('template: test_email_alert_with_config', body)

    def test_set_duration(self):
        task = DummyOperator(task_id='op', email='test@test.test')
        ti = TI(
            task=task,
            execution_date=datetime.datetime.now(),
        )
        ti.start_date = datetime.datetime(2018, 10, 1, 1)
        ti.end_date = datetime.datetime(2018, 10, 1, 2)
        ti.set_duration()
        self.assertEqual(ti.duration, 3600)

    def test_set_duration_empty_dates(self):
        task = DummyOperator(task_id='op', email='test@test.test')
        ti = TI(task=task, execution_date=datetime.datetime.now())
        ti.set_duration()
        self.assertIsNone(ti.duration)

    def test_success_callbak_no_race_condition(self):
        class CallbackWrapper(object):
            def wrap_task_instance(self, ti):
                self.task_id = ti.task_id
                self.dag_id = ti.dag_id
                self.execution_date = ti.execution_date
                self.task_state_in_callback = ""
                self.callback_ran = False

            def success_handler(self, context):
                self.callback_ran = True
                session = settings.Session()
                temp_instance = session.query(TI).filter(
                    TI.task_id == self.task_id).filter(
                    TI.dag_id == self.dag_id).filter(
                    TI.execution_date == self.execution_date).one()
                self.task_state_in_callback = temp_instance.state

        cw = CallbackWrapper()
        dag = DAG('test_success_callbak_no_race_condition', start_date=DEFAULT_DATE,
                  end_date=DEFAULT_DATE + datetime.timedelta(days=10))
        task = DummyOperator(task_id='op', email='test@test.test',
                             on_success_callback=cw.success_handler, dag=dag)
        ti = TI(task=task, execution_date=datetime.datetime.now())
        ti.state = State.RUNNING
        session = settings.Session()
        session.merge(ti)
        session.commit()
        cw.wrap_task_instance(ti)
        ti._run_raw_task()
        self.assertTrue(cw.callback_ran)
        self.assertEqual(cw.task_state_in_callback, State.RUNNING)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    @staticmethod
    def _test_previous_dates_setup(schedule_interval, catchup, scenario):
        # type: (Union[str, datetime.timedelta, None], bool, List[str]) -> list
        dag_id = 'test_previous_dates'
        dag = models.DAG(dag_id=dag_id, schedule_interval=schedule_interval, catchup=catchup)
        task = DummyOperator(task_id='task', dag=dag, start_date=DEFAULT_DATE)

        def get_test_ti(session, execution_date, state):
            # type: (...) -> TI
            dag.create_dagrun(
                run_id='scheduled__{}'.format(execution_date.to_iso8601_string()),
                state=state,
                execution_date=execution_date,
                start_date=pendulum.utcnow(),
                session=session
            )
            ti = TI(task=task, execution_date=execution_date)
            ti.set_state(state=State.SUCCESS, session=session)
            return ti

        with create_session() as session:  # type: Session

            d0 = pendulum.parse('2019-01-01T00:00:00+00:00')

            ret = []

            for idx, state in enumerate(scenario):
                ed = d0.add(days=idx)
                ti = get_test_ti(session, ed, state)
                ret.append(ti)

            return ret

    _prev_dates_param_list = (
        param('cron/catchup', '0 0 * * * ', True),
        param('cron/no-catchup', '0 0 * * *', False),
        param('no-sched/catchup', None, True),
        param('no-sched/no-catchup', None, False),
        param('timedelta/catchup', datetime.timedelta(days=1), True),
        param('timedelta/no-catchup', datetime.timedelta(days=1), False),
    )

    @parameterized.expand(_prev_dates_param_list)
    def test_previous_ti(self, _, schedule_interval, catchup):

        scenario = [State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule_interval, catchup, scenario)

        self.assertIsNone(ti_list[0].previous_ti)

        self.assertEqual(
            ti_list[2].previous_ti.execution_date,
            ti_list[1].execution_date
        )

        self.assertNotEqual(
            ti_list[2].previous_ti.execution_date,
            ti_list[0].execution_date
        )

    @parameterized.expand(_prev_dates_param_list)
    def test_previous_ti_success(self, _, schedule_interval, catchup):

        scenario = [State.FAILED, State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule_interval, catchup, scenario)

        self.assertIsNone(ti_list[0].previous_ti_success)
        self.assertIsNone(ti_list[1].previous_ti_success)

        self.assertEqual(
            ti_list[3].previous_ti_success.execution_date,
            ti_list[1].execution_date
        )

        self.assertNotEqual(
            ti_list[3].previous_ti_success.execution_date,
            ti_list[2].execution_date
        )

    @parameterized.expand(_prev_dates_param_list)
    def test_previous_execution_date_success(self, _, schedule_interval, catchup):

        scenario = [State.FAILED, State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule_interval, catchup, scenario)

        self.assertIsNone(ti_list[0].previous_execution_date_success)
        self.assertIsNone(ti_list[1].previous_execution_date_success)
        self.assertEqual(
            ti_list[3].previous_execution_date_success,
            ti_list[1].execution_date
        )
        self.assertNotEqual(
            ti_list[3].previous_execution_date_success,
            ti_list[2].execution_date
        )

    @parameterized.expand(_prev_dates_param_list)
    def test_previous_start_date_success(self, _, schedule_interval, catchup):

        scenario = [State.FAILED, State.SUCCESS, State.FAILED, State.SUCCESS]

        ti_list = self._test_previous_dates_setup(schedule_interval, catchup, scenario)

        self.assertIsNone(ti_list[0].previous_start_date_success)
        self.assertIsNone(ti_list[1].previous_start_date_success)
        self.assertEqual(
            ti_list[3].previous_start_date_success,
            ti_list[1].start_date,
        )
        self.assertNotEqual(
            ti_list[3].previous_start_date_success,
            ti_list[2].start_date,
        )

    def test_pendulum_template_dates(self):
        dag = models.DAG(
            dag_id='test_pendulum_template_dates', schedule_interval='0 12 * * *',
            start_date=timezone.datetime(2016, 6, 1, 0, 0, 0))
        task = DummyOperator(task_id='test_pendulum_template_dates_task', dag=dag)

        ti = TI(task=task, execution_date=timezone.utcnow())

        template_context = ti.get_template_context()

        self.assertIsInstance(template_context["execution_date"], pendulum.datetime)
        self.assertIsInstance(template_context["next_execution_date"], pendulum.datetime)
        self.assertIsInstance(template_context["prev_execution_date"], pendulum.datetime)

    def test_handle_failure(self):
        from tests.compat import mock

        start_date = timezone.datetime(2016, 6, 1)
        dag = models.DAG(dag_id="test_handle_failure", schedule_interval=None, start_date=start_date)

        with mock.MagicMock() as mock_on_failure_1, mock.MagicMock() as mock_on_retry_1:
            task1 = DummyOperator(task_id="test_handle_failure_on_failure",
                                  on_failure_callback=mock_on_failure_1,
                                  on_retry_callback=mock_on_retry_1,
                                  dag=dag)
            ti1 = TI(task=task1, execution_date=start_date)
            ti1.state = State.FAILED
            ti1.handle_failure("test failure handling")

        context_arg_1 = mock_on_failure_1.call_args[0][0]
        assert context_arg_1 and "task_instance" in context_arg_1
        mock_on_retry_1.assert_not_called()

        with mock.MagicMock() as mock_on_failure_2, mock.MagicMock() as mock_on_retry_2:
            task2 = DummyOperator(task_id="test_handle_failure_on_retry",
                                  on_failure_callback=mock_on_failure_2,
                                  on_retry_callback=mock_on_retry_2,
                                  retries=1,
                                  dag=dag)
            ti2 = TI(task=task2, execution_date=start_date)
            ti2.state = State.FAILED
            ti2.handle_failure("test retry handling")

        mock_on_failure_2.assert_not_called()

        context_arg_2 = mock_on_retry_2.call_args[0][0]
        assert context_arg_2 and "task_instance" in context_arg_2

        # test the scenario where normally we would retry but have been asked to fail
        mock_on_failure_3 = mock.MagicMock()
        mock_on_retry_3 = mock.MagicMock()
        task3 = DummyOperator(task_id="test_handle_failure_on_force_fail",
                              on_failure_callback=mock_on_failure_3,
                              on_retry_callback=mock_on_retry_3,
                              retries=1,
                              dag=dag)
        ti3 = TI(task=task3, execution_date=start_date)
        ti3.state = State.FAILED
        ti3.handle_failure("test force_fail handling", force_fail=True)

        context_arg_3 = mock_on_failure_3.call_args[0][0]
        assert context_arg_3 and "task_instance" in context_arg_3
        mock_on_retry_3.assert_not_called()

    @parameterized.expand(
        [
            ('{{ var.value.a_variable }}', 'a test value'),
            ('{{ var.value.get("a_variable") }}', 'a test value'),
            ('{{ var.value.get("a_variable", "unused_fallback") }}', 'a test value'),
            ('{{ var.value.get("missing_variable", "fallback") }}', 'fallback'),
        ]
    )
    def test_template_with_variable(self, content, expected_output):
        """
        Test the availability of variables in templates
        """
        Variable.set('a_variable', 'a test value')

        with DAG('test-dag', start_date=DEFAULT_DATE):
            task = DummyOperator(task_id='op1')

        ti = TI(task=task, execution_date=DEFAULT_DATE)
        context = ti.get_template_context()
        result = task.render_template(content, context)
        self.assertEqual(result, expected_output)

    def test_template_with_variable_missing(self):
        """
        Test the availability of variables in templates
        """
        with DAG('test-dag', start_date=DEFAULT_DATE):
            task = DummyOperator(task_id='op1')

        ti = TI(task=task, execution_date=DEFAULT_DATE)
        context = ti.get_template_context()
        with self.assertRaises(KeyError):
            task.render_template('{{ var.value.get("missing_variable") }}', context)

    @parameterized.expand(
        [
            ('{{ var.value.a_variable }}', '{\n  "a": {\n    "test": "value"\n  }\n}'),
            ('{{ var.json.a_variable["a"]["test"] }}', 'value'),
            ('{{ var.json.get("a_variable")["a"]["test"] }}', 'value'),
            ('{{ var.json.get("a_variable", {"a": {"test": "unused_fallback"}})["a"]["test"] }}', 'value'),
            ('{{ var.json.get("missing_variable", {"a": {"test": "fallback"}})["a"]["test"] }}', 'fallback'),
        ]
    )
    def test_template_with_json_variable(self, content, expected_output):
        """
        Test the availability of variables in templates
        """
        Variable.set('a_variable', {'a': {'test': 'value'}}, serialize_json=True)

        with DAG('test-dag', start_date=DEFAULT_DATE):
            task = DummyOperator(task_id='op1')

        ti = TI(task=task, execution_date=DEFAULT_DATE)
        context = ti.get_template_context()
        result = task.render_template(content, context)
        self.assertEqual(result, expected_output)

    def test_template_with_json_variable_missing(self):
        with DAG('test-dag', start_date=DEFAULT_DATE):
            task = DummyOperator(task_id='op1')

        ti = TI(task=task, execution_date=DEFAULT_DATE)
        context = ti.get_template_context()
        with self.assertRaises(KeyError):
            task.render_template('{{ var.json.get("missing_variable") }}', context)

    def test_does_not_retry_on_airflow_fail_exception(self):
        def fail():
            raise AirflowFailException("hopeless")

        dag = models.DAG(dag_id='test_does_not_retry_on_airflow_fail_exception')
        task = PythonOperator(
            task_id='test_raise_airflow_fail_exception',
            dag=dag,
            python_callable=fail,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
            retries=1
        )
        ti = TI(task=task, execution_date=timezone.utcnow())
        try:
            ti.run()
        except AirflowFailException:
            pass  # expected
        self.assertEqual(State.FAILED, ti.state)

    def test_retries_on_other_exceptions(self):
        def fail():
            raise AirflowException("maybe this will pass?")

        dag = models.DAG(dag_id='test_retries_on_other_exceptions')
        task = PythonOperator(
            task_id='test_raise_other_exception',
            dag=dag,
            python_callable=fail,
            owner='airflow',
            start_date=timezone.datetime(2016, 2, 1, 0, 0, 0),
            retries=1
        )
        ti = TI(task=task, execution_date=timezone.utcnow())
        try:
            ti.run()
        except AirflowException:
            pass  # expected
        self.assertEqual(State.UP_FOR_RETRY, ti.state)

    @parameterized.expand([
        (True, ),
        (False, )
    ])
    def test_get_rendered_template_fields(self, store_serialized_dag):
        # SetUp
        settings.STORE_SERIALIZED_DAGS = store_serialized_dag

        with DAG('test-dag', start_date=DEFAULT_DATE):
            task = BashOperator(task_id='op1', bash_command="{{ task.task_id }}")

        ti = TI(task=task, execution_date=DEFAULT_DATE)

        with create_session() as session:
            session.add(RenderedTaskInstanceFields(ti))

        # Create new TI for the same Task
        with DAG('test-dag', start_date=DEFAULT_DATE):
            new_task = BashOperator(task_id='op1', bash_command="{{ task.task_id }}")

        new_ti = TI(task=new_task, execution_date=DEFAULT_DATE)
        new_ti.get_rendered_template_fields()

        self.assertEqual(settings.STORE_SERIALIZED_DAGS, store_serialized_dag)
        self.assertEqual("op1", ti.task.bash_command)

        # CleanUp
        with create_session() as session:
            session.query(RenderedTaskInstanceFields).delete()


@pytest.mark.parametrize("pool_override", [None, "test_pool2"])
def test_refresh_from_task(pool_override):
    task = DummyOperator(task_id="dummy", queue="test_queue", pool="test_pool1", pool_slots=3,
                         priority_weight=10, run_as_user="test", retries=30,
                         executor_config={"KubernetesExecutor": {"image": "myCustomDockerImage"}})
    ti = TI(task, execution_date=pendulum.datetime(2020, 1, 1))
    ti.refresh_from_task(task, pool_override=pool_override)

    assert ti.queue == task.queue

    if pool_override:
        assert ti.pool == pool_override
    else:
        assert ti.pool == task.pool

    assert ti.pool_slots == task.pool_slots
    assert ti.priority_weight == task.priority_weight_total
    assert ti.run_as_user == task.run_as_user
    assert ti.max_tries == task.retries
    assert ti.executor_config == task.executor_config
    assert ti.operator == DummyOperator.__name__
