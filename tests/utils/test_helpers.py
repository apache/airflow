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

import logging
import multiprocessing
import os
import signal
import time
import unittest
from datetime import datetime

import psutil

from airflow import DAG
from airflow.utils import helpers
from airflow.models import TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowException


class TestHelpers(unittest.TestCase):

    @staticmethod
    def _ignores_sigterm(child_pid, child_setup_done):
        def signal_handler(unused_signum, unused_frame):
            pass

        signal.signal(signal.SIGTERM, signal_handler)
        child_pid.value = os.getpid()
        child_setup_done.release()
        while True:
            time.sleep(1)

    @staticmethod
    def _parent_of_ignores_sigterm(parent_pid, child_pid, setup_done):
        def signal_handler(unused_signum, unused_frame):
            pass
        os.setsid()
        signal.signal(signal.SIGTERM, signal_handler)
        child_setup_done = multiprocessing.Semaphore(0)
        child = multiprocessing.Process(target=TestHelpers._ignores_sigterm,
                                        args=[child_pid, child_setup_done])
        child.start()
        child_setup_done.acquire(timeout=5.0)
        parent_pid.value = os.getpid()
        setup_done.release()
        while True:
            time.sleep(1)

    def test_render_log_filename(self):
        try_number = 1
        dag_id = 'test_render_log_filename_dag'
        task_id = 'test_render_log_filename_task'
        execution_date = datetime(2016, 1, 1)

        dag = DAG(dag_id, start_date=execution_date)
        task = DummyOperator(task_id=task_id, dag=dag)
        ti = TaskInstance(task=task, execution_date=execution_date)

        filename_template = "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log"

        ts = ti.get_template_context()['ts']
        expected_filename = "{dag_id}/{task_id}/{ts}/{try_number}.log".format(dag_id=dag_id,
                                                                              task_id=task_id,
                                                                              ts=ts,
                                                                              try_number=try_number)

        rendered_filename = helpers.render_log_filename(ti, try_number, filename_template)

        self.assertEqual(rendered_filename, expected_filename)

    def test_reap_process_group(self):
        """
        Spin up a process that can't be killed by SIGTERM and make sure
        it gets killed anyway.
        """
        parent_setup_done = multiprocessing.Semaphore(0)
        parent_pid = multiprocessing.Value('i', 0)
        child_pid = multiprocessing.Value('i', 0)
        args = [parent_pid, child_pid, parent_setup_done]
        parent = multiprocessing.Process(target=TestHelpers._parent_of_ignores_sigterm,
                                         args=args)
        try:
            parent.start()
            self.assertTrue(parent_setup_done.acquire(timeout=5.0))
            self.assertTrue(psutil.pid_exists(parent_pid.value))
            self.assertTrue(psutil.pid_exists(child_pid.value))

            helpers.reap_process_group(parent_pid.value, logging.getLogger(),
                                       timeout=1)

            self.assertFalse(psutil.pid_exists(parent_pid.value))
            self.assertFalse(psutil.pid_exists(child_pid.value))
        finally:
            try:
                os.kill(parent_pid.value, signal.SIGKILL)  # terminate doesnt work here
                os.kill(child_pid.value, signal.SIGKILL)  # terminate doesnt work here
            except OSError:
                pass

    def test_chunks(self):
        with self.assertRaises(ValueError):
            [i for i in helpers.chunks([1, 2, 3], 0)]

        with self.assertRaises(ValueError):
            [i for i in helpers.chunks([1, 2, 3], -3)]

        self.assertEqual([i for i in helpers.chunks([], 5)], [])
        self.assertEqual([i for i in helpers.chunks([1], 1)], [[1]])
        self.assertEqual([i for i in helpers.chunks([1, 2, 3], 2)],
                         [[1, 2], [3]])

    def test_reduce_in_chunks(self):
        self.assertEqual(helpers.reduce_in_chunks(lambda x, y: x + [y],
                                                  [1, 2, 3, 4, 5],
                                                  []),
                         [[1, 2, 3, 4, 5]])

        self.assertEqual(helpers.reduce_in_chunks(lambda x, y: x + [y],
                                                  [1, 2, 3, 4, 5],
                                                  [],
                                                  2),
                         [[1, 2], [3, 4], [5]])

        self.assertEqual(helpers.reduce_in_chunks(lambda x, y: x + y[0] * y[1],
                                                  [1, 2, 3, 4],
                                                  0,
                                                  2),
                         14)

    def test_is_container(self):
        self.assertFalse(helpers.is_container("a string is not a container"))
        self.assertTrue(helpers.is_container(["a", "list", "is", "a", "container"]))

    def test_as_tuple(self):
        self.assertEqual(
            helpers.as_tuple("a string is not a container"),
            ("a string is not a container",)
        )

        self.assertEqual(
            helpers.as_tuple(["a", "list", "is", "a", "container"]),
            ("a", "list", "is", "a", "container")
        )


class HelpersTest(unittest.TestCase):
    def test_as_tuple_iter(self):
        test_list = ['test_str']
        as_tup = helpers.as_tuple(test_list)
        self.assertTupleEqual(tuple(test_list), as_tup)

    def test_as_tuple_no_iter(self):
        test_str = 'test_str'
        as_tup = helpers.as_tuple(test_str)
        self.assertTupleEqual((test_str,), as_tup)

    def test_is_container(self):
        self.assertTrue(helpers.is_container(['test_list']))
        self.assertFalse(helpers.is_container('test_str_not_iterable'))
        # Pass an object that is not iter nor a string.
        self.assertFalse(helpers.is_container(10))

    def test_cross_downstream(self):
        """Test if all dependencies between tasks are all set correctly."""
        dag = DAG(dag_id="test_dag", start_date=datetime.now())
        start_tasks = [DummyOperator(task_id="t{i}".format(i=i), dag=dag) for i in range(1, 4)]
        end_tasks = [DummyOperator(task_id="t{i}".format(i=i), dag=dag) for i in range(4, 7)]
        helpers.cross_downstream(from_tasks=start_tasks, to_tasks=end_tasks)

        for start_task in start_tasks:
            self.assertCountEqual(start_task.get_direct_relatives(upstream=False), end_tasks)

    def test_chain(self):
        dag = DAG(dag_id='test_chain', start_date=datetime.now())
        [t1, t2, t3, t4, t5, t6] = [DummyOperator(task_id='t{i}'.format(i=i), dag=dag) for i in range(1, 7)]
        helpers.chain(t1, [t2, t3], [t4, t5], t6)

        self.assertCountEqual([t2, t3], t1.get_direct_relatives(upstream=False))
        self.assertEqual([t4], t2.get_direct_relatives(upstream=False))
        self.assertEqual([t5], t3.get_direct_relatives(upstream=False))
        self.assertCountEqual([t4, t5], t6.get_direct_relatives(upstream=True))

    def test_chain_not_support_type(self):
        dag = DAG(dag_id='test_chain', start_date=datetime.now())
        [t1, t2] = [DummyOperator(task_id='t{i}'.format(i=i), dag=dag) for i in range(1, 3)]
        with self.assertRaises(TypeError):
            helpers.chain([t1, t2], 1)

    def test_chain_different_length_iterable(self):
        dag = DAG(dag_id='test_chain', start_date=datetime.now())
        [t1, t2, t3, t4, t5] = [DummyOperator(task_id='t{i}'.format(i=i), dag=dag) for i in range(1, 6)]
        with self.assertRaises(AirflowException):
            helpers.chain([t1, t2], [t3, t4, t5])

    def test_convert_camel_to_snake(self):
        self.assertEqual(helpers.convert_camel_to_snake('LocalTaskJob'), 'local_task_job')
        self.assertEqual(helpers.convert_camel_to_snake('somethingVeryRandom'),
                         'something_very_random')


if __name__ == '__main__':
    unittest.main()
