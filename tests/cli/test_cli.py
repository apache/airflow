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
#

import unittest

from mock import patch, Mock, MagicMock
from time import sleep
from pendulum import Pendulum
import psutil
from argparse import Namespace
from airflow import settings
from airflow.models import TaskInstance
from airflow.bin.cli import get_num_ready_workers_running, kube_run, get_dag, run
from airflow.configuration import conf
from airflow.settings import Session
from airflow import models
import os

dag_folder_path='/'.join(os.path.realpath(__file__).split('/')[:-1])

TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(dag_folder_path), 'dags')
TEST_DAG_ID = 'unit_tests'

def reset(dag_id):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()

class TestCLI(unittest.TestCase):

    def setUp(self):
        self.gunicorn_master_proc = Mock(pid=None)
        self.children = MagicMock()
        self.child = MagicMock()
        self.process = MagicMock()

    def test_ready_prefix_on_cmdline(self):
        self.child.cmdline.return_value = [settings.GUNICORN_WORKER_READY_PREFIX]
        self.process.children.return_value = [self.child]

        with patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 1)

    def test_ready_prefix_on_cmdline_no_children(self):
        self.process.children.return_value = []

        with patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_ready_prefix_on_cmdline_zombie(self):
        self.child.cmdline.return_value = []
        self.process.children.return_value = [self.child]

        with patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_ready_prefix_on_cmdline_dead_process(self):
        self.child.cmdline.side_effect = psutil.NoSuchProcess(11347)
        self.process.children.return_value = [self.child]

        with patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_cli_webserver_debug(self):
        p = psutil.Popen(["airflow", "webserver", "-d"])
        sleep(3)  # wait for webserver to start
        return_code = p.poll()
        self.assertEqual(
            None,
            return_code,
            "webserver terminated with return code {} in debug mode".format(return_code))
        p.terminate()
        p.wait()




    def test_kube_run(self):
        args = MagicMock(spec=Namespace)
        args.task_id = 'print_the_context'
        args.dag_id = 'example_python_operator'
        args.subdir = '/root/dags/example_python_operator.py'
        args.task_params=None
        args.execution_date = Pendulum.parse('2018-04-27T08:39:51.298439+00:00')
        args.dry_run = False
        args.queue = None
        args.pool = None
        args.priority_weight_total = None
        args.retries = 0
        args.run_as_user = None
        args.executor_config={}

        print(TEST_DAG_FOLDER)
        dag = get_dag(args)
        task = dag.get_task(task_id=args.task_id) # type: TaskInstance
        ti = TaskInstance(task, args.execution_date)
        ti.delete()

        with patch('argparse.Namespace', args):
            ti = run(args)
            state = ti.current_state()
            print(state)

    def test_kube_run_faulty_argument(self):
        args = MagicMock(spec=Namespace)
        args.task_id = 'print_the_context'
        args.dag_id = 'example_python_operator'
        args.subdir = '/root/dags/example_python_operator.py'
        args.task_params=None
        args.execution_date = Pendulum.parse('2018-04-29T08:39:51.298439+00:00')
        args.dry_run = False
        args.queue = None
        args.pool = None
        args.priority_weight_total = None
        args.retries = 0
        args.local=True
        args.run_as_user = None
        args.executor_config={}
        args.cfg_path=None
        args.pickle=None
        args.raw=None
        args.interactive=None

        print(TEST_DAG_FOLDER)
        dag = get_dag(args)
        task = dag.get_task(task_id=args.task_id) # type: TaskInstance
        ti = TaskInstance(task, args.execution_date)
        ti.delete()

        with patch('argparse.Namespace', args):
            ti = run(args)
            state = ti.current_state()
            print(state)
