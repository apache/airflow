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
from airflow.bin.cli import get_num_ready_workers_running, kube_run
from airflow.utils.state import State
from airflow.settings import Session
from airflow import models
import os

dag_folder_path = '/'.join(os.path.realpath(__file__).split('/')[:-1])

TEST_DAG_FOLDER = os.path.join(
    os.path.dirname(dag_folder_path), 'dags')
TEST_DAG_ID = 'unit_tests'


def reset(dag_id):
    session = Session()
    tis = session.query(models.TaskInstance).filter_by(dag_id=dag_id)
    tis.delete()
    session.commit()
    session.close()


def create_mock_args(
    task_id,
    dag_id,
    subdir,
    execution_date,
    task_params=None,
    dry_run=False,
    queue=None,
    pool=None,
    priority_weight_total=None,
    retries=0,
    local=True,
    run_as_user=None,
    executor_config={},
    cfg_path=None,
    pickle=None,
    raw=None,
    interactive=None,
):
    args = MagicMock(spec=Namespace)
    args.task_id = task_id
    args.dag_id = dag_id
    args.subdir = subdir
    args.task_params = task_params
    args.execution_date = execution_date
    args.dry_run = dry_run
    args.queue = queue
    args.pool = pool
    args.priority_weight_total = priority_weight_total
    args.retries = retries
    args.local = local
    args.run_as_user = run_as_user
    args.executor_config = executor_config
    args.cfg_path = cfg_path
    args.pickle = pickle
    args.raw = raw
    args.interactive = interactive
    return args


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
        args = create_mock_args(
            task_id='print_the_context',
            dag_id='example_python_operator',
            subdir='/root/dags/example_python_operator.py',
            execution_date=Pendulum.parse('2018-04-27T08:39:51.298439+00:00')
        )

        reset(args.dag_id)
        #
        print(TEST_DAG_FOLDER)
        # dag = get_dag(args)
        # task = dag.get_task(task_id=args.task_id) # type: TaskInstance
        # ti = TaskInstance(task, args.execution_date)
        # ti.delete()

        with patch('argparse.Namespace', args):
            ti = kube_run(args)
            state = ti.current_state()
            self.assertEqual(state, State.SUCCESS)

    # def test_kube_run_faulty_argument(self):
    #     args = create_mock_args(
    #         task_id='print_the_context',
    #         dag_id='example_python_operator',
    #         subdir='/root/dags/example_python_operator.py',
    #         execution_date=Pendulum.parse('2018-04-27T08:39:51.298439+00:00')
    #     )
    #
    #     # dag = get_dag(args)
    #     # task = dag.get_task(task_id=args.task_id) # type: TaskInstance
    #     # ti = TaskInstance(task, args.execution_date)
    #     # ti.delete()
    #
    #     with patch('argparse.Namespace', args):
    #         ti = run(args)
    #         state = ti.current_state()
    #         print(state)
