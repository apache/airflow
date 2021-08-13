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
import getpass
import mock
import os
import psutil
import time
import unittest

from airflow import models, settings
from airflow.jobs import LocalTaskJob
from airflow.models import TaskInstance as TI
from airflow.task.task_runner import StandardTaskRunner
from airflow.utils import timezone
from airflow.utils.state import State

from logging.config import dictConfig

from tests.core.test_core import TEST_DAG_FOLDER
from tests.test_utils.db import clear_db_runs

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': '[%(asctime)s] {{%(filename)s:%(lineno)d}} %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'airflow.task',
            'stream': 'ext://sys.stdout'
        }
    },
    'loggers': {
        'airflow': {
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False
        }
    }
}


class TestStandardTaskRunner(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dictConfig(LOGGING_CONFIG)

    @classmethod
    def tearDownClass(cls):
        try:
            clear_db_runs()
        except Exception:  # noqa pylint: disable=broad-except
            # It might happen that we lost connection to the server here so we need to ignore any errors here
            pass

    def test_start_and_terminate(self):
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.run_as_user = None
        local_task_job.task_instance.command_as_list.return_value = [
            'airflow', 'test', 'test_on_kill', 'task1', '2016-01-01'
        ]

        runner = StandardTaskRunner(local_task_job)
        runner.start()
        time.sleep(0.5)

        pgid = os.getpgid(runner.process.pid)
        self.assertGreater(pgid, 0)
        self.assertNotEqual(pgid, os.getpgid(0), "Task should be in a different process group to us")

        procs = list(self._procs_in_pgroup(pgid))

        runner.terminate()

        for p in procs:
            self.assertFalse(psutil.pid_exists(p.pid), "{} is still alive".format(p))

        self.assertIsNotNone(runner.return_code())

    def test_start_and_terminate_run_as_user(self):
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.run_as_user = getpass.getuser()
        local_task_job.task_instance.command_as_list.return_value = [
            'airflow', 'test', 'test_on_kill', 'task1', '2016-01-01'
        ]

        runner = StandardTaskRunner(local_task_job)

        runner.start()
        time.sleep(0.5)

        pgid = os.getpgid(runner.process.pid)
        self.assertGreater(pgid, 0)
        self.assertNotEqual(pgid, os.getpgid(0), "Task should be in a different process group to us")

        procs = list(self._procs_in_pgroup(pgid))

        runner.terminate()

        for p in procs:
            self.assertFalse(psutil.pid_exists(p.pid), "{} is still alive".format(p))

        self.assertIsNotNone(runner.return_code())

    def test_on_kill(self):
        """
        Test that ensures that clearing in the UI SIGTERMS
        the task
        """
        path = "/tmp/airflow_on_kill"
        try:
            os.unlink(path)
        except OSError:
            pass

        dagbag = models.DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get('test_on_kill')
        task = dag.get_task('task1')

        session = settings.Session()

        dag.clear()
        dag.create_dagrun(run_id="test",
                          state=State.RUNNING,
                          execution_date=DEFAULT_DATE,
                          start_date=DEFAULT_DATE,
                          session=session)
        ti = TI(task=task, execution_date=DEFAULT_DATE)
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True)

        runner = StandardTaskRunner(job1)
        runner.start()

        # Give the task some time to startup
        time.sleep(3)

        pgid = os.getpgid(runner.process.pid)
        self.assertGreater(pgid, 0)
        self.assertNotEqual(pgid, os.getpgid(0), "Task should be in a different process group to us")

        procs = list(self._procs_in_pgroup(pgid))

        runner.terminate()

        # Wait some time for the result
        for _ in range(20):
            if os.path.exists(path):
                break
            time.sleep(2)

        with open(path, "r") as f:
            self.assertEqual("ON_KILL_TEST", f.readline())

        for p in procs:
            self.assertFalse(psutil.pid_exists(p.pid), "{} is still alive".format(p))

    @staticmethod
    def _procs_in_pgroup(pgid):
        for p in psutil.process_iter(attrs=['pid', 'name']):
            try:
                if os.getpgid(p.pid) == pgid and p.pid != 0:
                    yield p
            except OSError:
                pass


if __name__ == '__main__':
    unittest.main()
