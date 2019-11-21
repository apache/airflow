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
import os
import time
import unittest
from logging.config import dictConfig
from unittest import mock

import psutil

from airflow import models, settings
from airflow.jobs import LocalTaskJob
from airflow.models import TaskInstance as TI
from airflow.task.task_runner import StandardTaskRunner
from airflow.utils import timezone
from airflow.utils.state import State
from tests.core import TEST_DAG_FOLDER

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow.task': {
            'format': '[%%(asctime)s] {{%%(filename)s:%%(lineno)d}} %%(levelname)s - '
                      '%%(message)s'
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
    def setUp(self):
        dictConfig(LOGGING_CONFIG)

    def test_start_and_terminate(self):
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.run_as_user = None
        local_task_job.task_instance.command_as_list.return_value = ['sleep', '1000']

        runner = StandardTaskRunner(local_task_job)
        runner.start()

        pgid = os.getpgid(runner.process.pid)
        self.assertTrue(pgid)

        procs = []
        for p in psutil.process_iter():
            try:
                if os.getpgid(p.pid) == pgid:
                    procs.append(p)
            except OSError:
                pass

        runner.terminate()

        for p in procs:
            self.assertFalse(psutil.pid_exists(p.pid))

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

        # give the task some time to startup
        time.sleep(3)

        runner.terminate()

        with open(path, "r") as f:
            self.assertEqual("ON_KILL_TEST", f.readline())


if __name__ == '__main__':
    unittest.main()
