# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import unittest
import sys

from airflow.executors.celery_executor import app
from airflow.executors.celery_executor import CeleryExecutor
from airflow.utils.state import State
from celery.contrib.testing.worker import start_worker

# leave this it is used by the test worker
import celery.contrib.testing.tasks


class CeleryExecutorTest(unittest.TestCase):

    def test_celery_integration(self):
        executor = CeleryExecutor()
        executor.start()
        with start_worker(app=app, logfile=sys.stdout, loglevel='debug'):

            success_command = 'echo 1'
            fail_command = 'exit 1'

            executor.execute_async(key='success', command=success_command)
            # errors are propagated for some reason
            try:
                executor.execute_async(key='fail', command=fail_command)
            except:
                pass
            executor.running['success'] = True
            executor.running['fail'] = True

            executor.end(synchronous=True)

        self.assertTrue(executor.event_buffer['success'], State.SUCCESS)
        self.assertTrue(executor.event_buffer['fail'], State.FAILED)

        self.assertNotIn('success', executor.tasks)
        self.assertNotIn('fail', executor.tasks)


if __name__ == '__main__':
    unittest.main()
