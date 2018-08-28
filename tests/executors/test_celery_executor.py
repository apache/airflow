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
import sys
import unittest
from celery.contrib.testing.worker import start_worker

from airflow.executors.celery_executor import CeleryExecutor
from airflow.executors.celery_executor import app
from airflow.utils.state import State

# leave this it is used by the test worker
import celery.contrib.testing.tasks


class CeleryExecutorTest(unittest.TestCase):
    def test_celery_integration(self):
        executor = CeleryExecutor()
        executor.start()
        with start_worker(app=app, logfile=sys.stdout, loglevel='debug'):

            success_command = ['true', 'some_parameter']
            fail_command = ['false', 'some_parameter']

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

        self.assertNotIn('success', executor.last_state)
        self.assertNotIn('fail', executor.last_state)

if __name__ == '__main__':
    unittest.main()
