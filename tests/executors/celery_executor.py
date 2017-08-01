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

from celery import states as cstates

from airflow.executors.celery_executor import CeleryExecutor

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class CeleryExecutorTest(unittest.TestCase):
    """Unit tests for the CeleryExecutor."""

    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('airflow.executors.celery_executor.time')
    @mock.patch('airflow.executors.celery_executor.execute_command')
    @mock.patch('airflow.executors.celery_executor.app')
    def test_executor(self, app, execute_command, time):
        insp = mock.MagicMock(name='inspector')
        insp.active.return_value = {}
        insp.scheduled.return_value = {}
        insp.reserved.return_value = {}
        app.control.inspect.return_value = insp

        async1 = mock.MagicMock(name='async1')
        async1.state = cstates.PENDING
        async2 = mock.MagicMock(name='async2')
        async2.state = cstates.FAILURE
        execute_command.apply_async.side_effect = [async1, async2]

        executor = CeleryExecutor()
        executor.start()

        executor.execute_async(key='one', command='echo 1')
        executor.execute_async(key='two', command='echo 2', queue='other')

        self.assertEquals(executor.tasks, {'one': async1, 'two': async2})
        self.assertEquals(executor.last_state,
                          {'one': cstates.PENDING, 'two': cstates.PENDING})

        self.assertEquals(execute_command.mock_calls, [
            mock.call.apply_async(args=['echo 1'], queue='default'),
            mock.call.apply_async(args=['echo 2'], queue='other'),
        ])
        self.assertFalse(async1.mock_calls)
        self.assertFalse(async2.mock_calls)
        self.assertFalse(time.mock_calls)

        executor.sync()

        self.assertFalse(async1.mock_calls)
        self.assertFalse(async2.mock_calls)

        self.assertEquals(executor.tasks, {'one': async1})
        self.assertEquals(executor.last_state,
                          {'one': cstates.PENDING, 'two': cstates.FAILURE})

    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('airflow.executors.celery_executor.execute_command')
    @mock.patch('airflow.executors.celery_executor.app')
    def test_repeat_key(self, app, execute_command):
        insp = mock.MagicMock(name='inspector')
        insp.active.return_value = {}
        insp.scheduled.return_value = {}
        insp.reserved.return_value = {}
        app.control.inspect.return_value = insp

        executor = CeleryExecutor()
        executor.start()

        executor.execute_async(key='one', command='echo 1')
        executor.execute_async(key='two', command='echo 2', queue='other')
        executor.execute_async(key='one', command='echo 3', queue='other')

        self.assertEquals(execute_command.mock_calls, [
            mock.call.apply_async(args=['echo 1'], queue='default'),
            mock.call.apply_async(args=['echo 2'], queue='other'),
        ])

    @unittest.skipIf(mock is None, 'mock package not present')
    @mock.patch('airflow.executors.celery_executor.AsyncResult')
    @mock.patch('airflow.executors.celery_executor.execute_command')
    @mock.patch('airflow.executors.celery_executor.app')
    def test_existing_command(self, app, execute_command, result):
        insp = mock.MagicMock(name='inspector')
        insp.active.return_value = {
            'worker1': [{
                'id': 'id1',
                'args': ['echo 1']}],
        }
        insp.scheduled.return_value = {
            'worker2': [{
                'id': 'id2',
                'args': ['echo 2']}],
        }
        insp.reserved.return_value = {
            'worker3': [{
                'id': 'id3',
                'args': ['echo 3']}],
        }
        app.control.inspect.return_value = insp

        executor = CeleryExecutor()
        executor.start()

        executor.execute_async(key='one', command='echo 1')
        executor.execute_async(key='two', command='echo 2', queue='other')
        executor.execute_async(key='three', command='echo 3', queue='other')
        executor.execute_async(key='four', command='echo 4')

        self.assertEquals(execute_command.mock_calls, [
            mock.call.apply_async(args=['echo 4'], queue='default'),
        ])
        self.assertEquals(result.mock_calls, [
            mock.call(id='id1', app=app),
            mock.call(id='id2', app=app),
            mock.call(id='id3', app=app),
        ])
        self.assertEquals(executor.tasks, {
            'one': result.return_value,
            'two': result.return_value,
            'three': result.return_value,
            'four': execute_command.apply_async.return_value,
        })
