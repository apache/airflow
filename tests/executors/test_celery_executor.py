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
import contextlib
import datetime
import os
import sys
import unittest
from multiprocessing import Pool
from unittest import mock

# leave this it is used by the test worker
# noinspection PyUnresolvedReferences
import celery.contrib.testing.tasks  # noqa: F401 pylint: disable=ungrouped-imports
from celery import Celery, states as celery_states
from celery.contrib.testing.worker import start_worker
from kombu.asynchronous import set_event_loop
from parameterized import parameterized

from airflow import DAG
from airflow.configuration import conf
from airflow.executors import celery_executor
from airflow.models import TaskInstance
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.operators.bash_operator import BashOperator
from airflow.utils.state import State


def _prepare_test_bodies():
    if 'CELERY_BROKER_URLS' in os.environ:
        return [
            (url, )
            for url in os.environ['CELERY_BROKER_URLS'].split(',')
        ]
    return [(conf.get('celery', 'BROKER_URL'))]


class TestCeleryExecutor(unittest.TestCase):

    @contextlib.contextmanager
    def _prepare_app(self, broker_url=None, execute=None):
        broker_url = broker_url or conf.get('celery', 'BROKER_URL')
        execute = execute or celery_executor.execute_command.__wrapped__

        test_config = dict(celery_executor.celery_configuration)
        test_config.update({'broker_url': broker_url})
        test_app = Celery(broker_url, config_source=test_config)
        test_execute = test_app.task(execute)
        patch_app = mock.patch('airflow.executors.celery_executor.app', test_app)
        patch_execute = mock.patch('airflow.executors.celery_executor.execute_command', test_execute)

        with patch_app, patch_execute:
            try:
                yield test_app
            finally:
                # Clear event loop to tear down each celery instance
                set_event_loop(None)

    @parameterized.expand(_prepare_test_bodies())
    @unittest.skipIf('sqlite' in conf.get('core', 'sql_alchemy_conn'),
                     "sqlite is configured with SequentialExecutor")
    def test_celery_integration(self, broker_url):
        with self._prepare_app(broker_url) as app:
            executor = celery_executor.CeleryExecutor()
            executor.start()

            with start_worker(app=app, logfile=sys.stdout, loglevel='info'):
                success_command = ['true', 'some_parameter']
                fail_command = ['false', 'some_parameter']
                execute_date = datetime.datetime.now()

                cached_celery_backend = celery_executor.execute_command.backend
                task_tuples_to_send = [
                    (('success', 'fake_simple_ti', execute_date, 0),
                     None, success_command, celery_executor.celery_configuration['task_default_queue'],
                     celery_executor.execute_command),
                    (('fail', 'fake_simple_ti', execute_date, 0),
                     None, fail_command, celery_executor.celery_configuration['task_default_queue'],
                     celery_executor.execute_command)
                ]

                chunksize = executor._num_tasks_per_send_process(len(task_tuples_to_send))
                num_processes = min(len(task_tuples_to_send), executor._sync_parallelism)

                send_pool = Pool(processes=num_processes)
                key_and_async_results = send_pool.map(
                    celery_executor.send_task_to_executor,
                    task_tuples_to_send,
                    chunksize=chunksize)

                send_pool.close()
                send_pool.join()

                for task_instance_key, _, result in key_and_async_results:
                    # Only pops when enqueued successfully, otherwise keep it
                    # and expect scheduler loop to deal with it.
                    result.backend = cached_celery_backend
                    executor.running.add(task_instance_key)
                    executor.tasks[task_instance_key] = result
                    executor.last_state[task_instance_key] = celery_states.PENDING

                executor.running.add(('success', 'fake_simple_ti', execute_date, 0))
                executor.running.add(('fail', 'fake_simple_ti', execute_date, 0))

                executor.end(synchronous=True)

        self.assertEqual(executor.event_buffer[('success', 'fake_simple_ti', execute_date, 0)], State.SUCCESS)
        self.assertEqual(executor.event_buffer[('fail', 'fake_simple_ti', execute_date, 0)], State.FAILED)

        self.assertNotIn('success', executor.tasks)
        self.assertNotIn('fail', executor.tasks)

        self.assertNotIn('success', executor.last_state)
        self.assertNotIn('fail', executor.last_state)

    @unittest.skipIf('sqlite' in conf.get('core', 'sql_alchemy_conn'),
                     "sqlite is configured with SequentialExecutor")
    def test_error_sending_task(self):
        def fake_execute_command():
            pass

        with self._prepare_app(execute=fake_execute_command):
            # fake_execute_command takes no arguments while execute_command takes 1,
            # which will cause TypeError when calling task.apply_async()
            executor = celery_executor.CeleryExecutor()
            task = BashOperator(
                task_id="test",
                bash_command="true",
                dag=DAG(dag_id='id'),
                start_date=datetime.datetime.now()
            )
            value_tuple = 'command', 1, None, \
                SimpleTaskInstance(ti=TaskInstance(task=task, execution_date=datetime.datetime.now()))
            key = ('fail', 'fake_simple_ti', datetime.datetime.now(), 0)
            executor.queued_tasks[key] = value_tuple
            executor.heartbeat()
        self.assertEqual(1, len(executor.queued_tasks))
        self.assertEqual(executor.queued_tasks[key], value_tuple)

    def test_exception_propagation(self):
        with self._prepare_app() as app:
            @app.task
            def fake_celery_task():
                return {}

            mock_log = mock.MagicMock()
            executor = celery_executor.CeleryExecutor()
            executor._log = mock_log

            executor.tasks = {'key': fake_celery_task()}
            executor.sync()

        assert mock_log.error.call_count == 1
        args, kwargs = mock_log.error.call_args_list[0]
        # Result of queuing is not a celery task but a dict,
        # and it should raise AttributeError and then get propagated
        # to the error log.
        self.assertIn(celery_executor.CELERY_FETCH_ERR_MSG_HEADER, args[0])
        self.assertIn('AttributeError', args[1])

    @mock.patch('airflow.executors.celery_executor.CeleryExecutor.sync')
    @mock.patch('airflow.executors.celery_executor.CeleryExecutor.trigger_tasks')
    @mock.patch('airflow.stats.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = celery_executor.CeleryExecutor()
        executor.heartbeat()
        calls = [mock.call('executor.open_slots', mock.ANY),
                 mock.call('executor.queued_tasks', mock.ANY),
                 mock.call('executor.running_tasks', mock.ANY)]
        mock_stats_gauge.assert_has_calls(calls)


if __name__ == '__main__':
    unittest.main()
