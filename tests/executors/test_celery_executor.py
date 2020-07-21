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
import json
import os
import sys
import unittest
from unittest import mock

# leave this it is used by the test worker
# noinspection PyUnresolvedReferences
import celery.contrib.testing.tasks  # noqa: F401 pylint: disable=unused-import
import pytest
from celery import Celery
from celery.backends.base import BaseBackend, BaseKeyValueStoreBackend
from celery.backends.database import DatabaseBackend
from celery.contrib.testing.worker import start_worker
from kombu.asynchronous import set_event_loop
from parameterized import parameterized

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors import celery_executor
from airflow.executors.celery_executor import BulkStateFetcher
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils.state import State


def _prepare_test_bodies():
    if 'CELERY_BROKER_URLS' in os.environ:
        return [
            (url, )
            for url in os.environ['CELERY_BROKER_URLS'].split(',')
        ]
    return [(conf.get('celery', 'BROKER_URL'))]


class FakeCeleryResult:
    @property
    def state(self):
        raise Exception()

    def task_id(self):
        return "task_id"


@contextlib.contextmanager
def _prepare_app(broker_url=None, execute=None):
    broker_url = broker_url or conf.get('celery', 'BROKER_URL')
    execute = execute or celery_executor.execute_command.__wrapped__

    test_config = dict(celery_executor.celery_configuration)
    test_config.update({'broker_url': broker_url})
    test_app = Celery(broker_url, config_source=test_config)
    test_execute = test_app.task(execute)
    patch_app = mock.patch('airflow.executors.celery_executor.app', test_app)
    patch_execute = mock.patch('airflow.executors.celery_executor.execute_command', test_execute)

    backend = test_app.backend

    if hasattr(backend, 'ResultSession'):
        # Pre-create the database tables now, otherwise SQLA vis Celery has a
        # race condition where it one of the subprocesses can die with "Table
        # already exists" error, because SQLA checks for which tables exist,
        # then issues a CREATE TABLE, rather than doing CREATE TABLE IF NOT
        # EXISTS
        session = backend.ResultSession()
        session.close()

    with patch_app, patch_execute:
        try:
            yield test_app
        finally:
            # Clear event loop to tear down each celery instance
            set_event_loop(None)


class TestCeleryExecutor(unittest.TestCase):

    @parameterized.expand(_prepare_test_bodies())
    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_celery_integration(self, broker_url):
        success_command = ['airflow', 'tasks', 'run', 'true', 'some_parameter']
        fail_command = ['airflow', 'version']

        def fake_execute_command(command):
            if command != success_command:
                raise AirflowException("fail")

        with _prepare_app(broker_url, execute=fake_execute_command) as app:
            executor = celery_executor.CeleryExecutor()
            executor.start()

            with start_worker(app=app, logfile=sys.stdout, loglevel='info'):
                execute_date = datetime.datetime.now()

                task_tuples_to_send = [
                    (('success', 'fake_simple_ti', execute_date, 0),
                     None, success_command, celery_executor.celery_configuration['task_default_queue'],
                     celery_executor.execute_command),
                    (('fail', 'fake_simple_ti', execute_date, 0),
                     None, fail_command, celery_executor.celery_configuration['task_default_queue'],
                     celery_executor.execute_command)
                ]

                # "Enqueue" them. We don't have a real SimpleTaskInstance, so directly edit the dict
                for (key, simple_ti, command, queue, task) in task_tuples_to_send:  # pylint: disable=W0612
                    executor.queued_tasks[key] = (command, 1, queue, simple_ti)

                executor._process_tasks(task_tuples_to_send)

                executor.end(synchronous=True)

        self.assertEqual(executor.event_buffer[('success', 'fake_simple_ti', execute_date, 0)][0],
                         State.SUCCESS)
        self.assertEqual(executor.event_buffer[('fail', 'fake_simple_ti', execute_date, 0)][0],
                         State.FAILED)

        self.assertNotIn('success', executor.tasks)
        self.assertNotIn('fail', executor.tasks)

        self.assertNotIn('success', executor.last_state)
        self.assertNotIn('fail', executor.last_state)

    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_error_sending_task(self):
        def fake_execute_command():
            pass

        with _prepare_app(execute=fake_execute_command):
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

    @pytest.mark.backend("mysql", "postgres")
    def test_exception_propagation(self):

        with _prepare_app(), self.assertLogs(celery_executor.log) as cm:
            executor = celery_executor.CeleryExecutor()
            executor.tasks = {
                'key': FakeCeleryResult()
            }
            executor.bulk_state_fetcher._get_many_using_multiprocessing(executor.tasks.values())

        self.assertTrue(any(celery_executor.CELERY_FETCH_ERR_MSG_HEADER in line for line in cm.output))
        self.assertTrue(any("Exception" in line for line in cm.output))

    @mock.patch('airflow.executors.celery_executor.CeleryExecutor.sync')
    @mock.patch('airflow.executors.celery_executor.CeleryExecutor.trigger_tasks')
    @mock.patch('airflow.executors.base_executor.Stats.gauge')
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = celery_executor.CeleryExecutor()
        executor.heartbeat()
        calls = [mock.call('executor.open_slots', mock.ANY),
                 mock.call('executor.queued_tasks', mock.ANY),
                 mock.call('executor.running_tasks', mock.ANY)]
        mock_stats_gauge.assert_has_calls(calls)

    @parameterized.expand((
        [['true'], ValueError],
        [['airflow', 'version'], ValueError],
        [['airflow', 'tasks', 'run'], None]
    ))
    @mock.patch('subprocess.check_call')
    def test_command_validation(self, command, expected_exception, mock_check_call):
        # Check that we validate _on the receiving_ side, not just sending side
        if expected_exception:
            with pytest.raises(expected_exception):
                celery_executor.execute_command(command)
            mock_check_call.assert_not_called()
        else:
            celery_executor.execute_command(command)
            mock_check_call.assert_called_once_with(
                command, stderr=mock.ANY, close_fds=mock.ANY, env=mock.ANY,
            )


def test_operation_timeout_config():
    assert celery_executor.OPERATION_TIMEOUT == 2


class ClassWithCustomAttributes:
    """Class for testing purpose: allows to create objects with custom attributes in one single statement."""

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        return "{}({})".format(ClassWithCustomAttributes.__name__, str(self.__dict__))

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)


class TestBulkStateFetcher(unittest.TestCase):

    @mock.patch("celery.backends.base.BaseKeyValueStoreBackend.mget", return_value=[
        json.dumps({"status": "SUCCESS", "task_id": "123"})
    ])
    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_should_support_kv_backend(self, mock_mget):
        with _prepare_app():
            mock_backend = BaseKeyValueStoreBackend(app=celery_executor.app)
            with mock.patch.object(celery_executor.app, 'backend', mock_backend):
                fetcher = BulkStateFetcher()
                result = fetcher.get_many([
                    mock.MagicMock(task_id="123"),
                    mock.MagicMock(task_id="456"),
                ])

        # Assert called - ignore order
        mget_args, _ = mock_mget.call_args
        self.assertEqual(set(mget_args[0]), {b'celery-task-meta-456', b'celery-task-meta-123'})
        mock_mget.assert_called_once_with(mock.ANY)

        self.assertEqual(result, {'123': ('SUCCESS', None), '456': ("PENDING", None)})

    @mock.patch("celery.backends.database.DatabaseBackend.ResultSession")
    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_should_support_db_backend(self, mock_session):
        with _prepare_app():
            mock_backend = DatabaseBackend(app=celery_executor.app, url="sqlite3://")

            with mock.patch.object(celery_executor.app, 'backend', mock_backend):
                mock_session = mock_backend.ResultSession.return_value  # pylint: disable=no-member
                mock_session.query.return_value.filter.return_value.all.return_value = [
                    mock.MagicMock(**{"to_dict.return_value": {"status": "SUCCESS", "task_id": "123"}})
                ]

        fetcher = BulkStateFetcher()
        result = fetcher.get_many([
            mock.MagicMock(task_id="123"),
            mock.MagicMock(task_id="456"),
        ])

        self.assertEqual(result, {'123': ('SUCCESS', None), '456': ("PENDING", None)})

    @pytest.mark.integration("redis")
    @pytest.mark.integration("rabbitmq")
    @pytest.mark.backend("mysql", "postgres")
    def test_should_support_base_backend(self):
        with _prepare_app():
            mock_backend = mock.MagicMock(autospec=BaseBackend)

            with mock.patch.object(celery_executor.app, 'backend', mock_backend):
                fetcher = BulkStateFetcher(1)
                result = fetcher.get_many([
                    ClassWithCustomAttributes(task_id="123", state='SUCCESS'),
                    ClassWithCustomAttributes(task_id="456", state="PENDING"),
                ])

        self.assertEqual(result, {'123': ('SUCCESS', None), '456': ("PENDING", None)})
