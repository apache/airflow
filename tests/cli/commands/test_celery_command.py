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
import importlib
import unittest
from argparse import Namespace
from tempfile import NamedTemporaryFile

import mock
import pytest
import sqlalchemy

import airflow
from airflow.bin import cli
from airflow.cli.commands import celery_command
from airflow.configuration import conf
from tests.test_utils.config import conf_vars

mock.patch('airflow.utils.cli.action_logging', lambda x: x).start()
mock_args = Namespace(queues=1, concurrency=1)


class TestWorkerPrecheck(unittest.TestCase):
    @mock.patch('airflow.settings.validate_session')
    def test_error(self, mock_validate_session):
        """
        Test to verify the exit mechanism of airflow-worker cli
        by mocking validate_session method
        """
        mock_validate_session.return_value = False
        with self.assertRaises(SystemExit) as cm:
            celery_command.worker(mock_args)
        self.assertEqual(cm.exception.code, 1)

    @conf_vars({('core', 'worker_precheck'): 'False'})
    def test_worker_precheck_exception(self):
        """
        Test to check the behaviour of validate_session method
        when worker_precheck is absent in airflow configuration
        """
        self.assertTrue(airflow.settings.validate_session())

    @mock.patch('sqlalchemy.orm.session.Session.execute')
    @conf_vars({('core', 'worker_precheck'): 'True'})
    def test_validate_session_dbapi_exception(self, mock_session):
        """
        Test to validate connection failure scenario on SELECT 1 query
        """
        mock_session.side_effect = sqlalchemy.exc.OperationalError("m1", "m2", "m3", "m4")
        self.assertEqual(airflow.settings.validate_session(), False)


@pytest.mark.integration("redis")
@pytest.mark.integration("rabbitmq")
class TestWorkerServeLogs(unittest.TestCase):

    @classmethod
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def setUpClass(cls):
        importlib.reload(cli)
        cls.parser = cli.CLIFactory.get_parser()

    def tearDown(self):
        importlib.reload(cli)

    @mock.patch('airflow.cli.commands.celery_command.worker_bin')
    def test_serve_logs_on_worker_start(self, mock_worker):
        with mock.patch('airflow.cli.commands.celery_command.Process') as mock_process:
            args = self.parser.parse_args(['celery', 'worker', '--concurrency', '1'])

            with mock.patch('celery.platforms.check_privileges') as mock_privil:
                mock_privil.return_value = 0
                celery_command.worker(args)
                mock_process.assert_called()

    @mock.patch('airflow.cli.commands.celery_command.worker_bin')
    def test_skip_serve_logs_on_worker_start(self, mock_worker):
        with mock.patch('airflow.cli.commands.celery_command.Process') as mock_popen:
            args = self.parser.parse_args(['celery', 'worker', '--concurrency', '1', '--skip-serve-logs'])

            with mock.patch('celery.platforms.check_privileges') as mock_privil:
                mock_privil.return_value = 0
                celery_command.worker(args)
                mock_popen.assert_not_called()


class TestCeleryStopCommand(unittest.TestCase):
    @classmethod
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def setUpClass(cls):
        importlib.reload(cli)
        cls.parser = cli.CLIFactory.get_parser()

    def tearDown(self):
        importlib.reload(cli)

    @mock.patch("airflow.cli.commands.celery_command.setup_locations")
    @mock.patch("airflow.cli.commands.celery_command.psutil.Process")
    def test_if_right_pid_is_read(self, mock_process, mock_setup_locations):
        args = self.parser.parse_args(['celery', 'stop'])
        pid = "123"

        # Calling stop_worker should delete the temporary pid file
        with self.assertRaises(FileNotFoundError):
            with NamedTemporaryFile("w+") as f:
                # Create pid file
                f.write(pid)
                f.flush()
                # Setup mock
                mock_setup_locations.return_value = (f.name, None, None, None)
                # Check if works as expected
                celery_command.stop_worker(args)
                mock_process.assert_called_once_with(int(pid))
                mock_process.return_value.terminate.assert_called_once_with()

    @mock.patch("airflow.cli.commands.celery_command.read_pid_from_pidfile")
    @mock.patch("airflow.cli.commands.celery_command.worker_bin.worker")
    @mock.patch("airflow.cli.commands.celery_command.setup_locations")
    def test_same_pid_file_is_used_in_start_and_stop(
        self,
        mock_setup_locations,
        mock_celery_worker,
        mock_read_pid_from_pidfile
    ):
        pid_file = "test_pid_file"
        mock_setup_locations.return_value = (pid_file, None, None, None)
        mock_read_pid_from_pidfile.return_value = None

        # Call worker
        worker_args = self.parser.parse_args(['celery', 'worker', '--skip-serve-logs'])
        celery_command.worker(worker_args)
        run_mock = mock_celery_worker.return_value.run
        assert run_mock.call_args
        assert 'pidfile' in run_mock.call_args.kwargs
        assert run_mock.call_args.kwargs['pidfile'] == pid_file

        # Call stop
        stop_args = self.parser.parse_args(['celery', 'stop'])
        celery_command.stop_worker(stop_args)
        mock_read_pid_from_pidfile.assert_called_once_with(pid_file)


class TestWorkerStart(unittest.TestCase):
    @classmethod
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def setUpClass(cls):
        importlib.reload(cli)
        cls.parser = cli.CLIFactory.get_parser()

    def tearDown(self):
        importlib.reload(cli)

    @mock.patch("airflow.cli.commands.celery_command.setup_locations")
    @mock.patch('airflow.cli.commands.celery_command.Process')
    @mock.patch('airflow.cli.commands.celery_command.worker_bin')
    def test_worker_started_with_required_arguments(self, mock_worker, mock_popen, mock_locations):
        pid_file = "pid_file"
        mock_locations.return_value = (pid_file, None, None, None)
        concurrency = '1'
        celery_hostname = "celery_hostname"
        queues = "queue"
        autoscale = "2,5"
        args = self.parser.parse_args([
            'celery',
            'worker',
            '--autoscale',
            autoscale,
            '--concurrency',
            concurrency,
            '--celery-hostname',
            celery_hostname,
            '--queues',
            queues
        ])

        with mock.patch('celery.platforms.check_privileges') as mock_privil:
            mock_privil.return_value = 0
            celery_command.worker(args)

        mock_worker.worker.return_value.run.assert_called_once_with(
            pool='prefork',
            optimization='fair',
            O='fair',  # noqa
            queues=queues,
            pidfile=pid_file,
            concurrency=int(concurrency),
            autoscale=autoscale,
            hostname=celery_hostname,
            loglevel=conf.get('logging', 'LOGGING_LEVEL'),
        )
