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
import unittest
from argparse import Namespace
from tempfile import NamedTemporaryFile
from unittest import mock

import pytest
import sqlalchemy

import airflow
from airflow.cli import cli_parser
from airflow.cli.commands import celery_command
from airflow.configuration import conf
from tests.test_utils.config import conf_vars


class TestWorkerPrecheck(unittest.TestCase):
    @mock.patch('airflow.settings.validate_session')
    def test_error(self, mock_validate_session):
        """
        Test to verify the exit mechanism of airflow-worker cli
        by mocking validate_session method
        """
        mock_validate_session.return_value = False
        with pytest.raises(SystemExit) as ctx:
            celery_command.worker(Namespace(queues=1, concurrency=1))
        assert str(ctx.value) == "Worker exiting, database connection precheck failed."

    @conf_vars({('celery', 'worker_precheck'): 'False'})
    def test_worker_precheck_exception(self):
        """
        Test to check the behaviour of validate_session method
        when worker_precheck is absent in airflow configuration
        """
        assert airflow.settings.validate_session()

    @mock.patch('sqlalchemy.orm.session.Session.execute')
    @conf_vars({('celery', 'worker_precheck'): 'True'})
    def test_validate_session_dbapi_exception(self, mock_session):
        """
        Test to validate connection failure scenario on SELECT 1 query
        """
        mock_session.side_effect = sqlalchemy.exc.OperationalError("m1", "m2", "m3", "m4")
        assert airflow.settings.validate_session() is False


@pytest.mark.integration("redis")
@pytest.mark.integration("rabbitmq")
@pytest.mark.backend("mysql", "postgres")
class TestWorkerServeLogs(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch('airflow.cli.commands.celery_command.celery_app')
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_serve_logs_on_worker_start(self, mock_celery_app):
        with mock.patch('airflow.cli.commands.celery_command.Process') as mock_process:
            args = self.parser.parse_args(['celery', 'worker', '--concurrency', '1'])

            with mock.patch('celery.platforms.check_privileges') as mock_privil:
                mock_privil.return_value = 0
                celery_command.worker(args)
                mock_process.assert_called()

    @mock.patch('airflow.cli.commands.celery_command.celery_app')
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_skip_serve_logs_on_worker_start(self, mock_celery_app):
        with mock.patch('airflow.cli.commands.celery_command.Process') as mock_popen:
            args = self.parser.parse_args(['celery', 'worker', '--concurrency', '1', '--skip-serve-logs'])

            with mock.patch('celery.platforms.check_privileges') as mock_privil:
                mock_privil.return_value = 0
                celery_command.worker(args)
                mock_popen.assert_not_called()


@pytest.mark.backend("mysql", "postgres")
class TestCeleryStopCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.celery_command.setup_locations")
    @mock.patch("airflow.cli.commands.celery_command.psutil.Process")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_if_right_pid_is_read(self, mock_process, mock_setup_locations):
        args = self.parser.parse_args(['celery', 'stop'])
        pid = "123"

        # Calling stop_worker should delete the temporary pid file
        with pytest.raises(FileNotFoundError):
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
    @mock.patch('airflow.cli.commands.celery_command.celery_app')
    @mock.patch("airflow.cli.commands.celery_command.setup_locations")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_same_pid_file_is_used_in_start_and_stop(
        self, mock_setup_locations, mock_celery_app, mock_read_pid_from_pidfile
    ):
        pid_file = "test_pid_file"
        mock_setup_locations.return_value = (pid_file, None, None, None)
        mock_read_pid_from_pidfile.return_value = None

        # Call worker
        worker_args = self.parser.parse_args(['celery', 'worker', '--skip-serve-logs'])
        celery_command.worker(worker_args)
        assert mock_celery_app.worker_main.call_args
        args, _ = mock_celery_app.worker_main.call_args
        args_str = ' '.join(map(str, args[0]))
        assert f'--pidfile {pid_file}' in args_str

        # Call stop
        stop_args = self.parser.parse_args(['celery', 'stop'])
        celery_command.stop_worker(stop_args)
        mock_read_pid_from_pidfile.assert_called_once_with(pid_file)

    @mock.patch("airflow.cli.commands.celery_command.remove_existing_pidfile")
    @mock.patch("airflow.cli.commands.celery_command.read_pid_from_pidfile")
    @mock.patch('airflow.cli.commands.celery_command.celery_app')
    @mock.patch("airflow.cli.commands.celery_command.psutil.Process")
    @mock.patch("airflow.cli.commands.celery_command.setup_locations")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_custom_pid_file_is_used_in_start_and_stop(
        self,
        mock_setup_locations,
        mock_process,
        mock_celery_app,
        mock_read_pid_from_pidfile,
        mock_remove_existing_pidfile,
    ):
        pid_file = "custom_test_pid_file"
        mock_setup_locations.return_value = (pid_file, None, None, None)
        # Call worker
        worker_args = self.parser.parse_args(['celery', 'worker', '--skip-serve-logs', '--pid', pid_file])
        celery_command.worker(worker_args)
        assert mock_celery_app.worker_main.call_args
        args, _ = mock_celery_app.worker_main.call_args
        args_str = ' '.join(map(str, args[0]))
        assert f'--pidfile {pid_file}' in args_str

        stop_args = self.parser.parse_args(['celery', 'stop', '--pid', pid_file])
        celery_command.stop_worker(stop_args)

        mock_read_pid_from_pidfile.assert_called_once_with(pid_file)
        mock_process.return_value.terminate.assert_called()
        mock_remove_existing_pidfile.assert_called_once_with(pid_file)


@pytest.mark.backend("mysql", "postgres")
class TestWorkerStart(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.celery_command.setup_locations")
    @mock.patch('airflow.cli.commands.celery_command.Process')
    @mock.patch('airflow.cli.commands.celery_command.celery_app')
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_worker_started_with_required_arguments(self, mock_celery_app, mock_popen, mock_locations):
        pid_file = "pid_file"
        mock_locations.return_value = (pid_file, None, None, None)
        concurrency = '1'
        celery_hostname = "celery_hostname"
        queues = "queue"
        autoscale = "2,5"
        args = self.parser.parse_args(
            [
                'celery',
                'worker',
                '--autoscale',
                autoscale,
                '--concurrency',
                concurrency,
                '--celery-hostname',
                celery_hostname,
                '--queues',
                queues,
                '--without-mingle',
                '--without-gossip',
            ]
        )

        celery_command.worker(args)

        mock_celery_app.worker_main.assert_called_once_with(
            [
                'worker',
                '-O',
                'fair',
                '--queues',
                queues,
                '--concurrency',
                int(concurrency),
                '--hostname',
                celery_hostname,
                '--loglevel',
                conf.get('logging', 'CELERY_LOGGING_LEVEL'),
                '--pidfile',
                pid_file,
                '--autoscale',
                autoscale,
                '--without-mingle',
                '--without-gossip',
                '--pool',
                'prefork',
            ]
        )


@pytest.mark.backend("mysql", "postgres")
class TestWorkerFailure(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch('airflow.cli.commands.celery_command.Process')
    @mock.patch('airflow.cli.commands.celery_command.celery_app')
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_worker_failure_gracefull_shutdown(self, mock_celery_app, mock_popen):
        args = self.parser.parse_args(['celery', 'worker'])
        mock_celery_app.run.side_effect = Exception('Mock exception to trigger runtime error')
        try:
            celery_command.worker(args)
        finally:
            mock_popen().terminate.assert_called()


@pytest.mark.backend("mysql", "postgres")
class TestFlowerCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch('airflow.cli.commands.celery_command.celery_app')
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_run_command(self, mock_celery_app):
        args = self.parser.parse_args(
            [
                'celery',
                'flower',
                '--basic-auth',
                'admin:admin',
                '--broker-api',
                'http://username:password@rabbitmq-server-name:15672/api/',
                '--flower-conf',
                'flower_config',
                '--hostname',
                'my-hostname',
                '--port',
                '3333',
                '--url-prefix',
                'flower-monitoring',
            ]
        )

        celery_command.flower(args)
        mock_celery_app.start.assert_called_once_with(
            [
                'flower',
                'amqp://guest:guest@rabbitmq:5672/',
                '--address=my-hostname',
                '--port=3333',
                '--broker-api=http://username:password@rabbitmq-server-name:15672/api/',
                '--url-prefix=flower-monitoring',
                '--basic-auth=admin:admin',
                '--conf=flower_config',
            ]
        )

    @mock.patch('airflow.cli.commands.celery_command.TimeoutPIDLockFile')
    @mock.patch('airflow.cli.commands.celery_command.setup_locations')
    @mock.patch('airflow.cli.commands.celery_command.daemon')
    @mock.patch('airflow.cli.commands.celery_command.celery_app')
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_run_command_daemon(self, mock_celery_app, mock_daemon, mock_setup_locations, mock_pid_file):
        mock_setup_locations.return_value = (
            mock.MagicMock(name='pidfile'),
            mock.MagicMock(name='stdout'),
            mock.MagicMock(name='stderr'),
            mock.MagicMock(name="INVALID"),
        )
        args = self.parser.parse_args(
            [
                'celery',
                'flower',
                '--basic-auth',
                'admin:admin',
                '--broker-api',
                'http://username:password@rabbitmq-server-name:15672/api/',
                '--flower-conf',
                'flower_config',
                '--hostname',
                'my-hostname',
                '--log-file',
                '/tmp/flower.log',
                '--pid',
                '/tmp/flower.pid',
                '--port',
                '3333',
                '--stderr',
                '/tmp/flower-stderr.log',
                '--stdout',
                '/tmp/flower-stdout.log',
                '--url-prefix',
                'flower-monitoring',
                '--daemon',
            ]
        )
        mock_open = mock.mock_open()
        with mock.patch('airflow.cli.commands.celery_command.open', mock_open):
            celery_command.flower(args)

        mock_celery_app.start.assert_called_once_with(
            [
                'flower',
                'amqp://guest:guest@rabbitmq:5672/',
                '--address=my-hostname',
                '--port=3333',
                '--broker-api=http://username:password@rabbitmq-server-name:15672/api/',
                '--url-prefix=flower-monitoring',
                '--basic-auth=admin:admin',
                '--conf=flower_config',
            ]
        )
        assert mock_daemon.mock_calls == [
            mock.call.DaemonContext(
                pidfile=mock_pid_file.return_value,
                stderr=mock_open.return_value,
                stdout=mock_open.return_value,
            ),
            mock.call.DaemonContext().__enter__(),
            mock.call.DaemonContext().__exit__(None, None, None),
        ]

        assert mock_setup_locations.mock_calls == [
            mock.call(
                log='/tmp/flower.log',
                pid='/tmp/flower.pid',
                process='flower',
                stderr='/tmp/flower-stderr.log',
                stdout='/tmp/flower-stdout.log',
            )
        ]
        mock_pid_file.assert_has_calls([mock.call(mock_setup_locations.return_value[0], -1)])
        assert mock_open.mock_calls == [
            mock.call(mock_setup_locations.return_value[1], 'a'),
            mock.call().__enter__(),
            mock.call(mock_setup_locations.return_value[2], 'a'),
            mock.call().__enter__(),
            mock.call().truncate(0),
            mock.call().truncate(0),
            mock.call().__exit__(None, None, None),
            mock.call().__exit__(None, None, None),
        ]
