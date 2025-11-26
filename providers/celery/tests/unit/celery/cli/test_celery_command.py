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
from __future__ import annotations

import contextlib
import importlib
import json
import os
import sys
from io import StringIO
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from airflow.cli import cli_parser
from airflow.configuration import conf
from airflow.executors import executor_loader
from airflow.providers.celery.cli import celery_command
from airflow.providers.celery.cli.celery_command import _run_stale_bundle_cleanup

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

PY313 = sys.version_info >= (3, 13)


@pytest.fixture(autouse=False)
def conf_stale_bundle_cleanup_disabled():
    with conf_vars({("dag_processor", "stale_bundle_cleanup_interval"): "0"}):
        yield


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.usefixtures("conf_stale_bundle_cleanup_disabled")
class TestCeleryStopCommand:
    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            importlib.reload(executor_loader)
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
    @mock.patch("airflow.providers.celery.cli.celery_command.psutil.Process")
    def test_if_right_pid_is_read(self, mock_process, mock_setup_locations, tmp_path):
        args = self.parser.parse_args(["celery", "stop"])
        pid = "123"
        path = tmp_path / "testfile"
        # Create pid file
        path.write_text(pid)
        # Setup mock
        mock_setup_locations.return_value = (os.fspath(path), None, None, None)

        # Calling stop_worker should delete the temporary pid file
        celery_command.stop_worker(args)
        # Check if works as expected
        assert not path.exists()
        mock_process.assert_called_once_with(int(pid))
        mock_process.return_value.terminate.assert_called_once_with()

    @mock.patch("airflow.providers.celery.cli.celery_command.read_pid_from_pidfile")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
    def test_same_pid_file_is_used_in_start_and_stop(
        self, mock_setup_locations, mock_celery_app, mock_read_pid_from_pidfile
    ):
        pid_file = "test_pid_file"
        mock_setup_locations.return_value = (pid_file, None, None, None)
        mock_read_pid_from_pidfile.return_value = None

        # Call worker
        worker_args = self.parser.parse_args(["celery", "worker", "--skip-serve-logs"])
        celery_command.worker(worker_args)
        assert mock_celery_app.worker_main.call_args
        args, _ = mock_celery_app.worker_main.call_args
        args_str = " ".join(map(str, args[0]))
        assert f"--pidfile {pid_file}" not in args_str

        # Call stop
        stop_args = self.parser.parse_args(["celery", "stop"])
        celery_command.stop_worker(stop_args)
        mock_read_pid_from_pidfile.assert_called_once_with(pid_file)

    @mock.patch("airflow.providers.celery.cli.celery_command.remove_existing_pidfile")
    @mock.patch("airflow.providers.celery.cli.celery_command.read_pid_from_pidfile")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    @mock.patch("airflow.providers.celery.cli.celery_command.psutil.Process")
    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
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
        worker_args = self.parser.parse_args(["celery", "worker", "--skip-serve-logs", "--pid", pid_file])
        celery_command.worker(worker_args)
        assert mock_celery_app.worker_main.call_args
        args, _ = mock_celery_app.worker_main.call_args
        args_str = " ".join(map(str, args[0]))
        assert f"--pidfile {pid_file}" not in args_str

        stop_args = self.parser.parse_args(["celery", "stop", "--pid", pid_file])
        celery_command.stop_worker(stop_args)

        mock_read_pid_from_pidfile.assert_called_once_with(pid_file)
        mock_process.return_value.terminate.assert_called()
        mock_remove_existing_pidfile.assert_called_once_with(pid_file)


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.usefixtures("conf_stale_bundle_cleanup_disabled")
class TestWorkerStart:
    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            importlib.reload(executor_loader)
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.providers.celery.cli.celery_command.setup_locations")
    @mock.patch("airflow.providers.celery.cli.celery_command.Process")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_worker_started_with_required_arguments(self, mock_celery_app, mock_popen, mock_locations):
        pid_file = "pid_file"
        mock_locations.return_value = (pid_file, None, None, None)
        concurrency = "1"
        celery_hostname = "celery_hostname"
        queues = "queue"
        autoscale = "2,5"
        args = self.parser.parse_args(
            [
                "celery",
                "worker",
                "--autoscale",
                autoscale,
                "--concurrency",
                concurrency,
                "--celery-hostname",
                celery_hostname,
                "--queues",
                queues,
                "--without-mingle",
                "--without-gossip",
            ]
        )

        celery_command.worker(args)

        mock_celery_app.worker_main.assert_called_once_with(
            [
                "worker",
                "-O",
                "fair",
                "--queues",
                queues,
                "--concurrency",
                int(concurrency),
                "--loglevel",
                conf.get("logging", "CELERY_LOGGING_LEVEL"),
                "--hostname",
                celery_hostname,
                "--autoscale",
                autoscale,
                "--without-mingle",
                "--without-gossip",
                "--pool",
                "prefork",
            ]
        )


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.usefixtures("conf_stale_bundle_cleanup_disabled")
class TestWorkerFailure:
    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.providers.celery.cli.celery_command.Process")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_worker_failure_gracefull_shutdown(self, mock_celery_app, mock_popen):
        args = self.parser.parse_args(["celery", "worker"])
        mock_celery_app.run.side_effect = Exception("Mock exception to trigger runtime error")
        try:
            celery_command.worker(args)
        finally:
            mock_popen().terminate.assert_called()


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.usefixtures("conf_stale_bundle_cleanup_disabled")
class TestWorkerDuplicateHostnameCheck:
    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            importlib.reload(executor_loader)
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.inspect")
    def test_worker_fails_when_hostname_already_exists(self, mock_inspect):
        """Test that worker command fails when trying to start a worker with a duplicate hostname."""
        args = self.parser.parse_args(["celery", "worker", "--celery-hostname", "existing_host"])

        # Mock the inspect to return an active worker with the same hostname
        mock_instance = MagicMock()
        mock_instance.active_queues.return_value = {
            "celery@existing_host": [{"name": "queue1"}],
        }
        mock_inspect.return_value = mock_instance

        # Test that SystemExit is raised with appropriate error message
        with pytest.raises(SystemExit) as exc_info:
            celery_command.worker(args)

        assert "existing_host" in str(exc_info.value)
        assert "already running" in str(exc_info.value)

    @pytest.mark.db_test
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.inspect")
    @mock.patch("airflow.providers.celery.cli.celery_command.Process")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_worker_starts_when_hostname_is_unique(self, mock_celery_app, mock_popen, mock_inspect):
        """Test that worker command succeeds when the hostname is unique."""
        args = self.parser.parse_args(["celery", "worker", "--celery-hostname", "new_host"])

        # Mock the inspect to return active workers without the new hostname
        mock_instance = MagicMock()
        mock_instance.active_queues.return_value = {
            "celery@existing_host": [{"name": "queue1"}],
        }
        mock_inspect.return_value = mock_instance

        # Worker should start successfully
        celery_command.worker(args)

        # Verify that worker_main was called
        assert mock_celery_app.worker_main.called


@pytest.mark.backend("mysql", "postgres")
@pytest.mark.usefixtures("conf_stale_bundle_cleanup_disabled")
class TestFlowerCommand:
    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_run_command(self, mock_celery_app):
        args = self.parser.parse_args(
            [
                "celery",
                "flower",
                "--basic-auth",
                "admin:admin",
                "--broker-api",
                "http://username:password@rabbitmq-server-name:15672/api/",
                "--flower-conf",
                "flower_config",
                "--hostname",
                "my-hostname",
                "--port",
                "3333",
                "--url-prefix",
                "flower-monitoring",
            ]
        )

        celery_command.flower(args)
        mock_celery_app.start.assert_called_once_with(
            [
                "flower",
                conf.get("celery", "BROKER_URL"),
                "--address=my-hostname",
                "--port=3333",
                "--broker-api=http://username:password@rabbitmq-server-name:15672/api/",
                "--url-prefix=flower-monitoring",
                "--basic-auth=admin:admin",
                "--conf=flower_config",
            ]
        )

    def _test_run_command_daemon(self, mock_celery_app, mock_daemon, mock_setup_locations, mock_pid_file):
        mock_setup_locations.return_value = (
            mock.MagicMock(name="pidfile"),
            mock.MagicMock(name="stdout"),
            mock.MagicMock(name="stderr"),
            mock.MagicMock(name="INVALID"),
        )
        args = self.parser.parse_args(
            [
                "celery",
                "flower",
                "--basic-auth",
                "admin:admin",
                "--broker-api",
                "http://username:password@rabbitmq-server-name:15672/api/",
                "--flower-conf",
                "flower_config",
                "--hostname",
                "my-hostname",
                "--log-file",
                "/tmp/flower.log",
                "--pid",
                "/tmp/flower.pid",
                "--port",
                "3333",
                "--stderr",
                "/tmp/flower-stderr.log",
                "--stdout",
                "/tmp/flower-stdout.log",
                "--url-prefix",
                "flower-monitoring",
                "--daemon",
            ]
        )
        mock_open = mock.mock_open()
        with mock.patch("airflow.cli.commands.daemon_utils.open", mock_open):
            celery_command.flower(args)

        mock_celery_app.start.assert_called_once_with(
            [
                "flower",
                conf.get("celery", "BROKER_URL"),
                "--address=my-hostname",
                "--port=3333",
                "--broker-api=http://username:password@rabbitmq-server-name:15672/api/",
                "--url-prefix=flower-monitoring",
                "--basic-auth=admin:admin",
                "--conf=flower_config",
            ]
        )
        assert mock_daemon.mock_calls[:3] == [
            mock.call.DaemonContext(
                pidfile=mock_pid_file.return_value,
                files_preserve=None,
                stdout=mock_open.return_value,
                stderr=mock_open.return_value,
                umask=0o077,
            ),
            mock.call.DaemonContext().__enter__(),
            mock.call.DaemonContext().__exit__(None, None, None),
        ]

        assert mock_setup_locations.mock_calls == [
            mock.call(
                process="flower",
                pid="/tmp/flower.pid",
                stdout="/tmp/flower-stdout.log",
                stderr="/tmp/flower-stderr.log",
                log="/tmp/flower.log",
            )
        ]
        mock_pid_file.assert_has_calls([mock.call(mock_setup_locations.return_value[0], -1)])

        if PY313:
            assert mock_open.mock_calls == [
                mock.call(mock_setup_locations.return_value[1], "a"),
                mock.call().__enter__(),
                mock.call(mock_setup_locations.return_value[2], "a"),
                mock.call().__enter__(),
                mock.call().truncate(0),
                mock.call().truncate(0),
                mock.call().__exit__(None, None, None),
                mock.call().close(),
                mock.call().__exit__(None, None, None),
                mock.call().close(),
            ]
        else:
            assert mock_open.mock_calls == [
                mock.call(mock_setup_locations.return_value[1], "a"),
                mock.call().__enter__(),
                mock.call(mock_setup_locations.return_value[2], "a"),
                mock.call().__enter__(),
                mock.call().truncate(0),
                mock.call().truncate(0),
                mock.call().__exit__(None, None, None),
                mock.call().__exit__(None, None, None),
            ]

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0-")
    @mock.patch("airflow.cli.commands.daemon_utils.TimeoutPIDLockFile")
    @mock.patch("airflow.cli.commands.daemon_utils.setup_locations")
    @mock.patch("airflow.cli.commands.daemon_utils.daemon")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_run_command_daemon_v_3_below(
        self, mock_celery_app, mock_daemon, mock_setup_locations, mock_pid_file
    ):
        self._test_run_command_daemon(mock_celery_app, mock_daemon, mock_setup_locations, mock_pid_file)

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+")
    @mock.patch("airflow.cli.commands.daemon_utils.TimeoutPIDLockFile")
    @mock.patch("airflow.cli.commands.daemon_utils.setup_locations")
    @mock.patch("airflow.cli.commands.daemon_utils.daemon")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app")
    def test_run_command_daemon_v3_above(
        self, mock_celery_app, mock_daemon, mock_setup_locations, mock_pid_file
    ):
        self._test_run_command_daemon(mock_celery_app, mock_daemon, mock_setup_locations, mock_pid_file)


class TestRemoteCeleryControlCommands:
    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            importlib.reload(executor_loader)
            importlib.reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @pytest.mark.db_test
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.inspect")
    def test_list_celery_workers(self, mock_inspect):
        args = self.parser.parse_args(["celery", "list-workers", "--output", "json"])
        mock_instance = MagicMock()
        mock_instance.active_queues.return_value = {
            "celery@host_1": [{"name": "queue1"}, {"name": "queue2"}],
            "celery@host_2": [{"name": "queue3"}],
        }
        mock_inspect.return_value = mock_instance
        with contextlib.redirect_stdout(StringIO()) as temp_stdout:
            celery_command.list_workers(args)
            out = temp_stdout.getvalue()
            celery_workers = json.loads(out)
        for key in ["worker_name", "queues"]:
            assert key in celery_workers[0]
        assert any("celery@host_1" in h["worker_name"] for h in celery_workers)

    @pytest.mark.db_test
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.shutdown")
    def test_shutdown_worker(self, mock_shutdown):
        args = self.parser.parse_args(["celery", "shutdown-worker", "-H", "celery@host_1"])
        with patch(
            "airflow.providers.celery.cli.celery_command._check_if_active_celery_worker", return_value=None
        ):
            celery_command.shutdown_worker(args)
            mock_shutdown.assert_called_once_with(destination=["celery@host_1"])

    @pytest.mark.db_test
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.broadcast")
    def test_shutdown_all_workers(self, mock_broadcast):
        args = self.parser.parse_args(["celery", "shutdown-all-workers", "-y"])
        with patch(
            "airflow.providers.celery.cli.celery_command._check_if_active_celery_worker", return_value=None
        ):
            celery_command.shutdown_all_workers(args)
            mock_broadcast.assert_called_once_with("shutdown")

    @pytest.mark.db_test
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.add_consumer")
    def test_add_queue(self, mock_add_consumer):
        args = self.parser.parse_args(["celery", "add-queue", "-q", "test1", "-H", "celery@host_1"])
        with patch(
            "airflow.providers.celery.cli.celery_command._check_if_active_celery_worker", return_value=None
        ):
            celery_command.add_queue(args)
            mock_add_consumer.assert_called_once_with("test1", destination=["celery@host_1"])

    @pytest.mark.db_test
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.cancel_consumer")
    def test_remove_queue(self, mock_cancel_consumer):
        args = self.parser.parse_args(["celery", "remove-queue", "-q", "test1", "-H", "celery@host_1"])
        with patch(
            "airflow.providers.celery.cli.celery_command._check_if_active_celery_worker", return_value=None
        ):
            celery_command.remove_queue(args)
            mock_cancel_consumer.assert_called_once_with("test1", destination=["celery@host_1"])

    @pytest.mark.db_test
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.cancel_consumer")
    @mock.patch("airflow.providers.celery.executors.celery_executor.app.control.inspect")
    def test_remove_all_queues(self, mock_inspect, mock_cancel_consumer):
        args = self.parser.parse_args(["celery", "remove-all-queues", "-H", "celery@host_1"])
        mock_instance = MagicMock()
        mock_instance.active_queues.return_value = {
            "celery@host_1": [{"name": "queue1"}, {"name": "queue2"}],
            "celery@host_2": [{"name": "queue3"}],
        }
        mock_inspect.return_value = mock_instance
        with patch(
            "airflow.providers.celery.cli.celery_command._check_if_active_celery_worker", return_value=None
        ):
            celery_command.remove_all_queues(args)
            # Verify cancel_consumer was called for each queue
            expected_calls = [
                mock.call("queue1", destination=["celery@host_1"]),
                mock.call("queue2", destination=["celery@host_1"]),
            ]
            mock_cancel_consumer.assert_has_calls(expected_calls, any_order=True)
            assert mock_cancel_consumer.call_count == 2


@patch("airflow.providers.celery.cli.celery_command.Process")
@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Doesn't apply to pre-3.0")
def test_stale_bundle_cleanup(mock_process):
    mock_process.__bool__.return_value = True
    with _run_stale_bundle_cleanup():
        ...
    calls = mock_process.call_args_list
    assert len(calls) == 1
    actual = [x.kwargs["target"] for x in calls]
    assert actual[0].__name__ == "bundle_cleanup_main"
