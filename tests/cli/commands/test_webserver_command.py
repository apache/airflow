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
import subprocess
import tempfile
import unittest
from time import sleep, time
from unittest import mock

import psutil
import pytest

from airflow import settings
from airflow.cli import cli_parser
from airflow.cli.commands import webserver_command
from airflow.cli.commands.webserver_command import GunicornMonitor
from airflow.utils.cli import setup_locations
from tests.test_utils.config import conf_vars


class TestGunicornMonitor(unittest.TestCase):

    def setUp(self,) -> None:
        self.gunicorn_master_proc = mock.Mock(pid=2137)
        self.monitor = GunicornMonitor(
            gunicorn_master_proc=self.gunicorn_master_proc,
            num_workers_expected=4,
            master_timeout=60,
            worker_refresh_interval=60,
            worker_refresh_batch_size=2,
            reload_on_plugin_change=True,
        )
        mock.patch.object(self.monitor, '_generate_plugin_state', return_value={}).start()
        mock.patch.object(self.monitor, '_get_num_ready_workers_running', return_value=4).start()
        mock.patch.object(self.monitor, '_get_num_workers_running', return_value=4).start()
        mock.patch.object(self.monitor, '_spawn_new_workers', return_value=None).start()
        mock.patch.object(self.monitor, '_kill_old_workers', return_value=None).start()
        mock.patch.object(self.monitor, '_reload_gunicorn', return_value=None).start()

    @mock.patch('airflow.cli.commands.webserver_command.sleep')
    def test_should_wait_for_workers_to_start(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 0
        self.monitor._get_num_workers_running.return_value = 4
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

    @mock.patch('airflow.cli.commands.webserver_command.sleep')
    def test_should_kill_excess_workers(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 10
        self.monitor._get_num_workers_running.return_value = 10
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_called_once_with(2)  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

    @mock.patch('airflow.cli.commands.webserver_command.sleep')
    def test_should_start_new_workers_when_missing(self, mock_sleep):
        self.monitor._get_num_ready_workers_running.return_value = 2
        self.monitor._get_num_workers_running.return_value = 2
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_called_once_with(2)  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

    @mock.patch('airflow.cli.commands.webserver_command.sleep')
    def test_should_start_new_workers_when_refresh_interval_has_passed(self, mock_sleep):
        self.monitor._last_refresh_time -= 200
        self.monitor._check_workers()
        self.monitor._spawn_new_workers.assert_called_once_with(2)  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member
        self.assertAlmostEqual(self.monitor._last_refresh_time, time(), delta=5)

    @mock.patch('airflow.cli.commands.webserver_command.sleep')
    def test_should_reload_when_plugin_has_been_changed(self, mock_sleep):
        self.monitor._generate_plugin_state.return_value = {'AA': 12}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

        self.monitor._generate_plugin_state.return_value = {'AA': 32}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_not_called()  # pylint: disable=no-member

        self.monitor._generate_plugin_state.return_value = {'AA': 32}

        self.monitor._check_workers()

        self.monitor._spawn_new_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._kill_old_workers.assert_not_called()  # pylint: disable=no-member
        self.monitor._reload_gunicorn.assert_called_once_with()  # pylint: disable=no-member
        self.assertAlmostEqual(self.monitor._last_refresh_time, time(), delta=5)


class TestGunicornMonitorGeneratePluginState(unittest.TestCase):
    @staticmethod
    def _prepare_test_file(filepath: str, size: int):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w") as file:
            file.write("A" * size)
            file.flush()

    def test_should_detect_changes_in_directory(self):
        with tempfile.TemporaryDirectory() as tempdir,\
             mock.patch("airflow.cli.commands.webserver_command.settings.PLUGINS_FOLDER", tempdir):
            self._prepare_test_file(f"{tempdir}/file1.txt", 100)
            self._prepare_test_file(f"{tempdir}/nested/nested/nested/nested/file2.txt", 200)
            self._prepare_test_file(f"{tempdir}/file3.txt", 300)

            monitor = GunicornMonitor(
                gunicorn_master_proc=mock.MagicMock(),
                num_workers_expected=4,
                master_timeout=60,
                worker_refresh_interval=60,
                worker_refresh_batch_size=2,
                reload_on_plugin_change=True,
            )

            # When the files have not changed, the result should be constant
            state_a = monitor._generate_plugin_state()
            state_b = monitor._generate_plugin_state()

            self.assertEqual(state_a, state_b)
            self.assertEqual(3, len(state_a))

            # Should detect new file
            self._prepare_test_file(f"{tempdir}/file4.txt", 400)

            state_c = monitor._generate_plugin_state()

            self.assertNotEqual(state_b, state_c)
            self.assertEqual(4, len(state_c))

            # Should detect changes in files
            self._prepare_test_file(f"{tempdir}/file4.txt", 450)

            state_d = monitor._generate_plugin_state()

            self.assertNotEqual(state_c, state_d)
            self.assertEqual(4, len(state_d))

            # Should support large files
            self._prepare_test_file(f"{tempdir}/file4.txt", 4000000)

            state_d = monitor._generate_plugin_state()

            self.assertNotEqual(state_c, state_d)
            self.assertEqual(4, len(state_d))


class TestCLIGetNumReadyWorkersRunning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    def setUp(self):
        self.gunicorn_master_proc = mock.Mock(pid=2137)
        self.children = mock.MagicMock()
        self.child = mock.MagicMock()
        self.process = mock.MagicMock()
        self.monitor = GunicornMonitor(
            gunicorn_master_proc=self.gunicorn_master_proc,
            num_workers_expected=4,
            master_timeout=60,
            worker_refresh_interval=60,
            worker_refresh_batch_size=2,
            reload_on_plugin_change=True,
        )

    def test_ready_prefix_on_cmdline(self):
        self.child.cmdline.return_value = [settings.GUNICORN_WORKER_READY_PREFIX]
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(self.monitor._get_num_ready_workers_running(), 1)

    def test_ready_prefix_on_cmdline_no_children(self):
        self.process.children.return_value = []

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(self.monitor._get_num_ready_workers_running(), 0)

    def test_ready_prefix_on_cmdline_zombie(self):
        self.child.cmdline.return_value = []
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(self.monitor._get_num_ready_workers_running(), 0)

    def test_ready_prefix_on_cmdline_dead_process(self):
        self.child.cmdline.side_effect = psutil.NoSuchProcess(11347)
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(self.monitor._get_num_ready_workers_running(), 0)


@pytest.mark.quarantined
class TestCliWebServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    def setUp(self) -> None:
        self._check_processes()
        self._clean_pidfiles()

    def _check_processes(self):
        try:
            # Confirm that webserver hasn't been launched.
            # pgrep returns exit status 1 if no process matched.
            self.assertEqual(1, subprocess.Popen(["pgrep", "--full", "--count", "airflow webserver"]).wait())
            self.assertEqual(1, subprocess.Popen(["pgrep", "--count", "gunicorn"]).wait())
        except:  # noqa: E722
            subprocess.Popen(["ps", "-ax"]).wait()
            raise

    def tearDown(self) -> None:
        self._check_processes()

    def _clean_pidfiles(self):
        pidfile_webserver = setup_locations("webserver")[0]
        pidfile_monitor = setup_locations("webserver-monitor")[0]
        if os.path.exists(pidfile_webserver):
            os.remove(pidfile_webserver)
        if os.path.exists(pidfile_monitor):
            os.remove(pidfile_monitor)

    def _wait_pidfile(self, pidfile):
        while True:
            try:
                with open(pidfile) as file:
                    return int(file.read())
            except Exception:  # pylint: disable=broad-except
                sleep(1)

    def test_cli_webserver_foreground(self):
        # Run webserver in foreground and terminate it.
        proc = subprocess.Popen(["airflow", "webserver"])
        proc.terminate()
        proc.wait()

    def test_cli_webserver_foreground_with_pid(self):
        # Run webserver in foreground with --pid option
        pidfile = tempfile.mkstemp()[1]
        proc = subprocess.Popen(["airflow", "webserver", "--pid", pidfile])

        # Check the file specified by --pid option exists
        self._wait_pidfile(pidfile)

        # Terminate webserver
        proc.terminate()
        proc.wait()

    def test_cli_webserver_background(self):
        pidfile_webserver = setup_locations("webserver")[0]
        pidfile_monitor = setup_locations("webserver-monitor")[0]

        # Run webserver as daemon in background. Note that the wait method is not called.
        subprocess.Popen(["airflow", "webserver", "--daemon"])

        pid_monitor = self._wait_pidfile(pidfile_monitor)
        self._wait_pidfile(pidfile_webserver)

        # Assert that gunicorn and its monitor are launched.
        self.assertEqual(0, subprocess.Popen(["pgrep", "--full", "--count", "airflow webserver"]).wait())
        self.assertEqual(0, subprocess.Popen(["pgrep", "--count", "gunicorn"]).wait())

        # Terminate monitor process.
        proc = psutil.Process(pid_monitor)
        proc.terminate()
        proc.wait()

    # Patch for causing webserver timeout
    @mock.patch("airflow.cli.commands.webserver_command.get_num_workers_running", return_value=0)
    def test_cli_webserver_shutdown_when_gunicorn_master_is_killed(self, _):
        # Shorten timeout so that this test doesn't take too long time
        args = self.parser.parse_args(['webserver'])
        with conf_vars({('webserver', 'web_server_master_timeout'): '10'}):
            with self.assertRaises(SystemExit) as e:
                webserver_command.webserver(args)
        self.assertEqual(e.exception.code, 1)

    def test_cli_webserver_debug(self):
        env = os.environ.copy()
        proc = psutil.Popen(["airflow", "webserver", "--debug"], env=env)
        sleep(3)  # wait for webserver to start
        return_code = proc.poll()
        self.assertEqual(
            None,
            return_code,
            "webserver terminated with return code {} in debug mode".format(return_code))
        proc.terminate()
        proc.wait()
