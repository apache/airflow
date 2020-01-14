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
from time import sleep
from unittest import mock

import psutil

from airflow import settings
from airflow.bin import cli
from airflow.cli.commands import webserver_command
from airflow.cli.commands.webserver_command import get_num_ready_workers_running
from airflow.models import DagBag
from airflow.utils.cli import setup_locations
from tests.test_utils.config import conf_vars


class TestCLIGetNumReadyWorkersRunning(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)
        cls.parser = cli.CLIFactory.get_parser()

    def setUp(self):
        self.gunicorn_master_proc = mock.Mock(pid=None)
        self.children = mock.MagicMock()
        self.child = mock.MagicMock()
        self.process = mock.MagicMock()

    def test_ready_prefix_on_cmdline(self):
        self.child.cmdline.return_value = [settings.GUNICORN_WORKER_READY_PREFIX]
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 1)

    def test_ready_prefix_on_cmdline_no_children(self):
        self.process.children.return_value = []

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_ready_prefix_on_cmdline_zombie(self):
        self.child.cmdline.return_value = []
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_ready_prefix_on_cmdline_dead_process(self):
        self.child.cmdline.side_effect = psutil.NoSuchProcess(11347)
        self.process.children.return_value = [self.child]

        with mock.patch('psutil.Process', return_value=self.process):
            self.assertEqual(get_num_ready_workers_running(self.gunicorn_master_proc), 0)

    def test_cli_webserver_debug(self):
        env = os.environ.copy()
        proc = psutil.Popen(["airflow", "webserver", "-d"], env=env)
        sleep(3)  # wait for webserver to start
        return_code = proc.poll()
        self.assertEqual(
            None,
            return_code,
            "webserver terminated with return code {} in debug mode".format(return_code))
        proc.terminate()
        proc.wait()


class TestCliWebServer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli.CLIFactory.get_parser()

    def setUp(self) -> None:
        self._check_processes()
        self._clean_pidfiles()

    def _check_processes(self):
        try:
            # Confirm that webserver hasn't been launched.
            # pgrep returns exit status 1 if no process matched.
            self.assertEqual(1, subprocess.Popen(["pgrep", "-f", "-c", "airflow webserver"]).wait())
            self.assertEqual(1, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())
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

    @unittest.skipIf("TRAVIS" in os.environ and bool(os.environ["TRAVIS"]),
                     "Skipping test due to lack of required file permission")
    def test_cli_webserver_foreground_with_pid(self):
        # Run webserver in foreground with --pid option
        pidfile = tempfile.mkstemp()[1]
        proc = subprocess.Popen(["airflow", "webserver", "--pid", pidfile])

        # Check the file specified by --pid option exists
        self._wait_pidfile(pidfile)

        # Terminate webserver
        proc.terminate()
        proc.wait()

    @unittest.skipIf("TRAVIS" in os.environ and bool(os.environ["TRAVIS"]),
                     "Skipping test due to lack of required file permission")
    def test_cli_webserver_background(self):
        pidfile_webserver = setup_locations("webserver")[0]
        pidfile_monitor = setup_locations("webserver-monitor")[0]

        # Run webserver as daemon in background. Note that the wait method is not called.
        subprocess.Popen(["airflow", "webserver", "-D"])

        pid_monitor = self._wait_pidfile(pidfile_monitor)
        self._wait_pidfile(pidfile_webserver)

        # Assert that gunicorn and its monitor are launched.
        self.assertEqual(0, subprocess.Popen(["pgrep", "-f", "-c", "airflow webserver"]).wait())
        self.assertEqual(0, subprocess.Popen(["pgrep", "-c", "gunicorn"]).wait())

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
