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

import logging
import os
import signal
import subprocess
import sys
from contextlib import suppress
from subprocess import CalledProcessError
from time import sleep
from unittest import mock

import psutil
import pytest

from airflow.exceptions import AirflowException
from airflow.utils import process_utils
from airflow.utils.process_utils import (
    check_if_pidfile_process_is_running,
    execute_in_subprocess,
    execute_in_subprocess_with_kwargs,
    set_new_process_group,
)


class TestReapProcessGroup:
    # Inline script that creates a new session, spawns a SIGTERM-ignoring child,
    # prints both PIDs to stdout, then loops forever.  Uses subprocess.Popen
    # instead of multiprocessing.Process to avoid fork()-ing the (large) test
    # worker process — which causes OOM under xdist on Python 3.14.
    _PARENT_SCRIPT = """\
import os, signal, subprocess, sys, time
os.setsid()
signal.signal(signal.SIGTERM, lambda s, f: None)
child = subprocess.Popen(
    [sys.executable, "-c",
     "import signal, time; signal.signal(signal.SIGTERM, lambda s,f: None); time.sleep(300)"]
)
print(f"{os.getpid()} {child.pid}", flush=True)
time.sleep(300)
"""

    def test_reap_process_group(self):
        """
        Spin up a process that can't be killed by SIGTERM and make sure
        it gets killed anyway.
        """
        parent = subprocess.Popen(
            [sys.executable, "-c", self._PARENT_SCRIPT],
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            line = parent.stdout.readline()
            assert line, "Parent script did not print PIDs"
            parent_pid, child_pid = map(int, line.strip().split())

            assert psutil.pid_exists(parent_pid)
            assert psutil.pid_exists(child_pid)

            process_utils.reap_process_group(parent_pid, logging.getLogger(), timeout=1)

            assert not psutil.pid_exists(parent_pid)
            assert not psutil.pid_exists(child_pid)
        finally:
            try:
                os.kill(parent.pid, signal.SIGKILL)
            except OSError:
                pass
            parent.stdout.close()
            parent.wait()


@pytest.mark.db_test
class TestExecuteInSubProcess:
    def test_should_print_all_messages1(self, caplog):
        execute_in_subprocess(["bash", "-c", "echo CAT; echo KITTY;"])
        assert caplog.messages == [
            "Executing cmd: bash -c 'echo CAT; echo KITTY;'",
            "Output:",
            "CAT",
            "KITTY",
        ]

    def test_should_print_all_messages_from_cwd(self, caplog, tmp_path):
        execute_in_subprocess(["bash", "-c", "echo CAT; pwd; echo KITTY;"], cwd=str(tmp_path))
        assert [
            "Executing cmd: bash -c 'echo CAT; pwd; echo KITTY;'",
            "Output:",
            "CAT",
            str(tmp_path),
            "KITTY",
        ] == caplog.messages

    def test_using_env_works(self, caplog):
        execute_in_subprocess(["bash", "-c", 'echo "My value is ${VALUE}"'], env=dict(VALUE="1"))
        assert "My value is 1" in caplog.text

    def test_should_raise_exception(self):
        with pytest.raises(CalledProcessError):
            process_utils.execute_in_subprocess(["bash", "-c", "exit 1"])

    def test_using_env_as_kwarg_works(self, caplog):
        execute_in_subprocess_with_kwargs(["bash", "-c", 'echo "My value is ${VALUE}"'], env=dict(VALUE="1"))
        assert "My value is 1" in caplog.text


@pytest.mark.db_test
class TestKillChildProcessesByPids:
    def test_should_kill_process(self):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(300)"])

        assert psutil.pid_exists(process.pid)

        process_utils.kill_child_processes_by_pids([process.pid])

        assert not psutil.pid_exists(process.pid)

    def test_should_force_kill_process(self, caplog):
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import signal, time; "
                "signal.signal(signal.SIGINT, lambda s,f: None); "
                "signal.signal(signal.SIGTERM, lambda s,f: None); "
                "print('ready', flush=True); "
                "time.sleep(300)",
            ],
            stdout=subprocess.PIPE,
            text=True,
        )
        # Wait until signal handlers are installed before sending signals
        process.stdout.readline()

        assert psutil.pid_exists(process.pid)

        with caplog.at_level(logging.INFO, logger=process_utils.log.name):
            caplog.clear()
            process_utils.kill_child_processes_by_pids([process.pid], timeout=0)
            assert f"Killing child PID: {process.pid}" in caplog.messages
        sleep(0)
        assert not psutil.pid_exists(process.pid)


class TestPatchEnviron:
    def test_should_update_variable_and_restore_state_when_exit(self):
        with mock.patch.dict("os.environ", {"TEST_NOT_EXISTS": "BEFORE", "TEST_EXISTS": "BEFORE"}):
            del os.environ["TEST_NOT_EXISTS"]

            assert os.environ["TEST_EXISTS"] == "BEFORE"
            assert "TEST_NOT_EXISTS" not in os.environ

            with process_utils.patch_environ({"TEST_NOT_EXISTS": "AFTER", "TEST_EXISTS": "AFTER"}):
                assert os.environ["TEST_NOT_EXISTS"] == "AFTER"
                assert os.environ["TEST_EXISTS"] == "AFTER"

            assert os.environ["TEST_EXISTS"] == "BEFORE"
            assert "TEST_NOT_EXISTS" not in os.environ

    def test_should_restore_state_when_exception(self):
        with mock.patch.dict("os.environ", {"TEST_NOT_EXISTS": "BEFORE", "TEST_EXISTS": "BEFORE"}):
            del os.environ["TEST_NOT_EXISTS"]

            assert os.environ["TEST_EXISTS"] == "BEFORE"
            assert "TEST_NOT_EXISTS" not in os.environ

            with suppress(AirflowException):
                with process_utils.patch_environ({"TEST_NOT_EXISTS": "AFTER", "TEST_EXISTS": "AFTER"}):
                    assert os.environ["TEST_NOT_EXISTS"] == "AFTER"
                    assert os.environ["TEST_EXISTS"] == "AFTER"
                    raise AirflowException("Unknown exception")

            assert os.environ["TEST_EXISTS"] == "BEFORE"
            assert "TEST_NOT_EXISTS" not in os.environ


class TestCheckIfPidfileProcessIsRunning:
    def test_ok_if_no_file(self):
        check_if_pidfile_process_is_running("some/pid/file", process_name="test")

    def test_remove_if_no_process(self, tmp_path):
        path = tmp_path / "testfile"
        # limit pid as max of int32, otherwise this test could fail on some platform
        path.write_text(f"{2**31 - 1}")
        check_if_pidfile_process_is_running(os.fspath(path), process_name="test")
        # Assert file is deleted
        assert not path.exists()

    def test_raise_error_if_process_is_running(self, tmp_path):
        path = tmp_path / "testfile"
        pid = os.getpid()
        path.write_text(f"{pid}")
        with pytest.raises(AirflowException, match="is already running under PID"):
            check_if_pidfile_process_is_running(os.fspath(path), process_name="test")


class TestSetNewProcessGroup:
    @mock.patch("os.setpgid")
    def test_not_session_leader(self, mock_set_pid):
        pid = os.getpid()
        with mock.patch("os.getsid", autospec=True) as mock_get_sid:
            mock_get_sid.return_value = pid + 1
            set_new_process_group()
            assert mock_set_pid.call_count == 1

    @mock.patch("os.setpgid")
    def test_session_leader(self, mock_set_pid):
        pid = os.getpid()
        with mock.patch("os.getsid", autospec=True) as mock_get_sid:
            mock_get_sid.return_value = pid
            set_new_process_group()
            assert mock_set_pid.call_count == 0
