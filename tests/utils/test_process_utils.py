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
import multiprocessing
import os
import signal
import subprocess
import time
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

pytestmark = pytest.mark.skip_if_database_isolation_mode


class TestReapProcessGroup:
    @staticmethod
    def _ignores_sigterm(child_pid, child_setup_done):
        def signal_handler(unused_signum, unused_frame):
            pass

        signal.signal(signal.SIGTERM, signal_handler)
        child_pid.value = os.getpid()
        child_setup_done.release()
        while True:
            time.sleep(1)

    @staticmethod
    def _parent_of_ignores_sigterm(parent_pid, child_pid, setup_done):
        def signal_handler(unused_signum, unused_frame):
            pass

        os.setsid()
        signal.signal(signal.SIGTERM, signal_handler)
        child_setup_done = multiprocessing.Semaphore(0)
        child = multiprocessing.Process(
            target=TestReapProcessGroup._ignores_sigterm, args=[child_pid, child_setup_done]
        )
        child.start()
        child_setup_done.acquire(timeout=5.0)
        parent_pid.value = os.getpid()
        setup_done.release()
        while True:
            time.sleep(1)

    def test_reap_process_group(self):
        """
        Spin up a process that can't be killed by SIGTERM and make sure
        it gets killed anyway.
        """
        parent_setup_done = multiprocessing.Semaphore(0)
        parent_pid = multiprocessing.Value("i", 0)
        child_pid = multiprocessing.Value("i", 0)
        args = [parent_pid, child_pid, parent_setup_done]
        parent = multiprocessing.Process(target=TestReapProcessGroup._parent_of_ignores_sigterm, args=args)
        try:
            parent.start()
            assert parent_setup_done.acquire(timeout=5.0)
            assert psutil.pid_exists(parent_pid.value)
            assert psutil.pid_exists(child_pid.value)

            process_utils.reap_process_group(parent_pid.value, logging.getLogger(), timeout=1)

            assert not psutil.pid_exists(parent_pid.value)
            assert not psutil.pid_exists(child_pid.value)
        finally:
            try:
                os.kill(parent_pid.value, signal.SIGKILL)  # terminate doesn't work here
                os.kill(child_pid.value, signal.SIGKILL)  # terminate doesn't work here
            except OSError:
                pass


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
class TestExecuteInSubProcess:
    def test_should_print_all_messages1(self, caplog):
        execute_in_subprocess(["bash", "-c", "echo CAT; echo KITTY;"])
        msgs = [record.getMessage() for record in caplog.records]
        assert ["Executing cmd: bash -c 'echo CAT; echo KITTY;'", "Output:", "CAT", "KITTY"] == msgs

    def test_should_print_all_messages_from_cwd(self, caplog, tmp_path):
        execute_in_subprocess(["bash", "-c", "echo CAT; pwd; echo KITTY;"], cwd=str(tmp_path))
        msgs = [record.getMessage() for record in caplog.records]
        assert [
            "Executing cmd: bash -c 'echo CAT; pwd; echo KITTY;'",
            "Output:",
            "CAT",
            str(tmp_path),
            "KITTY",
        ] == msgs

    def test_using_env_works(self, caplog):
        execute_in_subprocess(["bash", "-c", 'echo "My value is ${VALUE}"'], env=dict(VALUE="1"))
        assert "My value is 1" in caplog.text

    def test_should_raise_exception(self):
        with pytest.raises(CalledProcessError):
            process_utils.execute_in_subprocess(["bash", "-c", "exit 1"])

    def test_using_env_as_kwarg_works(self, caplog):
        execute_in_subprocess_with_kwargs(["bash", "-c", 'echo "My value is ${VALUE}"'], env=dict(VALUE="1"))
        assert "My value is 1" in caplog.text


def my_sleep_subprocess():
    sleep(100)


def my_sleep_subprocess_with_signals():
    signal.signal(signal.SIGINT, lambda signum, frame: None)
    signal.signal(signal.SIGTERM, lambda signum, frame: None)
    sleep(100)


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
class TestKillChildProcessesByPids:
    def test_should_kill_process(self):
        before_num_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")

        process = multiprocessing.Process(target=my_sleep_subprocess, args=())
        process.start()
        sleep(0)

        num_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")
        assert before_num_process + 1 == num_process

        process_utils.kill_child_processes_by_pids([process.pid])

        num_process = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().count("\n")
        assert before_num_process == num_process

    def test_should_force_kill_process(self, caplog):
        process = multiprocessing.Process(target=my_sleep_subprocess_with_signals, args=())
        process.start()
        sleep(0)

        all_processes = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().splitlines()
        assert str(process.pid) in (x.strip() for x in all_processes)

        with caplog.at_level(logging.INFO, logger=process_utils.log.name):
            caplog.clear()
            process_utils.kill_child_processes_by_pids([process.pid], timeout=0)
            assert f"Killing child PID: {process.pid}" in caplog.messages
        sleep(0)
        all_processes = subprocess.check_output(["ps", "-ax", "-o", "pid="]).decode().splitlines()
        assert str(process.pid) not in (x.strip() for x in all_processes)


class TestPatchEnviron:
    def test_should_update_variable_and_restore_state_when_exit(self):
        with mock.patch.dict("os.environ", {"TEST_NOT_EXISTS": "BEFORE", "TEST_EXISTS": "BEFORE"}):
            del os.environ["TEST_NOT_EXISTS"]

            assert "BEFORE" == os.environ["TEST_EXISTS"]
            assert "TEST_NOT_EXISTS" not in os.environ

            with process_utils.patch_environ({"TEST_NOT_EXISTS": "AFTER", "TEST_EXISTS": "AFTER"}):
                assert "AFTER" == os.environ["TEST_NOT_EXISTS"]
                assert "AFTER" == os.environ["TEST_EXISTS"]

            assert "BEFORE" == os.environ["TEST_EXISTS"]
            assert "TEST_NOT_EXISTS" not in os.environ

    def test_should_restore_state_when_exception(self):
        with mock.patch.dict("os.environ", {"TEST_NOT_EXISTS": "BEFORE", "TEST_EXISTS": "BEFORE"}):
            del os.environ["TEST_NOT_EXISTS"]

            assert "BEFORE" == os.environ["TEST_EXISTS"]
            assert "TEST_NOT_EXISTS" not in os.environ

            with suppress(AirflowException):
                with process_utils.patch_environ({"TEST_NOT_EXISTS": "AFTER", "TEST_EXISTS": "AFTER"}):
                    assert "AFTER" == os.environ["TEST_NOT_EXISTS"]
                    assert "AFTER" == os.environ["TEST_EXISTS"]
                    raise AirflowException("Unknown exception")

            assert "BEFORE" == os.environ["TEST_EXISTS"]
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
