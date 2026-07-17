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
from unittest.mock import patch

import pytest

from airflow.providers.edge3.cli.signalling import write_pid_to_pidfile


def test_write_pid_to_pidfile_success(caplog, tmp_path):
    with caplog.at_level(logging.DEBUG):
        pid_file_path = tmp_path / "file.pid"
        write_pid_to_pidfile(pid_file_path)
        assert pid_file_path.exists()
        assert "An existing PID file has been found" not in caplog.text


def test_write_pid_to_pidfile_called_twice(tmp_path):
    pid_file_path = tmp_path / "file.pid"
    write_pid_to_pidfile(pid_file_path)
    with pytest.raises(SystemExit, match=r"A PID file has already been written"):
        write_pid_to_pidfile(pid_file_path)
    assert pid_file_path.exists()


def test_write_pid_to_pidfile_created_by_other_instance(tmp_path):
    # write a PID file with the PID of this process
    pid_file_path = tmp_path / "file.pid"
    write_pid_to_pidfile(pid_file_path)
    # write a PID file, but set the current PID to 0
    with patch("os.getpid", return_value=0):
        with pytest.raises(SystemExit, match=r"contains the PID of another running process"):
            write_pid_to_pidfile(pid_file_path)


def test_write_pid_to_pidfile_created_by_crashed_instance(tmp_path):
    # write a PID file with process ID 0
    with patch("os.getpid", return_value=0):
        pid_file_path = tmp_path / "file.pid"
        write_pid_to_pidfile(pid_file_path)
        assert pid_file_path.read_text().strip() == "0"
    # write a PID file with the current process ID, call should not raise an exception
    write_pid_to_pidfile(pid_file_path)
    assert str(os.getpid()) == pid_file_path.read_text().strip()
