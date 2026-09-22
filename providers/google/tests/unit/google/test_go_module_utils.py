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

import subprocess
from unittest import mock

import pytest

from airflow.providers.google.go_module_utils import _execute_in_subprocess


class TestExecuteInSubprocess:
    @mock.patch("airflow.providers.google.go_module_utils.subprocess.Popen")
    def test_success(self, mock_popen):
        process = mock_popen.return_value.__enter__.return_value
        process.stdout = None
        process.wait.return_value = 0

        _execute_in_subprocess(["go", "version"], cwd="/tmp")

        mock_popen.assert_called_once_with(
            ["go", "version"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=0,
            close_fds=True,
            cwd="/tmp",
            env=None,
        )
        process.wait.assert_called_once_with()

    @mock.patch("airflow.providers.google.go_module_utils.subprocess.Popen")
    def test_nonzero_exit_code_raises(self, mock_popen):
        process = mock_popen.return_value.__enter__.return_value
        process.stdout = None
        process.wait.return_value = 1

        with pytest.raises(subprocess.CalledProcessError) as exc_info:
            _execute_in_subprocess(["go", "version"])

        assert exc_info.value.returncode == 1
        assert exc_info.value.cmd == ["go", "version"]

    @mock.patch("airflow.providers.google.go_module_utils.subprocess.Popen")
    def test_logs_subprocess_output(self, mock_popen, caplog):
        process = mock_popen.return_value.__enter__.return_value
        process.stdout.readline.side_effect = [
            b"first line\n",
            b"second line\n",
            b"",
        ]
        process.wait.return_value = 0

        _execute_in_subprocess(["go", "version"])

        assert "first line" in caplog.text
        assert "second line" in caplog.text
