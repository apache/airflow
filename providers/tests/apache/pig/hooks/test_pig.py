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

from unittest import mock

import pytest

from airflow.providers.apache.pig.hooks.pig import PigCliHook


class TestPigCliHook:
    def setup_method(self):
        self.extra_dejson = mock.MagicMock()
        self.extra_dejson.get.return_value = None
        self.conn = mock.MagicMock()
        self.conn.extra_dejson = self.extra_dejson
        conn = self.conn

        class SubPigCliHook(PigCliHook):
            def get_connection(self, unused_id):
                return conn

        self.pig_hook = SubPigCliHook

    def test_init(self):
        self.pig_hook()

    @mock.patch("subprocess.Popen")
    def test_run_cli_success(self, popen_mock):
        proc_mock = mock.MagicMock()
        proc_mock.returncode = 0
        proc_mock.stdout.readline.return_value = b""
        popen_mock.return_value = proc_mock

        hook = self.pig_hook()
        stdout = hook.run_cli("")

        assert stdout == ""

    @mock.patch("subprocess.Popen")
    def test_run_cli_fail(self, popen_mock):
        proc_mock = mock.MagicMock()
        proc_mock.returncode = 1
        proc_mock.stdout.readline.return_value = b""
        popen_mock.return_value = proc_mock

        hook = self.pig_hook()

        from airflow.exceptions import AirflowException

        with pytest.raises(AirflowException):
            hook.run_cli("")

    @mock.patch("subprocess.Popen")
    def test_run_cli_with_properties(self, popen_mock):
        test_properties = "one two"

        proc_mock = mock.MagicMock()
        proc_mock.returncode = 0
        proc_mock.stdout.readline.return_value = b""
        popen_mock.return_value = proc_mock

        hook = self.pig_hook(pig_properties=["one", "two"])

        stdout = hook.run_cli("")
        assert stdout == ""

        popen_first_arg = popen_mock.call_args.args[0]
        for pig_prop in test_properties.split():
            assert pig_prop in popen_first_arg

    @pytest.mark.db_test
    def test_runtime_exception_not_raised_by_default(self):
        PigCliHook()

    @mock.patch("airflow.providers.apache.pig.hooks.pig.PigCliHook.get_connection")
    def test_runtime_exception_when_properties_passed_by_connection(
        self, mock_get_connection
    ):
        mock_get_connection.return_value.extra_dejson = {
            "pig_properties": "one two three"
        }
        with pytest.raises(RuntimeError):
            PigCliHook()

    @mock.patch("subprocess.Popen")
    def test_run_cli_verbose(self, popen_mock):
        test_stdout_lines = [b"one", b"two", b""]
        test_stdout_strings = [s.decode("utf-8") for s in test_stdout_lines]

        proc_mock = mock.MagicMock()
        proc_mock.returncode = 0
        proc_mock.stdout.readline = mock.Mock(side_effect=test_stdout_lines)
        popen_mock.return_value = proc_mock

        hook = self.pig_hook()
        stdout = hook.run_cli("", verbose=True)

        assert stdout == "".join(test_stdout_strings)

    def test_kill_no_sp(self):
        sp_mock = mock.Mock()
        hook = self.pig_hook()
        hook.sub_process = sp_mock

        hook.kill()
        assert not sp_mock.kill.called

    def test_kill_sp_done(self):
        sp_mock = mock.Mock()
        sp_mock.poll.return_value = 0

        hook = self.pig_hook()
        hook.sub_process = sp_mock

        hook.kill()
        assert not sp_mock.kill.called

    def test_kill(self):
        sp_mock = mock.Mock()
        sp_mock.poll.return_value = None

        hook = self.pig_hook()
        hook.sub_process = sp_mock

        hook.kill()
        assert sp_mock.kill.called
