# -*- coding: utf-8 -*-
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
from airflow.hooks.pig_hook import PigCliHook
from tests.compat import mock


class TestPigCliHook(unittest.TestCase):

    def setUp(self):
        super().setUp()

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
        self.extra_dejson.get.assert_called_with('pig_properties', '')

    @mock.patch('subprocess.Popen')
    def test_run_cli_success(self, popen_mock):
        proc_mock = mock.MagicMock()
        proc_mock.returncode = 0
        proc_mock.stdout.readline.return_value = b''
        popen_mock.return_value = proc_mock

        hook = self.pig_hook()
        stdout = hook.run_cli("")

        self.assertEqual(stdout, "")

    @mock.patch('subprocess.Popen')
    def test_run_cli_fail(self, popen_mock):
        proc_mock = mock.MagicMock()
        proc_mock.returncode = 1
        proc_mock.stdout.readline.return_value = b''
        popen_mock.return_value = proc_mock

        hook = self.pig_hook()

        from airflow.exceptions import AirflowException
        self.assertRaises(AirflowException, hook.run_cli, "")

    @mock.patch('subprocess.Popen')
    def test_run_cli_with_properties(self, popen_mock):
        test_properties = "one two"

        proc_mock = mock.MagicMock()
        proc_mock.returncode = 0
        proc_mock.stdout.readline.return_value = b''
        popen_mock.return_value = proc_mock

        hook = self.pig_hook()
        hook.pig_properties = test_properties

        stdout = hook.run_cli("")
        self.assertEqual(stdout, "")

        popen_first_arg = popen_mock.call_args[0][0]
        for pig_prop in test_properties.split():
            self.assertIn(pig_prop, popen_first_arg)

    @mock.patch('subprocess.Popen')
    def test_run_cli_verbose(self, popen_mock):
        test_stdout_lines = [b"one", b"two", b""]
        test_stdout_strings = [s.decode('utf-8') for s in test_stdout_lines]

        proc_mock = mock.MagicMock()
        proc_mock.returncode = 0
        proc_mock.stdout.readline = mock.Mock(side_effect=test_stdout_lines)
        popen_mock.return_value = proc_mock

        hook = self.pig_hook()
        stdout = hook.run_cli("", verbose=True)

        self.assertEqual(stdout, "".join(test_stdout_strings))

    def test_kill_no_sp(self):
        sp_mock = mock.Mock()
        hook = self.pig_hook()
        hook.sp = sp_mock

        hook.kill()
        self.assertFalse(sp_mock.kill.called)

    def test_kill_sp_done(self):
        sp_mock = mock.Mock()
        sp_mock.poll.return_value = 0

        hook = self.pig_hook()
        hook.sp = sp_mock

        hook.kill()
        self.assertFalse(sp_mock.kill.called)

    def test_kill(self):
        sp_mock = mock.Mock()
        sp_mock.poll.return_value = None

        hook = self.pig_hook()
        hook.sp = sp_mock

        hook.kill()
        self.assertTrue(sp_mock.kill.called)
