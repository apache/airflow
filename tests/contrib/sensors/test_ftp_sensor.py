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
from unittest.mock import MagicMock

from ftplib import error_perm

from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.sensors.ftp_sensor import FTPSensor


class TestFTPSensor(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self._create_hook_orig = FTPSensor._create_hook
        self.hook_mock = MagicMock(spec=FTPHook)

        def _create_hook_mock(sensor):
            mock = MagicMock()
            mock.__enter__ = lambda x: self.hook_mock

            return mock

        FTPSensor._create_hook = _create_hook_mock

    def tearDown(self):
        FTPSensor._create_hook = self._create_hook_orig
        super().tearDown()

    def test_poke(self):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task")

        self.hook_mock.get_mod_time.side_effect = \
            [error_perm("550: Can't check for file existence"),
                error_perm("550: Directory or file does not exist"),
                error_perm("550 - Directory or file does not exist"),
                None]

        self.assertFalse(op.poke(None))
        self.assertFalse(op.poke(None))
        self.assertFalse(op.poke(None))
        self.assertTrue(op.poke(None))

    def test_poke_fails_due_error(self):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task")

        self.hook_mock.get_mod_time.side_effect = \
            error_perm("530: Login authentication failed")

        with self.assertRaises(error_perm) as context:
            op.execute(None)

        self.assertTrue("530" in str(context.exception))

    def test_poke_fail_on_transient_error(self):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task")

        self.hook_mock.get_mod_time.side_effect = \
            error_perm("434: Host unavailable")

        with self.assertRaises(error_perm) as context:
            op.execute(None)

        self.assertTrue("434" in str(context.exception))

    def test_poke_ignore_transient_error(self):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task", fail_on_transient_errors=False)

        self.hook_mock.get_mod_time.side_effect = \
            [error_perm("434: Host unavailable"), None]

        self.assertFalse(op.poke(None))
        self.assertTrue(op.poke(None))


if __name__ == '__main__':
    unittest.main()
