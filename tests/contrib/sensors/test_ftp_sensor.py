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
from ftplib import error_perm
from unittest import mock

from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.sensors.ftp_sensor import FTPSensor


class TestFTPSensor(unittest.TestCase):

    @mock.patch('airflow.contrib.sensors.ftp_sensor.FTPHook', spec=FTPHook)
    def test_poke(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task")

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = [
            error_perm("550: Can't check for file existence"),
            error_perm("550: Directory or file does not exist"),
            error_perm("550 - Directory or file does not exist"), None
        ]

        self.assertFalse(op.poke(None))
        self.assertFalse(op.poke(None))
        self.assertFalse(op.poke(None))
        self.assertTrue(op.poke(None))

    @mock.patch('airflow.contrib.sensors.ftp_sensor.FTPHook', spec=FTPHook)
    def test_poke_fails_due_error(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task")

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = \
            error_perm("530: Login authentication failed")

        with self.assertRaises(error_perm) as context:
            op.execute(None)

        self.assertTrue("530" in str(context.exception))

    @mock.patch('airflow.contrib.sensors.ftp_sensor.FTPHook', spec=FTPHook)
    def test_poke_fail_on_transient_error(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task")

        mock_hook.return_value.__enter__.return_value\
            .get_mod_time.side_effect = error_perm("434: Host unavailable")

        with self.assertRaises(error_perm) as context:
            op.execute(None)

        self.assertTrue("434" in str(context.exception))

    @mock.patch('airflow.contrib.sensors.ftp_sensor.FTPHook', spec=FTPHook)
    def test_poke_ignore_transient_error(self, mock_hook):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task", fail_on_transient_errors=False)

        mock_hook.return_value.__enter__.return_value.get_mod_time.side_effect = [
            error_perm("434: Host unavailable"), None
        ]

        self.assertFalse(op.poke(None))
        self.assertTrue(op.poke(None))


if __name__ == '__main__':
    unittest.main()
