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
from mock import MagicMock

from airflow.contrib.hooks.ftp_hook import FTPHook
from airflow.contrib.sensors.ftp_sensor import FTPSensor


class TestFTPSensor(unittest.TestCase):
    def setUp(self):
        super(TestFTPSensor, self).setUp()
        self._create_hook_orig = FTPSensor._create_hook
        self.hook_mock = MagicMock(spec=FTPHook)

        def _create_hook_mock(sensor):
            mock = MagicMock()
            mock.__enter__ = lambda x: self.hook_mock

            return mock

        FTPSensor._create_hook = _create_hook_mock

    def tearDown(self):
        FTPSensor._create_hook = self._create_hook_orig
        super(TestFTPSensor, self).tearDown()

    def test_poke(self):
        op = FTPSensor(path="foobar.json", ftp_conn_id="bob_ftp",
                       task_id="test_task")

        self.hook_mock.get_mod_time.side_effect = \
            [error_perm("550: Can't check for file existence"), None]

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


if __name__ == '__main__':
    unittest.main()
