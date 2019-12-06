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
from mock import patch
from paramiko import SFTP_NO_SUCH_FILE, SFTP_FAILURE
from airflow.contrib.sensors.sftp_sensor import SFTPSensor


class SFTPSensorTest(unittest.TestCase):
    @patch('airflow.contrib.sensors.sftp_sensor.SFTPHook')
    def test_file_present(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = '19700101000000'
        sftp_sensor = SFTPSensor(
            task_id='unit_test',
            path='/path/to/file/1970-01-01.txt')
        context = {
            'ds': '1970-01-01'
        }
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_with(
            '/path/to/file/1970-01-01.txt')
        self.assertTrue(output)

    @patch('airflow.contrib.sensors.sftp_sensor.SFTPHook')
    def test_file_absent(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.side_effect = IOError(
            SFTP_NO_SUCH_FILE, 'File missing')
        sftp_sensor = SFTPSensor(
            task_id='unit_test',
            path='/path/to/file/1970-01-01.txt')
        context = {
            'ds': '1970-01-01'
        }
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_with(
            '/path/to/file/1970-01-01.txt')
        self.assertFalse(output)

    @patch('airflow.contrib.sensors.sftp_sensor.SFTPHook')
    def test_sftp_failure(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.side_effect = IOError(
            SFTP_FAILURE, 'SFTP failure')
        sftp_sensor = SFTPSensor(
            task_id='unit_test',
            path='/path/to/file/1970-01-01.txt')
        context = {
            'ds': '1970-01-01'
        }
        with self.assertRaises(IOError):
            sftp_sensor.poke(context)
            sftp_hook_mock.return_value.get_mod_time.assert_called_with(
                '/path/to/file/1970-01-01.txt')

    def test_hook_not_created_during_init(self):
        sftp_sensor = SFTPSensor(
            task_id='unit_test',
            path='/path/to/file/1970-01-01.txt')
        self.assertIsNone(sftp_sensor.hook)
