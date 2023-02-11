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

from datetime import datetime
from unittest.mock import patch

import pytest
from paramiko.sftp import SFTP_FAILURE, SFTP_NO_SUCH_FILE
from pendulum import datetime as pendulum_datetime, timezone

from airflow.providers.sftp.sensors.sftp import SFTPSensor


class TestSFTPSensor:
    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_present(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_absent(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.side_effect = OSError(SFTP_NO_SUCH_FILE, "File missing")
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        assert not output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_sftp_failure(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.side_effect = OSError(SFTP_FAILURE, "SFTP failure")
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        context = {"ds": "1970-01-01"}
        with pytest.raises(OSError):
            sftp_sensor.poke(context)
            sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")

    def test_hook_not_created_during_init(self):
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        assert sftp_sensor.hook is None

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_new_enough(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        tz = timezone("America/Toronto")
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/1970-01-01.txt",
            newer_than=tz.convert(datetime(1960, 1, 2)),
        )
        context = {"ds": "1970-01-00"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_not_new_enough(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        tz = timezone("Europe/Paris")
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/1970-01-01.txt",
            newer_than=tz.convert(pendulum_datetime(2020, 1, 2)),
        )
        context = {"ds": "1970-01-00"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        assert not output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_naive_datetime(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_sensor = SFTPSensor(
            task_id="unit_test", path="/path/to/file/1970-01-01.txt", newer_than=datetime(2020, 1, 2)
        )
        context = {"ds": "1970-01-00"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        assert not output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_present_with_pattern(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_hook_mock.return_value.get_file_by_pattern.return_value = "text_file.txt"
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/", file_pattern="*.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/text_file.txt")
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_not_present_with_pattern(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_hook_mock.return_value.get_file_by_pattern.return_value = ""
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/", file_pattern="*.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        assert not output
