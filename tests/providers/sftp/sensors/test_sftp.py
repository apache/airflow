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
from unittest import mock
from unittest.mock import Mock, call, patch

import pytest
from paramiko.sftp import SFTP_FAILURE, SFTP_NO_SUCH_FILE
from pendulum import datetime as pendulum_datetime, timezone

from airflow.exceptions import AirflowSkipException
from airflow.providers.sftp.sensors.sftp import SFTPSensor
from airflow.sensors.base import PokeReturnValue

# Ignore missing args provided by default_args
# mypy: disable-error-code="arg-type"


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

    @pytest.mark.parametrize(
        "soft_fail, expected_exception", ((False, OSError), (True, AirflowSkipException))
    )
    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_sftp_failure(self, sftp_hook_mock, soft_fail: bool, expected_exception):
        sftp_hook_mock.return_value.get_mod_time.side_effect = OSError(SFTP_FAILURE, "SFTP failure")
        sftp_sensor = SFTPSensor(
            task_id="unit_test", path="/path/to/file/1970-01-01.txt", soft_fail=soft_fail
        )
        context = {"ds": "1970-01-01"}
        with pytest.raises(expected_exception):
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
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = ["text_file.txt"]
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/", file_pattern="*.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/text_file.txt")
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_not_present_with_pattern(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = []
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/", file_pattern="*.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        assert not output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_multiple_files_present_with_pattern(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = [
            "text_file.txt",
            "another_text_file.txt",
        ]
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/", file_pattern="*.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        get_mod_time = sftp_hook_mock.return_value.get_mod_time
        expected_calls = [call("/path/to/file/text_file.txt"), call("/path/to/file/another_text_file.txt")]
        assert get_mod_time.mock_calls == expected_calls
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_multiple_files_present_with_pattern_and_newer_than(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = [
            "text_file1.txt",
            "text_file2.txt",
            "text_file3.txt",
        ]
        sftp_hook_mock.return_value.get_mod_time.side_effect = [
            "19500101000000",
            "19700101000000",
            "19800101000000",
        ]
        tz = timezone("America/Toronto")
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/",
            file_pattern="*.txt",
            newer_than=tz.convert(datetime(1960, 1, 2)),
        )
        context = {"ds": "1970-01-00"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_has_calls(
            [mock.call("/path/to/file/text_file1.txt"), mock.call("/path/to/file/text_file2.txt")]
        )
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_multiple_old_files_present_with_pattern_and_newer_than(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = [
            "text_file1.txt",
            "text_file2.txt",
            "text_file3.txt",
        ]
        sftp_hook_mock.return_value.get_mod_time.side_effect = [
            "19500101000000",
            "19510101000000",
            "19520101000000",
        ]
        tz = timezone("America/Toronto")
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/",
            file_pattern="*.txt",
            newer_than=tz.convert(datetime(1960, 1, 2)),
        )
        context = {"ds": "1970-01-00"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_has_calls(
            [
                mock.call("/path/to/file/text_file1.txt"),
                mock.call("/path/to/file/text_file2.txt"),
                mock.call("/path/to/file/text_file3.txt"),
            ]
        )
        assert not output

    @pytest.mark.parametrize(
        "op_args, op_kwargs,",
        [
            pytest.param(("op_arg_1",), {"key": "value"}),
            pytest.param((), {}),
        ],
    )
    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_path_present_with_callback(self, sftp_hook_mock, op_args, op_kwargs):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sample_callable = Mock()
        sample_callable.return_value = ["sample_return"]
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/1970-01-01.txt",
            python_callable=sample_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
        )
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)

        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sample_callable.assert_called_once_with(*op_args, **op_kwargs)
        assert isinstance(output, PokeReturnValue)
        assert output.is_done
        assert output.xcom_value == {
            "files_found": ["/path/to/file/1970-01-01.txt"],
            "decorator_return_value": ["sample_return"],
        }

    @pytest.mark.parametrize(
        "op_args, op_kwargs,",
        [
            pytest.param(("op_arg_1",), {"key": "value"}),
            pytest.param((), {}),
        ],
    )
    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_pattern_present_with_callback(self, sftp_hook_mock, op_args, op_kwargs):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sample_callable = Mock()
        sample_callable.return_value = ["sample_return"]
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = [
            "text_file.txt",
            "another_text_file.txt",
        ]
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/",
            file_pattern=".txt",
            python_callable=sample_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
        )
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)

        sample_callable.assert_called_once_with(*op_args, **op_kwargs)
        assert isinstance(output, PokeReturnValue)
        assert output.is_done
        assert output.xcom_value == {
            "files_found": ["/path/to/file/text_file.txt", "/path/to/file/another_text_file.txt"],
            "decorator_return_value": ["sample_return"],
        }
