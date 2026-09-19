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

import importlib
import json
import os
import subprocess
import sys
import time
import warnings
from datetime import datetime, timezone as stdlib_timezone
from unittest import mock
from unittest.mock import Mock, patch

import pytest
from paramiko.sftp import SFTP_FAILURE
from pendulum import datetime as pendulum_datetime, timezone

from airflow.providers.common.compat.sdk import AirflowException, PokeReturnValue
from airflow.providers.sftp.sensors.sftp import SFTPSensor

WARNING_CATEGORY: type[Warning]
try:
    from airflow.utils.deprecation_tools import DeprecatedImportWarning
except ImportError:
    WARNING_CATEGORY = DeprecationWarning
else:
    WARNING_CATEGORY = DeprecatedImportWarning


# Runs in a fresh process (see ``test_newer_than_is_timezone_independent``) so that ``TZ`` and
# ``AIRFLOW__CORE__DEFAULT_TIMEZONE`` are applied before Airflow reads them. It exercises the real
# ``SFTPHook.get_mod_time`` conversion through ``SFTPSensor`` with and without a file pattern, with the
# SFTP server mocked out. ``MTIME`` is 2024-01-01 12:00:00 UTC.
_SENSOR_TIMEZONE_PROBE = r"""
import datetime
import json
import stat
import time
from unittest import mock

time.tzset()

import paramiko

from airflow.providers.common.compat.sdk import Connection, timezone
from airflow.providers.sftp.hooks.sftp import SFTPHook
from airflow.providers.sftp.sensors.sftp import SFTPSensor

MTIME = 1704110400


def main():
    attrs = paramiko.SFTPAttributes()
    attrs.st_mtime = MTIME
    attrs.st_mode = stat.S_IFREG | 0o644
    attrs.filename = "file.txt"
    client = mock.MagicMock(spec=paramiko.SFTPClient)
    client.stat.return_value = attrs
    client.listdir_attr.return_value = [attrs]

    thresholds = {
        "older": datetime.datetime(2024, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
        "equal": datetime.datetime(2024, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
        "newer": datetime.datetime(2024, 1, 1, 14, 0, tzinfo=datetime.timezone.utc),
    }

    with mock.patch.object(SFTPHook, "get_managed_conn", autospec=True) as managed_conn:
        with mock.patch.object(SFTPHook, "get_connection", spec=True) as get_connection:
            get_connection.return_value = Connection(
                conn_id="sftp_default", conn_type="sftp", host="mock.invalid"
            )
            managed_conn.return_value.__enter__.return_value = client
            hook = SFTPHook()
            result = {
                "process_hour": time.localtime(MTIME).tm_hour,
                "default_timezone": str(timezone.datetime(2024, 1, 1).tzinfo),
                "mod_time": hook.get_mod_time("/files/file.txt"),
                "single_file": {},
                "pattern": {},
            }
            for name, threshold in thresholds.items():
                for file_pattern, key in (("", "single_file"), ("*.txt", "pattern")):
                    sensor = SFTPSensor(
                        task_id="probe",
                        path="/files" if file_pattern else "/files/file.txt",
                        file_pattern=file_pattern,
                        newer_than=threshold,
                    )
                    sensor.hook = hook
                    result[key][name] = bool(sensor._get_files())
    print(json.dumps(result))


if __name__ == "__main__":
    main()
"""


# Ignore missing args provided by default_args
# mypy: disable-error-code="arg-type"


class TestSFTPSensor:
    def test_no_timezone_deprecated_import_warning_on_module_reload(self):
        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            import airflow.providers.sftp.sensors.sftp as sftp_sensor_module

            importlib.reload(sftp_sensor_module)

        assert not any(
            issubclass(warning.category, WARNING_CATEGORY)
            and "airflow.utils.timezone" in str(warning.message)
            for warning in captured_warnings
        )

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_present(self, sftp_hook_mock):
        sftp_hook_mock.return_value.isfile.return_value = True
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.isfile.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_irregular(self, sftp_hook_mock):
        # This mocks the behavior of SFTPHook.isfile when an OSError is raised in that method, resulting in
        # False being returned
        sftp_hook_mock.return_value.isfile.return_value = False
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.isfile.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert not output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_absent(self, sftp_hook_mock):
        # This is the same implementation above, however, it's simulating instead the absence of a file
        sftp_hook_mock.return_value.isfile.return_value = False
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.isfile.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert not output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_sftp_failure(self, sftp_hook_mock):
        sftp_hook_mock.return_value.isfile.side_effect = OSError(SFTP_FAILURE, "SFTP failure")

        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        context = {"ds": "1970-01-01"}
        with pytest.raises(AirflowException):
            sftp_sensor.poke(context)

    def test_hook_not_created_during_init(self):
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        assert sftp_sensor.hook is None

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_new_enough(self, sftp_hook_mock):
        sftp_hook_mock.return_value.isfile.return_value = True
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        tz = timezone("America/Toronto")
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/1970-01-01.txt",
            newer_than=tz.convert(datetime(1960, 1, 2)),
        )
        context = {"ds": "1970-01-00"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.isfile.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_only_creating_one_connection_with_unmanaged_conn(self, sftp_hook_mock):
        sftp_hook_mock.return_value.isfile.return_value = True
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_sensor = SFTPSensor(task_id="test", path="path/to/whatever/test", use_managed_conn=False)
        sftp_sensor.poke({})
        assert sftp_hook_mock.return_value.get_managed_conn.called is True

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_not_new_enough(self, sftp_hook_mock):
        sftp_hook_mock.return_value.isfile.return_value = True
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        tz = timezone("Europe/Paris")
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/1970-01-01.txt",
            newer_than=tz.convert(pendulum_datetime(2020, 1, 2)),
        )
        context = {"ds": "1970-01-00"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.isfile.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert not output

    @pytest.mark.parametrize(
        "newer_than",
        (
            datetime(2020, 1, 2),
            datetime(2020, 1, 2, tzinfo=stdlib_timezone.utc),
            "2020-01-02",
            "2020-01-02 00:00:00+00:00",
            "2020-01-02 00:00:00.001+00:00",
            "2020-01-02T00:00:00+00:00",
            "2020-01-02T00:00:00Z",
            "2020-01-02T00:00:00+04:00",
            "2020-01-02T00:00:00.000001+04:00",
        ),
    )
    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_multiple_datetime_format_in_newer_than(self, sftp_hook_mock, newer_than):
        sftp_hook_mock.return_value.isfile.return_value = True
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_sensor = SFTPSensor(
            task_id="unit_test", path="/path/to/file/1970-01-01.txt", newer_than=newer_than
        )
        context = {"ds": "1970-01-00"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.isfile.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert not output

    @pytest.mark.skipif(not hasattr(time, "tzset"), reason="requires time.tzset (POSIX)")
    @pytest.mark.parametrize(
        ("process_timezone", "airflow_timezone"),
        [
            ("UTC", "America/New_York"),
            ("America/New_York", "UTC"),
            ("UTC", "UTC"),
        ],
    )
    def test_newer_than_is_timezone_independent(self, process_timezone, airflow_timezone, tmp_path):
        """The same mtime and threshold must decide identically whatever the process and Airflow timezones are."""
        result = subprocess.run(
            [sys.executable, "-c", _SENSOR_TIMEZONE_PROBE],
            env={
                **os.environ,
                "TZ": process_timezone,
                "AIRFLOW__CORE__DEFAULT_TIMEZONE": airflow_timezone,
                "AIRFLOW_HOME": str(tmp_path),
                "_AIRFLOW_PROCESS_CONTEXT": "client",
            },
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        observed = json.loads(result.stdout.splitlines()[-1])
        assert observed["default_timezone"] == airflow_timezone
        assert observed["process_hour"] == (7 if process_timezone == "America/New_York" else 12)
        assert observed["mod_time"] == "20240101120000"
        expected = {"older": True, "equal": True, "newer": False}
        assert observed["single_file"] == expected
        assert observed["pattern"] == expected

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_present_with_pattern(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = ["text_file.txt"]
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/", file_pattern="*.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_not_present_with_pattern(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = []
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/", file_pattern="*.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert not output

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_multiple_files_present_with_pattern(self, sftp_hook_mock):
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = [
            "text_file.txt",
            "another_text_file.txt",
        ]
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/", file_pattern="*.txt")
        context = {"ds": "1970-01-01"}
        output = sftp_sensor.poke(context)
        sftp_hook_mock.return_value.close_conn.assert_not_called()
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
        assert (
            [
                mock.call("/path/to/file/text_file1.txt"),
                mock.call("/path/to/file/text_file2.txt"),
            ]
        ) in sftp_hook_mock.return_value.get_mod_time.mock_calls
        sftp_hook_mock.return_value.close_conn.assert_not_called()
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
        sftp_hook_mock.return_value.get_mod_time.mock_call(
            [
                mock.call("/path/to/file/text_file1.txt"),
                mock.call("/path/to/file/text_file2.txt"),
                mock.call("/path/to/file/text_file3.txt"),
            ]
        )
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert not output

    @pytest.mark.parametrize(
        ("op_args", "op_kwargs"),
        [
            pytest.param(("op_arg_1",), {"key": "value"}),
            pytest.param((), {}),
        ],
    )
    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_path_present_with_callback(self, sftp_hook_mock, op_args, op_kwargs):
        sftp_hook_mock.return_value.isfile.return_value = True
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

        sftp_hook_mock.return_value.isfile.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        sample_callable.assert_called_once_with(*op_args, **op_kwargs)
        assert isinstance(output, PokeReturnValue)
        assert output.is_done
        assert output.xcom_value == {
            "files_found": ["/path/to/file/1970-01-01.txt"],
            "decorator_return_value": ["sample_return"],
        }

    @pytest.mark.parametrize(
        ("op_args", "op_kwargs"),
        [
            pytest.param(("op_arg_1",), {"key": "value"}),
            pytest.param((), {}),
        ],
    )
    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_file_pattern_present_with_callback(self, sftp_hook_mock, op_args, op_kwargs):
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
        sftp_hook_mock.return_value.close_conn.assert_not_called()
        assert isinstance(output, PokeReturnValue)
        assert output.is_done
        assert output.xcom_value == {
            "files_found": ["/path/to/file/text_file.txt", "/path/to/file/another_text_file.txt"],
            "decorator_return_value": ["sample_return"],
        }

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_mod_time_called_when_newer_than_set(self, sftp_hook_mock):
        sftp_hook_mock.return_value.isfile.return_value = True
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        tz = timezone("America/Toronto")
        sftp_sensor = SFTPSensor(
            task_id="unit_test",
            path="/path/to/file/1970-01-01.txt",
            newer_than=tz.convert(datetime(1960, 1, 2)),
        )
        context = {"ds": "1970-01-01"}
        sftp_sensor.poke(context)
        sftp_hook_mock.return_value.isfile.assert_called_once_with("/path/to/file/1970-01-01.txt")
        sftp_hook_mock.return_value.get_mod_time.assert_called_once_with("/path/to/file/1970-01-01.txt")

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_mod_time_not_called_when_newer_than_not_set(self, sftp_hook_mock):
        sftp_hook_mock.return_value.isfile.return_value = True
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        sftp_sensor = SFTPSensor(task_id="unit_test", path="/path/to/file/1970-01-01.txt")
        context = {"ds": "1970-01-01"}
        sftp_sensor.poke(context)
        sftp_hook_mock.return_value.get_mod_time.assert_not_called()
