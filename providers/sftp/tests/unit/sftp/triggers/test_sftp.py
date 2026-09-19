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

import asyncio
import datetime
import importlib
import json
import os
import subprocess
import sys
import time
import warnings
from unittest import mock

import pytest
from asyncssh.sftp import SFTPAttrs, SFTPName

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.sftp.triggers.sftp import SFTPTrigger
from airflow.triggers.base import TriggerEvent

WARNING_CATEGORY: type[Warning]
try:
    from airflow.utils.deprecation_tools import DeprecatedImportWarning
except ImportError:
    WARNING_CATEGORY = DeprecationWarning
else:
    WARNING_CATEGORY = DeprecatedImportWarning


# Runs in a fresh process (see ``test_newer_than_is_timezone_independent``) so that ``TZ`` and
# ``AIRFLOW__CORE__DEFAULT_TIMEZONE`` are applied before Airflow reads them. It exercises the real
# ``SFTPHookAsync.get_mod_time`` conversion through both ``SFTPTrigger`` branches with the SFTP server
# mocked out. ``MTIME`` is 2024-01-01 12:00:00 UTC.
_TRIGGER_TIMEZONE_PROBE = r"""
import asyncio
import datetime
import json
import time
from unittest import mock

time.tzset()

import asyncssh

from airflow.providers.common.compat.sdk import timezone
from airflow.providers.sftp.hooks.sftp import SFTPHookAsync
from airflow.providers.sftp.triggers.sftp import SFTPTrigger

MTIME = 1704110400


def make_conn():
    client = mock.AsyncMock(spec=asyncssh.SFTPClient)
    client.stat.return_value = asyncssh.SFTPAttrs(mtime=MTIME)
    client.readdir.return_value = [
        asyncssh.sftp.SFTPName("file.txt", attrs=asyncssh.SFTPAttrs(mtime=MTIME))
    ]
    sftp_cm = mock.MagicMock()
    sftp_cm.__aenter__ = mock.AsyncMock(return_value=client)
    ssh = mock.MagicMock(spec=asyncssh.SSHClientConnection)
    ssh.__aenter__.return_value = ssh
    ssh.start_sftp_client.return_value = sftp_cm
    return ssh


async def run_trigger(threshold, file_pattern):
    with mock.patch.object(SFTPHookAsync, "_get_conn", autospec=True) as get_conn:
        get_conn.return_value = make_conn()
        with mock.patch(
            "airflow.providers.sftp.triggers.sftp.asyncio.sleep",
            autospec=True,
            side_effect=asyncio.CancelledError,
        ):
            trigger = SFTPTrigger(
                path="/files" if file_pattern else "/files/file.txt",
                file_pattern=file_pattern,
                newer_than=threshold,
            )
            generator = trigger.run()
            try:
                try:
                    event = await anext(generator)
                except asyncio.CancelledError:
                    event = None
                return event is not None
            finally:
                await generator.aclose()


async def get_mod_time():
    with mock.patch.object(SFTPHookAsync, "_get_conn", autospec=True) as get_conn:
        get_conn.return_value = make_conn()
        return await SFTPHookAsync().get_mod_time("/files/file.txt")


def main():
    thresholds = {
        "older": datetime.datetime(2024, 1, 1, 10, 0, tzinfo=datetime.timezone.utc),
        "equal": datetime.datetime(2024, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
        "newer": datetime.datetime(2024, 1, 1, 14, 0, tzinfo=datetime.timezone.utc),
    }
    result = {
        "process_hour": time.localtime(MTIME).tm_hour,
        "default_timezone": str(timezone.datetime(2024, 1, 1).tzinfo),
        "mod_time": asyncio.run(get_mod_time()),
        "single_file": {},
        "pattern": {},
    }
    for name, threshold in thresholds.items():
        result["single_file"][name] = asyncio.run(run_trigger(threshold, ""))
        result["pattern"][name] = asyncio.run(run_trigger(threshold, "*.txt"))
    print(json.dumps(result))


if __name__ == "__main__":
    main()
"""


class TestSFTPTrigger:
    def test_no_timezone_deprecated_import_warning_on_module_reload(self):
        with warnings.catch_warnings(record=True) as captured_warnings:
            warnings.simplefilter("always")
            import airflow.providers.sftp.triggers.sftp as sftp_trigger_module

            importlib.reload(sftp_trigger_module)

        assert not any(
            issubclass(warning.category, WARNING_CATEGORY)
            and "airflow.utils.timezone" in str(warning.message)
            for warning in captured_warnings
        )

    def test_sftp_trigger_serialization(self):
        """
        Asserts that the SFTPTrigger correctly serializes its arguments and classpath.
        """
        trigger = SFTPTrigger(path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file")
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.sftp.triggers.sftp.SFTPTrigger"
        assert kwargs == {
            "path": "test/path/",
            "file_pattern": "my_test_file",
            "sftp_conn_id": "sftp_default",
            "newer_than": None,
            "poke_interval": 5.0,
        }

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
            [sys.executable, "-c", _TRIGGER_TIMEZONE_PROBE],
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

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "newer_than",
        ["19700101053001", None],
    )
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_files_and_attrs_by_pattern")
    async def test_sftp_trigger_run_trigger_success_state(self, mock_get_files_by_pattern, newer_than):
        """
        Assert that a TriggerEvent with a success status is yielded if a file
        matching the pattern is returned by the hook
        """
        mock_get_files_by_pattern.return_value = [
            SFTPName("some_file", attrs=SFTPAttrs(mtime=1684244898)),
            SFTPName("some_other_file"),
        ]

        trigger = SFTPTrigger(
            path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file", newer_than=newer_than
        )

        if newer_than:
            expected_event = {"status": "success", "message": "Sensed 1 files: ['some_file']"}
        else:
            expected_event = {
                "status": "success",
                "message": "Sensed 2 files: ['some_file', 'some_other_file']",
            }

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert TriggerEvent(expected_event) == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_mod_time")
    async def test_sftp_success_without_file_pattern(self, mock_mod_time):
        """
        Test SFTPTrigger run method by mocking the file path and without file pattern,
        assert that a TriggerEvent with a success status is yielded.
        """

        mock_mod_time.return_value = "19700101053001"

        trigger = SFTPTrigger(path="test/path/test.txt", sftp_conn_id="sftp_default", file_pattern="")

        expected_event = {"status": "success", "message": "Sensed file: test/path/test.txt"}

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert TriggerEvent(expected_event) == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_mod_time")
    async def test_sftp_success_with_newer_then(self, mock_mod_time):
        """
        Test SFTPTrigger run method by mocking the file path, without file pattern, and with newer then datetime
        assert that a TriggerEvent with a success status is yielded.
        """
        mock_mod_time.return_value = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        trigger = SFTPTrigger(
            path="test/path/test.txt", sftp_conn_id="sftp_default", file_pattern="", newer_than=yesterday
        )

        expected_event = {"status": "success", "message": "Sensed file: test/path/test.txt"}

        generator = trigger.run()
        actual_event = await generator.asend(None)

        assert TriggerEvent(expected_event) == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_files_and_attrs_by_pattern")
    async def test_sftp_trigger_run_trigger_defer_state(
        self,
        mock_get_files_by_pattern,
    ):
        """
        Assert that a the task does not complete,
        indicating that the task needs to be deferred
        """
        mock_get_files_by_pattern.return_value = [SFTPName("my_test_file.txt", attrs=SFTPAttrs(mtime=49129))]
        yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
        trigger = SFTPTrigger(
            path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file", newer_than=yesterday
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_mod_time")
    async def test_sftp_with_newer_then_date_greater(self, mock_mod_time):
        """
        Test the Trigger run method by passing full file path, without file pattern and along with newer then datetime.
        mock the datetime as greater then the last modified date and make the trigger task in running
        state and assert to success
        """
        today_time = time.time()
        mock_mod_time.return_value = datetime.date.fromtimestamp(today_time).strftime("%Y%m%d%H%M%S")
        newer_then_time = datetime.datetime.now() + datetime.timedelta(hours=1)
        trigger = SFTPTrigger(
            path="test/path/test.txt",
            sftp_conn_id="sftp_default",
            file_pattern="",
            newer_than=newer_then_time,
        )

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_files_and_attrs_by_pattern")
    async def test_sftp_trigger_run_trigger_failure_state(self, mock_get_files_by_pattern):
        """
        Mock the hook to raise other than an AirflowException and assert that a TriggerEvent with a failure status
        """
        mock_get_files_by_pattern.side_effect = Exception("An unexpected exception")

        trigger = SFTPTrigger(path="test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file")
        expected_event = {"status": "error", "message": "An unexpected exception"}
        generator = trigger.run()
        actual_event = await generator.asend(None)
        assert TriggerEvent(expected_event) == actual_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_files_and_attrs_by_pattern")
    async def test_sftp_trigger_run_airflow_exception(self, mock_get_files_by_pattern):
        """
        Assert that a the task does not complete if the hook raises an AirflowException,
        indicating that the task needs to be deferred
        """

        mock_get_files_by_pattern.side_effect = AirflowException("No files at path /test/path/ found...")

        trigger = SFTPTrigger(path="/test/path/", sftp_conn_id="sftp_default", file_pattern="my_test_file")

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        # TriggerEvent was not returned
        assert task.done() is False
        asyncio.get_event_loop().stop()
