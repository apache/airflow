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
import textwrap
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


class TestSFTPTrigger:
    @pytest.mark.parametrize(
        ("process_timezone", "airflow_timezone", "mtime", "newer_than", "expected"),
        [
            ("UTC", "America/New_York", 1704110400, "2024-01-01T14:00:00+00:00", False),
            ("America/New_York", "UTC", 1704110400, "2024-01-01T10:00:00+00:00", True),
            ("UTC", "UTC", 1704110400, "2024-01-01T10:00:00+00:00", True),
            ("UTC", "UTC", 1704110400, "2024-01-01T14:00:00+00:00", False),
            ("UTC", "America/New_York", 1704110400, "2024-01-01T12:00:00+00:00", True),
            ("America/New_York", "UTC", 1704110400, "2024-01-01T17:45:00+05:45", True),
            ("UTC", "Asia/Kathmandu", 1704110400, "2024-01-01T10:00:00+00:00", True),
            ("UTC", "America/New_York", 1704110400.75, "2024-01-01T12:00:00.500000+00:00", False),
            ("UTC", "America/New_York", None, "2024-01-01T10:00:00+00:00", False),
            ("UTC", "America/New_York", None, None, True),
            ("America/New_York", "UTC", 1704110400, None, True),
        ],
    )
    def test_file_pattern_newer_than_timezones(
        self, process_timezone, airflow_timezone, mtime, newer_than, expected
    ):
        # Initialize Airflow normally and isolate the process-global timezone settings.
        code = textwrap.dedent(
            """
            import asyncio
            import datetime
            import json
            import sys
            import time
            from unittest import mock

            time.tzset()

            from asyncssh.sftp import SFTPAttrs, SFTPName
            from airflow.providers.common.compat.sdk import timezone
            from airflow.providers.sftp.triggers.sftp import SFTPTrigger

            mtime, threshold = json.loads(sys.argv[1])
            newer_than = datetime.datetime.fromisoformat(threshold) if threshold else None

            @mock.patch("airflow.providers.sftp.triggers.sftp.asyncio.sleep", autospec=True,
                        side_effect=asyncio.CancelledError)
            @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.get_files_and_attrs_by_pattern",
                        autospec=True)
            async def run(get_files, sleep):
                get_files.return_value = [SFTPName("file.txt", attrs=SFTPAttrs(mtime=mtime))]
                trigger = SFTPTrigger(path="/files", file_pattern="*.txt", newer_than=newer_than)
                generator = trigger.run()
                try:
                    try:
                        event = await anext(generator)
                    except asyncio.CancelledError:
                        event = None
                    get_files.assert_awaited_once()
                    if event is None:
                        sleep.assert_awaited_once_with(trigger.poke_interval)
                    return event.payload if event else None
                finally:
                    await generator.aclose()

            print(json.dumps({
                "event": asyncio.run(run()),
                "process_hour": time.localtime(1704110400).tm_hour,
                "default_timezone": str(timezone.datetime(2024, 1, 1).tzinfo),
            }))
            """
        )
        result = subprocess.run(
            [sys.executable, "-c", code, json.dumps([mtime, newer_than])],
            env={**os.environ, "TZ": process_timezone, "AIRFLOW__CORE__DEFAULT_TIMEZONE": airflow_timezone},
            capture_output=True,
            text=True,
            check=False,
            timeout=60,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        observed = json.loads(result.stdout.splitlines()[-1])
        assert observed["default_timezone"] == airflow_timezone
        assert observed["process_hour"] == (7 if process_timezone == "America/New_York" else 12)
        expected_event = (
            {"status": "success", "message": "Sensed 1 files: ['file.txt']"} if expected else None
        )
        assert observed["event"] == expected_event, observed

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
