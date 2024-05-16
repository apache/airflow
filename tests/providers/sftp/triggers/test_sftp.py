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
import time
from unittest import mock

import pytest
from asyncssh.sftp import SFTPAttrs, SFTPName

from airflow.exceptions import AirflowException
from airflow.providers.sftp.triggers.sftp import SFTPTrigger
from airflow.triggers.base import TriggerEvent


class TestSFTPTrigger:
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
