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
import time
import warnings
from unittest import mock

import pytest
from asyncssh.sftp import SFTPAttrs, SFTPName

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.sftp.triggers.sftp import SFTPTransferTrigger, SFTPTrigger
from airflow.triggers.base import TriggerEvent

WARNING_CATEGORY: type[Warning]
try:
    from airflow.utils.deprecation_tools import DeprecatedImportWarning
except ImportError:
    WARNING_CATEGORY = DeprecationWarning
else:
    WARNING_CATEGORY = DeprecatedImportWarning


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


class TestSFTPTransferTrigger:
    def test_serialize(self):
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation="get",
            create_intermediate_dirs=True,
            remote_host="example.com",
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.sftp.triggers.sftp.SFTPTransferTrigger"
        assert kwargs == {
            "ssh_conn_id": "sftp_default",
            "local_filepath": "/tmp/local",
            "remote_filepath": "/tmp/remote",
            "operation": "get",
            "create_intermediate_dirs": True,
            "remote_host": "example.com",
        }

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.retrieve_file", new_callable=mock.AsyncMock)
    async def test_run_get_success(self, mock_retrieve):
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation="get",
        )
        event = await trigger.run().__anext__()
        mock_retrieve.assert_awaited_once_with("/tmp/remote", "/tmp/local")
        assert event == TriggerEvent({"status": "success", "local_filepath": "/tmp/local"})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.retrieve_file", new_callable=mock.AsyncMock)
    async def test_run_get_multiple_files(self, mock_retrieve):
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            local_filepath=["/tmp/a", "/tmp/b"],
            remote_filepath=["/remote/a", "/remote/b"],
            operation="get",
        )
        event = await trigger.run().__anext__()
        assert mock_retrieve.await_count == 2
        mock_retrieve.assert_any_await("/remote/a", "/tmp/a")
        mock_retrieve.assert_any_await("/remote/b", "/tmp/b")
        assert event == TriggerEvent({"status": "success", "local_filepath": ["/tmp/a", "/tmp/b"]})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.store_file", new_callable=mock.AsyncMock)
    async def test_run_put_success(self, mock_store):
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation="put",
        )
        event = await trigger.run().__anext__()
        mock_store.assert_awaited_once_with("/tmp/remote", "/tmp/local")
        assert event == TriggerEvent({"status": "success", "local_filepath": "/tmp/local"})

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.retrieve_file", new_callable=mock.AsyncMock)
    async def test_run_get_creates_local_parents(self, mock_retrieve, tmp_path):
        local_path = tmp_path / "nested" / "file.txt"
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            local_filepath=str(local_path),
            remote_filepath="/tmp/remote",
            operation="get",
            create_intermediate_dirs=True,
        )
        event = await trigger.run().__anext__()
        assert local_path.parent.is_dir()
        mock_retrieve.assert_awaited_once()
        assert event.payload["status"] == "success"

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.sftp.hooks.sftp.SFTPHookAsync.retrieve_file", new_callable=mock.AsyncMock)
    async def test_run_error(self, mock_retrieve):
        mock_retrieve.side_effect = RuntimeError("sftp failed")
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation="get",
        )
        event = await trigger.run().__anext__()
        assert event == TriggerEvent({"status": "error", "message": "sftp failed"})

    @pytest.mark.asyncio
    async def test_run_unsupported_operation(self):
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            local_filepath="/tmp/local",
            remote_filepath="/tmp/remote",
            operation="delete",
        )
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "error"
        assert "Unsupported operation" in event.payload["message"]

    @pytest.mark.asyncio
    async def test_run_missing_local_filepath(self):
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            remote_filepath="/tmp/remote",
            operation="get",
        )
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "error"
        assert "local_filepath is required" in event.payload["message"]

    @pytest.mark.asyncio
    async def test_run_mismatched_path_counts(self):
        trigger = SFTPTransferTrigger(
            ssh_conn_id="sftp_default",
            local_filepath=["/tmp/a"],
            remote_filepath=["/remote/a", "/remote/b"],
            operation="get",
        )
        event = await trigger.run().__anext__()
        assert event.payload["status"] == "error"
        assert "zip" in event.payload["message"].lower() or "argument" in event.payload["message"].lower()
