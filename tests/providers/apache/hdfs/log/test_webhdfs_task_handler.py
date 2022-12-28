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

import contextlib
import copy
import os
from typing import Any
from unittest import mock

# import boto3
# import moto
import pytest
from hdfs.ext.kerberos import KerberosClient
from requests_mock import Mocker

from airflow.compat.functools import cached_property
from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.hdfs.log.webhdfs_task_handler import WebHDFSTaskHandler
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars

GETFILESTATUS_FILE_NOT_FOUND_JSON = {
    "RemoteException": {
        "exception": "FileNotFoundException",
        "javaClassName": "java.io.FileNotFoundException",
        "message": "File does not exist: /root/path",
    }
}
GETFILESTATUS_FILE_FOUND = {
    "FileStatus": {
        "accessTime": 1671547795947,
        "blockSize": 134217728,
        "childrenNum": 0,
        "fileId": 72162413,
        "group": "hdfs",
        "length": 9,
        "modificationTime": 1671547795955,
        "owner": "user",
        "pathSuffix": "",
        "permission": "644",
        "replication": 2,
        "storagePolicy": 0,
        "type": "FILE",
    }
}

GETFILESTATUS_DIRECTORY_FOUND = {
    "FileStatus": {
        "accessTime": 0,
        "blockSize": 0,
        "childrenNum": 7,
        "fileId": 63204162,
        "group": "hdfs",
        "length": 0,
        "modificationTime": 1671716416412,
        "owner": "user",
        "pathSuffix": "",
        "permission": "700",
        "replication": 0,
        "storagePolicy": 0,
        "type": "DIRECTORY",
    }
}

MKDIRS_PERMISSION_DENIED = {
    "RemoteException": {
        "exception": "AccessControlException",
        "javaClassName": "org.apache.hadoop.security.AccessControlException",
        "message": 'Permission denied: user=user, access=WRITE, inode="/user/user2":user2:hdfs:drwxr-xr-x',
    }
}


class MockedWebHDFSHook(WebHDFSHook):
    """Mock used to test all WebHDFS interaction using requests_mock."""

    def get_conn(self) -> Any:
        return KerberosClient("https://example.com:50471/")


class MockedWebHDFSTaskHandler(WebHDFSTaskHandler):
    @cached_property
    def hook(self):
        return MockedWebHDFSHook()


class TestWebHDFSTaskHandler:
    @conf_vars({("logging", "remote_log_conn_id"): "webhdfs_default"})
    @pytest.fixture(autouse=True)
    def setup(self, create_log_template, tmp_path_factory, requests_mock):
        self.remote_log_base = "webhdfs:///remote/log/location"
        self.remote_log_location = "/remote/log/location/1.log"
        self.local_log_location = str(tmp_path_factory.mktemp("local-webhdfs-log-location"))
        create_log_template("{try_number}.log")
        self.webhdfs_task_handler = MockedWebHDFSTaskHandler(self.local_log_location, self.remote_log_base)
        # Verify the hook now with the config override
        assert self.webhdfs_task_handler.hook is not None

        date = datetime(2016, 1, 1)
        self.dag = DAG("dag_for_testing_webhdfs_task_handler", start_date=date)
        task = EmptyOperator(task_id="task_for_testing_webhdfs_log_handler", dag=self.dag)
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=date, run_id="test", run_type="manual")
        with create_session() as session:
            session.add(dag_run)
            session.commit()
            session.refresh(dag_run)

        self.ti = TaskInstance(task=task, run_id=dag_run.run_id)
        self.ti.dag_run = dag_run
        self.ti.try_number = 1
        self.ti.state = State.RUNNING

        # We need to create the bucket since this is all in Moto's 'virtual'
        # AWS account
        yield

        self.dag.clear()

        with create_session() as session:
            session.query(DagRun).delete()

        if self.webhdfs_task_handler.handler:
            with contextlib.suppress(Exception):
                os.remove(self.webhdfs_task_handler.handler.baseFilename)

    def test_hook(self):
        assert isinstance(self.webhdfs_task_handler.hook, WebHDFSHook)

    def test_remote_base(self):
        assert self.webhdfs_task_handler.remote_base == "/remote/log/location"

    def test_set_context_raw(self):
        self.ti.raw = True
        mock_open = mock.mock_open()
        with mock.patch("airflow.providers.apache.hdfs.log.webhdfs_task_handler.open", mock_open):
            self.webhdfs_task_handler.set_context(self.ti)

        assert not self.webhdfs_task_handler.upload_on_close
        mock_open.assert_not_called()

    def test_set_context_not_raw(self):
        mock_open = mock.mock_open()
        with mock.patch("airflow.providers.apache.hdfs.log.webhdfs_task_handler.open", mock_open):
            self.webhdfs_task_handler.set_context(self.ti)

        assert self.webhdfs_task_handler.upload_on_close
        mock_open.assert_called_once_with(os.path.join(self.local_log_location, "1.log"), "w")
        mock_open().write.assert_not_called()

    def test_read_non_running(self, requests_mock: Mocker):
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/remote/log/location/1.log?op=GETFILESTATUS",
            json=GETFILESTATUS_FILE_FOUND,
        )

        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/remote/log/location/1.log?offset=0&op=OPEN",
            content=b"Log line\n",
        )

        self.ti.state = State.SUCCESS
        log, metadata = self.webhdfs_task_handler.read(self.ti)
        assert log[0][0][-1] == (
            "*** Found logs in webhdfs: /remote/log/location/1.log\n"
            "*** Reading remote log from HDFS: /remote/log/location/1.log.\n"
            "Log line"
        )
        assert metadata == [{"end_of_log": True, "log_pos": 8}]

    def test_read_running(self, requests_mock: Mocker):
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/remote/log/location/1.log?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )
        self.webhdfs_task_handler._read_from_logs_server = mock.Mock()
        self.webhdfs_task_handler._read_from_logs_server.return_value = (
            ["Found logs on executor"],
            ["this\nlog\ncontent"],
        )
        log, metadata = self.webhdfs_task_handler.read(self.ti)
        assert log[0][0][-1] == (
            "*** No logs found on webhdfs for ti=<TaskInstance: dag_for_testing_webhdfs_task_handler.task_for_testing_webhdfs_log_handler test [running]>\n"  # noqa: E501
            "*** Found logs on executor\n"
            "this\nlog\ncontent"
        )
        assert metadata == [{"end_of_log": False, "log_pos": 16}]

    def test_read_when_webhdfs_log_missing(self, requests_mock: Mocker):
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        self.webhdfs_task_handler._read_from_logs_server = mock.Mock(return_value=([], []))

        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/remote/log/location/1.log?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )
        log, metadata = self.webhdfs_task_handler.read(ti)

        assert 1 == len(log)
        assert len(log) == len(metadata)
        actual = log[0][0][-1]
        expected = "*** No logs found on webhdfs for ti=<TaskInstance: dag_for_testing_webhdfs_task_handler.task_for_testing_webhdfs_log_handler test [success]>\n"  # noqa: E501
        assert actual == expected
        assert {"end_of_log": True, "log_pos": 0} == metadata[0]

    def test_read_when_webhdfs_log_missing_and_log_pos_missing_pre_26(self, requests_mock: Mocker):
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/remote/log/location/1.log?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )

        # mock that super class has no _read_remote_logs method
        with mock.patch("airflow.providers.apache.hdfs.log.webhdfs_task_handler.hasattr", return_value=False):
            log, metadata = self.webhdfs_task_handler.read(ti)
        assert 1 == len(log)
        assert log[0][0][-1].startswith("*** Falling back to local log")

    def test_read_when_webhdfs_log_missing_and_log_pos_zero_pre_26(self, requests_mock: Mocker):
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/remote/log/location/1.log?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )

        # mock that super class has no _read_remote_logs method
        with mock.patch("airflow.providers.apache.hdfs.log.webhdfs_task_handler.hasattr", return_value=False):
            log, metadata = self.webhdfs_task_handler.read(ti, metadata={"log_pos": 0})
        assert 1 == len(log)
        assert log[0][0][-1].startswith("*** Falling back to local log")

    def test_read_when_webhdfs_log_missing_and_log_pos_over_zero_pre_26(self, requests_mock: Mocker):
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        requests_mock.get(
            "https://example.com:50471/webhdfs/v1/remote/log/location/1.log?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )

        # mock that super class has no _read_remote_logs method
        with mock.patch("airflow.providers.apache.hdfs.log.webhdfs_task_handler.hasattr", return_value=False):
            log, metadata = self.webhdfs_task_handler.read(ti, metadata={"log_pos": 1})
        assert 1 == len(log)
        assert not log[0][0][-1].startswith("*** Falling back to local log")

    def test_close_and_write(self, requests_mock):
        requests_mock.get(
            f"https://example.com:50471/webhdfs/v1{self.webhdfs_task_handler.remote_base}?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )
        requests_mock.get(
            f"https://example.com:50471/webhdfs/v1{self.remote_log_location}?op=GETFILESTATUS",
            status_code=404,
            json=GETFILESTATUS_FILE_NOT_FOUND_JSON,
        )
        requests_mock.put(
            f"https://example.com:50471/webhdfs/v1{self.webhdfs_task_handler.remote_base}?op=MKDIRS"
        )

        requests_mock.put(
            f"https://example.com:50471/webhdfs/v1{self.remote_log_location}?op=CREATE",
            status_code=307,
            headers={
                # A data-node.
                "location": f"https://example1.com:1022/webhdfs/v1{self.remote_log_location}?op=CREATE"
            },
        )
        requests_mock.put(
            f"https://example1.com:1022/webhdfs/v1{self.remote_log_location}?op=CREATE",  # the data node.
            status_code=201,
        )

        self.webhdfs_task_handler.set_context(self.ti)
        assert self.webhdfs_task_handler.upload_on_close
        self.webhdfs_task_handler.close()
        assert requests_mock.call_count == 6

    def test_close_and_write_existing(self, requests_mock: Mocker):
        requests_mock.get(
            f"https://example.com:50471/webhdfs/v1{self.webhdfs_task_handler.remote_base}?op=GETFILESTATUS",
            json=GETFILESTATUS_DIRECTORY_FOUND,
        )
        requests_mock.get(
            f"https://example.com:50471/webhdfs/v1{self.remote_log_location}?op=GETFILESTATUS",
            json=GETFILESTATUS_FILE_FOUND,
        )

        requests_mock.post(
            f"https://example.com:50471/webhdfs/v1{self.remote_log_location}?op=APPEND",
            status_code=307,
            headers={
                # A data-node.
                "location": f"https://example1.com:1022/webhdfs/v1{self.remote_log_location}?op=APPEND"
            },
        )
        requests_mock.post(
            f"https://example1.com:1022/webhdfs/v1{self.remote_log_location}?op=APPEND",
            # the data node.
            status_code=201,
        )

        self.webhdfs_task_handler.set_context(self.ti)
        assert self.webhdfs_task_handler.upload_on_close
        self.webhdfs_task_handler.hdfs_write("log", self.remote_log_location)
        assert requests_mock.call_count == 5

    def test_write_raises(self):
        handler = self.webhdfs_task_handler
        with mock.patch.object(handler.log, "error") as mock_error:
            handler.hdfs_write("text", self.remote_log_location)
            mock_error.assert_called_once_with(
                "Could not write logs to %s",
                self.remote_log_location,
                exc_info=True,
            )

    def test_close_no_upload(self, requests_mock: Mocker):
        self.ti.raw = True
        self.webhdfs_task_handler.set_context(self.ti)
        assert not self.webhdfs_task_handler.upload_on_close
        self.webhdfs_task_handler.close()

        assert not requests_mock.called


@pytest.mark.parametrize(
    "given,expected",
    [
        ("run_id=scheduled_2023-03-15T00-30-00+00:00", "run_id=scheduled_2023-03-15T00-30-00-00-00"),
        (
            "/some/path/run_id=scheduled_2023-03-15T00-30-00+00:00",
            "/some/path/run_id=scheduled_2023-03-15T00-30-00-00-00",
        ),
    ],
)
def test_webhdfs_task_handler_encode_remote_path(given, expected) -> None:
    assert WebHDFSTaskHandler.encode_remote_path(given) == expected
