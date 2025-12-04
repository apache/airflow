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

import os
import tempfile
from pathlib import Path
from unittest import mock
from unittest.mock import PropertyMock

import pytest

from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.hdfs.log.hdfs_task_handler import HdfsTaskHandler
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import datetime

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test

DEFAULT_DATE = datetime(2020, 8, 10)


class TestHdfsTaskHandler:
    @pytest.fixture(autouse=True)
    def ti(self, create_task_instance, create_log_template):
        create_log_template("{try_number}.log")
        ti = create_task_instance(
            dag_id="dag_for_testing_hdfs_task_handler",
            task_id="task_for_testing_hdfs_log_handler",
            logical_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            dagrun_state=TaskInstanceState.RUNNING,
            state=TaskInstanceState.RUNNING,
        )
        ti.try_number = 1
        ti.hostname = "localhost"
        ti.raw = False
        yield ti
        clear_db_runs()
        clear_db_dags()

    def setup_method(self):
        self.hdfs_log_folder = "hdfs://namenode/remote/log/location"
        self.remote_log_location = "remote/log/location/1.log"
        self.local_log_location = str(Path(tempfile.gettempdir()) / "local/log/location")
        self.hdfs_task_handler = HdfsTaskHandler(
            base_log_folder=self.local_log_location,
            hdfs_log_folder=self.hdfs_log_folder,
            delete_local_copy=True,
        )

    def test_hook(self):
        assert isinstance(self.hdfs_task_handler.io.hook, WebHDFSHook)

    def test_set_context_raw(self, ti):
        ti.raw = True
        mock_open = mock.mock_open()
        with mock.patch("airflow.providers.apache.hdfs.log.hdfs_task_handler.open", mock_open):
            self.hdfs_task_handler.set_context(ti)

        assert self.hdfs_task_handler.upload_on_close is False
        mock_open.assert_not_called()

    def test_set_context_not_raw(self, ti):
        mock_open = mock.mock_open()
        with mock.patch("airflow.providers.apache.hdfs.log.hdfs_task_handler.open", mock_open):
            self.hdfs_task_handler.io.upload = mock.MagicMock()
            self.hdfs_task_handler.set_context(ti)

        assert self.hdfs_task_handler.upload_on_close is True
        mock_open.assert_called_once_with(self.hdfs_task_handler.handler.baseFilename, "w")
        mock_open().write.assert_not_called()

    @mock.patch(
        "airflow.providers.apache.hdfs.log.hdfs_task_handler.HdfsRemoteLogIO.hook", new_callable=PropertyMock
    )
    def test_hdfs_read(self, mock_hook):
        expected_file_path = os.path.join(self.hdfs_task_handler.io.remote_base, "1.log")
        mock_hook.return_value.check_for_path.return_value = True
        mock_hook.return_value.read_file.return_value = b"mock_content"
        dummy_ti = "dummy_ti"
        messages, logs = self.hdfs_task_handler.io.read("1.log", dummy_ti)
        mock_hook.return_value.check_for_path.assert_called_once_with(expected_file_path)
        mock_hook.return_value.read_file.assert_called_once_with(expected_file_path)
        assert messages == []
        assert logs == ["mock_content"]

    @mock.patch(
        "airflow.providers.apache.hdfs.log.hdfs_task_handler.HdfsRemoteLogIO.hook", new_callable=PropertyMock
    )
    def test_hdfs_read_not_exists(self, mock_hook):
        expected_file_path = os.path.join(self.hdfs_task_handler.io.remote_base, "1.log")
        mock_hook.return_value.check_for_path.return_value = False
        dummy_ti = "dummy_ti"
        messages, logs = self.hdfs_task_handler.io.read("1.log", dummy_ti)
        mock_hook.return_value.check_for_path.assert_called_once_with(expected_file_path)
        assert messages == [f"No logs found on hdfs for ti={dummy_ti}"]
        assert logs == []

    @mock.patch("shutil.rmtree")
    def test_upload_absolute_path(self, mock_rmtree, ti):
        base_path = Path(self.local_log_location)
        base_path.mkdir(parents=True, exist_ok=True)
        test_file = base_path / "test.log"
        test_file.write_text("log content")
        absolute_path = str(test_file.resolve())
        relative_path = test_file.relative_to(base_path)
        expected_remote_loc = os.path.join(self.hdfs_task_handler.io.remote_base, str(relative_path))

        self.hdfs_task_handler.io.hook = mock.MagicMock(spec=WebHDFSHook)
        self.hdfs_task_handler.io.hook.load_file = mock.MagicMock()
        self.hdfs_task_handler.io.upload(absolute_path, ti)
        self.hdfs_task_handler.io.hook.load_file.assert_called_once_with(test_file, expected_remote_loc)
        mock_rmtree.assert_called_once_with(os.path.dirname(str(test_file)))

    @mock.patch("shutil.rmtree")
    def test_upload_relative_path(self, mock_rmtree, ti):
        base_path = Path(self.local_log_location)
        base_path.mkdir(parents=True, exist_ok=True)
        test_file = base_path / "relative.log"
        test_file.write_text("relative log content")
        relative_path = "relative.log"
        expected_remote_loc = os.path.join(self.hdfs_task_handler.io.remote_base, relative_path)
        self.hdfs_task_handler.io.hook = mock.MagicMock(spec=WebHDFSHook)
        self.hdfs_task_handler.io.hook.load_file = mock.MagicMock()
        self.hdfs_task_handler.io.upload(relative_path, ti)
        self.hdfs_task_handler.io.hook.load_file.assert_called_once_with(test_file, expected_remote_loc)
        mock_rmtree.assert_called_once_with(os.path.dirname(str(test_file)))

    def test_close_uploads_once(self, ti):
        self.hdfs_task_handler.set_context(ti)
        self.hdfs_task_handler.io.upload = mock.MagicMock()
        self.hdfs_task_handler.close()
        self.hdfs_task_handler.io.upload.assert_called_once_with(self.hdfs_task_handler.log_relative_path, ti)

        self.hdfs_task_handler.io.upload.reset_mock()
        self.hdfs_task_handler.close()
        self.hdfs_task_handler.io.upload.assert_not_called()

    def test_read_remote_logs_found(self, ti):
        self.hdfs_task_handler._render_filename = lambda ti, try_number: "dummy.log"
        self.hdfs_task_handler.io.read = mock.MagicMock(return_value=([], ["dummy log"]))
        messages, logs = self.hdfs_task_handler._read_remote_logs(ti, try_number=1)
        assert messages == []
        assert logs == ["dummy log"]

    def test_read_remote_logs_not_found(self, ti):
        self.hdfs_task_handler._render_filename = lambda ti, try_number: "dummy.log"
        self.hdfs_task_handler.io.read = mock.MagicMock(
            return_value=([f"No logs found on hdfs for ti={ti}"], [])
        )
        messages, logs = self.hdfs_task_handler._read_remote_logs(ti, try_number=1)
        assert [f"No logs found on hdfs for ti={ti}"]
        assert logs == []

    @pytest.mark.parametrize(
        ("delete_local_copy", "expected_existence_of_local_copy"),
        [(True, False), (False, True)],
    )
    def test_close_with_delete_local_logs_conf(
        self, ti, tmp_path_factory, delete_local_copy, expected_existence_of_local_copy
    ):
        local_log_dir = tmp_path_factory.mktemp("local-hdfs-log-location")
        local_log_file = local_log_dir / "1.log"
        local_log_file.write_text("test")
        with conf_vars({("logging", "delete_local_logs"): str(delete_local_copy)}):
            handler = HdfsTaskHandler(
                base_log_folder=str(local_log_dir),
                hdfs_log_folder=self.hdfs_log_folder,
                delete_local_copy=delete_local_copy,
            )
        handler.log.info("test")
        handler.local_base = local_log_dir
        with mock.patch.object(handler.io, "hook") as mock_hook:
            mock_hook.load_file.return_value = None
            handler.set_context(ti)
            assert handler.upload_on_close
            handler.close()
        assert os.path.exists(handler.handler.baseFilename) == expected_existence_of_local_copy
