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

import os
from unittest import mock

from airflow.providers.apache.hdfs.sensors.web_hdfs import MultipleFilesWebHdfsSensor, WebHdfsSensor

TEST_HDFS_CONN = "webhdfs_default"
TEST_HDFS_DIRECTORY = "hdfs://user/hive/warehouse/airflow.db"
TEST_HDFS_FILENAMES = ["static_babynames1", "static_babynames2", "static_babynames3"]
TEST_HDFS_PATH = os.path.join(TEST_HDFS_DIRECTORY, TEST_HDFS_FILENAMES[0])


class TestWebHdfsSensor:
    @mock.patch("airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook")
    def test_poke(self, mock_hook):
        sensor = WebHdfsSensor(
            task_id="test_task",
            webhdfs_conn_id=TEST_HDFS_CONN,
            filepath=TEST_HDFS_PATH,
        )
        exists = sensor.poke(dict())

        assert exists

        mock_hook.return_value.check_for_path.assert_called_once_with(hdfs_path=TEST_HDFS_PATH)
        mock_hook.assert_called_once_with(TEST_HDFS_CONN)

    @mock.patch("airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook")
    def test_poke_should_return_false_for_non_existing_table(self, mock_hook):
        mock_hook.return_value.check_for_path.return_value = False

        sensor = WebHdfsSensor(
            task_id="test_task",
            webhdfs_conn_id=TEST_HDFS_CONN,
            filepath=TEST_HDFS_PATH,
        )
        exists = sensor.poke(dict())

        assert not exists

        mock_hook.return_value.check_for_path.assert_called_once_with(hdfs_path=TEST_HDFS_PATH)
        mock_hook.assert_called_once_with(TEST_HDFS_CONN)


class TestMultipleFilesWebHdfsSensor:
    @mock.patch("airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook")
    def test_poke(self, mock_hook, caplog):
        mock_hook.return_value.get_conn.return_value.list.return_value = TEST_HDFS_FILENAMES

        sensor = MultipleFilesWebHdfsSensor(
            task_id="test_task",
            webhdfs_conn_id=TEST_HDFS_CONN,
            directory_path=TEST_HDFS_DIRECTORY,
            expected_filenames=TEST_HDFS_FILENAMES,
        )
        result = sensor.poke(dict())

        assert result
        assert "Files Found in directory: " in caplog.text

        mock_hook.return_value.get_conn.return_value.list.assert_called_once_with(TEST_HDFS_DIRECTORY)
        mock_hook.return_value.get_conn.assert_called_once()
        mock_hook.assert_called_once_with(TEST_HDFS_CONN)

    @mock.patch("airflow.providers.apache.hdfs.hooks.webhdfs.WebHDFSHook")
    def test_poke_should_return_false_for_missing_file(self, mock_hook, caplog):
        mock_hook.return_value.get_conn.return_value.list.return_value = TEST_HDFS_FILENAMES[0]

        sensor = MultipleFilesWebHdfsSensor(
            task_id="test_task",
            webhdfs_conn_id=TEST_HDFS_CONN,
            directory_path=TEST_HDFS_DIRECTORY,
            expected_filenames=TEST_HDFS_FILENAMES,
        )
        exists = sensor.poke(dict())

        assert not exists
        assert "Files Found in directory: " in caplog.text
        assert "There are missing files: " in caplog.text

        mock_hook.return_value.get_conn.return_value.list.assert_called_once_with(TEST_HDFS_DIRECTORY)
        mock_hook.return_value.get_conn.assert_called_once()
        mock_hook.assert_called_once_with(TEST_HDFS_CONN)
