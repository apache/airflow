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

from unittest import mock
from unittest.mock import PropertyMock

from airflow.providers.alibaba.cloud.log.oss_task_handler import OSSTaskHandler

OSS_TASK_HANDLER_STRING = "airflow.providers.alibaba.cloud.log.oss_task_handler.{}"
MOCK_OSS_CONN_ID = "mock_id"
MOCK_BUCKET_NAME = "mock_bucket_name"
MOCK_KEY = "mock_key"
MOCK_KEYS = ["mock_key1", "mock_key2", "mock_key3"]
MOCK_CONTENT = "mock_content"
MOCK_FILE_PATH = "mock_file_path"


class TestOSSTaskHandler:
    def setup_method(self):
        self.base_log_folder = "local/airflow/logs/1.log"
        self.oss_log_folder = f"oss://{MOCK_BUCKET_NAME}/airflow/logs"
        self.oss_task_handler = OSSTaskHandler(self.base_log_folder, self.oss_log_folder)

    @mock.patch(OSS_TASK_HANDLER_STRING.format("conf.get"))
    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSHook"))
    def test_hook(self, mock_service, mock_conf_get):
        # Given
        mock_conf_get.return_value = "oss_default"

        # When
        self.oss_task_handler.hook

        # Then
        mock_conf_get.assert_called_once_with("logging", "REMOTE_LOG_CONN_ID")
        mock_service.assert_called_once_with(oss_conn_id="oss_default")

    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.hook"), new_callable=PropertyMock)
    def test_oss_log_exists(self, mock_service):
        self.oss_task_handler.oss_log_exists("1.log")
        mock_service.assert_called_once_with()
        mock_service.return_value.key_exist.assert_called_once_with(MOCK_BUCKET_NAME, "airflow/logs/1.log")

    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.hook"), new_callable=PropertyMock)
    def test_oss_read(self, mock_service):
        self.oss_task_handler.oss_read("1.log")
        mock_service.assert_called_once_with()
        mock_service.return_value.read_key(MOCK_BUCKET_NAME, "airflow/logs/1.log")

    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.oss_log_exists"))
    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.hook"), new_callable=PropertyMock)
    def test_oss_write_into_remote_existing_file_via_append(self, mock_service, mock_oss_log_exists):
        # Given
        mock_oss_log_exists.return_value = True
        mock_service.return_value.head_key.return_value.content_length = 1

        # When
        self.oss_task_handler.oss_write(MOCK_CONTENT, "1.log", append=True)

        # Then
        assert mock_service.call_count == 2
        mock_service.return_value.head_key.assert_called_once_with(MOCK_BUCKET_NAME, "airflow/logs/1.log")
        mock_oss_log_exists.assert_called_once_with("airflow/logs/1.log")
        mock_service.return_value.append_string.assert_called_once_with(
            MOCK_BUCKET_NAME, MOCK_CONTENT, "airflow/logs/1.log", 1
        )

    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.oss_log_exists"))
    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.hook"), new_callable=PropertyMock)
    def test_oss_write_into_remote_non_existing_file_via_append(self, mock_service, mock_oss_log_exists):
        # Given
        mock_oss_log_exists.return_value = False

        # When
        self.oss_task_handler.oss_write(MOCK_CONTENT, "1.log", append=True)

        # Then
        assert mock_service.call_count == 1
        mock_service.return_value.head_key.assert_not_called()
        mock_oss_log_exists.assert_called_once_with("airflow/logs/1.log")
        mock_service.return_value.append_string.assert_called_once_with(
            MOCK_BUCKET_NAME, MOCK_CONTENT, "airflow/logs/1.log", 0
        )

    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.oss_log_exists"))
    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.hook"), new_callable=PropertyMock)
    def test_oss_write_into_remote_existing_file_not_via_append(self, mock_service, mock_oss_log_exists):
        # Given
        mock_oss_log_exists.return_value = True

        # When
        self.oss_task_handler.oss_write(MOCK_CONTENT, "1.log", append=False)

        # Then
        assert mock_service.call_count == 1
        mock_service.return_value.head_key.assert_not_called()
        mock_oss_log_exists.assert_not_called()
        mock_service.return_value.append_string.assert_called_once_with(
            MOCK_BUCKET_NAME, MOCK_CONTENT, "airflow/logs/1.log", 0
        )

    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.oss_log_exists"))
    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.hook"), new_callable=PropertyMock)
    def test_oss_write_into_remote_non_existing_file_not_via_append(self, mock_service, mock_oss_log_exists):
        # Given
        mock_oss_log_exists.return_value = False

        # When
        self.oss_task_handler.oss_write(MOCK_CONTENT, "1.log", append=False)

        # Then
        assert mock_service.call_count == 1
        mock_service.return_value.head_key.assert_not_called()
        mock_oss_log_exists.assert_not_called()
        mock_service.return_value.append_string.assert_called_once_with(
            MOCK_BUCKET_NAME, MOCK_CONTENT, "airflow/logs/1.log", 0
        )
