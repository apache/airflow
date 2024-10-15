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
from unittest.mock import PropertyMock

import pytest
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

from airflow.providers.alibaba.cloud.log.oss_task_handler import OSSTaskHandler
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import datetime

pytestmark = pytest.mark.db_test

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

    @pytest.fixture(autouse=True)
    def task_instance(self, create_task_instance, dag_maker):
        self.ti = ti = create_task_instance(
            dag_id="dag_for_testing_oss_task_handler",
            task_id="task_for_testing_oss_task_handler",
            execution_date=datetime(2020, 1, 1),
            state=TaskInstanceState.RUNNING,
        )
        ti.try_number = 1
        ti.raw = False
        dag_maker.session.merge(ti)
        dag_maker.session.commit()
        yield
        clear_db_runs()
        clear_db_dags()

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
        mock_oss_log_exists.assert_called_once_with("1.log")
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
        mock_oss_log_exists.assert_called_once_with("1.log")
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

    @pytest.mark.parametrize(
        "delete_local_copy, expected_existence_of_local_copy",
        [(True, False), (False, True)],
    )
    @mock.patch(OSS_TASK_HANDLER_STRING.format("OSSTaskHandler.hook"), new_callable=PropertyMock)
    def test_close_with_delete_local_copy_conf(
        self,
        mock_service,
        tmp_path_factory,
        delete_local_copy,
        expected_existence_of_local_copy,
    ):
        local_log_path = str(tmp_path_factory.mktemp("local-oss-log-location"))
        with conf_vars({("logging", "delete_local_logs"): str(delete_local_copy)}):
            handler = OSSTaskHandler(local_log_path, self.oss_log_folder)

        handler.log.info("test")
        handler.set_context(self.ti)
        assert handler.upload_on_close

        handler.close()
        assert os.path.exists(handler.handler.baseFilename) == expected_existence_of_local_copy

    def test_filename_template_for_backward_compatibility(self):
        # filename_template arg support for running the latest provider on airflow 2
        OSSTaskHandler(self.base_log_folder, self.oss_log_folder, filename_template=None)
