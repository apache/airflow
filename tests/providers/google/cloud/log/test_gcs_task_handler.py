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

import copy
import logging
import os
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.providers.google.cloud.log.gcs_task_handler import GCSTaskHandler
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs


@pytest.mark.db_test
class TestGCSTaskHandler:
    @pytest.fixture(autouse=True)
    def task_instance(self, create_task_instance):
        self.ti = ti = create_task_instance(
            dag_id="dag_for_testing_gcs_task_handler",
            task_id="task_for_testing_gcs_task_handler",
            execution_date=datetime(2020, 1, 1),
            state=TaskInstanceState.RUNNING,
        )
        ti.try_number = 1
        ti.raw = False
        yield
        clear_db_runs()
        clear_db_dags()

    @pytest.fixture(autouse=True)
    def local_log_location(self, tmp_path_factory):
        return str(tmp_path_factory.mktemp("local-gcs-log-location"))

    @pytest.fixture(autouse=True)
    def gcs_task_handler(self, create_log_template, local_log_location):
        create_log_template("{try_number}.log")
        self.gcs_task_handler = GCSTaskHandler(
            base_log_folder=local_log_location,
            gcs_log_folder="gs://bucket/remote/log/location",
        )
        yield self.gcs_task_handler

    @mock.patch("airflow.providers.google.cloud.log.gcs_task_handler.GCSHook")
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id")
    @pytest.mark.parametrize(
        "conn_id", [pytest.param("", id="no-conn"), pytest.param("my_gcs_conn", id="with-conn")]
    )
    def test_client_conn_id_behavior(self, mock_get_cred, mock_client, mock_hook, conn_id):
        """When remote log conn id configured, hook will be used"""
        mock_hook.return_value.get_credentials_and_project_id.return_value = ("test_cred", "test_proj")
        mock_get_cred.return_value = ("test_cred", "test_proj")
        with conf_vars({("logging", "remote_log_conn_id"): conn_id}):
            return_value = self.gcs_task_handler.client
        if conn_id:
            mock_hook.assert_called_once_with(gcp_conn_id="my_gcs_conn")
            mock_get_cred.assert_not_called()
        else:
            mock_hook.assert_not_called()
            mock_get_cred.assert_called()

        mock_client.assert_called_once_with(
            client_info=mock.ANY, credentials="test_cred", project="test_proj"
        )
        assert mock_client.return_value == return_value

    @conf_vars({("logging", "remote_log_conn_id"): "gcs_default"})
    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_should_read_logs_from_remote(self, mock_blob, mock_client, mock_creds):
        mock_obj = MagicMock()
        mock_obj.name = "remote/log/location/1.log"
        mock_client.return_value.list_blobs.return_value = [mock_obj]
        mock_blob.from_string.return_value.download_as_bytes.return_value = b"CONTENT"
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        logs, metadata = self.gcs_task_handler._read(ti, self.ti.try_number)
        mock_blob.from_string.assert_called_once_with(
            "gs://bucket/remote/log/location/1.log", mock_client.return_value
        )
        assert logs == "*** Found remote logs:\n***   * gs://bucket/remote/log/location/1.log\nCONTENT"
        assert {"end_of_log": True, "log_pos": 7} == metadata

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_should_read_from_local_on_logs_read_error(self, mock_blob, mock_client, mock_creds):
        mock_obj = MagicMock()
        mock_obj.name = "remote/log/location/1.log"
        mock_client.return_value.list_blobs.return_value = [mock_obj]
        mock_blob.from_string.return_value.download_as_bytes.side_effect = Exception("Failed to connect")

        self.gcs_task_handler.set_context(self.ti)
        ti = copy.copy(self.ti)
        ti.state = TaskInstanceState.SUCCESS
        log, metadata = self.gcs_task_handler._read(ti, self.ti.try_number)

        assert log == (
            "*** Found remote logs:\n"
            "***   * gs://bucket/remote/log/location/1.log\n"
            "*** Unable to read remote log Failed to connect\n"
            "*** Found local files:\n"
            f"***   * {self.gcs_task_handler.local_base}/1.log\n"
        )
        assert metadata == {"end_of_log": True, "log_pos": 0}
        mock_blob.from_string.assert_called_once_with(
            "gs://bucket/remote/log/location/1.log", mock_client.return_value
        )

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_write_to_remote_on_close(self, mock_blob, mock_client, mock_creds):
        mock_blob.from_string.return_value.download_as_bytes.return_value = b"CONTENT"

        self.gcs_task_handler.set_context(self.ti)
        self.gcs_task_handler.emit(
            logging.LogRecord(
                name="NAME",
                level="DEBUG",
                pathname=None,
                lineno=None,
                msg="MESSAGE",
                args=None,
                exc_info=None,
            )
        )
        self.gcs_task_handler.close()

        mock_blob.assert_has_calls(
            [
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().download_as_bytes(),
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().upload_from_string("CONTENT\nMESSAGE\n", content_type="text/plain"),
            ],
            any_order=False,
        )
        mock_blob.from_string.return_value.upload_from_string(data="CONTENT\nMESSAGE\n")
        assert self.gcs_task_handler.closed is True

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_failed_write_to_remote_on_close(self, mock_blob, mock_client, mock_creds, caplog):
        caplog.at_level(logging.ERROR, logger=self.gcs_task_handler.log.name)
        mock_blob.from_string.return_value.upload_from_string.side_effect = Exception("Failed to connect")
        mock_blob.from_string.return_value.download_as_bytes.return_value = b"Old log"

        self.gcs_task_handler.set_context(self.ti)
        self.gcs_task_handler.emit(
            logging.LogRecord(
                name="NAME",
                level="DEBUG",
                pathname=None,
                lineno=None,
                msg="MESSAGE",
                args=None,
                exc_info=None,
            )
        )
        self.gcs_task_handler.close()

        assert caplog.record_tuples == [
            (
                "airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler",
                logging.ERROR,
                "Could not write logs to gs://bucket/remote/log/location/1.log: Failed to connect",
            ),
        ]
        mock_blob.assert_has_calls(
            [
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().download_as_bytes(),
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().upload_from_string("Old log\nMESSAGE\n", content_type="text/plain"),
            ],
            any_order=False,
        )

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_write_to_remote_on_close_failed_read_old_logs(self, mock_blob, mock_client, mock_creds):
        mock_blob.from_string.return_value.download_as_bytes.side_effect = Exception("Fail to download")

        self.gcs_task_handler.set_context(self.ti)
        self.gcs_task_handler.emit(
            logging.LogRecord(
                name="NAME",
                level="DEBUG",
                pathname=None,
                lineno=None,
                msg="MESSAGE",
                args=None,
                exc_info=None,
            )
        )
        self.gcs_task_handler.close()

        mock_blob.assert_has_calls(
            [
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().download_as_bytes(),
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().upload_from_string(
                    "MESSAGE\nError checking for previous log; if exists, may be overwritten: Fail to download\n",
                    content_type="text/plain",
                ),
            ],
            any_order=False,
        )

    @pytest.mark.parametrize(
        "delete_local_copy, expected_existence_of_local_copy, airflow_version",
        [(True, False, "2.6.0"), (False, True, "2.6.0"), (True, True, "2.5.0"), (False, True, "2.5.0")],
    )
    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_close_with_delete_local_copy_conf(
        self,
        mock_blob,
        mock_client,
        mock_creds,
        local_log_location,
        delete_local_copy,
        expected_existence_of_local_copy,
        airflow_version,
    ):
        mock_blob.from_string.return_value.download_as_bytes.return_value = b"CONTENT"
        with conf_vars({("logging", "delete_local_logs"): str(delete_local_copy)}), mock.patch(
            "airflow.version.version", airflow_version
        ):
            handler = GCSTaskHandler(
                base_log_folder=local_log_location,
                gcs_log_folder="gs://bucket/remote/log/location",
            )

        handler.log.info("test")
        handler.set_context(self.ti)
        assert handler.upload_on_close

        handler.close()
        assert os.path.exists(handler.handler.baseFilename) == expected_existence_of_local_copy
