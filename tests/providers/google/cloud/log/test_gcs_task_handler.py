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

import logging
import tempfile
from unittest import mock

import pytest

from airflow.providers.google.cloud.log.gcs_task_handler import GCSTaskHandler
from airflow.utils.state import TaskInstanceState
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs


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
    def local_log_location(self):
        with tempfile.TemporaryDirectory() as td:
            self.local_log_location = td
            yield td

    @pytest.fixture(autouse=True)
    def gcs_task_handler(self, create_log_template, local_log_location):
        create_log_template("{try_number}.log")
        self.gcs_task_handler = GCSTaskHandler(
            base_log_folder=local_log_location,
            gcs_log_folder="gs://bucket/remote/log/location",
        )
        yield self.gcs_task_handler

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    def test_hook(self, mock_client, mock_creds):
        return_value = self.gcs_task_handler.client
        mock_client.assert_called_once_with(
            client_info=mock.ANY, credentials="TEST_CREDENTIALS", project="TEST_PROJECT_ID"
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
        mock_blob.from_string.return_value.download_as_bytes.return_value = b"CONTENT"

        logs, metadata = self.gcs_task_handler._read(self.ti, self.ti.try_number)
        mock_blob.from_string.assert_called_once_with(
            "gs://bucket/remote/log/location/1.log", mock_client.return_value
        )

        assert "*** Reading remote log from gs://bucket/remote/log/location/1.log.\nCONTENT\n" == logs
        assert {"end_of_log": True} == metadata

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_should_read_from_local(self, mock_blob, mock_client, mock_creds):
        mock_blob.from_string.return_value.download_as_bytes.side_effect = Exception("Failed to connect")

        self.gcs_task_handler.set_context(self.ti)
        log, metadata = self.gcs_task_handler._read(self.ti, self.ti.try_number)

        assert (
            log == "*** Unable to read remote log from gs://bucket/remote/log/location/1.log\n*** "
            f"Failed to connect\n\n*** Reading local file: {self.local_log_location}/1.log\n"
        )
        assert metadata == {'end_of_log': False, 'log_pos': 31 + len(self.local_log_location)}
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
                    "*** Previous log discarded: Fail to download\n\nMESSAGE\n", content_type="text/plain"
                ),
            ],
            any_order=False,
        )
