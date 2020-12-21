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
import logging
import shutil
import tempfile
import unittest
from datetime import datetime
from unittest import mock

from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.log.gcs_task_handler import GCSTaskHandler
from airflow.utils.state import State
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs


class TestGCSTaskHandler(unittest.TestCase):
    def setUp(self) -> None:
        date = datetime(2020, 1, 1)
        self.gcs_log_folder = "test"
        self.logger = logging.getLogger("logger")
        self.dag = DAG("dag_for_testing_task_handler", start_date=date)
        task = DummyOperator(task_id="task_for_testing_gcs_task_handler")
        self.ti = TaskInstance(task=task, execution_date=date)
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        self.remote_log_base = "gs://bucket/remote/log/location"
        self.remote_log_location = "gs://my-bucket/path/to/1.log"
        self.local_log_location = tempfile.mkdtemp()
        self.filename_template = "{try_number}.log"
        self.addCleanup(self.dag.clear)
        self.gcs_task_handler = GCSTaskHandler(
            base_log_folder=self.local_log_location,
            gcs_log_folder=self.remote_log_base,
            filename_template=self.filename_template,
        )

    def tearDown(self) -> None:
        clear_db_runs()
        shutil.rmtree(self.local_log_location, ignore_errors=True)

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
        self.assertEqual(mock_client.return_value, return_value)

    @conf_vars({("logging", "remote_log_conn_id"): "gcs_default"})
    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_should_read_logs_from_remote(self, mock_blob, mock_client, mock_creds):
        mock_blob.from_string.return_value.download_as_string.return_value = "CONTENT"

        logs, metadata = self.gcs_task_handler._read(self.ti, self.ti.try_number)
        mock_blob.from_string.assert_called_once_with(
            "gs://bucket/remote/log/location/1.log", mock_client.return_value
        )

        self.assertEqual(
            "*** Reading remote log from gs://bucket/remote/log/location/1.log.\nCONTENT\n", logs
        )
        self.assertEqual({"end_of_log": True}, metadata)

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_should_read_from_local(self, mock_blob, mock_client, mock_creds):
        mock_blob.from_string.return_value.download_as_string.side_effect = Exception("Failed to connect")

        self.gcs_task_handler.set_context(self.ti)
        log, metadata = self.gcs_task_handler._read(self.ti, self.ti.try_number)

        self.assertEqual(
            log,
            "*** Unable to read remote log from gs://bucket/remote/log/location/1.log\n*** "
            f"Failed to connect\n\n*** Reading local file: {self.local_log_location}/1.log\n",
        )
        self.assertDictEqual(metadata, {"end_of_log": True})
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
        mock_blob.from_string.return_value.download_as_string.return_value = "CONTENT"

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
                mock.call.from_string().download_as_string(),
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().upload_from_string("CONTENT\nMESSAGE\n", content_type="text/plain"),
            ],
            any_order=False,
        )
        mock_blob.from_string.return_value.upload_from_string(data="CONTENT\nMESSAGE\n")
        self.assertEqual(self.gcs_task_handler.closed, True)

    @mock.patch(
        "airflow.providers.google.cloud.log.gcs_task_handler.get_credentials_and_project_id",
        return_value=("TEST_CREDENTIALS", "TEST_PROJECT_ID"),
    )
    @mock.patch("google.cloud.storage.Client")
    @mock.patch("google.cloud.storage.Blob")
    def test_failed_write_to_remote_on_close(self, mock_blob, mock_client, mock_creds):
        mock_blob.from_string.return_value.upload_from_string.side_effect = Exception("Failed to connect")
        mock_blob.from_string.return_value.download_as_string.return_value = b"Old log"

        self.gcs_task_handler.set_context(self.ti)
        with self.assertLogs(self.gcs_task_handler.log) as cm:
            self.gcs_task_handler.close()

        self.assertEqual(
            cm.output,
            [
                'INFO:airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler:Previous '
                'log discarded: sequence item 0: expected str instance, bytes found',
                'ERROR:airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler:Could '
                'not write logs to gs://bucket/remote/log/location/1.log: Failed to connect',
            ],
        )
        mock_blob.assert_has_calls(
            [
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().download_as_string(),
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().upload_from_string(
                    "*** Previous log discarded: sequence item 0: expected str instance, bytes found\n\n",
                    content_type="text/plain",
                ),
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
        mock_blob.from_string.return_value.download_as_string.side_effect = Exception("Fail to download")

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
                mock.call.from_string().download_as_string(),
                mock.call.from_string("gs://bucket/remote/log/location/1.log", mock_client.return_value),
                mock.call.from_string().upload_from_string(
                    "*** Previous log discarded: Fail to download\n\nMESSAGE\n", content_type="text/plain"
                ),
            ],
            any_order=False,
        )
