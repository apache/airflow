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
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
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
            self.local_log_location, self.remote_log_base, self.filename_template
        )

    def tearDown(self) -> None:
        clear_db_runs()
        shutil.rmtree(self.local_log_location, ignore_errors=True)

    def test_hook(self):
        self.assertIsInstance(self.gcs_task_handler.hook, GCSHook)

    @conf_vars({("logging", "remote_log_conn_id"): "gcs_default"})
    def test_hook_raises(self):
        exp_messages = "Failed to connect"
        with self.assertLogs(self.gcs_task_handler.log) as cm:
            with mock.patch(
                "airflow.providers.google.cloud.hooks.gcs.GCSHook"
            ) as mock_hook:
                mock_hook.side_effect = Exception(exp_messages)
                self.gcs_task_handler.hook
        self.assertEqual(
            cm.output,
            ['ERROR:airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler:Could '
             'not create a GoogleCloudStorageHook with connection id "gcs_default". Failed '
             'to connect\n'
             '\n'
             'Please make sure that airflow[gcp] is installed and the GCS connection '
             'exists.']
        )

    @conf_vars({("logging", "remote_log_conn_id"): "gcs_default"})
    def test_should_read_logs_from_remote(self):
        with mock.patch(
            "airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler.gcs_read"
        ) as mock_remote_read:
            self.gcs_task_handler._read(self.ti, self.ti.try_number)
        mock_remote_read.assert_called_once()

    def test_should_read_from_local(self):
        with mock.patch(
            "airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler.gcs_read"
        ) as mock_remote_read:
            mock_remote_read.side_effect = Exception("Failed to connect")
            self.gcs_task_handler.set_context(self.ti)
            return_val = self.gcs_task_handler._read(self.ti, self.ti.try_number)
            self.assertEqual(len(return_val), 2)
            self.assertEqual(
                return_val[0],
                "*** Unable to read remote log from gs://bucket/remote/log/location/1.log\n*** "
                f"Failed to connect\n\n*** Reading local file: {self.local_log_location}/1.log\n",
            )
            self.assertDictEqual(return_val[1], {"end_of_log": True})
            mock_remote_read.assert_called_once()

    def test_write_to_remote_on_close(self):
        with mock.patch(
            "airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler.gcs_write"
        ) as mock_remote_write:
            self.gcs_task_handler.set_context(self.ti)
            self.gcs_task_handler.close()
            mock_remote_write.assert_called_once()
            self.assertEqual(self.gcs_task_handler.closed, True)

    @mock.patch.object(GCSTaskHandler, "hook")
    def test_gcs_read(self, mock_hook):
        mock_hook.download.return_value = "Test log".encode("utf-8")
        return_val = self.gcs_task_handler.gcs_read(self.remote_log_location)
        self.assertEqual(return_val, "Test log")

    @mock.patch.object(GCSTaskHandler, "gcs_read")
    @mock.patch.object(GCSTaskHandler, "hook")
    def test_gcs_write(self, mock_hook, mock_gcs_read):
        mock_gcs_read.return_value = "Old log"
        self.gcs_task_handler.gcs_write("New log", self.remote_log_location)
        mock_gcs_read.assert_called_once()

    @mock.patch.object(GCSTaskHandler, "hook")
    @mock.patch.object(GCSTaskHandler, "gcs_read")
    def test_gcs_write_fail_remote(self, mock_remote_read, mock_hook):
        with self.assertLogs(self.gcs_task_handler.log) as cm:
            mock_hook.upload.side_effect = Exception("Failed to connect", )
            mock_remote_read.return_value = "Old log"
            self.gcs_task_handler.gcs_write("New log", self.remote_log_location)
        self.assertEqual(
            cm.output,
            [
                'ERROR:airflow.providers.google.cloud.log.gcs_task_handler.GCSTaskHandler:Could '
                'not write logs to gs://my-bucket/path/to/1.log: Failed to connect'
            ]
        )
