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
import importlib
import random
import string
import subprocess
import unittest
from unittest import mock

import pytest

from airflow import settings
from airflow.example_dags import example_complex
from airflow.models import TaskInstance
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import provide_session
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_STACKDDRIVER
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs
from tests.test_utils.gcp_system_helpers import provide_gcp_context, resolve_full_gcp_key_path


@pytest.mark.system("google")
@pytest.mark.credential_file(GCP_STACKDDRIVER)
class TestStackdriverLoggingHandlerSystemTest(unittest.TestCase):

    def setUp(self) -> None:
        clear_db_runs()
        self.log_name = 'stackdriver-tests-'.join(random.sample(string.ascii_lowercase, 16))

    def tearDown(self) -> None:
        from airflow.config_templates import airflow_local_settings
        importlib.reload(airflow_local_settings)
        settings.configure_logging()
        clear_db_runs()

    @provide_session
    def test_should_support_key_auth(self, session):
        with mock.patch.dict(
            'os.environ',
            AIRFLOW__LOGGING__REMOTE_LOGGING="true",
            AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=f"stackdriver://{self.log_name}",
            AIRFLOW__LOGGING__STACKDRIVER_KEY_PATH=resolve_full_gcp_key_path(GCP_STACKDDRIVER),
            AIRFLOW__CORE__LOAD_EXAMPLES="false",
            AIRFLOW__CORE__DAGS_FOLDER=example_complex.__file__
        ):
            self.assertEqual(0, subprocess.Popen(
                ["airflow", "dags", "trigger", "example_complex"]
            ).wait())
            self.assertEqual(0, subprocess.Popen(
                ["airflow", "scheduler", "--num-runs", "1"]
            ).wait())
        ti = session.query(TaskInstance).filter(TaskInstance.task_id == "create_entry_group").first()

        self.assert_remote_logs("INFO - Task exited with return code 0", ti)

    @provide_session
    def test_should_support_adc(self, session):
        with mock.patch.dict(
            'os.environ',
            AIRFLOW__LOGGING__REMOTE_LOGGING="true",
            AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=f"stackdriver://{self.log_name}",
            AIRFLOW__CORE__LOAD_EXAMPLES="false",
            AIRFLOW__CORE__DAGS_FOLDER=example_complex.__file__,
            GOOGLE_APPLICATION_CREDENTIALS=resolve_full_gcp_key_path(GCP_STACKDDRIVER)
        ):
            self.assertEqual(0, subprocess.Popen(
                ["airflow", "dags", "trigger", "example_complex"]
            ).wait())
            self.assertEqual(0, subprocess.Popen(
                ["airflow", "scheduler", "--num-runs", "1"]
            ).wait())
        ti = session.query(TaskInstance).filter(TaskInstance.task_id == "create_entry_group").first()

        self.assert_remote_logs("INFO - Task exited with return code 0", ti)

    def assert_remote_logs(self, expected_message, ti):
        with provide_gcp_context(GCP_STACKDDRIVER), conf_vars({
            ('logging', 'remote_logging'): 'True',
            ('logging', 'remote_base_log_folder'): f"stackdriver://{self.log_name}",
        }):
            from airflow.config_templates import airflow_local_settings
            importlib.reload(airflow_local_settings)
            settings.configure_logging()

            task_log_reader = TaskLogReader()
            logs = "\n".join(task_log_reader.read_log_stream(ti, try_number=None, metadata={}))
            self.assertIn(expected_message, logs)
