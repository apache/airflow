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
from unittest import mock

import pytest

from airflow import settings
from airflow.example_dags import example_complex
from airflow.models import DagBag, TaskInstance
from airflow.utils.log.log_reader import TaskLogReader
from airflow.utils.session import provide_session
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_connections, clear_db_runs
from tests.test_utils.gcp_system_helpers import (
    GoogleSystemTest,
    provide_gcp_context,
    resolve_full_gcp_key_path,
)


@pytest.mark.system("google")
@pytest.mark.credential_file(GCP_GCS_KEY)
class TestGCSTaskHandlerSystemTest(GoogleSystemTest):
    @classmethod
    def setUpClass(cls) -> None:
        unique_suffix = ''.join(random.sample(string.ascii_lowercase, 16))
        cls.bucket_name = f"airflow-gcs-task-handler-tests-{unique_suffix}"  # type: ignore
        cls.create_gcs_bucket(cls.bucket_name)  # type: ignore
        clear_db_connections()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.delete_gcs_bucket(cls.bucket_name)  # type: ignore

    def setUp(self) -> None:
        clear_db_runs()

    def tearDown(self) -> None:
        from airflow.config_templates import airflow_local_settings

        importlib.reload(airflow_local_settings)
        settings.configure_logging()
        clear_db_runs()

    @provide_session
    def test_should_read_logs(self, session):
        with mock.patch.dict(
            'os.environ',
            AIRFLOW__LOGGING__REMOTE_LOGGING="true",
            AIRFLOW__LOGGING__REMOTE_BASE_LOG_FOLDER=f"gs://{self.bucket_name}/path/to/logs",
            AIRFLOW__LOGGING__REMOTE_LOG_CONN_ID="google_cloud_default",
            AIRFLOW__CORE__LOAD_EXAMPLES="false",
            AIRFLOW__CORE__DAGS_FOLDER=example_complex.__file__,
            GOOGLE_APPLICATION_CREDENTIALS=resolve_full_gcp_key_path(GCP_GCS_KEY),
        ):
            self.assertEqual(0, subprocess.Popen(["airflow", "dags", "trigger", "example_complex"]).wait())
            self.assertEqual(0, subprocess.Popen(["airflow", "scheduler", "--num-runs", "1"]).wait())

        ti = session.query(TaskInstance).filter(TaskInstance.task_id == "create_entry_group").first()
        dag = DagBag(dag_folder=example_complex.__file__).dags['example_complex']
        task = dag.task_dict["create_entry_group"]
        ti.task = task
        self.assert_remote_logs("INFO - Task exited with return code 0", ti)

    def assert_remote_logs(self, expected_message, ti):
        with provide_gcp_context(GCP_GCS_KEY), conf_vars(
            {
                ('logging', 'remote_logging'): 'True',
                ('logging', 'remote_base_log_folder'): f"gs://{self.bucket_name}/path/to/logs",
                ('logging', 'remote_log_conn_id'): "google_cloud_default",
            }
        ):
            from airflow.config_templates import airflow_local_settings

            importlib.reload(airflow_local_settings)
            settings.configure_logging()

            task_log_reader = TaskLogReader()
            logs = "\n".join(task_log_reader.read_log_stream(ti, try_number=None, metadata={}))
            self.assertIn(expected_message, logs)
