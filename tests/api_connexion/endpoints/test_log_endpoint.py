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
import copy
import logging.config
import os
import shutil
import sys
import tempfile
import unittest

from itsdangerous.url_safe import URLSafeSerializer

from airflow import DAG, settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DagRun, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session
from airflow.utils.types import DagRunType
from airflow.www import app
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs


class TestGetLog(unittest.TestCase):
    DAG_ID = 'dag_for_testing_log_endpoint'
    DAG_ID_REMOVED = 'removed_dag_for_testing_log_endpoint'
    TASK_ID = 'task_for_testing_log_endpoint'
    TRY_NUMBER = 1

    @classmethod
    def setUpClass(cls):
        settings.configure_orm()
        cls.session = settings.Session
        cls.app = app.create_app(testing=True)

    def setUp(self) -> None:
        self.default_time = "2020-06-10T20:00:00+00:00"

        # Make sure that the configure_logging is not cached
        self.old_modules = dict(sys.modules)

        # Create a custom logging configuration
        logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        logging_config['handlers']['task']['base_log_folder'] = os.path.normpath(
            os.path.join(current_dir, 'test_logs'))

        logging_config['handlers']['task']['filename_template'] = \
            '{{ ti.dag_id }}/{{ ti.task_id }}/' \
            '{{ ts | replace(":", ".") }}/{{ try_number }}.log'

        # Write the custom logging configuration to a file
        self.settings_folder = tempfile.mkdtemp()
        settings_file = os.path.join(self.settings_folder, "airflow_local_settings.py")
        new_logging_file = "LOGGING_CONFIG = {}".format(logging_config)
        with open(settings_file, 'w') as handle:
            handle.writelines(new_logging_file)
        sys.path.append(self.settings_folder)

        with conf_vars({('logging', 'logging_config_class'): 'airflow_local_settings.LOGGING_CONFIG'}):
            self.app = app.create_app(testing=True)  # type:ignore
            self.client = self.app.test_client()  # type:ignore
            settings.configure_orm()
            from airflow.api_connexion.endpoints.log_endpoint import dagbag
            dag = DAG(self.DAG_ID, start_date=timezone.parse(self.default_time))
            dag.sync_to_db()
            dag_removed = DAG(self.DAG_ID_REMOVED, start_date=timezone.parse(self.default_time))
            dag_removed.sync_to_db()
            dagbag.bag_dag(dag, parent_dag=dag, root_dag=dag)
            with create_session() as session:
                self.ti = TaskInstance(
                    task=DummyOperator(task_id=self.TASK_ID, dag=dag),
                    execution_date=timezone.parse(self.default_time)
                )
                self.ti.try_number = 1
                self.ti_removed_dag = TaskInstance(
                    task=DummyOperator(task_id=self.TASK_ID, dag=dag_removed),
                    execution_date=timezone.parse(self.default_time)
                )
                self.ti_removed_dag.try_number = 1

                session.merge(self.ti)
                session.merge(self.ti_removed_dag)

    def _create_dagrun(self, session):
        dagrun_model = DagRun(
            dag_id=self.DAG_ID,
            run_id='TEST_DAG_RUN_ID',
            run_type=DagRunType.MANUAL.value,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
        )
        session.add(dagrun_model)
        session.commit()

    def tearDown(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        clear_db_runs()

        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

        sys.path.remove(self.settings_folder)
        shutil.rmtree(self.settings_folder)

        super().tearDown()

    @provide_session
    def test_should_response_200_json(self, session):
        self._create_dagrun(session)
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})
        headers = {'Content-Type': 'application/json'}
        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/{self.TASK_ID}/logs/1?token={token}",
            headers=headers
        )
        self.assertIn('content', response.json)
        self.assertIn('continuation_token', response.json)
        self.assertIn('Log for testing.', response.json.get('content'))
        self.assertEqual(200, response.status_code)

    @provide_session
    def test_should_response_200_text_plain(self, session):
        self._create_dagrun(session)
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/{self.TASK_ID}/logs/1?token={token}",
        )
        assert response.status_code == 200
        expected_filename = '{}/{}/{}/{}.log'.format(self.DAG_ID,
                                                     self.TASK_ID,
                                                     self.default_time,
                                                     self.TRY_NUMBER)

        content_disposition = response.headers.get('Content-Disposition')
        self.assertTrue(content_disposition.startswith('attachment'))
        self.assertTrue(expected_filename in content_disposition)
        self.assertEqual(200, response.status_code)
        self.assertIn('Log for testing.', response.data.decode('utf-8'))
