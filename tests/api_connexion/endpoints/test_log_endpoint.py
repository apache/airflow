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
from unittest import mock
from unittest.mock import PropertyMock

from itsdangerous.url_safe import URLSafeSerializer

from airflow import DAG, settings
from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.models import DagRun, TaskInstance
from airflow.operators.dummy_operator import DummyOperator
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session
from airflow.utils.types import DagRunType
from airflow.www import app
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_runs


class TestGetLog(unittest.TestCase):
    DAG_ID = 'dag_for_testing_log_endpoint'
    TASK_ID = 'task_for_testing_log_endpoint'
    TRY_NUMBER = 1

    @classmethod
    def setUpClass(cls):
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)

        create_user(
            cls.app,
            username="test",
            role_name="Test",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK_INSTANCE),
            ],
        )
        create_user(cls.app, username="test_no_permissions", role_name="TestNoPermissions")

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")
        delete_user(cls.app, username="test_no_permissions")

    def setUp(self) -> None:
        self.default_time = "2020-06-10T20:00:00+00:00"
        self.client = self.app.test_client()
        self.log_dir = tempfile.mkdtemp()
        # Make sure that the configure_logging is not cached
        self.old_modules = dict(sys.modules)
        self._prepare_log_files()
        self._configure_loggers()
        self._prepare_db()

    def _create_dagrun(self, session):
        dagrun_model = DagRun(
            dag_id=self.DAG_ID,
            run_id='TEST_DAG_RUN_ID',
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
        )
        session.add(dagrun_model)
        session.commit()

    def _configure_loggers(self):
        # Create a custom logging configuration
        logging_config = copy.deepcopy(DEFAULT_LOGGING_CONFIG)
        logging_config['handlers']['task']['base_log_folder'] = self.log_dir

        logging_config['handlers']['task'][
            'filename_template'
        ] = '{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts | replace(":", ".") }}/{{ try_number }}.log'

        # Write the custom logging configuration to a file
        self.settings_folder = tempfile.mkdtemp()
        settings_file = os.path.join(self.settings_folder, "airflow_local_settings.py")
        new_logging_file = f"LOGGING_CONFIG = {logging_config}"
        with open(settings_file, 'w') as handle:
            handle.writelines(new_logging_file)
        sys.path.append(self.settings_folder)

        with conf_vars(
            {
                ('logging', 'logging_config_class'): 'airflow_local_settings.LOGGING_CONFIG',
                ("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend",
            }
        ):
            self.app = app.create_app(testing=True)
            self.client = self.app.test_client()
            settings.configure_logging()

    def _prepare_db(self):
        dagbag = self.app.dag_bag  # pylint: disable=no-member
        dag = DAG(self.DAG_ID, start_date=timezone.parse(self.default_time))
        dag.sync_to_db()
        dagbag.bag_dag(dag=dag, root_dag=dag)
        with create_session() as session:
            self.ti = TaskInstance(
                task=DummyOperator(task_id=self.TASK_ID, dag=dag),
                execution_date=timezone.parse(self.default_time),
            )
            self.ti.try_number = 1
            session.merge(self.ti)

    def _prepare_log_files(self):
        dir_path = f"{self.log_dir}/{self.DAG_ID}/{self.TASK_ID}/" f"{self.default_time.replace(':', '.')}/"
        os.makedirs(dir_path)
        with open(f"{dir_path}/1.log", "w+") as file:
            file.write("Log for testing.")
            file.flush()

    def tearDown(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        clear_db_runs()

        # Remove any new modules imported during the test run. This lets us
        # import the same source files for more than one test.
        for mod in [m for m in sys.modules if m not in self.old_modules]:
            del sys.modules[mod]

        sys.path.remove(self.settings_folder)
        shutil.rmtree(self.settings_folder)
        shutil.rmtree(self.log_dir)

        super().tearDown()

    @provide_session
    def test_should_respond_200_json(self, session):
        self._create_dagrun(session)
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})
        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/{self.TASK_ID}/logs/1?token={token}",
            headers={'Accept': 'application/json'},
            environ_overrides={'REMOTE_USER': "test"},
        )
        expected_filename = "{}/{}/{}/{}/1.log".format(
            self.log_dir, self.DAG_ID, self.TASK_ID, self.default_time.replace(":", ".")
        )
        self.assertEqual(
            response.json['content'],
            f"[('', '*** Reading local file: {expected_filename}\\nLog for testing.')]",
        )
        info = serializer.loads(response.json['continuation_token'])
        self.assertEqual(info, {'end_of_log': True})
        self.assertEqual(200, response.status_code)

    @provide_session
    def test_should_respond_200_text_plain(self, session):
        self._create_dagrun(session)
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/{self.TASK_ID}/logs/1?token={token}",
            headers={'Accept': 'text/plain'},
            environ_overrides={'REMOTE_USER': "test"},
        )
        expected_filename = "{}/{}/{}/{}/1.log".format(
            self.log_dir, self.DAG_ID, self.TASK_ID, self.default_time.replace(':', '.')
        )
        self.assertEqual(200, response.status_code)
        self.assertEqual(
            response.data.decode('utf-8'),
            f"\n*** Reading local file: {expected_filename}\nLog for testing.\n",
        )

    @provide_session
    def test_get_logs_response_with_ti_equal_to_none(self, session):
        self._create_dagrun(session)
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/Invalid-Task-ID/logs/1?token={token}",
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['detail'], "Task instance did not exist in the DB")

    @provide_session
    def test_get_logs_with_metadata_as_download_large_file(self, session):
        self._create_dagrun(session)
        with mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.read") as read_mock:
            first_return = ([[('', '1st line')]], [{}])
            second_return = ([[('', '2nd line')]], [{'end_of_log': False}])
            third_return = ([[('', '3rd line')]], [{'end_of_log': True}])
            fourth_return = ([[('', 'should never be read')]], [{'end_of_log': True}])
            read_mock.side_effect = [first_return, second_return, third_return, fourth_return]

            response = self.client.get(
                f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
                f"taskInstances/{self.TASK_ID}/logs/1?full_content=True",
                headers={"Accept": 'text/plain'},
                environ_overrides={'REMOTE_USER': "test"},
            )

            self.assertIn('1st line', response.data.decode('utf-8'))
            self.assertIn('2nd line', response.data.decode('utf-8'))
            self.assertIn('3rd line', response.data.decode('utf-8'))
            self.assertNotIn('should never be read', response.data.decode('utf-8'))

    @mock.patch("airflow.api_connexion.endpoints.log_endpoint.TaskLogReader")
    def test_get_logs_for_handler_without_read_method(self, mock_log_reader):
        type(mock_log_reader.return_value).supports_read = PropertyMock(return_value=False)

        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})

        # check guessing
        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/{self.TASK_ID}/logs/1?token={token}",
            headers={'Content-Type': 'application/jso'},
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(400, response.status_code)
        self.assertIn('Task log handler does not support read logs.', response.data.decode('utf-8'))

    @provide_session
    def test_bad_signature_raises(self, session):
        self._create_dagrun(session)
        token = {"download_logs": False}

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/{self.TASK_ID}/logs/1?token={token}",
            headers={'Accept': 'application/json'},
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(
            response.json,
            {
                'detail': None,
                'status': 400,
                'title': "Bad Signature. Please use only the tokens provided by the API.",
                'type': EXCEPTIONS_LINK_MAP[400],
            },
        )

    def test_raises_404_for_invalid_dag_run_id(self):
        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN/"  # invalid dagrun_id
            f"taskInstances/{self.TASK_ID}/logs/1?",
            headers={'Accept': 'application/json'},
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(
            response.json,
            {'detail': None, 'status': 404, 'title': "DAG Run not found", 'type': EXCEPTIONS_LINK_MAP[404]},
        )

    def test_should_raises_401_unauthenticated(self):
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": False})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/{self.TASK_ID}/logs/1?token={token}",
            headers={'Accept': 'application/json'},
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        key = self.app.config["SECRET_KEY"]
        serializer = URLSafeSerializer(key)
        token = serializer.dumps({"download_logs": True})

        response = self.client.get(
            f"api/v1/dags/{self.DAG_ID}/dagRuns/TEST_DAG_RUN_ID/"
            f"taskInstances/{self.TASK_ID}/logs/1?token={token}",
            headers={'Accept': 'text/plain'},
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403
