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
import ast
import os
import unittest
from unittest import mock

from itsdangerous import URLSafeSerializer
from parameterized import parameterized

from airflow import DAG
from airflow.configuration import conf
from airflow.models import DagBag
from airflow.www import app
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dag_code, clear_db_dags, clear_db_serialized_dags

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
EXAMPLE_DAG_FILE = os.path.join("airflow", "example_dags", "example_bash_operator.py")


class TestGetSource(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore
        # TODO: Add new role for each view to test permission.
        create_user(cls.app, username="test", role="Admin")  # type: ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        self.clear_db()

    def tearDown(self) -> None:
        self.clear_db()

    @staticmethod
    def clear_db():
        clear_db_dags()
        clear_db_serialized_dags()
        clear_db_dag_code()

    @staticmethod
    def _get_dag_file_docstring(fileloc: str) -> str:
        with open(fileloc) as f:
            file_contents = f.read()
        module = ast.parse(file_contents)
        docstring = ast.get_docstring(module)
        return docstring

    @parameterized.expand([(True,), (False,)])
    def test_should_response_200_text(self, store_dag_code):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        with mock.patch("airflow.models.dag.settings.STORE_DAG_CODE", store_dag_code), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", store_dag_code
        ):
            dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
            dagbag.sync_to_db()
            first_dag: DAG = next(iter(dagbag.dags.values()))
            dag_docstring = self._get_dag_file_docstring(first_dag.fileloc)

            url = f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}"
            response = self.client.get(
                url, headers={"Accept": "text/plain"}, environ_overrides={'REMOTE_USER': "test"}
            )

            self.assertEqual(200, response.status_code)
            self.assertIn(dag_docstring, response.data.decode())
            self.assertEqual('text/plain', response.headers['Content-Type'])

    @parameterized.expand([(True,), (False,)])
    def test_should_response_200_json(self, store_dag_code):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        with mock.patch("airflow.models.dag.settings.STORE_DAG_CODE", store_dag_code), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", store_dag_code
        ):
            dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
            dagbag.sync_to_db()
            first_dag: DAG = next(iter(dagbag.dags.values()))
            dag_docstring = self._get_dag_file_docstring(first_dag.fileloc)

            url = f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}"
            response = self.client.get(
                url, headers={"Accept": 'application/json'}, environ_overrides={'REMOTE_USER': "test"}
            )

            self.assertEqual(200, response.status_code)
            self.assertIn(dag_docstring, response.json['content'])
            self.assertEqual('application/json', response.headers['Content-Type'])

    @parameterized.expand([(True,), (False,)])
    def test_should_response_406(self, store_dag_code):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        with mock.patch("airflow.models.dag.settings.STORE_DAG_CODE", store_dag_code), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", store_dag_code
        ):
            dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
            dagbag.sync_to_db()
            first_dag: DAG = next(iter(dagbag.dags.values()))

            url = f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}"
            response = self.client.get(
                url, headers={"Accept": 'image/webp'}, environ_overrides={'REMOTE_USER': "test"}
            )

            self.assertEqual(406, response.status_code)

    @parameterized.expand([(True,), (False,)])
    def test_should_response_404(self, store_dag_code):
        with mock.patch("airflow.models.dag.settings.STORE_DAG_CODE", store_dag_code), mock.patch(
            "airflow.models.dagcode.STORE_DAG_CODE", store_dag_code
        ):
            wrong_fileloc = "abcd1234"
            url = f"/api/v1/dagSources/{wrong_fileloc}"
            response = self.client.get(
                url, headers={"Accept": 'application/json'}, environ_overrides={'REMOTE_USER': "test"}
            )

            self.assertEqual(404, response.status_code)

    def test_should_raises_401_unauthenticated(self):
        serializer = URLSafeSerializer(conf.get('webserver', 'SECRET_KEY'))
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        first_dag: DAG = next(iter(dagbag.dags.values()))

        response = self.client.get(
            f"/api/v1/dagSources/{serializer.dumps(first_dag.fileloc)}",
            headers={"Accept": "text/plain"},
        )

        assert_401(response)
