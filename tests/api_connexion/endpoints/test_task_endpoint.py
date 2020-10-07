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
import os
import unittest
from datetime import datetime

from airflow import DAG
from airflow.models import DagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.dummy_operator import DummyOperator
from airflow.security import permissions
from airflow.www import app
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags


class TestTaskEndpoint(unittest.TestCase):
    dag_id = "test_dag"
    task_id = "op1"

    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore
        create_user(
            cls.app,  # type: ignore
            username="test",
            role_name="Test",
            permissions=[
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAGS),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_TASK),
            ],
        )
        create_user(cls.app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

        with DAG(cls.dag_id, start_date=datetime(2020, 6, 15), doc_md="details") as dag:
            DummyOperator(task_id=cls.task_id)

        cls.dag = dag  # type:ignore
        dag_bag = DagBag(os.devnull, include_examples=False)
        dag_bag.dags = {dag.dag_id: dag}
        cls.app.dag_bag = dag_bag  # type:ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore
        delete_user(cls.app, username="test_no_permissions")  # type: ignore

    def setUp(self) -> None:
        self.clean_db()
        self.client = self.app.test_client()  # type:ignore

    def tearDown(self) -> None:
        self.clean_db()


class TestGetTask(TestTaskEndpoint):
    def test_should_response_200(self):
        expected = {
            "class_ref": {
                "class_name": "DummyOperator",
                "module_path": "airflow.operators.dummy_operator",
            },
            "depends_on_past": False,
            "downstream_task_ids": [],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "owner": "airflow",
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": False,
            "start_date": "2020-06-15T00:00:00+00:00",
            "task_id": "op1",
            "template_fields": [],
            "trigger_rule": "all_success",
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
            "wait_for_downstream": False,
            "weight_rule": "downstream",
        }
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks/{self.task_id}", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == expected

    def test_should_response_200_serialized(self):
        # Create empty app with empty dagbag to check if DAG is read from db
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            app_serialized = app.create_app(testing=True)
        dag_bag = DagBag(os.devnull, include_examples=False, read_dags_from_db=True)
        app_serialized.dag_bag = dag_bag
        client = app_serialized.test_client()

        SerializedDagModel.write_dag(self.dag)

        expected = {
            "class_ref": {
                "class_name": "DummyOperator",
                "module_path": "airflow.operators.dummy_operator",
            },
            "depends_on_past": False,
            "downstream_task_ids": [],
            "end_date": None,
            "execution_timeout": None,
            "extra_links": [],
            "owner": "airflow",
            "pool": "default_pool",
            "pool_slots": 1.0,
            "priority_weight": 1.0,
            "queue": "default",
            "retries": 0.0,
            "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
            "retry_exponential_backoff": False,
            "start_date": "2020-06-15T00:00:00+00:00",
            "task_id": "op1",
            "template_fields": [],
            "trigger_rule": "all_success",
            "ui_color": "#e8f7e4",
            "ui_fgcolor": "#000",
            "wait_for_downstream": False,
            "weight_rule": "downstream",
        }
        response = client.get(
            f"/api/v1/dags/{self.dag_id}/tasks/{self.task_id}", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == expected

    def test_should_response_404(self):
        task_id = "xxxx_not_existing"
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks/{task_id}", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(f"/api/v1/dags/{self.dag_id}/tasks/{self.task_id}")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403


class TestGetTasks(TestTaskEndpoint):
    def test_should_response_200(self):
        expected = {
            "tasks": [
                {
                    "class_ref": {
                        "class_name": "DummyOperator",
                        "module_path": "airflow.operators.dummy_operator",
                    },
                    "depends_on_past": False,
                    "downstream_task_ids": [],
                    "end_date": None,
                    "execution_timeout": None,
                    "extra_links": [],
                    "owner": "airflow",
                    "pool": "default_pool",
                    "pool_slots": 1.0,
                    "priority_weight": 1.0,
                    "queue": "default",
                    "retries": 0.0,
                    "retry_delay": {"__type": "TimeDelta", "days": 0, "seconds": 300, "microseconds": 0},
                    "retry_exponential_backoff": False,
                    "start_date": "2020-06-15T00:00:00+00:00",
                    "task_id": "op1",
                    "template_fields": [],
                    "trigger_rule": "all_success",
                    "ui_color": "#e8f7e4",
                    "ui_fgcolor": "#000",
                    "wait_for_downstream": False,
                    "weight_rule": "downstream",
                }
            ],
            "total_entries": 1,
        }
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/tasks", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == expected

    def test_should_response_404(self):
        dag_id = "xxxx_not_existing"
        response = self.client.get(f"/api/v1/dags/{dag_id}/tasks", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(f"/api/v1/dags/{self.dag_id}/tasks")

        assert_401(response)
