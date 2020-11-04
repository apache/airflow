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

from parameterized import parameterized

from airflow import DAG
from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import DagBag, DagModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.dummy_operator import DummyOperator
from airflow.security import permissions
from airflow.utils.session import provide_session
from airflow.www import app
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags


class TestDagEndpoint(unittest.TestCase):
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
                (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
                (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG),
            ],
        )
        create_user(cls.app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore
        create_user(
            cls.app, username="test_granular_permissions", role_name="TestGranularDag"  # type: ignore
        )
        cls.app.appbuilder.sm.sync_perm_for_dag(  # type: ignore  # pylint: disable=no-member
            "TEST_DAG_1",
            access_control={'TestGranularDag': [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]},
        )

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
        delete_user(cls.app, username="test_granular_permissions")  # type: ignore

    def setUp(self) -> None:
        self.clean_db()
        self.client = self.app.test_client()  # type:ignore

    def tearDown(self) -> None:
        self.clean_db()

    @provide_session
    def _create_dag_models(self, count, session=None):
        for num in range(1, count + 1):
            dag_model = DagModel(
                dag_id=f"TEST_DAG_{num}",
                fileloc=f"/tmp/dag_{num}.py",
                schedule_interval="2 2 * * *",
            )
            session.add(dag_model)


class TestGetDag(TestDagEndpoint):
    def test_should_respond_200(self):
        self._create_dag_models(1)
        response = self.client.get("/api/v1/dags/TEST_DAG_1", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

        current_response = response.json
        current_response["fileloc"] = "/tmp/test-dag.py"
        self.assertEqual(
            {
                "dag_id": "TEST_DAG_1",
                "description": None,
                "fileloc": "/tmp/test-dag.py",
                "is_paused": False,
                "is_subdag": False,
                "owners": [],
                "root_dag_id": None,
                "schedule_interval": {"__type": "CronExpression", "value": "2 2 * * *"},
                "tags": [],
            },
            current_response,
        )

    @provide_session
    def test_should_respond_200_with_schedule_interval_none(self, session=None):
        dag_model = DagModel(
            dag_id="TEST_DAG_1",
            fileloc="/tmp/dag_1.py",
            schedule_interval=None,
        )
        session.add(dag_model)
        session.commit()
        response = self.client.get("/api/v1/dags/TEST_DAG_1", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

        current_response = response.json
        current_response["fileloc"] = "/tmp/test-dag.py"
        self.assertEqual(
            {
                "dag_id": "TEST_DAG_1",
                "description": None,
                "fileloc": "/tmp/test-dag.py",
                "is_paused": False,
                "is_subdag": False,
                "owners": [],
                "root_dag_id": None,
                "schedule_interval": None,
                "tags": [],
            },
            current_response,
        )

    def test_should_respond_200_with_granular_dag_access(self):
        self._create_dag_models(1)
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_1", environ_overrides={'REMOTE_USER': "test_granular_permissions"}
        )
        assert response.status_code == 200

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/dags/INVALID_DAG", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 404

    def test_should_raises_401_unauthenticated(self):
        self._create_dag_models(1)

        response = self.client.get("/api/v1/dags/TEST_DAG_1")

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403

    def test_should_respond_403_with_granular_access_for_different_dag(self):
        self._create_dag_models(3)
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_2", environ_overrides={'REMOTE_USER': "test_granular_permissions"}
        )
        assert response.status_code == 403


class TestGetDagDetails(TestDagEndpoint):
    def test_should_respond_200(self):
        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        expected = {
            "catchup": True,
            "concurrency": 16,
            "dag_id": "test_dag",
            "dag_run_timeout": None,
            "default_view": "tree",
            "description": None,
            "doc_md": "details",
            "fileloc": __file__,
            "is_paused": None,
            "is_subdag": False,
            "orientation": "LR",
            "owners": [],
            "schedule_interval": {
                "__type": "TimeDelta",
                "days": 1,
                "microseconds": 0,
                "seconds": 0,
            },
            "start_date": "2020-06-15T00:00:00+00:00",
            "tags": None,
            "timezone": "Timezone('UTC')",
        }
        assert response.json == expected

    def test_should_respond_200_serialized(self):
        # Create empty app with empty dagbag to check if DAG is read from db
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            app_serialized = app.create_app(testing=True)
        dag_bag = DagBag(os.devnull, include_examples=False, read_dags_from_db=True)
        app_serialized.dag_bag = dag_bag
        client = app_serialized.test_client()

        SerializedDagModel.write_dag(self.dag)

        expected = {
            "catchup": True,
            "concurrency": 16,
            "dag_id": "test_dag",
            "dag_run_timeout": None,
            "default_view": "tree",
            "description": None,
            "doc_md": "details",
            "fileloc": __file__,
            "is_paused": None,
            "is_subdag": False,
            "orientation": "LR",
            "owners": [],
            "schedule_interval": {
                "__type": "TimeDelta",
                "days": 1,
                "microseconds": 0,
                "seconds": 0,
            },
            "start_date": "2020-06-15T00:00:00+00:00",
            "tags": None,
            "timezone": "Timezone('UTC')",
        }
        response = client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={'REMOTE_USER': "test"}
        )

        assert response.status_code == 200
        assert response.json == expected

        response = self.client.get(
            f"/api/v1/dags/{self.dag_id}/details", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        expected = {
            'catchup': True,
            'concurrency': 16,
            'dag_id': 'test_dag',
            'dag_run_timeout': None,
            'default_view': 'tree',
            'description': None,
            'doc_md': 'details',
            'fileloc': __file__,
            'is_paused': None,
            'is_subdag': False,
            'orientation': 'LR',
            'owners': [],
            'schedule_interval': {'__type': 'TimeDelta', 'days': 1, 'microseconds': 0, 'seconds': 0},
            'start_date': '2020-06-15T00:00:00+00:00',
            'tags': None,
            'timezone': "Timezone('UTC')",
        }
        assert response.json == expected

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(f"/api/v1/dags/{self.dag_id}/details")

        assert_401(response)


class TestGetDags(TestDagEndpoint):
    def test_should_respond_200(self):
        self._create_dag_models(2)

        response = self.client.get("api/v1/dags", environ_overrides={'REMOTE_USER': "test"})

        assert response.status_code == 200
        self.assertEqual(
            {
                "dags": [
                    {
                        "dag_id": "TEST_DAG_1",
                        "description": None,
                        "fileloc": "/tmp/dag_1.py",
                        "is_paused": False,
                        "is_subdag": False,
                        "owners": [],
                        "root_dag_id": None,
                        "schedule_interval": {
                            "__type": "CronExpression",
                            "value": "2 2 * * *",
                        },
                        "tags": [],
                    },
                    {
                        "dag_id": "TEST_DAG_2",
                        "description": None,
                        "fileloc": "/tmp/dag_2.py",
                        "is_paused": False,
                        "is_subdag": False,
                        "owners": [],
                        "root_dag_id": None,
                        "schedule_interval": {
                            "__type": "CronExpression",
                            "value": "2 2 * * *",
                        },
                        "tags": [],
                    },
                ],
                "total_entries": 2,
            },
            response.json,
        )

    def test_should_respond_200_with_granular_dag_access(self):
        self._create_dag_models(3)
        response = self.client.get(
            "/api/v1/dags", environ_overrides={'REMOTE_USER': "test_granular_permissions"}
        )
        assert response.status_code == 200
        assert len(response.json['dags']) == 1
        assert response.json['dags'][0]['dag_id'] == 'TEST_DAG_1'

    @parameterized.expand(
        [
            ("api/v1/dags?limit=1", ["TEST_DAG_1"]),
            ("api/v1/dags?limit=2", ["TEST_DAG_1", "TEST_DAG_10"]),
            (
                "api/v1/dags?offset=5",
                ["TEST_DAG_5", "TEST_DAG_6", "TEST_DAG_7", "TEST_DAG_8", "TEST_DAG_9"],
            ),
            (
                "api/v1/dags?offset=0",
                [
                    "TEST_DAG_1",
                    "TEST_DAG_10",
                    "TEST_DAG_2",
                    "TEST_DAG_3",
                    "TEST_DAG_4",
                    "TEST_DAG_5",
                    "TEST_DAG_6",
                    "TEST_DAG_7",
                    "TEST_DAG_8",
                    "TEST_DAG_9",
                ],
            ),
            ("api/v1/dags?limit=1&offset=5", ["TEST_DAG_5"]),
            ("api/v1/dags?limit=1&offset=1", ["TEST_DAG_10"]),
            ("api/v1/dags?limit=2&offset=2", ["TEST_DAG_2", "TEST_DAG_3"]),
        ]
    )
    def test_should_respond_200_and_handle_pagination(self, url, expected_dag_ids):
        self._create_dag_models(10)

        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})

        assert response.status_code == 200

        dag_ids = [dag["dag_id"] for dag in response.json["dags"]]

        self.assertEqual(expected_dag_ids, dag_ids)
        self.assertEqual(10, response.json["total_entries"])

    def test_should_respond_200_default_limit(self):
        self._create_dag_models(101)

        response = self.client.get("api/v1/dags", environ_overrides={'REMOTE_USER': "test"})

        assert response.status_code == 200

        self.assertEqual(100, len(response.json["dags"]))
        self.assertEqual(101, response.json["total_entries"])

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("api/v1/dags")

        assert_401(response)

    def test_should_respond_403_unauthorized(self):
        self._create_dag_models(1)

        response = self.client.get("api/v1/dags", environ_overrides={'REMOTE_USER': "test_no_permissions"})

        assert response.status_code == 403


class TestPatchDag(TestDagEndpoint):
    def test_should_respond_200_on_patch_is_paused(self):
        dag_model = self._create_dag_model()
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}",
            json={
                "is_paused": False,
            },
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(response.status_code, 200)
        expected_response = {
            "dag_id": "TEST_DAG_1",
            "description": None,
            "fileloc": "/tmp/dag_1.py",
            "is_paused": False,
            "is_subdag": False,
            "owners": [],
            "root_dag_id": None,
            "schedule_interval": {
                "__type": "CronExpression",
                "value": "2 2 * * *",
            },
            "tags": [],
        }
        self.assertEqual(response.json, expected_response)

    def test_should_respond_200_on_patch_with_granular_dag_access(self):
        self._create_dag_models(1)
        response = self.client.patch(
            "/api/v1/dags/TEST_DAG_1",
            json={
                "is_paused": False,
            },
            environ_overrides={'REMOTE_USER': "test_granular_permissions"},
        )
        assert response.status_code == 200

    def test_should_respond_400_on_invalid_request(self):
        patch_body = {
            "is_paused": True,
            "schedule_interval": {
                "__type": "CronExpression",
                "value": "1 1 * * *",
            },
        }
        dag_model = self._create_dag_model()
        response = self.client.patch(f"/api/v1/dags/{dag_model.dag_id}", json=patch_body)
        self.assertEqual(response.status_code, 400)
        self.assertEqual(
            response.json,
            {
                'detail': "Property is read-only - 'schedule_interval'",
                'status': 400,
                'title': 'Bad Request',
                'type': EXCEPTIONS_LINK_MAP[400],
            },
        )

    def test_should_respond_404(self):
        response = self.client.get("/api/v1/dags/INVALID_DAG", environ_overrides={'REMOTE_USER': "test"})
        self.assertEqual(response.status_code, 404)

    @provide_session
    def _create_dag_model(self, session=None):
        dag_model = DagModel(
            dag_id="TEST_DAG_1", fileloc="/tmp/dag_1.py", schedule_interval="2 2 * * *", is_paused=True
        )
        session.add(dag_model)
        return dag_model

    def test_should_raises_401_unauthenticated(self):
        dag_model = self._create_dag_model()
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}",
            json={
                "is_paused": False,
            },
        )

        assert_401(response)

    def test_should_respond_200_with_update_mask(self):
        dag_model = self._create_dag_model()
        payload = {
            "is_paused": False,
        }
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}?update_mask=is_paused",
            json=payload,
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(response.status_code, 200)
        expected_response = {
            "dag_id": "TEST_DAG_1",
            "description": None,
            "fileloc": "/tmp/dag_1.py",
            "is_paused": False,
            "is_subdag": False,
            "owners": [],
            "root_dag_id": None,
            "schedule_interval": {
                "__type": "CronExpression",
                "value": "2 2 * * *",
            },
            "tags": [],
        }
        self.assertEqual(response.json, expected_response)

    @parameterized.expand(
        [
            (
                {
                    "is_paused": True,
                },
                "update_mask=description",
                "Only `is_paused` field can be updated through the REST API",
            ),
            (
                {
                    "is_paused": True,
                },
                "update_mask=schedule_interval, description",
                "Only `is_paused` field can be updated through the REST API",
            ),
        ]
    )
    def test_should_respond_400_for_invalid_fields_in_update_mask(self, payload, update_mask, error_message):
        dag_model = self._create_dag_model()

        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}?{update_mask}",
            json=payload,
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json['detail'], error_message)

    def test_should_respond_403_unauthorized(self):
        dag_model = self._create_dag_model()
        response = self.client.patch(
            f"/api/v1/dags/{dag_model.dag_id}",
            json={
                "is_paused": False,
            },
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )

        assert response.status_code == 403
