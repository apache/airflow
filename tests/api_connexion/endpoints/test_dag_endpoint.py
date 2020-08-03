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

import pytest
from parameterized import parameterized

from airflow import DAG
from airflow.models import DagBag, DagModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.session import provide_session
from airflow.www import app
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
        cls.app = app.create_app(testing=True)  # type:ignore

        with DAG(cls.dag_id, start_date=datetime(2020, 6, 15), doc_md="details") as dag:
            DummyOperator(task_id=cls.task_id)

        cls.dag = dag  # type:ignore
        dag_bag = DagBag(os.devnull, include_examples=False)
        dag_bag.dags = {dag.dag_id: dag}
        cls.app.dag_bag = dag_bag  # type:ignore

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
                schedule_interval="2 2 * * *"
            )
            session.add(dag_model)


class TestGetDag(TestDagEndpoint):
    def test_should_response_200(self):
        self._create_dag_models(1)
        response = self.client.get("/api/v1/dags/TEST_DAG_1")
        assert response.status_code == 200

        current_response = response.json
        current_response["fileloc"] = "/tmp/test-dag.py"
        self.assertEqual({
            'dag_id': 'TEST_DAG_1',
            'description': None,
            'fileloc': '/tmp/test-dag.py',
            'is_paused': False,
            'is_subdag': False,
            'owners': [],
            'root_dag_id': None,
            'schedule_interval': {'__type': 'CronExpression', 'value': '2 2 * * *'},
            'tags': []
        }, current_response)

    def test_should_response_404(self):
        response = self.client.get("/api/v1/dags/INVALID_DAG")
        assert response.status_code == 404


class TestGetDagDetails(TestDagEndpoint):
    def test_should_response_200(self):
        response = self.client.get(f"/api/v1/dags/{self.dag_id}/details")
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
            'schedule_interval': {
                '__type': 'TimeDelta',
                'days': 1,
                'microseconds': 0,
                'seconds': 0
            },
            'start_date': '2020-06-15T00:00:00+00:00',
            'tags': None,
            'timezone': "Timezone('UTC')"
        }
        assert response.json == expected

    def test_should_response_200_serialized(self):
        # Create empty app with empty dagbag to check if DAG is read from db
        app_serialized = app.create_app(testing=True)
        dag_bag = DagBag(os.devnull, include_examples=False, read_dags_from_db=True)
        app_serialized.dag_bag = dag_bag
        client = app_serialized.test_client()

        SerializedDagModel.write_dag(self.dag)

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
            'schedule_interval': {
                '__type': 'TimeDelta',
                'days': 1,
                'microseconds': 0,
                'seconds': 0
            },
            'start_date': '2020-06-15T00:00:00+00:00',
            'tags': None,
            'timezone': "Timezone('UTC')"
        }
        response = client.get(f"/api/v1/dags/{self.dag_id}/details")
        assert response.status_code == 200
        assert response.json == expected


class TestGetDags(TestDagEndpoint):

    def test_should_response_200(self):
        self._create_dag_models(2)

        response = self.client.get("api/v1/dags")

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
                        "schedule_interval": {"__type": "CronExpression", "value": "2 2 * * *"},
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
                        "schedule_interval": {"__type": "CronExpression", "value": "2 2 * * *"},
                        "tags": [],
                    },
                ],
                "total_entries": 2,
            },
            response.json,
        )

    @parameterized.expand(
        [
            ("api/v1/dags?limit=1", ["TEST_DAG_1"]),
            ("api/v1/dags?limit=2", ["TEST_DAG_1", "TEST_DAG_10"]),
            (
                "api/v1/dags?offset=5",
                [
                    "TEST_DAG_5",
                    "TEST_DAG_6",
                    "TEST_DAG_7",
                    "TEST_DAG_8",
                    "TEST_DAG_9",
                ],
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
    def test_should_response_200_and_handle_pagination(self, url, expected_dag_ids):
        self._create_dag_models(10)

        response = self.client.get(url)

        assert response.status_code == 200

        dag_ids = [dag["dag_id"] for dag in response.json['dags']]

        self.assertEqual(expected_dag_ids, dag_ids)
        self.assertEqual(10, response.json['total_entries'])

    def test_should_response_200_default_limit(self):
        self._create_dag_models(101)

        response = self.client.get("api/v1/dags")

        assert response.status_code == 200

        self.assertEqual(100, len(response.json['dags']))
        self.assertEqual(101, response.json['total_entries'])


class TestPatchDag(TestDagEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.patch("/api/v1/dags/1")
        assert response.status_code == 200
