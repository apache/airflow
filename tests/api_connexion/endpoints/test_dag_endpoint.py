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

from airflow import DAG
from airflow.models import DagBag
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.dummy_operator import DummyOperator
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


class TestGetDag(TestDagEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.get("/api/v1/dags/1/")
        assert response.status_code == 200


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
        app_serialized = app.create_app(testing=True)  # type:ignore
        dag_bag = DagBag(os.devnull, include_examples=False, store_serialized_dags=True)
        app_serialized.dag_bag = dag_bag  # type:ignore
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
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.get("/api/v1/dags/1")
        assert response.status_code == 200


class TestPatchDag(TestDagEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.patch("/api/v1/dags/1")
        assert response.status_code == 200
