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
import unittest
from datetime import timedelta

import pytest

from airflow.models import DagRun
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType
from airflow.www import app
from tests.test_utils.db import clear_db_runs


class TestDagRunEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.app = app.create_app(testing=True)  # type:ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore
        self.now = timezone.utcnow()
        clear_db_runs()

    def tearDown(self) -> None:
        clear_db_runs()


class TestDeleteDagRun(TestDagRunEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.delete("api/v1/dags/TEST_DAG_ID}/dagRuns/TEST_DAG_RUN_ID")
        assert response.status_code == 204


class TestGetDagRun(TestDagRunEndpoint):

    @provide_session
    def test_should_response_200(self, session):
        dagrun_model = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,
            start_date=self.now,
            external_trigger=True,
        )
        session.add(dagrun_model)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 1
        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID")
        assert response.status_code == 200
        self.assertEqual(
            response.json,
            {
                'dag_id': 'TEST_DAG_ID',
                'dag_run_id': 'TEST_DAG_RUN_ID',
                'end_date': None,
                'state': 'running',
                'execution_date': self.now.isoformat(),
                'external_trigger': True,
                'start_date': self.now.isoformat(),
                'conf': {},
            }
        )

    def test_should_response_404(self):
        response = self.client.get("api/v1/dags/invalid-id/dagRuns/invalid-id")
        assert response.status_code == 404
        self.assertEqual(
            {
                'detail': None,
                'status': 404,
                'title': 'DAGRun not found',
                'type': 'about:blank'
            },
            response.json
        )


class TestGetDagRuns(TestDagRunEndpoint):

    @provide_session
    def test_should_response_200(self, session):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,
            start_date=self.now,
            external_trigger=True,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(minutes=1),
            start_date=self.now,
            external_trigger=True,
        )
        session.add_all([dagrun_model_1, dagrun_model_2])
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 2
        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns")
        assert response.status_code == 200
        self.assertEqual(
            response.json,
            {
                "dag_runs": [
                    {
                        'dag_id': 'TEST_DAG_ID',
                        'dag_run_id': 'TEST_DAG_RUN_ID_1',
                        'end_date': None,
                        'state': 'running',
                        'execution_date': self.now.isoformat(),
                        'external_trigger': True,
                        'start_date': self.now.isoformat(),
                        'conf': {},
                    },
                    {
                        'dag_id': 'TEST_DAG_ID',
                        'dag_run_id': 'TEST_DAG_RUN_ID_2',
                        'end_date': None,
                        'state': 'running',
                        'execution_date': (self.now + timedelta(minutes=1)).isoformat(),
                        'external_trigger': True,
                        'start_date': self.now.isoformat(),
                        'conf': {},
                    }
                ],
                "total_entries": 2
            }
        )

    @provide_session
    def test_should_return_all_with_tilde_as_dag_id(self, session):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,
            start_date=self.now,
            external_trigger=True,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(minutes=1),
            start_date=self.now,
            external_trigger=True,
        )
        session.add_all([dagrun_model_1, dagrun_model_2])
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 2
        response = self.client.get("api/v1/dags/~/dagRuns")
        assert response.status_code == 200
        self.assertEqual(
            response.json,
            {
                "dag_runs": [
                    {
                        'dag_id': 'TEST_DAG_ID',
                        'dag_run_id': 'TEST_DAG_RUN_ID_1',
                        'end_date': None,
                        'state': 'running',
                        'execution_date': self.now.isoformat(),
                        'external_trigger': True,
                        'start_date': self.now.isoformat(),
                        'conf': {},
                    },
                    {
                        'dag_id': 'TEST_DAG_ID',
                        'dag_run_id': 'TEST_DAG_RUN_ID_2',
                        'end_date': None,
                        'state': 'running',
                        'execution_date': (self.now + timedelta(minutes=1)).isoformat(),
                        'external_trigger': True,
                        'start_date': self.now.isoformat(),
                        'conf': {},
                    }
                ],
                "total_entries": 2
            }
        )

    @provide_session
    def test_handle_limit_in_query(self, session):
        dagrun_models = [DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID' + str(i),
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(minutes=i),
            start_date=self.now,
            external_trigger=True,
        ) for i in range(100)]

        session.add_all(dagrun_models)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 100

        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?limit=1"
        )
        assert response.status_code == 200
        self.assertEqual(response.json.get('total_entries'), 1)
        self.assertEqual(
            response.json,
            {
                "dag_runs": [
                    {
                        'dag_id': 'TEST_DAG_ID',
                        'dag_run_id': 'TEST_DAG_RUN_ID0',
                        'end_date': None,
                        'state': 'running',
                        'execution_date': (self.now + timedelta(minutes=0)).isoformat(),
                        'external_trigger': True,
                        'start_date': self.now.isoformat(),
                        'conf': {},
                    }
                ],
                "total_entries": 1
            }
        )

    @provide_session
    def test_handle_offset_in_query(self, session):
        dagrun_models = [DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID' + str(i),
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(minutes=i),
            start_date=self.now,
            external_trigger=True,
        ) for i in range(4)]

        session.add_all(dagrun_models)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 4

        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?offset=1"
        )
        assert response.status_code == 200
        self.assertEqual(response.json.get('total_entries'), 3)

    @provide_session
    def test_handle_limit_and_offset_in_query(self, session):
        dagrun_models = [DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID' + str(i),
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(minutes=i),
            start_date=self.now,
            external_trigger=True,
        ) for i in range(10)]

        session.add_all(dagrun_models)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 10

        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?limit=6&offset=5"
        )
        assert response.status_code == 200
        self.assertEqual(response.json.get('total_entries'), 5)


class TestGetDagRunsStartDateFilter(TestDagRunEndpoint):

    @provide_session
    def test_start_date_gte_and_lte(self, session):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,
            start_date=self.now,  # today
            external_trigger=True,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(minutes=1),
            start_date=self.now + timedelta(days=3),  # next 3 days
            external_trigger=True,
        )
        session.add_all([dagrun_model_1, dagrun_model_2])
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 2
        start_date_gte = self.now + timedelta(days=1)  # gte tomorrow
        start_date_lte = self.now + timedelta(days=10)  # lte next 10 days

        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns?start_date_gte={start_date_gte}"
            f"&start_date_lte={start_date_lte}"
        )
        assert response.status_code == 200
        self.assertEqual(response.json.get('total_entries'), 1)
        self.assertEqual(response.json.get('dag_runs')[0].get('start_date'),
                         (self.now + timedelta(days=3)).isoformat())

    @provide_session
    def test_only_start_date_gte_provided(self, session):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,
            start_date=self.now,  # today
            external_trigger=True,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(minutes=1),
            start_date=self.now + timedelta(days=3),  # next 3 days
            external_trigger=True,
        )
        session.add_all([dagrun_model_1, dagrun_model_2])
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 2
        start_date_gte = self.now + timedelta(days=1)  # gte tomorrow
        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns?start_date_gte={start_date_gte}"
        )
        assert response.status_code == 200
        self.assertEqual(response.json.get('total_entries'), 1)
        self.assertEqual(response.json.get('dag_runs')[0].get('start_date'),
                         (self.now + timedelta(days=3)).isoformat())

    @provide_session
    def test_only_start_date_lte_provided(self, session):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,
            start_date=self.now,  # today
            external_trigger=True,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(minutes=1),
            start_date=self.now + timedelta(days=3),  # next 3 days
            external_trigger=True,
        )
        session.add_all([dagrun_model_1, dagrun_model_2])
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 2
        assert result[0].start_date == self.now
        start_date_lte = self.now + timedelta(days=1)  # lte tomorrow
        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns?start_date_lte={start_date_lte}"
        )
        assert response.status_code == 200

        self.assertEqual(response.json.get('total_entries'), 1)
        self.assertEqual(response.json.get('dag_runs')[0].get('start_date'),
                         self.now.isoformat())  # today


class TestGetDagRunsExecutionDateFilter(TestDagRunEndpoint):

    @provide_session
    def test_execution_date_gte_and_lte(self, session):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,  # today
            start_date=self.now,
            external_trigger=True,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(days=3),  # next 3 days,
            start_date=self.now,
            external_trigger=True,
        )
        session.add_all([dagrun_model_1, dagrun_model_2])
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 2
        execution_date_gte = self.now + timedelta(days=1)  # gte tomorrow
        execution_date_lte = self.now + timedelta(days=10)  # lte next 10 days

        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_gte={execution_date_gte}"
            f"&execution_date_lte={execution_date_lte}"
        )
        assert response.status_code == 200
        self.assertEqual(response.json.get('total_entries'), 1)
        self.assertEqual(response.json.get('dag_runs')[0].get('execution_date'),
                         (self.now + timedelta(days=3)).isoformat())

    @provide_session
    def test_only_execution_date_gte_provided(self, session):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,  # today
            start_date=self.now,
            external_trigger=True,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(days=3),  # next 3 days
            start_date=self.now,
            external_trigger=True,
        )
        session.add_all([dagrun_model_1, dagrun_model_2])
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 2
        execution_date_gte = self.now + timedelta(days=1)  # gte tomorrow
        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_gte={execution_date_gte}"
        )
        assert response.status_code == 200
        self.assertEqual(response.json.get('total_entries'), 1)
        self.assertEqual(response.json.get('dag_runs')[0].get('execution_date'),
                         (self.now + timedelta(days=3)).isoformat())

    @provide_session
    def test_only_execution_date_lte_provided(self, session):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now,  # today
            start_date=self.now,
            external_trigger=True,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=self.now + timedelta(days=3),  # next 3 days
            start_date=self.now,
            external_trigger=True,
        )
        session.add_all([dagrun_model_1, dagrun_model_2])
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 2
        execution_date_lte = self.now + timedelta(days=1)  # lte tomorrow
        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_lte={execution_date_lte}"
        )
        assert response.status_code == 200

        self.assertEqual(response.json.get('total_entries'), 1)
        self.assertEqual(response.json.get('dag_runs')[0].get('execution_date'),
                         self.now.isoformat())  # today


#  TODO: add tests for filters


class TestPatchDagRun(TestDagRunEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.patch("/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID")
        assert response.status_code == 200


class TestPostDagRun(TestDagRunEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.post("/dags/TEST_DAG_ID/dagRuns")
        assert response.status_code == 200
