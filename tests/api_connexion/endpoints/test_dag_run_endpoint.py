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
                'end_date': '',
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
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.get("/dags/TEST_DAG_ID/dagRuns/")
        assert response.status_code == 200


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
