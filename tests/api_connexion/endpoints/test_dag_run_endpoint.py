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
from parameterized import parameterized

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
        self.default_time = '2020-06-11T18:00:00+00:00'
        self.default_time_2 = '2020-06-12T18:00:00+00:00'
        clear_db_runs()

    def tearDown(self) -> None:
        clear_db_runs()

    def _create_test_dag_run(self, state='running', extra_dag=False):
        dagrun_model_1 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_1',
            run_type=DagRunType.MANUAL.value,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state=state,
        )
        dagrun_model_2 = DagRun(
            dag_id='TEST_DAG_ID',
            run_id='TEST_DAG_RUN_ID_2',
            run_type=DagRunType.MANUAL.value,
            execution_date=timezone.parse(self.default_time_2),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
        )
        if extra_dag:
            dagrun_extra = [DagRun(
                dag_id='TEST_DAG_ID_' + str(i),
                run_id='TEST_DAG_RUN_ID_' + str(i),
                run_type=DagRunType.MANUAL.value,
                execution_date=timezone.parse(self.default_time_2),
                start_date=timezone.parse(self.default_time),
                external_trigger=True,
            ) for i in range(3, 5)]
            return [dagrun_model_1, dagrun_model_2] + dagrun_extra
        return [dagrun_model_1, dagrun_model_2]


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
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
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
                'execution_date': self.default_time,
                'external_trigger': True,
                'start_date': self.default_time,
                'conf': {},
            },
        )

    def test_should_response_404(self):
        response = self.client.get("api/v1/dags/invalid-id/dagRuns/invalid-id")
        assert response.status_code == 404
        self.assertEqual(
            {'detail': None, 'status': 404, 'title': 'DAGRun not found', 'type': 'about:blank'}, response.json
        )


class TestGetDagRuns(TestDagRunEndpoint):
    @provide_session
    def test_should_response_200(self, session):
        dagruns = self._create_test_dag_run()
        session.add_all(dagruns)
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
                        'execution_date': self.default_time,
                        'external_trigger': True,
                        'start_date': self.default_time,
                        'conf': {},
                    },
                    {
                        'dag_id': 'TEST_DAG_ID',
                        'dag_run_id': 'TEST_DAG_RUN_ID_2',
                        'end_date': None,
                        'state': 'running',
                        'execution_date': self.default_time_2,
                        'external_trigger': True,
                        'start_date': self.default_time,
                        'conf': {},
                    },
                ],
                "total_entries": 2,
            },
        )

    @provide_session
    def test_should_return_all_with_tilde_as_dag_id(self, session):
        dagruns = self._create_test_dag_run(extra_dag=True)
        expected_dag_run_ids = ['TEST_DAG_ID', 'TEST_DAG_ID',
                                "TEST_DAG_ID_3", "TEST_DAG_ID_4"]
        session.add_all(dagruns)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 4
        response = self.client.get("api/v1/dags/~/dagRuns")
        assert response.status_code == 200
        dag_run_ids = [dag_run["dag_id"] for dag_run in response.json["dag_runs"]]
        self.assertEqual(dag_run_ids, expected_dag_run_ids)


class TestGetDagRunsPagination(TestDagRunEndpoint):
    @parameterized.expand(
        [
            ("api/v1/dags/TEST_DAG_ID/dagRuns?limit=1", ["TEST_DAG_RUN_ID1"]),
            ("api/v1/dags/TEST_DAG_ID/dagRuns?limit=2", ["TEST_DAG_RUN_ID1", "TEST_DAG_RUN_ID2"]),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?offset=5",
                [
                    "TEST_DAG_RUN_ID6",
                    "TEST_DAG_RUN_ID7",
                    "TEST_DAG_RUN_ID8",
                    "TEST_DAG_RUN_ID9",
                    "TEST_DAG_RUN_ID10",
                ],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?offset=0",
                [
                    "TEST_DAG_RUN_ID1",
                    "TEST_DAG_RUN_ID2",
                    "TEST_DAG_RUN_ID3",
                    "TEST_DAG_RUN_ID4",
                    "TEST_DAG_RUN_ID5",
                    "TEST_DAG_RUN_ID6",
                    "TEST_DAG_RUN_ID7",
                    "TEST_DAG_RUN_ID8",
                    "TEST_DAG_RUN_ID9",
                    "TEST_DAG_RUN_ID10",
                ],
            ),
            ("api/v1/dags/TEST_DAG_ID/dagRuns?limit=1&offset=5", ["TEST_DAG_RUN_ID6"]),
            ("api/v1/dags/TEST_DAG_ID/dagRuns?limit=1&offset=1", ["TEST_DAG_RUN_ID2"]),
            ("api/v1/dags/TEST_DAG_ID/dagRuns?limit=2&offset=2", ["TEST_DAG_RUN_ID3", "TEST_DAG_RUN_ID4"],),
        ]
    )
    @provide_session
    def test_handle_limit_and_offset(self, url, expected_dag_run_ids, session):
        dagrun_models = self._create_dag_runs(10)
        session.add_all(dagrun_models)
        session.commit()

        response = self.client.get(url)
        assert response.status_code == 200

        self.assertEqual(response.json["total_entries"], 10)
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"]]
        self.assertEqual(dag_run_ids, expected_dag_run_ids)

    @provide_session
    def test_should_respect_page_size_limit(self, session):
        dagrun_models = self._create_dag_runs(200)
        session.add_all(dagrun_models)
        session.commit()

        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns")  # default is 100
        assert response.status_code == 200

        self.assertEqual(response.json["total_entries"], 200)
        self.assertEqual(len(response.json["dag_runs"]), 100)  # default is 100

    def _create_dag_runs(self, count):
        return [
            DagRun(
                dag_id="TEST_DAG_ID",
                run_id="TEST_DAG_RUN_ID" + str(i),
                run_type=DagRunType.MANUAL.value,
                execution_date=timezone.parse(self.default_time) + timedelta(minutes=i),
                start_date=timezone.parse(self.default_time),
                external_trigger=True,
            )
            for i in range(1, count + 1)
        ]


class TestGetDagRunsPaginationFilters(TestDagRunEndpoint):
    @parameterized.expand(
        [
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?start_date_gte=2020-06-18T18:00:00+00:00",
                ["TEST_START_EXEC_DAY_18", "TEST_START_EXEC_DAY_19"],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?start_date_lte=2020-06-11T18:00:00+00:00",
                ["TEST_START_EXEC_DAY_10", "TEST_START_EXEC_DAY_11"],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?start_date_lte=2020-06-15T18:00:00+00:00"
                "&start_date_gte=2020-06-12T18:00:00Z",
                ["TEST_START_EXEC_DAY_12", "TEST_START_EXEC_DAY_13",
                 "TEST_START_EXEC_DAY_14", "TEST_START_EXEC_DAY_15"],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_lte=2020-06-13T18:00:00+00:00",
                ["TEST_START_EXEC_DAY_10", "TEST_START_EXEC_DAY_11",
                 "TEST_START_EXEC_DAY_12", "TEST_START_EXEC_DAY_13"],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_gte=2020-06-16T18:00:00+00:00",
                ["TEST_START_EXEC_DAY_16", "TEST_START_EXEC_DAY_17",
                 "TEST_START_EXEC_DAY_18", "TEST_START_EXEC_DAY_19"],
            ),
        ]
    )
    @provide_session
    def test_date_filters_gte_and_lte(self, url, expected_dag_run_ids, session):
        dagrun_models = self._create_dag_runs()
        session.add_all(dagrun_models)
        session.commit()

        response = self.client.get(url)
        assert response.status_code == 200
        self.assertEqual(response.json["total_entries"], 10)
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"]]
        self.assertEqual(dag_run_ids, expected_dag_run_ids)

    def _create_dag_runs(self):
        dates = [
            '2020-06-10T18:00:00+00:00',
            '2020-06-11T18:00:00+00:00',
            '2020-06-12T18:00:00+00:00',
            '2020-06-13T18:00:00+00:00',
            '2020-06-14T18:00:00+00:00',
            '2020-06-15T18:00:00Z',
            '2020-06-16T18:00:00Z',
            '2020-06-17T18:00:00Z',
            '2020-06-18T18:00:00Z',
            '2020-06-19T18:00:00Z',
        ]

        return [
            DagRun(
                dag_id="TEST_DAG_ID",
                run_id="TEST_START_EXEC_DAY_1" + str(i),
                run_type=DagRunType.MANUAL.value,
                execution_date=timezone.parse(dates[i]),
                start_date=timezone.parse(dates[i]),
                external_trigger=True,
                state='success',
            )
            for i in range(len(dates))
        ]


class TestGetDagRunsEndDateFilters(TestDagRunEndpoint):
    @parameterized.expand(
        [
            (
                f"api/v1/dags/TEST_DAG_ID/dagRuns?end_date_gte="
                f"{(timezone.utcnow() + timedelta(days=1)).isoformat()}",
                [],
            ),
            (
                f"api/v1/dags/TEST_DAG_ID/dagRuns?end_date_lte="
                f"{(timezone.utcnow() + timedelta(days=1)).isoformat()}",
                ["TEST_DAG_RUN_ID_1"],
            ),
        ]
    )
    @provide_session
    def test_end_date_gte_lte(self, url, expected_dag_run_ids, session):
        dagruns = self._create_test_dag_run('success')  # state==success, then end date is today
        session.add_all(dagruns)
        session.commit()

        response = self.client.get(url)
        assert response.status_code == 200
        self.assertEqual(response.json["total_entries"], 2)
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"] if dag_run]
        self.assertEqual(dag_run_ids, expected_dag_run_ids)


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
