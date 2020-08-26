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

from parameterized import parameterized

from airflow.models import DagModel, DagRun
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session
from airflow.utils.types import DagRunType
from airflow.www import app
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs


class TestDagRunEndpoint(unittest.TestCase):
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
        self.default_time = "2020-06-11T18:00:00+00:00"
        self.default_time_2 = "2020-06-12T18:00:00+00:00"
        clear_db_runs()
        clear_db_dags()

    def tearDown(self) -> None:
        clear_db_runs()

    def _create_test_dag_run(self, state='running', extra_dag=False, commit=True):
        dag_runs = []
        dagrun_model_1 = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID_1",
            run_type=DagRunType.MANUAL.value,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state=state,
        )
        dag_runs.append(dagrun_model_1)
        dagrun_model_2 = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID_2",
            run_type=DagRunType.MANUAL.value,
            execution_date=timezone.parse(self.default_time_2),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
        )
        dag_runs.append(dagrun_model_2)
        if extra_dag:
            dagrun_extra = [
                DagRun(
                    dag_id='TEST_DAG_ID_' + str(i),
                    run_id='TEST_DAG_RUN_ID_' + str(i),
                    run_type=DagRunType.MANUAL.value,
                    execution_date=timezone.parse(self.default_time_2),
                    start_date=timezone.parse(self.default_time),
                    external_trigger=True,
                )
                for i in range(3, 5)
            ]
            dag_runs.extend(dagrun_extra)
        if commit:
            with create_session() as session:
                session.add_all(dag_runs)
        return dag_runs


class TestDeleteDagRun(TestDagRunEndpoint):
    @provide_session
    def test_should_response_204(self, session):
        session.add_all(self._create_test_dag_run())
        session.commit()
        response = self.client.delete(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1", environ_overrides={'REMOTE_USER': "test"}
        )
        self.assertEqual(response.status_code, 204)
        # Check if the Dag Run is deleted from the database
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1", environ_overrides={'REMOTE_USER': "test"}
        )
        self.assertEqual(response.status_code, 404)

    def test_should_response_404(self):
        response = self.client.delete(
            "api/v1/dags/INVALID_DAG_RUN/dagRuns/INVALID_DAG_RUN", environ_overrides={'REMOTE_USER': "test"}
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(
            response.json,
            {
                "detail": "DAGRun with DAG ID: 'INVALID_DAG_RUN' and DagRun ID: 'INVALID_DAG_RUN' not found",
                "status": 404,
                "title": "Object not found",
                "type": "about:blank",
            },
        )

    @provide_session
    def test_should_raises_401_unauthenticated(self, session):
        session.add_all(self._create_test_dag_run())
        session.commit()

        response = self.client.delete("api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",)

        assert_401(response)


class TestGetDagRun(TestDagRunEndpoint):
    @provide_session
    def test_should_response_200(self, session):
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.MANUAL.value,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
        )
        session.add(dagrun_model)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 1
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        expected_response = {
            'dag_id': 'TEST_DAG_ID',
            'dag_run_id': 'TEST_DAG_RUN_ID',
            'end_date': None,
            'state': 'running',
            'execution_date': self.default_time,
            'external_trigger': True,
            'start_date': self.default_time,
            'conf': {},
        }
        assert response.json == expected_response

    def test_should_response_404(self):
        response = self.client.get(
            "api/v1/dags/invalid-id/dagRuns/invalid-id", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404
        expected_resp = {'detail': None, 'status': 404, 'title': 'DAGRun not found', 'type': 'about:blank'}
        assert expected_resp == response.json

    @provide_session
    def test_should_raises_401_unauthenticated(self, session):
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.MANUAL.value,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
        )
        session.add(dagrun_model)
        session.commit()

        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID")

        assert_401(response)


class TestGetDagRuns(TestDagRunEndpoint):
    @provide_session
    def test_should_response_200(self, session):
        self._create_test_dag_run()
        result = session.query(DagRun).all()
        assert len(result) == 2
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json == {
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
        }

    @provide_session
    def test_should_return_all_with_tilde_as_dag_id(self, session):
        self._create_test_dag_run(extra_dag=True)
        expected_dag_run_ids = ['TEST_DAG_ID', 'TEST_DAG_ID', "TEST_DAG_ID_3", "TEST_DAG_ID_4"]
        result = session.query(DagRun).all()
        assert len(result) == 4
        response = self.client.get("api/v1/dags/~/dagRuns", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        dag_run_ids = [dag_run["dag_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

    def test_should_raises_401_unauthenticated(self):
        self._create_test_dag_run()

        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns")

        assert_401(response)


class TestGetDagRunsPagination(TestDagRunEndpoint):
    @parameterized.expand(
        [
            ("api/v1/dags/TEST_DAG_ID/dagRuns?limit=1", ["TEST_DAG_RUN_ID1"]),
            ("api/v1/dags/TEST_DAG_ID/dagRuns?limit=2", ["TEST_DAG_RUN_ID1", "TEST_DAG_RUN_ID2"],),
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
    def test_handle_limit_and_offset(self, url, expected_dag_run_ids):
        self._create_dag_runs(10)
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200

        assert response.json["total_entries"] == 10
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

    def test_should_respect_page_size_limit(self):
        self._create_dag_runs(200)
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["dag_runs"]) == 100  # default is 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self):
        self._create_dag_runs(200)
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?limit=180", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        self.assertEqual(len(response.json["dag_runs"]), 150)

    def _create_dag_runs(self, count):
        dag_runs = [
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
        with create_session() as session:
            session.add_all(dag_runs)


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
                "api/v1/dags/TEST_DAG_ID/dagRuns?start_date_lte= 2020-06-15T18:00:00+00:00"
                "&start_date_gte=2020-06-12T18:00:00Z",
                [
                    "TEST_START_EXEC_DAY_12",
                    "TEST_START_EXEC_DAY_13",
                    "TEST_START_EXEC_DAY_14",
                    "TEST_START_EXEC_DAY_15",
                ],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_lte=2020-06-13T18:00:00+00:00",
                [
                    "TEST_START_EXEC_DAY_10",
                    "TEST_START_EXEC_DAY_11",
                    "TEST_START_EXEC_DAY_12",
                    "TEST_START_EXEC_DAY_13",
                ],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_gte=2020-06-16T18:00:00+00:00",
                [
                    "TEST_START_EXEC_DAY_16",
                    "TEST_START_EXEC_DAY_17",
                    "TEST_START_EXEC_DAY_18",
                    "TEST_START_EXEC_DAY_19",
                ],
            ),
        ]
    )
    @provide_session
    def test_date_filters_gte_and_lte(self, url, expected_dag_run_ids, session):
        dagrun_models = self._create_dag_runs()
        session.add_all(dagrun_models)
        session.commit()

        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 10
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

    def _create_dag_runs(self):
        dates = [
            "2020-06-10T18:00:00+00:00",
            "2020-06-11T18:00:00+00:00",
            "2020-06-12T18:00:00+00:00",
            "2020-06-13T18:00:00+00:00",
            "2020-06-14T18:00:00+00:00",
            "2020-06-15T18:00:00Z",
            "2020-06-16T18:00:00Z",
            "2020-06-17T18:00:00Z",
            "2020-06-18T18:00:00Z",
            "2020-06-19T18:00:00Z",
        ]

        return [
            DagRun(
                dag_id="TEST_DAG_ID",
                run_id="TEST_START_EXEC_DAY_1" + str(i),
                run_type=DagRunType.MANUAL.value,
                execution_date=timezone.parse(dates[i]),
                start_date=timezone.parse(dates[i]),
                external_trigger=True,
                state="success",
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
    def test_end_date_gte_lte(self, url, expected_dag_run_ids):
        self._create_test_dag_run('success')  # state==success, then end date is today
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 2
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"] if dag_run]
        assert dag_run_ids == expected_dag_run_ids


class TestGetDagRunBatch(TestDagRunEndpoint):
    def test_should_respond_200(self):
        self._create_test_dag_run()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": ["TEST_DAG_ID"]},
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 200
        assert response.json == {
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
        }

    @parameterized.expand(
        [
            (
                {"dag_ids": ["TEST_DAG_ID"], "page_offset": -1},
                "-1 is less than the minimum of 0 - 'page_offset'",
            ),
            ({"dag_ids": ["TEST_DAG_ID"], "page_limit": 0}, "0 is less than the minimum of 1 - 'page_limit'"),
            ({"dag_ids": "TEST_DAG_ID"}, "'TEST_DAG_ID' is not of type 'array' - 'dag_ids'"),
            ({"start_date_gte": "2020-06-12T18"}, "{'start_date_gte': ['Not a valid datetime.']}"),
        ]
    )
    def test_payload_validation(self, payload, error):
        self._create_test_dag_run()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        assert error == response.json.get("detail")

    def test_should_raises_401_unauthenticated(self):
        self._create_test_dag_run()

        response = self.client.post("api/v1/dags/~/dagRuns/list", json={"dag_ids": ["TEST_DAG_ID"]})

        assert_401(response)


class TestGetDagRunBatchPagination(TestDagRunEndpoint):
    @parameterized.expand(
        [
            ({"page_limit": 1}, ["TEST_DAG_RUN_ID1"]),
            ({"page_limit": 2}, ["TEST_DAG_RUN_ID1", "TEST_DAG_RUN_ID2"]),
            (
                {"page_offset": 5},
                [
                    "TEST_DAG_RUN_ID6",
                    "TEST_DAG_RUN_ID7",
                    "TEST_DAG_RUN_ID8",
                    "TEST_DAG_RUN_ID9",
                    "TEST_DAG_RUN_ID10",
                ],
            ),
            (
                {"page_offset": 0},
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
            ({"page_offset": 5, "page_limit": 1}, ["TEST_DAG_RUN_ID6"]),
            ({"page_offset": 1, "page_limit": 1}, ["TEST_DAG_RUN_ID2"]),
            ({"page_offset": 2, "page_limit": 2}, ["TEST_DAG_RUN_ID3", "TEST_DAG_RUN_ID4"],),
        ]
    )
    def test_handle_limit_and_offset(self, payload, expected_dag_run_ids):
        self._create_dag_runs(10)
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200

        assert response.json["total_entries"] == 10
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

    def test_should_respect_page_size_limit(self):
        self._create_dag_runs(200)
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json={}, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["dag_runs"]) == 100  # default is 100

    def _create_dag_runs(self, count):
        dag_runs = [
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
        with create_session() as session:
            session.add_all(dag_runs)


class TestGetDagRunBatchDateFilters(TestDagRunEndpoint):
    @parameterized.expand(
        [
            (
                {"start_date_gte": "2020-06-18T18:00:00+00:00"},
                ["TEST_START_EXEC_DAY_18", "TEST_START_EXEC_DAY_19"],
            ),
            (
                {"start_date_lte": "2020-06-11T18:00:00+00:00"},
                ["TEST_START_EXEC_DAY_10", "TEST_START_EXEC_DAY_11"],
            ),
            (
                {"start_date_lte": "2020-06-15T18:00:00+00:00", "start_date_gte": "2020-06-12T18:00:00Z"},
                [
                    "TEST_START_EXEC_DAY_12",
                    "TEST_START_EXEC_DAY_13",
                    "TEST_START_EXEC_DAY_14",
                    "TEST_START_EXEC_DAY_15",
                ],
            ),
            (
                {"execution_date_lte": "2020-06-13T18:00:00+00:00"},
                [
                    "TEST_START_EXEC_DAY_10",
                    "TEST_START_EXEC_DAY_11",
                    "TEST_START_EXEC_DAY_12",
                    "TEST_START_EXEC_DAY_13",
                ],
            ),
            (
                {"execution_date_gte": "2020-06-16T18:00:00+00:00"},
                [
                    "TEST_START_EXEC_DAY_16",
                    "TEST_START_EXEC_DAY_17",
                    "TEST_START_EXEC_DAY_18",
                    "TEST_START_EXEC_DAY_19",
                ],
            ),
        ]
    )
    @provide_session
    def test_date_filters_gte_and_lte(self, payload, expected_dag_run_ids, session):
        dag_runs = self._create_dag_runs()
        session.add_all(dag_runs)
        session.commit()

        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 10
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

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

    @parameterized.expand(
        [
            ({"end_date_gte": f"{(timezone.utcnow() + timedelta(days=1)).isoformat()}"}, [],),
            (
                {"end_date_lte": f"{(timezone.utcnow() + timedelta(days=1)).isoformat()}"},
                ["TEST_DAG_RUN_ID_1"],
            ),
        ]
    )
    def test_end_date_gte_lte(self, payload, expected_dag_run_ids):
        self._create_test_dag_run('success')  # state==success, then end date is today
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 2
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"] if dag_run]
        assert dag_run_ids == expected_dag_run_ids


class TestPostDagRun(TestDagRunEndpoint):
    @parameterized.expand(
        [
            (
                "All fields present",
                {"dag_run_id": "TEST_DAG_RUN", "execution_date": "2020-06-11T18:00:00+00:00",},
            ),
            ("dag_run_id missing", {"execution_date": "2020-06-11T18:00:00+00:00"}),
            ("dag_run_id and execution_date missing", {}),
        ]
    )
    @provide_session
    def test_should_response_200(self, name, request_json, session):
        del name
        dag_instance = DagModel(dag_id="TEST_DAG_ID")
        session.add(dag_instance)
        session.commit()
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns", json=request_json, environ_overrides={'REMOTE_USER': "test"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            {
                "conf": {},
                "dag_id": "TEST_DAG_ID",
                "dag_run_id": response.json["dag_run_id"],
                "end_date": None,
                "execution_date": response.json["execution_date"],
                "external_trigger": True,
                "start_date": response.json["start_date"],
                "state": "running",
            },
            response.json,
        )

    def test_response_404(self):
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"dag_run_id": "TEST_DAG_RUN", "execution_date": self.default_time},
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(response.status_code, 404)
        self.assertEqual(
            {
                "detail": None,
                "status": 404,
                "title": "DAG with dag_id: 'TEST_DAG_ID' not found",
                "type": "about:blank",
            },
            response.json,
        )

    @parameterized.expand(
        [
            (
                "start_date in request json",
                "api/v1/dags/TEST_DAG_ID/dagRuns",
                {"start_date": "2020-06-11T18:00:00+00:00", "execution_date": "2020-06-12T18:00:00+00:00",},
                {
                    "detail": "Property is read-only - 'start_date'",
                    "status": 400,
                    "title": "Bad Request",
                    "type": "about:blank",
                },
            ),
            (
                "state in request json",
                "api/v1/dags/TEST_DAG_ID/dagRuns",
                {"state": "failed", "execution_date": "2020-06-12T18:00:00+00:00"},
                {
                    "detail": "Property is read-only - 'state'",
                    "status": 400,
                    "title": "Bad Request",
                    "type": "about:blank",
                },
            ),
        ]
    )
    @provide_session
    def test_response_400(self, name, url, request_json, expected_response, session):
        del name
        dag_instance = DagModel(dag_id="TEST_DAG_ID")
        session.add(dag_instance)
        session.commit()
        response = self.client.post(url, json=request_json, environ_overrides={'REMOTE_USER': "test"})
        self.assertEqual(response.status_code, 400, response.data)
        self.assertEqual(expected_response, response.json)

    @provide_session
    def test_response_409(self, session):
        dag_instance = DagModel(dag_id="TEST_DAG_ID")
        session.add(dag_instance)
        session.add_all(self._create_test_dag_run())
        session.commit()
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"dag_run_id": "TEST_DAG_RUN_ID_1", "execution_date": self.default_time,},
            environ_overrides={'REMOTE_USER': "test"},
        )
        self.assertEqual(response.status_code, 409, response.data)
        self.assertEqual(
            response.json,
            {
                "detail": "DAGRun with DAG ID: 'TEST_DAG_ID' and "
                "DAGRun ID: 'TEST_DAG_RUN_ID_1' already exists",
                "status": 409,
                "title": "Object already exists",
                "type": "about:blank",
            },
        )

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"dag_run_id": "TEST_DAG_RUN_ID_1", "execution_date": self.default_time,},
        )

        assert_401(response)
