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
from datetime import timedelta
from unittest import mock
from uuid import uuid4

import pendulum
import pytest
from freezegun import freeze_time
from parameterized import parameterized

from airflow import settings
from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.models import DAG, DagModel, DagRun, Dataset
from airflow.models.dataset import DatasetEvent
from airflow.operators.empty import EmptyOperator
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_roles, delete_user
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DATASET),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN),
        ],
    )
    create_user(
        app,  # type: ignore
        username="test_dag_view_only",
        role_name="TestViewDags",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG_RUN),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG_RUN),
        ],
    )
    create_user(
        app,  # type: ignore
        username="test_view_dags",
        role_name="TestViewDags",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_CREATE, permissions.RESOURCE_DAG_RUN),
        ],
    )
    create_user(
        app,  # type: ignore
        username="test_granular_permissions",
        role_name="TestGranularDag",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_RUN)],
    )
    app.appbuilder.sm.sync_perm_for_dag(  # type: ignore
        "TEST_DAG_ID",
        access_control={'TestGranularDag': [permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ]},
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_dag_view_only")  # type: ignore
    delete_user(app, username="test_view_dags")  # type: ignore
    delete_user(app, username="test_granular_permissions")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore
    delete_roles(app)


class TestDagRunEndpoint:
    default_time = "2020-06-11T18:00:00+00:00"
    default_time_2 = "2020-06-12T18:00:00+00:00"
    default_time_3 = "2020-06-13T18:00:00+00:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    def _create_dag(self, dag_id):
        dag_instance = DagModel(dag_id=dag_id)
        with create_session() as session:
            session.add(dag_instance)
        dag = DAG(dag_id=dag_id, schedule_interval=None)
        self.app.dag_bag.bag_dag(dag, root_dag=dag)
        return dag_instance

    def _create_test_dag_run(self, state='running', extra_dag=False, commit=True, idx_start=1):
        dag_runs = []
        dags = []

        for i in range(idx_start, idx_start + 2):
            if i == 1:
                dags.append(DagModel(dag_id='TEST_DAG_ID'))
            dagrun_model = DagRun(
                dag_id="TEST_DAG_ID",
                run_id="TEST_DAG_RUN_ID_" + str(i),
                run_type=DagRunType.MANUAL,
                execution_date=timezone.parse(self.default_time) + timedelta(days=i - 1),
                start_date=timezone.parse(self.default_time),
                external_trigger=True,
                state=state,
            )
            dag_runs.append(dagrun_model)

        if extra_dag:
            for i in range(idx_start + 2, idx_start + 4):
                dags.append(DagModel(dag_id='TEST_DAG_ID_' + str(i)))
                dag_runs.append(
                    DagRun(
                        dag_id='TEST_DAG_ID_' + str(i),
                        run_id='TEST_DAG_RUN_ID_' + str(i),
                        run_type=DagRunType.MANUAL,
                        execution_date=timezone.parse(self.default_time_2),
                        start_date=timezone.parse(self.default_time),
                        external_trigger=True,
                        state=state,
                    )
                )
        if commit:
            with create_session() as session:
                session.add_all(dag_runs)
                session.add_all(dags)
        return dag_runs


class TestDeleteDagRun(TestDagRunEndpoint):
    def test_should_respond_204(self, session):
        session.add_all(self._create_test_dag_run())
        session.commit()
        response = self.client.delete(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 204
        # Check if the Dag Run is deleted from the database
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404

    def test_should_respond_404(self):
        response = self.client.delete(
            "api/v1/dags/INVALID_DAG_RUN/dagRuns/INVALID_DAG_RUN", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404
        assert response.json == {
            "detail": "DAGRun with DAG ID: 'INVALID_DAG_RUN' and DagRun ID: 'INVALID_DAG_RUN' not found",
            "status": 404,
            "title": "Not Found",
            "type": EXCEPTIONS_LINK_MAP[404],
        }

    def test_should_raises_401_unauthenticated(self, session):
        session.add_all(self._create_test_dag_run())
        session.commit()

        response = self.client.delete(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID",
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403


class TestGetDagRun(TestDagRunEndpoint):
    def test_should_respond_200(self, session):
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state='running',
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
            'logical_date': self.default_time,
            'execution_date': self.default_time,
            'external_trigger': True,
            'start_date': self.default_time,
            'conf': {},
            'data_interval_end': None,
            'data_interval_start': None,
            'last_scheduling_decision': None,
            'run_type': 'manual',
        }
        assert response.json == expected_response

    def test_should_respond_404(self):
        response = self.client.get(
            "api/v1/dags/invalid-id/dagRuns/invalid-id", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 404
        expected_resp = {
            'detail': "DAGRun with DAG ID: 'invalid-id' and DagRun ID: 'invalid-id' not found",
            'status': 404,
            'title': 'DAGRun not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        }
        assert expected_resp == response.json

    def test_should_raises_401_unauthenticated(self, session):
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
        )
        session.add(dagrun_model)
        session.commit()

        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID")

        assert_401(response)


class TestGetDagRuns(TestDagRunEndpoint):
    def test_should_respond_200(self, session):
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
                    'logical_date': self.default_time,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
                {
                    'dag_id': 'TEST_DAG_ID',
                    'dag_run_id': 'TEST_DAG_RUN_ID_2',
                    'end_date': None,
                    'state': 'running',
                    'execution_date': self.default_time_2,
                    'logical_date': self.default_time_2,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
            ],
            "total_entries": 2,
        }

    def test_filter_by_state(self, session):
        self._create_test_dag_run()
        self._create_test_dag_run(state="queued", idx_start=3)
        assert session.query(DagRun).count() == 4
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?state=running,queued", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 4
        assert response.json["dag_runs"][0]["state"] == response.json["dag_runs"][1]["state"] == "running"
        assert response.json["dag_runs"][2]["state"] == response.json["dag_runs"][3]["state"] == "queued"

    def test_invalid_order_by_raises_400(self):
        self._create_test_dag_run()

        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?order_by=invalid", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        msg = "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        assert response.json['detail'] == msg

    def test_return_correct_results_with_order_by(self, session):
        self._create_test_dag_run()
        result = session.query(DagRun).all()
        assert len(result) == 2
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?order_by=-execution_date",
            environ_overrides={'REMOTE_USER': "test"},
        )

        assert response.status_code == 200
        assert self.default_time < self.default_time_2
        # - means descending
        assert response.json == {
            "dag_runs": [
                {
                    'dag_id': 'TEST_DAG_ID',
                    'dag_run_id': 'TEST_DAG_RUN_ID_2',
                    'end_date': None,
                    'state': 'running',
                    'execution_date': self.default_time_2,
                    'logical_date': self.default_time_2,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
                {
                    'dag_id': 'TEST_DAG_ID',
                    'dag_run_id': 'TEST_DAG_RUN_ID_1',
                    'end_date': None,
                    'state': 'running',
                    'execution_date': self.default_time,
                    'logical_date': self.default_time,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
            ],
            "total_entries": 2,
        }

    def test_should_return_all_with_tilde_as_dag_id_and_all_dag_permissions(self):
        self._create_test_dag_run(extra_dag=True)
        expected_dag_run_ids = ['TEST_DAG_ID', 'TEST_DAG_ID', "TEST_DAG_ID_3", "TEST_DAG_ID_4"]
        response = self.client.get("api/v1/dags/~/dagRuns", environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        dag_run_ids = [dag_run["dag_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

    def test_should_return_accessible_with_tilde_as_dag_id_and_dag_level_permissions(self):
        self._create_test_dag_run(extra_dag=True)
        expected_dag_run_ids = ['TEST_DAG_ID', 'TEST_DAG_ID']
        response = self.client.get(
            "api/v1/dags/~/dagRuns", environ_overrides={'REMOTE_USER': "test_granular_permissions"}
        )
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
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?limit=2",
                ["TEST_DAG_RUN_ID1", "TEST_DAG_RUN_ID2"],
            ),
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
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?limit=2&offset=2",
                ["TEST_DAG_RUN_ID3", "TEST_DAG_RUN_ID4"],
            ),
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
        assert len(response.json["dag_runs"]) == 150

    def _create_dag_runs(self, count):
        dag_runs = [
            DagRun(
                dag_id="TEST_DAG_ID",
                run_id="TEST_DAG_RUN_ID" + str(i),
                run_type=DagRunType.MANUAL,
                execution_date=timezone.parse(self.default_time) + timedelta(minutes=i),
                start_date=timezone.parse(self.default_time),
                external_trigger=True,
            )
            for i in range(1, count + 1)
        ]
        dag = DagModel(dag_id="TEST_DAG_ID")
        with create_session() as session:
            session.add_all(dag_runs)
            session.add(dag)


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
        assert response.json["total_entries"] == len(expected_dag_run_ids)
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
                run_type=DagRunType.MANUAL,
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
                ["TEST_DAG_RUN_ID_1", "TEST_DAG_RUN_ID_2"],
            ),
        ]
    )
    def test_end_date_gte_lte(self, url, expected_dag_run_ids):
        self._create_test_dag_run('success')  # state==success, then end date is today
        response = self.client.get(url, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == len(expected_dag_run_ids)
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
                    'logical_date': self.default_time,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
                {
                    'dag_id': 'TEST_DAG_ID',
                    'dag_run_id': 'TEST_DAG_RUN_ID_2',
                    'end_date': None,
                    'state': 'running',
                    'execution_date': self.default_time_2,
                    'logical_date': self.default_time_2,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
            ],
            "total_entries": 2,
        }

    def test_filter_by_state(self):
        self._create_test_dag_run()
        self._create_test_dag_run(state="queued", idx_start=3)
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": ["TEST_DAG_ID"], "states": ["running", "queued"]},
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 4
        assert response.json["dag_runs"][0]["state"] == response.json["dag_runs"][1]["state"] == "running"
        assert response.json["dag_runs"][2]["state"] == response.json["dag_runs"][3]["state"] == "queued"

    def test_order_by_descending_works(self):
        self._create_test_dag_run()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": ["TEST_DAG_ID"], "order_by": "-dag_run_id"},
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_runs": [
                {
                    'dag_id': 'TEST_DAG_ID',
                    'dag_run_id': 'TEST_DAG_RUN_ID_2',
                    'end_date': None,
                    'state': 'running',
                    'execution_date': self.default_time_2,
                    'logical_date': self.default_time_2,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
                {
                    'dag_id': 'TEST_DAG_ID',
                    'dag_run_id': 'TEST_DAG_RUN_ID_1',
                    'end_date': None,
                    'state': 'running',
                    'execution_date': self.default_time,
                    'logical_date': self.default_time,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_for_invalid_attr(self):
        self._create_test_dag_run()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": ["TEST_DAG_ID"], "order_by": "-dag_ru"},
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 400
        msg = "Ordering with 'dag_ru' is disallowed or the attribute does not exist on the model"
        assert response.json['detail'] == msg

    def test_should_return_accessible_with_tilde_as_dag_id_and_dag_level_permissions(self):
        self._create_test_dag_run(extra_dag=True)
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": []},
            environ_overrides={'REMOTE_USER': "test_granular_permissions"},
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
                    'logical_date': self.default_time,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
                },
                {
                    'dag_id': 'TEST_DAG_ID',
                    'dag_run_id': 'TEST_DAG_RUN_ID_2',
                    'end_date': None,
                    'state': 'running',
                    'execution_date': self.default_time_2,
                    'logical_date': self.default_time_2,
                    'external_trigger': True,
                    'start_date': self.default_time,
                    'conf': {},
                    'data_interval_end': None,
                    'data_interval_start': None,
                    'last_scheduling_decision': None,
                    'run_type': 'manual',
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
            (
                {"page_offset": 2, "page_limit": 2},
                ["TEST_DAG_RUN_ID3", "TEST_DAG_RUN_ID4"],
            ),
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
                state='running',
                run_type=DagRunType.MANUAL,
                execution_date=timezone.parse(self.default_time) + timedelta(minutes=i),
                start_date=timezone.parse(self.default_time),
                external_trigger=True,
            )
            for i in range(1, count + 1)
        ]
        dag = DagModel(dag_id="TEST_DAG_ID")
        with create_session() as session:
            session.add_all(dag_runs)
            session.add(dag)


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
    def test_date_filters_gte_and_lte(self, payload, expected_dag_run_ids):
        self._create_dag_runs()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == len(expected_dag_run_ids)
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

        dag = DagModel(dag_id="TEST_DAG_ID")
        dag_runs = [
            DagRun(
                dag_id="TEST_DAG_ID",
                run_id="TEST_START_EXEC_DAY_1" + str(i),
                run_type=DagRunType.MANUAL,
                execution_date=timezone.parse(dates[i]),
                start_date=timezone.parse(dates[i]),
                external_trigger=True,
                state='success',
            )
            for i in range(len(dates))
        ]
        with create_session() as session:
            session.add_all(dag_runs)
            session.add(dag)
        return dag_runs

    @parameterized.expand(
        [
            ({"execution_date_gte": '2020-11-09T16:25:56.939143'}, 'Naive datetime is disallowed'),
            (
                {"start_date_gte": "2020-06-18T16:25:56.939143"},
                'Naive datetime is disallowed',
            ),
            (
                {"start_date_lte": "2020-06-18T18:00:00.564434"},
                'Naive datetime is disallowed',
            ),
            (
                {"start_date_lte": "2020-06-15T18:00:00.653434", "start_date_gte": "2020-06-12T18:00.343534"},
                'Naive datetime is disallowed',
            ),
            (
                {"execution_date_lte": "2020-06-13T18:00:00.353454"},
                'Naive datetime is disallowed',
            ),
            ({"execution_date_gte": "2020-06-16T18:00:00.676443"}, 'Naive datetime is disallowed'),
        ]
    )
    def test_naive_date_filters_raises_400(self, payload, expected_response):
        self._create_dag_runs()

        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        assert response.json['detail'] == expected_response

    @parameterized.expand(
        [
            (
                {"end_date_gte": f"{(timezone.utcnow() + timedelta(days=1)).isoformat()}"},
                [],
            ),
            (
                {"end_date_lte": f"{(timezone.utcnow() + timedelta(days=1)).isoformat()}"},
                ["TEST_DAG_RUN_ID_1", "TEST_DAG_RUN_ID_2"],
            ),
        ]
    )
    def test_end_date_gte_lte(self, payload, expected_dag_run_ids):
        self._create_test_dag_run('success')  # state==success, then end date is today
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == len(expected_dag_run_ids)
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"] if dag_run]
        assert dag_run_ids == expected_dag_run_ids


class TestPostDagRun(TestDagRunEndpoint):
    @pytest.mark.parametrize("logical_date_field_name", ["execution_date", "logical_date"])
    @pytest.mark.parametrize(
        "dag_run_id, logical_date",
        [
            pytest.param("TEST_DAG_RUN", "2020-06-11T18:00:00+00:00", id="both-present"),
            pytest.param(None, "2020-06-11T18:00:00+00:00", id="only-date"),
            pytest.param(None, None, id="both-missing"),
        ],
    )
    def test_should_respond_200(self, logical_date_field_name, dag_run_id, logical_date):
        self._create_dag("TEST_DAG_ID")

        # We'll patch airflow.utils.timezone.utcnow to always return this so we
        # can check the returned dates.
        fixed_now = timezone.utcnow()

        request_json = {}
        if logical_date is not None:
            request_json[logical_date_field_name] = logical_date
        if dag_run_id is not None:
            request_json["dag_run_id"] = dag_run_id

        with mock.patch("airflow.utils.timezone.utcnow", lambda: fixed_now):
            response = self.client.post(
                "api/v1/dags/TEST_DAG_ID/dagRuns",
                json=request_json,
                environ_overrides={"REMOTE_USER": "test"},
            )
        assert response.status_code == 200

        if logical_date is None:
            expected_logical_date = fixed_now.isoformat()
        else:
            expected_logical_date = logical_date
        if dag_run_id is None:
            expected_dag_run_id = f"manual__{expected_logical_date}"
        else:
            expected_dag_run_id = dag_run_id
        assert {
            "conf": {},
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": expected_dag_run_id,
            "end_date": None,
            "execution_date": expected_logical_date,
            "logical_date": expected_logical_date,
            "external_trigger": True,
            "start_date": None,
            "state": "queued",
            "data_interval_end": expected_logical_date,
            "data_interval_start": expected_logical_date,
            "last_scheduling_decision": None,
            "run_type": "manual",
        } == response.json

    def test_should_respond_400_if_a_dag_has_import_errors(self, session):
        """Test that if a dagmodel has import errors, dags won't be triggered"""
        dm = self._create_dag("TEST_DAG_ID")
        dm.has_import_errors = True
        session.add(dm)
        session.flush()
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert {
            "detail": "DAG with dag_id: 'TEST_DAG_ID' has import errors",
            "status": 400,
            "title": 'DAG cannot be triggered',
            "type": EXCEPTIONS_LINK_MAP[400],
        } == response.json

    def test_should_response_200_for_matching_execution_date_logical_date(self):
        execution_date = "2020-11-10T08:25:56.939143+00:00"
        logical_date = "2020-11-10T08:25:56.939143+00:00"
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={
                "execution_date": execution_date,
                "logical_date": logical_date,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )
        dag_run_id = f"manual__{logical_date}"

        assert response.status_code == 200
        assert {
            "conf": {},
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": dag_run_id,
            "end_date": None,
            "execution_date": execution_date,
            "logical_date": logical_date,
            "external_trigger": True,
            "start_date": None,
            "state": "queued",
            "data_interval_end": logical_date,
            "data_interval_start": logical_date,
            "last_scheduling_decision": None,
            "run_type": "manual",
        } == response.json

    def test_should_response_400_for_conflicting_execution_date_logical_date(self):
        execution_date = "2020-11-10T08:25:56.939143+00:00"
        logical_date = "2020-11-11T08:25:56.939143+00:00"
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"execution_date": execution_date, "logical_date": logical_date},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json["title"] == "logical_date conflicts with execution_date"
        assert response.json["detail"] == (f"'{logical_date}' != '{execution_date}'")

    @parameterized.expand(
        [
            ({'execution_date': "2020-11-10T08:25:56.939143"}, 'Naive datetime is disallowed'),
            ({'execution_date': "2020-11-10T08:25:56P"}, "{'logical_date': ['Not a valid datetime.']}"),
            ({'logical_date': "2020-11-10T08:25:56.939143"}, 'Naive datetime is disallowed'),
            ({'logical_date': "2020-11-10T08:25:56P"}, "{'logical_date': ['Not a valid datetime.']}"),
        ]
    )
    def test_should_response_400_for_naive_datetime_and_bad_datetime(self, data, expected):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns", json=data, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        assert response.json['detail'] == expected

    @parameterized.expand(
        [
            (
                {
                    "dag_run_id": "TEST_DAG_RUN",
                    "execution_date": "2020-06-11T18:00:00+00:00",
                    "conf": "some string",
                },
                "'some string' is not of type 'object' - 'conf'",
            )
        ]
    )
    def test_should_response_400_for_non_dict_dagrun_conf(self, data, expected):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns", json=data, environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 400
        assert response.json['detail'] == expected

    def test_response_404(self):
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"dag_run_id": "TEST_DAG_RUN", "execution_date": self.default_time},
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 404
        assert {
            "detail": "DAG with dag_id: 'TEST_DAG_ID' not found",
            "status": 404,
            "title": "DAG not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    @parameterized.expand(
        [
            (
                "start_date in request json",
                "api/v1/dags/TEST_DAG_ID/dagRuns",
                {
                    "start_date": "2020-06-11T18:00:00+00:00",
                    "execution_date": "2020-06-12T18:00:00+00:00",
                },
                {
                    "detail": "Property is read-only - 'start_date'",
                    "status": 400,
                    "title": "Bad Request",
                    "type": EXCEPTIONS_LINK_MAP[400],
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
                    "type": EXCEPTIONS_LINK_MAP[400],
                },
            ),
        ]
    )
    def test_response_400(self, name, url, request_json, expected_response):
        del name
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(url, json=request_json, environ_overrides={'REMOTE_USER': "test"})
        assert response.status_code == 400, response.data
        assert expected_response == response.json

    def test_response_409(self):
        self._create_test_dag_run()
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "execution_date": self.default_time_3,
            },
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 409, response.data
        assert response.json == {
            "detail": "DAGRun with DAG ID: 'TEST_DAG_ID' and "
            "DAGRun ID: 'TEST_DAG_RUN_ID_1' already exists",
            "status": 409,
            "title": "Conflict",
            "type": EXCEPTIONS_LINK_MAP[409],
        }

    def test_response_409_when_execution_date_is_same(self):
        self._create_test_dag_run()

        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={
                "dag_run_id": "TEST_DAG_RUN_ID_6",
                "execution_date": self.default_time,
            },
            environ_overrides={'REMOTE_USER': "test"},
        )

        assert response.status_code == 409, response.data
        assert response.json == {
            "detail": "DAGRun with DAG ID: 'TEST_DAG_ID' and "
            "DAGRun logical date: '2020-06-11 18:00:00+00:00' already exists",
            "status": 409,
            "title": "Conflict",
            "type": EXCEPTIONS_LINK_MAP[409],
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "execution_date": self.default_time,
            },
        )

        assert_401(response)

    @parameterized.expand(
        ["test_dag_view_only", "test_view_dags", "test_granular_permissions", "test_no_permissions"]
    )
    def test_should_raises_403_unauthorized(self, username):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "execution_date": self.default_time,
            },
            environ_overrides={'REMOTE_USER': username},
        )
        assert response.status_code == 403


class TestPatchDagRunState(TestDagRunEndpoint):
    @pytest.mark.parametrize("state", ["failed", "success", "queued"])
    @pytest.mark.parametrize("run_type", [state.value for state in DagRunType])
    def test_should_respond_200(self, state, run_type, dag_maker, session):
        dag_id = "TEST_DAG_ID"
        dag_run_id = 'TEST_DAG_RUN_ID'
        with dag_maker(dag_id) as dag:
            task = EmptyOperator(task_id='task_id', dag=dag)
        self.app.dag_bag.bag_dag(dag, root_dag=dag)
        dr = dag_maker.create_dagrun(run_id=dag_run_id, run_type=run_type)
        ti = dr.get_task_instance(task_id='task_id')
        ti.task = task
        ti.state = State.RUNNING
        session.merge(ti)
        session.commit()

        request_json = {"state": state}

        response = self.client.patch(
            f"api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
            json=request_json,
            environ_overrides={"REMOTE_USER": "test"},
        )

        if state != "queued":
            ti.refresh_from_db()
            assert ti.state == state

        dr = session.query(DagRun).filter(DagRun.run_id == dr.run_id).first()
        assert response.status_code == 200
        assert response.json == {
            'conf': {},
            'dag_id': dag_id,
            'dag_run_id': dag_run_id,
            'end_date': dr.end_date.isoformat(),
            'execution_date': dr.execution_date.isoformat(),
            'external_trigger': False,
            'logical_date': dr.execution_date.isoformat(),
            'start_date': dr.start_date.isoformat(),
            'state': state,
            'data_interval_start': dr.data_interval_start.isoformat(),
            'data_interval_end': dr.data_interval_end.isoformat(),
            'last_scheduling_decision': None,
            'run_type': run_type,
        }

    @pytest.mark.parametrize('invalid_state', ["running"])
    @freeze_time(TestDagRunEndpoint.default_time)
    def test_should_response_400_for_non_existing_dag_run_state(self, invalid_state, dag_maker):
        dag_id = "TEST_DAG_ID"
        dag_run_id = 'TEST_DAG_RUN_ID'
        with dag_maker(dag_id):
            EmptyOperator(task_id='task_id')
        dag_maker.create_dagrun(run_id=dag_run_id)

        request_json = {"state": invalid_state}

        response = self.client.patch(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
            json=request_json,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            'detail': f"'{invalid_state}' is not one of ['success', 'failed', 'queued'] - 'state'",
            'status': 400,
            'title': 'Bad Request',
            'type': EXCEPTIONS_LINK_MAP[400],
        }

    def test_should_raises_401_unauthenticated(self, session):
        response = self.client.patch(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
            json={
                "state": 'success',
            },
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.patch(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
            json={
                "state": 'success',
            },
            environ_overrides={'REMOTE_USER': "test_no_permissions"},
        )
        assert response.status_code == 403

    def test_should_respond_404(self):
        response = self.client.patch(
            "api/v1/dags/INVALID_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
            json={
                "state": 'success',
            },
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404


class TestClearDagRun(TestDagRunEndpoint):
    def test_should_respond_200(self, dag_maker, session):
        dag_id = "TEST_DAG_ID"
        dag_run_id = "TEST_DAG_RUN_ID"
        with dag_maker(dag_id) as dag:
            task = EmptyOperator(task_id="task_id", dag=dag)
        self.app.dag_bag.bag_dag(dag, root_dag=dag)
        dr = dag_maker.create_dagrun(run_id=dag_run_id)
        ti = dr.get_task_instance(task_id="task_id")
        ti.task = task
        ti.state = State.SUCCESS
        session.merge(ti)
        session.commit()

        request_json = {"dry_run": False}

        response = self.client.post(
            f"api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/clear",
            json=request_json,
            environ_overrides={"REMOTE_USER": "test"},
        )

        dr = session.query(DagRun).filter(DagRun.run_id == dr.run_id).first()
        assert response.status_code == 200
        assert response.json == {
            "conf": {},
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "end_date": None,
            "execution_date": dr.execution_date.isoformat(),
            "external_trigger": False,
            "logical_date": dr.logical_date.isoformat(),
            "start_date": dr.logical_date.isoformat(),
            "state": "queued",
            "data_interval_start": dr.data_interval_start.isoformat(),
            "data_interval_end": dr.data_interval_end.isoformat(),
            "last_scheduling_decision": None,
            "run_type": dr.run_type,
        }

        ti.refresh_from_db()
        assert ti.state is None

    def test_dry_run(self, dag_maker, session):
        """Test that dry_run being True returns TaskInstances without clearing DagRun"""
        dag_id = "TEST_DAG_ID"
        dag_run_id = "TEST_DAG_RUN_ID"
        with dag_maker(dag_id) as dag:
            task = EmptyOperator(task_id="task_id", dag=dag)
        self.app.dag_bag.bag_dag(dag, root_dag=dag)
        dr = dag_maker.create_dagrun(run_id=dag_run_id)
        ti = dr.get_task_instance(task_id="task_id")
        ti.task = task
        ti.state = State.SUCCESS
        session.merge(ti)
        session.commit()

        request_json = {"dry_run": True}

        response = self.client.post(
            f"api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/clear",
            json=request_json,
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        assert response.json == {
            "task_instances": [
                {
                    "dag_id": dag_id,
                    "dag_run_id": dag_run_id,
                    "execution_date": dr.execution_date.isoformat(),
                    "task_id": "task_id",
                }
            ]
        }

        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

        dr = session.query(DagRun).filter(DagRun.run_id == dr.run_id).first()
        assert dr.state == "running"

    def test_should_raises_401_unauthenticated(self, session):
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1/clear",
            json={
                "dry_run": True,
            },
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1/clear",
            json={
                "dry_run": True,
            },
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

    def test_should_respond_404(self):
        response = self.client.post(
            "api/v1/dags/INVALID_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1/clear",
            json={
                "dry_run": True,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404


def test__get_upstream_dataset_events_no_prior(configured_app):
    """If no prior dag runs, return all events"""
    from airflow.api_connexion.endpoints.dag_run_endpoint import _get_upstream_dataset_events

    # setup dags and datasets
    unique_id = str(uuid4())
    session = settings.Session()
    dataset1a = Dataset(uri=f"s3://{unique_id}-1a")
    dataset1b = Dataset(uri=f"s3://{unique_id}-1b")
    dag2 = DAG(dag_id=f"datasets-{unique_id}-2", schedule_on=[dataset1a, dataset1b])
    DAG.bulk_write_to_db(dags=[dag2], session=session)
    session.add_all([dataset1a, dataset1b])
    session.commit()

    # add 5 events
    session.add_all([DatasetEvent(dataset_id=dataset1a.id), DatasetEvent(dataset_id=dataset1b.id)])
    session.add_all([DatasetEvent(dataset_id=dataset1a.id), DatasetEvent(dataset_id=dataset1b.id)])
    session.add_all([DatasetEvent(dataset_id=dataset1a.id)])
    session.commit()

    # create a single dag run, no prior dag runs
    dr = DagRun(dag2.dag_id, run_id=unique_id, run_type=DagRunType.DATASET_TRIGGERED)
    dr.dag = dag2
    session.add(dr)
    session.commit()
    session.expunge_all()

    # check result
    events = _get_upstream_dataset_events(dag_run=dr, session=session)
    assert len(events) == 5


def test__get_upstream_dataset_events_with_prior(configured_app):
    """
    Events returned should be those that occurred after last DATASET_TRIGGERED
    dag run and up to the exec date of current dag run.
    """
    from airflow.api_connexion.endpoints.dag_run_endpoint import _get_upstream_dataset_events

    # setup dags and datasets
    unique_id = str(uuid4())
    session = settings.Session()
    dataset1a = Dataset(uri=f"s3://{unique_id}-1a")
    dataset1b = Dataset(uri=f"s3://{unique_id}-1b")
    dag2 = DAG(dag_id=f"datasets-{unique_id}-2", schedule_on=[dataset1a, dataset1b])
    DAG.bulk_write_to_db(dags=[dag2], session=session)
    session.add_all([dataset1a, dataset1b])
    session.commit()

    # add 2 events, then a dag run, then 3 events, then another dag run then another event
    first_timestamp = pendulum.datetime(2022, 1, 1, tz='UTC')
    session.add_all(
        [
            DatasetEvent(dataset_id=dataset1a.id, timestamp=first_timestamp),
            DatasetEvent(dataset_id=dataset1b.id, timestamp=first_timestamp),
        ]
    )
    dr1 = DagRun(
        dag2.dag_id,
        run_id=unique_id + '-1',
        run_type=DagRunType.DATASET_TRIGGERED,
        execution_date=first_timestamp.add(microseconds=1000),
    )
    dr1.dag = dag2
    session.add(dr1)
    session.add_all(
        [
            DatasetEvent(dataset_id=dataset1a.id, timestamp=first_timestamp.add(microseconds=2000)),
            DatasetEvent(dataset_id=dataset1b.id, timestamp=first_timestamp.add(microseconds=3000)),
            DatasetEvent(dataset_id=dataset1b.id, timestamp=first_timestamp.add(microseconds=4000)),
        ]
    )
    dr2 = DagRun(  # this dag run should be ignored
        dag2.dag_id,
        run_id=unique_id + '-3',
        run_type=DagRunType.MANUAL,
        execution_date=first_timestamp.add(microseconds=3000),
    )
    dr2.dag = dag2
    session.add(dr2)
    dr3 = DagRun(
        dag2.dag_id,
        run_id=unique_id + '-2',
        run_type=DagRunType.DATASET_TRIGGERED,
        execution_date=first_timestamp.add(microseconds=4000),  # exact same time as 3rd event in window
    )
    dr3.dag = dag2
    session.add(dr3)
    session.add_all([DatasetEvent(dataset_id=dataset1a.id, timestamp=first_timestamp.add(microseconds=5000))])
    session.commit()
    session.expunge_all()

    events = _get_upstream_dataset_events(dag_run=dr3, session=session)

    event_times = [x.timestamp for x in events]
    assert event_times == [
        first_timestamp.add(microseconds=2000),
        first_timestamp.add(microseconds=3000),
        first_timestamp.add(microseconds=4000),
    ]


class TestGetDagRunDatasetTriggerEvents(TestDagRunEndpoint):
    @mock.patch('airflow.api_connexion.endpoints.dag_run_endpoint._get_upstream_dataset_events')
    def test_should_respond_200(self, mock_get_events, session):
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.DATASET_TRIGGERED,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state=DagRunState.RUNNING,
        )
        session.add(dagrun_model)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 1
        created_at = pendulum.now('UTC')
        # make sure whatever is returned by this func is what comes out in response.
        d = DatasetEvent(dataset_id=1, timestamp=created_at)
        d.dataset = Dataset(id=1, uri='hello', created_at=created_at, updated_at=created_at)
        mock_get_events.return_value = [d]
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/upstreamDatasetEvents",
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 200
        expected_response = {
            'dataset_events': [
                {
                    'timestamp': str(created_at),
                    'dataset_id': 1,
                    'dataset_uri': d.dataset.uri,
                    'extra': None,
                    'id': None,
                    'source_dag_id': None,
                    'source_map_index': None,
                    'source_run_id': None,
                    'source_task_id': None,
                }
            ],
            'total_entries': 1,
        }
        assert response.json == expected_response

    def test_should_respond_404(self):
        response = self.client.get(
            "api/v1/dags/invalid-id/dagRuns/invalid-id/upstreamDatasetEvents",
            environ_overrides={'REMOTE_USER': "test"},
        )
        assert response.status_code == 404
        expected_resp = {
            'detail': "DAGRun with DAG ID: 'invalid-id' and DagRun ID: 'invalid-id' not found",
            'status': 404,
            'title': 'DAGRun not found',
            'type': EXCEPTIONS_LINK_MAP[404],
        }
        assert expected_resp == response.json

    def test_should_raises_401_unauthenticated(self, session):
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
        )
        session.add(dagrun_model)
        session.commit()

        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/upstreamDatasetEvents")

        assert_401(response)
