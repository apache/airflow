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
from __future__ import annotations

import urllib
from datetime import timedelta
from unittest import mock

import pytest
import time_machine

from airflow.api_connexion.exceptions import EXCEPTIONS_LINK_MAP
from airflow.assets import Asset
from airflow.models.asset import AssetEvent, AssetModel
from airflow.models.dag import DAG, DagModel
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType

from tests_common.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags
from tests_common.test_utils.www import _check_last_log

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,
        username="test",
        role_name="admin",
    )
    create_user(app, username="test_no_permissions", role_name=None)

    yield app

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")


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
        dag_instance.is_active = True
        with create_session() as session:
            session.add(dag_instance)
        dag = DAG(dag_id=dag_id, schedule=None, params={"validated_number": Param(1, minimum=1, maximum=10)})
        self.app.dag_bag.bag_dag(dag)
        return dag_instance

    def _create_test_dag_run(self, state=DagRunState.RUNNING, extra_dag=False, commit=True, idx_start=1):
        dag_runs = []
        dags = []
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        for i in range(idx_start, idx_start + 2):
            if i == 1:
                dags.append(DagModel(dag_id="TEST_DAG_ID", is_active=True))
            dagrun_model = DagRun(
                dag_id="TEST_DAG_ID",
                run_id=f"TEST_DAG_RUN_ID_{i}",
                run_type=DagRunType.MANUAL,
                execution_date=timezone.parse(self.default_time) + timedelta(days=i - 1),
                start_date=timezone.parse(self.default_time),
                external_trigger=True,
                state=state,
                **triggered_by_kwargs,
            )
            dagrun_model.updated_at = timezone.parse(self.default_time)
            dag_runs.append(dagrun_model)

        if extra_dag:
            for i in range(idx_start + 2, idx_start + 4):
                dags.append(DagModel(dag_id=f"TEST_DAG_ID_{i}"))
                dag_runs.append(
                    DagRun(
                        dag_id=f"TEST_DAG_ID_{i}",
                        run_id=f"TEST_DAG_RUN_ID_{i}",
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
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 204
        # Check if the Dag Run is deleted from the database
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 404

    def test_should_respond_404(self):
        response = self.client.delete(
            "api/v1/dags/INVALID_DAG_RUN/dagRuns/INVALID_DAG_RUN", environ_overrides={"REMOTE_USER": "test"}
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
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403


class TestGetDagRun(TestDagRunEndpoint):
    def test_should_respond_200(self, session):
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="running",
            **triggered_by_kwargs,
        )
        expected_response_json = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID",
            "end_date": None,
            "state": "running",
            "logical_date": self.default_time,
            "execution_date": self.default_time,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})
        session.add(dagrun_model)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 1
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        assert response.json == expected_response_json

    def test_should_respond_404(self):
        response = self.client.get(
            "api/v1/dags/invalid-id/dagRuns/invalid-id", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 404
        expected_resp = {
            "detail": "DAGRun with DAG ID: 'invalid-id' and DagRun ID: 'invalid-id' not found",
            "status": 404,
            "title": "DAGRun not found",
            "type": EXCEPTIONS_LINK_MAP[404],
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

    @pytest.mark.parametrize(
        "fields",
        [
            ["dag_run_id", "logical_date"],
            ["dag_run_id", "state", "conf", "execution_date"],
        ],
    )
    def test_should_return_specified_fields(self, session, fields):
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="running",
        )
        session.add(dagrun_model)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 1
        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID?fields={','.join(fields)}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        res_json = response.json
        print("get dagRun", res_json)
        assert len(res_json.keys()) == len(fields)
        for field in fields:
            assert field in res_json

    def test_should_respond_400_with_not_exists_fields(self, session):
        dagrun_model = DagRun(
            dag_id="TEST_DAG_ID",
            run_id="TEST_DAG_RUN_ID",
            run_type=DagRunType.MANUAL,
            execution_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="running",
        )
        session.add(dagrun_model)
        session.commit()
        result = session.query(DagRun).all()
        assert len(result) == 1
        fields = ["#caw&c"]
        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID?fields={','.join(fields)}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400, f"Current code: {response.status_code}"


class TestGetDagRuns(TestDagRunEndpoint):
    def test_should_respond_200(self, session):
        self._create_test_dag_run()
        expected_response_json_1 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_1",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time,
            "logical_date": self.default_time,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_1.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})
        expected_response_json_2 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_2",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time_2,
            "logical_date": self.default_time_2,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_2.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})
        result = session.query(DagRun).all()
        assert len(result) == 2
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_runs": [
                expected_response_json_1,
                expected_response_json_2,
            ],
            "total_entries": 2,
        }

    def test_filter_by_state(self, session):
        self._create_test_dag_run()
        self._create_test_dag_run(state="queued", idx_start=3)
        assert session.query(DagRun).count() == 4
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?state=running,queued", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 4
        assert response.json["dag_runs"][0]["state"] == response.json["dag_runs"][1]["state"] == "running"
        assert response.json["dag_runs"][2]["state"] == response.json["dag_runs"][3]["state"] == "queued"

    def test_invalid_order_by_raises_400(self):
        self._create_test_dag_run()

        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?order_by=invalid", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        msg = "Ordering with 'invalid' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    def test_return_correct_results_with_order_by(self, session):
        self._create_test_dag_run()
        expected_response_json_2 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_2",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time_2,
            "logical_date": self.default_time_2,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_2.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})
        expected_response_json_1 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_1",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time,
            "logical_date": self.default_time,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_1.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        result = session.query(DagRun).all()
        assert len(result) == 2
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?order_by=-execution_date",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert response.status_code == 200
        assert self.default_time < self.default_time_2
        # - means descending
        assert response.json == {
            "dag_runs": [
                expected_response_json_2,
                expected_response_json_1,
            ],
            "total_entries": 2,
        }

    def test_should_return_all_with_tilde_as_dag_id_and_all_dag_permissions(self):
        self._create_test_dag_run(extra_dag=True)
        expected_dag_run_ids = ["TEST_DAG_ID", "TEST_DAG_ID", "TEST_DAG_ID_3", "TEST_DAG_ID_4"]
        response = self.client.get("api/v1/dags/~/dagRuns", environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        dag_run_ids = [dag_run["dag_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

    def test_should_raises_401_unauthenticated(self):
        self._create_test_dag_run()

        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns")

        assert_401(response)

    @pytest.mark.parametrize(
        "fields",
        [
            ["dag_run_id", "logical_date"],
            ["dag_run_id", "state", "conf", "execution_date"],
        ],
    )
    def test_should_return_specified_fields(self, session, fields):
        self._create_test_dag_run()
        result = session.query(DagRun).all()
        assert len(result) == 2
        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns?fields={','.join(fields)}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        for dag_run in response.json["dag_runs"]:
            assert len(dag_run.keys()) == len(fields)
            for field in fields:
                assert field in dag_run

    def test_should_respond_400_with_not_exists_fields(self):
        self._create_test_dag_run()
        fields = ["#caw&c"]
        response = self.client.get(
            f"api/v1/dags/TEST_DAG_ID/dagRuns?fields={','.join(fields)}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400, f"Current code: {response.status_code}"


class TestGetDagRunsPagination(TestDagRunEndpoint):
    @pytest.mark.parametrize(
        "url, expected_dag_run_ids",
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
        ],
    )
    def test_handle_limit_and_offset(self, url, expected_dag_run_ids):
        self._create_dag_runs(10)
        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200

        assert response.json["total_entries"] == 10
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

    def test_should_respect_page_size_limit(self):
        self._create_dag_runs(200)
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["dag_runs"]) == 100  # default is 100

    @conf_vars({("api", "maximum_page_limit"): "150"})
    def test_should_return_conf_max_if_req_max_above_conf(self):
        self._create_dag_runs(200)
        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns?limit=180", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        assert len(response.json["dag_runs"]) == 150

    def _create_dag_runs(self, count):
        dag_runs = [
            DagRun(
                dag_id="TEST_DAG_ID",
                run_id=f"TEST_DAG_RUN_ID{i}",
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
    @pytest.mark.parametrize(
        "url, expected_dag_run_ids",
        [
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?start_date_gte=2020-06-18T18%3A00%3A00%2B00%3A00",
                ["TEST_START_EXEC_DAY_18", "TEST_START_EXEC_DAY_19"],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?start_date_lte=2020-06-11T18%3A00%3A00%2B00%3A00",
                ["TEST_START_EXEC_DAY_10", "TEST_START_EXEC_DAY_11"],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?start_date_lte=2020-06-15T18%3A00%3A00%2B00%3A00"
                "&start_date_gte=2020-06-12T18:00:00Z",
                [
                    "TEST_START_EXEC_DAY_12",
                    "TEST_START_EXEC_DAY_13",
                    "TEST_START_EXEC_DAY_14",
                    "TEST_START_EXEC_DAY_15",
                ],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_lte=2020-06-13T18%3A00%3A00%2B00%3A00",
                [
                    "TEST_START_EXEC_DAY_10",
                    "TEST_START_EXEC_DAY_11",
                    "TEST_START_EXEC_DAY_12",
                    "TEST_START_EXEC_DAY_13",
                ],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?execution_date_gte=2020-06-16T18%3A00%3A00%2B00%3A00",
                [
                    "TEST_START_EXEC_DAY_16",
                    "TEST_START_EXEC_DAY_17",
                    "TEST_START_EXEC_DAY_18",
                    "TEST_START_EXEC_DAY_19",
                ],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?updated_at_lte=2020-06-13T18%3A00%3A00%2B00%3A00",
                [
                    "TEST_START_EXEC_DAY_10",
                    "TEST_START_EXEC_DAY_11",
                    "TEST_START_EXEC_DAY_12",
                    "TEST_START_EXEC_DAY_13",
                ],
            ),
            (
                "api/v1/dags/TEST_DAG_ID/dagRuns?updated_at_gte=2020-06-16T18%3A00%3A00%2B00%3A00",
                [
                    "TEST_START_EXEC_DAY_16",
                    "TEST_START_EXEC_DAY_17",
                    "TEST_START_EXEC_DAY_18",
                    "TEST_START_EXEC_DAY_19",
                ],
            ),
        ],
    )
    @provide_session
    def test_date_filters_gte_and_lte(self, url, expected_dag_run_ids, session):
        dagrun_models = self._create_dag_runs()
        session.add_all(dagrun_models)
        for d in dagrun_models:
            d.updated_at = d.execution_date
        session.commit()

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
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
                run_id=f"TEST_START_EXEC_DAY_1{i}",
                run_type=DagRunType.MANUAL,
                execution_date=timezone.parse(dates[i]),
                start_date=timezone.parse(dates[i]),
                external_trigger=True,
                state=DagRunState.SUCCESS,
            )
            for i in range(len(dates))
        ]


class TestGetDagRunsEndDateFilters(TestDagRunEndpoint):
    @pytest.mark.parametrize(
        "url, expected_dag_run_ids",
        [
            pytest.param(
                f"api/v1/dags/TEST_DAG_ID/dagRuns?end_date_gte="
                f"{urllib.parse.quote((timezone.utcnow() + timedelta(days=1)).isoformat())}",
                [],
                id="end_date_gte",
            ),
            pytest.param(
                f"api/v1/dags/TEST_DAG_ID/dagRuns?end_date_lte="
                f"{urllib.parse.quote((timezone.utcnow() + timedelta(days=1)).isoformat())}",
                ["TEST_DAG_RUN_ID_1", "TEST_DAG_RUN_ID_2"],
                id="end_date_lte",
            ),
        ],
    )
    def test_end_date_gte_lte(self, url, expected_dag_run_ids):
        self._create_test_dag_run("success")  # state==success, then end date is today
        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == len(expected_dag_run_ids)
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"] if dag_run]
        assert dag_run_ids == expected_dag_run_ids


class TestGetDagRunBatch(TestDagRunEndpoint):
    def test_should_respond_200(self):
        self._create_test_dag_run()
        expected_response_json_1 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_1",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time,
            "logical_date": self.default_time,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_1.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        expected_response_json_2 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_2",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time_2,
            "logical_date": self.default_time_2,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_2.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": ["TEST_DAG_ID"]},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_runs": [
                expected_response_json_1,
                expected_response_json_2,
            ],
            "total_entries": 2,
        }

    def test_raises_validation_error_for_invalid_request(self):
        self._create_test_dag_run()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dagids": ["TEST_DAG_ID"]},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "detail": "{'dagids': ['Unknown field.']}",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_filter_by_state(self):
        self._create_test_dag_run()
        self._create_test_dag_run(state="queued", idx_start=3)
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": ["TEST_DAG_ID"], "states": ["running", "queued"]},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == 4
        assert response.json["dag_runs"][0]["state"] == response.json["dag_runs"][1]["state"] == "running"
        assert response.json["dag_runs"][2]["state"] == response.json["dag_runs"][3]["state"] == "queued"

    def test_order_by_descending_works(self):
        self._create_test_dag_run()
        expected_response_json_1 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_2",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time_2,
            "logical_date": self.default_time_2,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_1.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})
        expected_response_json_2 = {
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": "TEST_DAG_RUN_ID_1",
            "end_date": None,
            "state": "running",
            "execution_date": self.default_time,
            "logical_date": self.default_time,
            "external_trigger": True,
            "start_date": self.default_time,
            "conf": {},
            "data_interval_end": None,
            "data_interval_start": None,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": None,
        }
        expected_response_json_2.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": ["TEST_DAG_ID"], "order_by": "-dag_run_id"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "dag_runs": [
                expected_response_json_1,
                expected_response_json_2,
            ],
            "total_entries": 2,
        }

    def test_order_by_raises_for_invalid_attr(self):
        self._create_test_dag_run()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list",
            json={"dag_ids": ["TEST_DAG_ID"], "order_by": "-dag_ru"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        msg = "Ordering with 'dag_ru' is disallowed or the attribute does not exist on the model"
        assert response.json["detail"] == msg

    @pytest.mark.parametrize(
        "payload, error",
        [
            (
                {"dag_ids": ["TEST_DAG_ID"], "page_offset": -1},
                "-1 is less than the minimum of 0 - 'page_offset'",
            ),
            ({"dag_ids": ["TEST_DAG_ID"], "page_limit": 0}, "0 is less than the minimum of 1 - 'page_limit'"),
            ({"dag_ids": "TEST_DAG_ID"}, "'TEST_DAG_ID' is not of type 'array' - 'dag_ids'"),
            ({"start_date_gte": "2020-06-12T18"}, "'2020-06-12T18' is not a 'date-time' - 'start_date_gte'"),
        ],
    )
    def test_payload_validation(self, payload, error):
        self._create_test_dag_run()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        assert response.json.get("detail") == error

    def test_should_raises_401_unauthenticated(self):
        self._create_test_dag_run()

        response = self.client.post("api/v1/dags/~/dagRuns/list", json={"dag_ids": ["TEST_DAG_ID"]})

        assert_401(response)


class TestGetDagRunBatchPagination(TestDagRunEndpoint):
    @pytest.mark.parametrize(
        "payload, expected_dag_run_ids",
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
        ],
    )
    def test_handle_limit_and_offset(self, payload, expected_dag_run_ids):
        self._create_dag_runs(10)
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200

        assert response.json["total_entries"] == 10
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"]]
        assert dag_run_ids == expected_dag_run_ids

    def test_should_respect_page_size_limit(self):
        self._create_dag_runs(200)
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json={}, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200

        assert response.json["total_entries"] == 200
        assert len(response.json["dag_runs"]) == 100  # default is 100

    def _create_dag_runs(self, count):
        dag_runs = [
            DagRun(
                dag_id="TEST_DAG_ID",
                run_id=f"TEST_DAG_RUN_ID{i}",
                state="running",
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
    @pytest.mark.parametrize(
        "payload, expected_dag_run_ids",
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
        ],
    )
    def test_date_filters_gte_and_lte(self, payload, expected_dag_run_ids):
        self._create_dag_runs()
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={"REMOTE_USER": "test"}
        )
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

        dag = DagModel(dag_id="TEST_DAG_ID")
        dag_runs = [
            DagRun(
                dag_id="TEST_DAG_ID",
                run_id=f"TEST_START_EXEC_DAY_1{i}",
                run_type=DagRunType.MANUAL,
                execution_date=timezone.parse(date),
                start_date=timezone.parse(date),
                external_trigger=True,
                state="success",
            )
            for i, date in enumerate(dates)
        ]
        with create_session() as session:
            session.add_all(dag_runs)
            session.add(dag)
        return dag_runs

    @pytest.mark.parametrize(
        "payload, expected_response",
        [
            (
                {"execution_date_gte": "2020-11-09T16:25:56.939143"},
                "'2020-11-09T16:25:56.939143' is not a 'date-time' - 'execution_date_gte'",
            ),
            (
                {"start_date_gte": "2020-06-18T16:25:56.939143"},
                "'2020-06-18T16:25:56.939143' is not a 'date-time' - 'start_date_gte'",
            ),
            (
                {"start_date_lte": "2020-06-18T18:00:00.564434"},
                "'2020-06-18T18:00:00.564434' is not a 'date-time' - 'start_date_lte'",
            ),
            (
                {"start_date_lte": "2020-06-15T18:00:00.653434", "start_date_gte": "2020-06-12T18:00.343534"},
                "'2020-06-12T18:00.343534' is not a 'date-time' - 'start_date_gte'",
            ),
            (
                {"execution_date_lte": "2020-06-13T18:00:00.353454"},
                "'2020-06-13T18:00:00.353454' is not a 'date-time' - 'execution_date_lte'",
            ),
            (
                {"execution_date_gte": "2020-06-16T18:00:00.676443"},
                "'2020-06-16T18:00:00.676443' is not a 'date-time' - 'execution_date_gte'",
            ),
        ],
    )
    def test_naive_date_filters_raises_400(self, payload, expected_response):
        self._create_dag_runs()

        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        assert response.json["detail"] == expected_response

    @pytest.mark.parametrize(
        "payload, expected_dag_run_ids",
        [
            (
                {"end_date_gte": f"{(timezone.utcnow() + timedelta(days=1)).isoformat()}"},
                [],
            ),
            (
                {"end_date_lte": f"{(timezone.utcnow() + timedelta(days=1)).isoformat()}"},
                ["TEST_DAG_RUN_ID_1", "TEST_DAG_RUN_ID_2"],
            ),
        ],
    )
    def test_end_date_gte_lte(self, payload, expected_dag_run_ids):
        self._create_test_dag_run("success")  # state==success, then end date is today
        response = self.client.post(
            "api/v1/dags/~/dagRuns/list", json=payload, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 200
        assert response.json["total_entries"] == len(expected_dag_run_ids)
        dag_run_ids = [dag_run["dag_run_id"] for dag_run in response.json["dag_runs"] if dag_run]
        assert dag_run_ids == expected_dag_run_ids


class TestPostDagRun(TestDagRunEndpoint):
    @time_machine.travel(timezone.utcnow(), tick=False)
    @pytest.mark.parametrize("logical_date_field_name", ["execution_date", "logical_date"])
    @pytest.mark.parametrize(
        "dag_run_id, logical_date, note, data_interval_start, data_interval_end",
        [
            pytest.param(
                "TEST_DAG_RUN", "2020-06-11T18:00:00+00:00", "test-note", None, None, id="all-present"
            ),
            pytest.param(
                "TEST_DAG_RUN",
                "2024-06-11T18:00:00+00:00",
                "test-note",
                "2024-01-03T00:00:00+00:00",
                "2024-01-04T05:00:00+00:00",
                id="all-present-with-dates",
            ),
            pytest.param(None, "2020-06-11T18:00:00+00:00", None, None, None, id="only-date"),
            pytest.param(None, None, None, None, None, id="all-missing"),
        ],
    )
    def test_should_respond_200(
        self,
        session,
        logical_date_field_name,
        dag_run_id,
        logical_date,
        note,
        data_interval_start,
        data_interval_end,
    ):
        self._create_dag("TEST_DAG_ID")

        # We freeze time for this test, so we could check it into the returned dates.
        fixed_now = timezone.utcnow()

        # raise NotImplementedError("TODO: Add tests for data_interval_start and data_interval_end")

        request_json = {}
        if logical_date is not None:
            request_json[logical_date_field_name] = logical_date
        if dag_run_id is not None:
            request_json["dag_run_id"] = dag_run_id
        if data_interval_start is not None:
            request_json["data_interval_start"] = data_interval_start
        if data_interval_end is not None:
            request_json["data_interval_end"] = data_interval_end

        request_json["note"] = note
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

        expected_data_interval_start = expected_logical_date
        expected_data_interval_end = expected_logical_date
        if data_interval_start is not None and data_interval_end is not None:
            expected_data_interval_start = data_interval_start
            expected_data_interval_end = data_interval_end

        expected_response_json = {
            "conf": {},
            "dag_id": "TEST_DAG_ID",
            "dag_run_id": expected_dag_run_id,
            "end_date": None,
            "execution_date": expected_logical_date,
            "logical_date": expected_logical_date,
            "external_trigger": True,
            "start_date": None,
            "state": "queued",
            "data_interval_end": expected_data_interval_end,
            "data_interval_start": expected_data_interval_start,
            "last_scheduling_decision": None,
            "run_type": "manual",
            "note": note,
        }
        expected_response_json.update({"triggered_by": "rest_api"} if AIRFLOW_V_3_0_PLUS else {})

        assert response.json == expected_response_json
        _check_last_log(session, dag_id="TEST_DAG_ID", event="api.post_dag_run", execution_date=None)

    def test_raises_validation_error_for_invalid_request(self):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"executiondate": "2020-11-10T08:25:56Z"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "detail": "{'executiondate': ['Unknown field.']}",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_raises_validation_error_for_invalid_params(self):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"conf": {"validated_number": 5000}},  # DAG param must be between 1 and 10
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert "Invalid input for param" in response.json["detail"]

    @mock.patch("airflow.api_connexion.endpoints.dag_run_endpoint.get_airflow_app")
    def test_dagrun_creation_exception_is_handled(self, mock_get_app, session):
        self._create_dag("TEST_DAG_ID")
        error_message = "Encountered Error"
        mock_get_app.return_value.dag_bag.get_dag.return_value.create_dagrun.side_effect = ValueError(
            error_message
        )
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"execution_date": "2020-11-10T08:25:56Z"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "detail": error_message,
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_should_respond_404_if_a_dag_is_inactive(self, session):
        dm = self._create_dag("TEST_INACTIVE_DAG_ID")
        dm.is_active = False
        session.add(dm)
        session.flush()
        response = self.client.post(
            "api/v1/dags/TEST_INACTIVE_DAG_ID/dagRuns",
            json={},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404

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
            "title": "DAG cannot be triggered",
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

        expected_response_json = {
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
            "note": None,
        }
        expected_response_json.update({"triggered_by": "rest_api"} if AIRFLOW_V_3_0_PLUS else {})

        assert response.status_code == 200
        assert response.json == expected_response_json

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

    @pytest.mark.parametrize(
        "data_interval_start, data_interval_end, expected",
        [
            (
                "2020-11-10T08:25:56.939143",
                None,
                "'2020-11-10T08:25:56.939143' is not a 'date-time' - 'data_interval_start'",
            ),
            (
                None,
                "2020-11-10T08:25:56.939143",
                "'2020-11-10T08:25:56.939143' is not a 'date-time' - 'data_interval_end'",
            ),
            (
                "2020-11-10T08:25:56.939143+00:00",
                None,
                "{'_schema': [\"Both 'data_interval_start' and 'data_interval_end' must be specified together\"]}",
            ),
            (
                None,
                "2020-11-10T08:25:56.939143+00:00",
                "{'_schema': [\"Both 'data_interval_start' and 'data_interval_end' must be specified together\"]}",
            ),
        ],
    )
    def test_should_response_400_for_missing_start_date_or_end_date(
        self, data_interval_start, data_interval_end, expected
    ):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={
                "execution_date": "2020-11-10T08:25:56.939143+00:00",
                "data_interval_start": data_interval_start,
                "data_interval_end": data_interval_end,
            },
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json["detail"] == expected

    @pytest.mark.parametrize(
        "data, expected",
        [
            (
                {"execution_date": "2020-11-10T08:25:56.939143"},
                "'2020-11-10T08:25:56.939143' is not a 'date-time' - 'execution_date'",
            ),
            (
                {"execution_date": "2020-11-10T08:25:56P"},
                "'2020-11-10T08:25:56P' is not a 'date-time' - 'execution_date'",
            ),
            (
                {"logical_date": "2020-11-10T08:25:56.939143"},
                "'2020-11-10T08:25:56.939143' is not a 'date-time' - 'logical_date'",
            ),
            (
                {"logical_date": "2020-11-10T08:25:56P"},
                "'2020-11-10T08:25:56P' is not a 'date-time' - 'logical_date'",
            ),
        ],
    )
    def test_should_response_400_for_naive_datetime_and_bad_datetime(self, data, expected):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns", json=data, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        assert response.json["detail"] == expected

    @pytest.mark.parametrize(
        "data, expected",
        [
            (
                {
                    "dag_run_id": "TEST_DAG_RUN",
                    "execution_date": "2020-06-11T18:00:00+00:00",
                    "conf": "some string",
                },
                "'some string' is not of type 'object' - 'conf'",
            )
        ],
    )
    def test_should_response_400_for_non_dict_dagrun_conf(self, data, expected):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns", json=data, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 400
        assert response.json["detail"] == expected

    def test_response_404(self):
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={"dag_run_id": "TEST_DAG_RUN", "execution_date": self.default_time},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert {
            "detail": "DAG with dag_id: 'TEST_DAG_ID' not found",
            "status": 404,
            "title": "DAG not found",
            "type": EXCEPTIONS_LINK_MAP[404],
        } == response.json

    @pytest.mark.parametrize(
        "url, request_json, expected_response",
        [
            pytest.param(
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
                id="start_date in request json",
            ),
            pytest.param(
                "api/v1/dags/TEST_DAG_ID/dagRuns",
                {"state": "failed", "execution_date": "2020-06-12T18:00:00+00:00"},
                {
                    "detail": "Property is read-only - 'state'",
                    "status": 400,
                    "title": "Bad Request",
                    "type": EXCEPTIONS_LINK_MAP[400],
                },
                id="state in request json",
            ),
        ],
    )
    def test_response_400(self, url, request_json, expected_response):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(url, json=request_json, environ_overrides={"REMOTE_USER": "test"})
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
            environ_overrides={"REMOTE_USER": "test"},
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
            environ_overrides={"REMOTE_USER": "test"},
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

    def test_should_raises_403_unauthorized(self):
        self._create_dag("TEST_DAG_ID")
        response = self.client.post(
            "api/v1/dags/TEST_DAG_ID/dagRuns",
            json={
                "dag_run_id": "TEST_DAG_RUN_ID_1",
                "execution_date": self.default_time,
            },
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403


class TestPatchDagRunState(TestDagRunEndpoint):
    @pytest.mark.parametrize("state", ["failed", "success", "queued"])
    @pytest.mark.parametrize("run_type", [DagRunType.MANUAL, DagRunType.SCHEDULED])
    def test_should_respond_200(self, state, run_type, dag_maker, session):
        dag_id = "TEST_DAG_ID"
        dag_run_id = "TEST_DAG_RUN_ID"
        with dag_maker(dag_id) as dag:
            task = EmptyOperator(task_id="task_id", dag=dag)
        self.app.dag_bag.bag_dag(dag)
        dr = dag_maker.create_dagrun(run_id=dag_run_id, run_type=run_type)
        ti = dr.get_task_instance(task_id="task_id")
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
        expected_response_json = {
            "conf": {},
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "end_date": dr.end_date.isoformat() if state != State.QUEUED else None,
            "execution_date": dr.execution_date.isoformat(),
            "external_trigger": False,
            "logical_date": dr.execution_date.isoformat(),
            "start_date": dr.start_date.isoformat() if state != State.QUEUED else None,
            "state": state,
            "data_interval_start": dr.data_interval_start.isoformat(),
            "data_interval_end": dr.data_interval_end.isoformat(),
            "last_scheduling_decision": None,
            "run_type": run_type,
            "note": None,
        }
        expected_response_json.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        assert response.status_code == 200
        assert response.json == expected_response_json

    def test_schema_validation_error_raises(self, dag_maker, session):
        dag_id = "TEST_DAG_ID"
        dag_run_id = "TEST_DAG_RUN_ID"
        with dag_maker(dag_id) as dag:
            EmptyOperator(task_id="task_id", dag=dag)
        self.app.dag_bag.bag_dag(dag)
        dag_maker.create_dagrun(run_id=dag_run_id)

        response = self.client.patch(
            f"api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
            json={"states": "success"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "detail": "{'states': ['Unknown field.']}",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    @pytest.mark.parametrize("invalid_state", ["running"])
    @time_machine.travel(TestDagRunEndpoint.default_time)
    def test_should_response_400_for_non_existing_dag_run_state(self, invalid_state, dag_maker):
        dag_id = "TEST_DAG_ID"
        dag_run_id = "TEST_DAG_RUN_ID"
        with dag_maker(dag_id):
            EmptyOperator(task_id="task_id")
        dag_maker.create_dagrun(run_id=dag_run_id)

        request_json = {"state": invalid_state}

        response = self.client.patch(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
            json=request_json,
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "detail": f"'{invalid_state}' is not one of ['success', 'failed', 'queued'] - 'state'",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_should_raises_401_unauthenticated(self, session):
        response = self.client.patch(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
            json={
                "state": "success",
            },
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.patch(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
            json={
                "state": "success",
            },
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

    def test_should_respond_404(self):
        response = self.client.patch(
            "api/v1/dags/INVALID_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1",
            json={
                "state": "success",
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
        self.app.dag_bag.bag_dag(dag)
        dr = dag_maker.create_dagrun(run_id=dag_run_id, state=DagRunState.FAILED)
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
        expected_response_json = {
            "conf": {},
            "dag_id": dag_id,
            "dag_run_id": dag_run_id,
            "end_date": None,
            "execution_date": dr.execution_date.isoformat(),
            "external_trigger": False,
            "logical_date": dr.logical_date.isoformat(),
            "start_date": None,
            "state": "queued",
            "data_interval_start": dr.data_interval_start.isoformat(),
            "data_interval_end": dr.data_interval_end.isoformat(),
            "last_scheduling_decision": None,
            "run_type": dr.run_type,
            "note": None,
        }
        expected_response_json.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        assert response.status_code == 200
        assert response.json == expected_response_json

        ti.refresh_from_db()
        assert ti.state is None

    def test_schema_validation_error_raises_for_invalid_fields(self, dag_maker, session):
        dag_id = "TEST_DAG_ID"
        dag_run_id = "TEST_DAG_RUN_ID"
        with dag_maker(dag_id) as dag:
            EmptyOperator(task_id="task_id", dag=dag)
        self.app.dag_bag.bag_dag(dag)
        dag_maker.create_dagrun(run_id=dag_run_id, state=DagRunState.FAILED)
        response = self.client.post(
            f"api/v1/dags/{dag_id}/dagRuns/{dag_run_id}/clear",
            json={"dryrun": False},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "detail": "{'dryrun': ['Unknown field.']}",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_dry_run(self, dag_maker, session):
        """Test that dry_run being True returns TaskInstances without clearing DagRun"""
        dag_id = "TEST_DAG_ID"
        dag_run_id = "TEST_DAG_RUN_ID"
        with dag_maker(dag_id) as dag:
            task = EmptyOperator(task_id="task_id", dag=dag)
        self.app.dag_bag.bag_dag(dag)
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


@pytest.mark.need_serialized_dag
class TestGetDagRunDatasetTriggerEvents(TestDagRunEndpoint):
    def test_should_respond_200(self, dag_maker, session):
        asset1 = Asset(uri="ds1")

        with dag_maker(dag_id="source_dag", start_date=timezone.utcnow(), session=session):
            EmptyOperator(task_id="task", outlets=[asset1])
        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]

        asset1_id = session.query(AssetModel.id).filter_by(uri=asset1.uri).scalar()
        event = AssetEvent(
            asset_id=asset1_id,
            source_task_id=ti.task_id,
            source_dag_id=ti.dag_id,
            source_run_id=ti.run_id,
            source_map_index=ti.map_index,
        )
        session.add(event)

        with dag_maker(dag_id="TEST_DAG_ID", start_date=timezone.utcnow(), session=session):
            pass
        dr = dag_maker.create_dagrun(run_id="TEST_DAG_RUN_ID", run_type=DagRunType.DATASET_TRIGGERED)
        dr.consumed_asset_events.append(event)

        session.commit()
        assert event.timestamp

        response = self.client.get(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/upstreamAssetEvents",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        expected_response = {
            "asset_events": [
                {
                    "timestamp": event.timestamp.isoformat(),
                    "asset_id": asset1_id,
                    "asset_uri": asset1.uri,
                    "extra": {},
                    "id": event.id,
                    "source_dag_id": ti.dag_id,
                    "source_map_index": ti.map_index,
                    "source_run_id": ti.run_id,
                    "source_task_id": ti.task_id,
                    "created_dagruns": [
                        {
                            "dag_id": "TEST_DAG_ID",
                            "dag_run_id": "TEST_DAG_RUN_ID",
                            "data_interval_end": dr.data_interval_end.isoformat(),
                            "data_interval_start": dr.data_interval_start.isoformat(),
                            "end_date": None,
                            "logical_date": dr.logical_date.isoformat(),
                            "start_date": dr.start_date.isoformat(),
                            "state": "running",
                        }
                    ],
                }
            ],
            "total_entries": 1,
        }
        assert response.json == expected_response

    def test_should_respond_404(self):
        response = self.client.get(
            "api/v1/dags/invalid-id/dagRuns/invalid-id/upstreamAssetEvents",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        expected_resp = {
            "detail": "DAGRun with DAG ID: 'invalid-id' and DagRun ID: 'invalid-id' not found",
            "status": 404,
            "title": "DAGRun not found",
            "type": EXCEPTIONS_LINK_MAP[404],
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

        response = self.client.get("api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID/upstreamAssetEvents")

        assert_401(response)


class TestSetDagRunNote(TestDagRunEndpoint):
    def test_should_respond_200(self, dag_maker, session):
        dag_runs: list[DagRun] = self._create_test_dag_run(DagRunState.SUCCESS)
        session.add_all(dag_runs)
        session.commit()
        created_dr: DagRun = dag_runs[0]
        new_note_value = "My super cool DagRun notes"
        response = self.client.patch(
            f"api/v1/dags/{created_dr.dag_id}/dagRuns/{created_dr.run_id}/setNote",
            json={"note": new_note_value},
            environ_overrides={"REMOTE_USER": "test"},
        )

        dr = session.query(DagRun).filter(DagRun.run_id == created_dr.run_id).first()
        expected_response_json = {
            "conf": {},
            "dag_id": dr.dag_id,
            "dag_run_id": dr.run_id,
            "end_date": dr.end_date.isoformat(),
            "execution_date": self.default_time,
            "external_trigger": True,
            "logical_date": self.default_time,
            "start_date": self.default_time,
            "state": "success",
            "data_interval_start": None,
            "data_interval_end": None,
            "last_scheduling_decision": None,
            "run_type": dr.run_type,
            "note": new_note_value,
        }
        expected_response_json.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        assert response.status_code == 200, response.text
        assert dr.note == new_note_value
        assert response.json == expected_response_json
        assert dr.dag_run_note.user_id is not None
        # Update the note again
        new_note_value = "My super cool DagRun notes 2"
        payload = {"note": new_note_value}
        response = self.client.patch(
            f"api/v1/dags/{created_dr.dag_id}/dagRuns/{created_dr.run_id}/setNote",
            json=payload,
            environ_overrides={"REMOTE_USER": "test"},
        )
        expected_response_json_new = {
            "conf": {},
            "dag_id": dr.dag_id,
            "dag_run_id": dr.run_id,
            "end_date": dr.end_date.isoformat(),
            "execution_date": self.default_time,
            "external_trigger": True,
            "logical_date": self.default_time,
            "start_date": self.default_time,
            "state": "success",
            "data_interval_start": None,
            "data_interval_end": None,
            "last_scheduling_decision": None,
            "run_type": dr.run_type,
            "note": new_note_value,
        }
        expected_response_json_new.update({"triggered_by": "test"} if AIRFLOW_V_3_0_PLUS else {})

        assert response.status_code == 200
        assert response.json == expected_response_json_new
        assert dr.dag_run_note.user_id is not None
        _check_last_log(
            session,
            dag_id=dr.dag_id,
            event="api.set_dag_run_note",
            execution_date=None,
            expected_extra=payload,
        )

    def test_schema_validation_error_raises(self, dag_maker, session):
        dag_runs: list[DagRun] = self._create_test_dag_run(DagRunState.SUCCESS)
        session.add_all(dag_runs)
        session.commit()
        created_dr: DagRun = dag_runs[0]

        new_note_value = "My super cool DagRun notes"
        response = self.client.patch(
            f"api/v1/dags/{created_dr.dag_id}/dagRuns/{created_dr.run_id}/setNote",
            json={"notes": new_note_value},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 400
        assert response.json == {
            "detail": "{'notes': ['Unknown field.']}",
            "status": 400,
            "title": "Bad Request",
            "type": EXCEPTIONS_LINK_MAP[400],
        }

    def test_should_raises_401_unauthenticated(self, session):
        response = self.client.patch(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1/setNote",
            json={"note": "I am setting a note while being unauthenticated."},
        )
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.patch(
            "api/v1/dags/TEST_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1/setNote",
            json={"note": "I am setting a note without the proper permissions."},
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

    def test_should_respond_404(self):
        response = self.client.patch(
            "api/v1/dags/INVALID_DAG_ID/dagRuns/TEST_DAG_RUN_ID_1/setNote",
            json={"note": "I am setting a note on a DAG that doesn't exist."},
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
