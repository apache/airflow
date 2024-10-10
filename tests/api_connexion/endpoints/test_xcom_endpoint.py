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

from datetime import timedelta
from unittest import mock

import pytest

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import BaseXCom, XCom, resolve_xcom_backend
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import parse_execution_date
from airflow.utils.session import create_session
from airflow.utils.timezone import utcnow
from airflow.utils.types import DagRunType

from dev.tests_common.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from dev.tests_common.test_utils.config import conf_vars
from dev.tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_xcom

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class CustomXCom(BaseXCom):
    @classmethod
    def deserialize_value(cls, xcom: XCom):
        return f"real deserialized {super().deserialize_value(xcom)}"

    def orm_deserialize_value(self):
        return f"orm deserialized {super().orm_deserialize_value()}"


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


def _compare_xcom_collections(collection1: dict, collection_2: dict):
    assert collection1.get("total_entries") == collection_2.get("total_entries")

    def sort_key(record):
        return (
            record.get("dag_id"),
            record.get("task_id"),
            record.get("execution_date"),
            record.get("map_index"),
            record.get("key"),
        )

    assert sorted(collection1.get("xcom_entries", []), key=sort_key) == sorted(
        collection_2.get("xcom_entries", []), key=sort_key
    )


class TestXComEndpoint:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_xcom()

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        """
        Setup For XCom endpoint TC
        """
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        # clear existing xcoms
        self.clean_db()

    def teardown_method(self) -> None:
        """
        Clear Hanging XComs
        """
        self.clean_db()


class TestGetXComEntry(TestXComEndpoint):
    def test_should_respond_200_stringify(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        xcom_key = "test-xcom-key"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entry(dag_id, run_id, execution_date_parsed, task_id, xcom_key, {"key": "value"})
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert 200 == response.status_code

        current_data = response.json
        current_data["timestamp"] = "TIMESTAMP"
        assert current_data == {
            "dag_id": dag_id,
            "execution_date": execution_date,
            "key": xcom_key,
            "task_id": task_id,
            "map_index": -1,
            "timestamp": "TIMESTAMP",
            "value": "{'key': 'value'}",
        }

    def test_should_respond_200_native(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        xcom_key = "test-xcom-key"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entry(dag_id, run_id, execution_date_parsed, task_id, xcom_key, {"key": "value"})
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}?stringify=false",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert 200 == response.status_code

        current_data = response.json
        current_data["timestamp"] = "TIMESTAMP"
        assert current_data == {
            "dag_id": dag_id,
            "execution_date": execution_date,
            "key": xcom_key,
            "task_id": task_id,
            "map_index": -1,
            "timestamp": "TIMESTAMP",
            "value": {"key": "value"},
        }

    @conf_vars({("core", "enable_xcom_pickling"): "True"})
    def test_should_respond_200_native_for_pickled(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        xcom_key = "test-xcom-key"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        value_non_serializable_key = {("201009_NB502104_0421_AHJY23BGXG (SEQ_WF: 138898)", None): 82359}
        self._create_xcom_entry(
            dag_id, run_id, execution_date_parsed, task_id, xcom_key, {"key": value_non_serializable_key}
        )
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert 200 == response.status_code

        current_data = response.json
        current_data["timestamp"] = "TIMESTAMP"
        assert current_data == {
            "dag_id": dag_id,
            "execution_date": execution_date,
            "key": xcom_key,
            "task_id": task_id,
            "map_index": -1,
            "timestamp": "TIMESTAMP",
            "value": f"{{'key': {str(value_non_serializable_key)}}}",
        }

    def test_should_raise_404_for_non_existent_xcom(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        xcom_key = "test-xcom-key"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entry(dag_id, run_id, execution_date_parsed, task_id, xcom_key)
        response = self.client.get(
            f"/api/v1/dags/nonexistentdagid/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert 404 == response.status_code
        assert response.json["title"] == "XCom entry not found"

    def test_should_raises_401_unauthenticated(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        xcom_key = "test-xcom-key"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entry(dag_id, run_id, execution_date_parsed, task_id, xcom_key)
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}"
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        xcom_key = "test-xcom-key"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)

        self._create_xcom_entry(dag_id, run_id, execution_date_parsed, task_id, xcom_key)
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{xcom_key}",
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

    def _create_xcom_entry(
        self, dag_id, run_id, execution_date, task_id, xcom_key, xcom_value="TEST_VALUE", *, backend=XCom
    ):
        with create_session() as session:
            dagrun = DagRun(
                dag_id=dag_id,
                run_id=run_id,
                execution_date=execution_date,
                start_date=execution_date,
                run_type=DagRunType.MANUAL,
            )
            session.add(dagrun)
            ti = TaskInstance(EmptyOperator(task_id=task_id), run_id=run_id)
            ti.dag_id = dag_id
            session.add(ti)
        backend.set(
            key=xcom_key,
            value=xcom_value,
            run_id=run_id,
            task_id=task_id,
            dag_id=dag_id,
        )

    @pytest.mark.parametrize(
        "allowed, query, expected_status_or_value",
        [
            pytest.param(
                True,
                "?deserialize=true",
                "real deserialized TEST_VALUE",
                id="true",
            ),
            pytest.param(
                False,
                "?deserialize=true",
                400,
                id="disallowed",
            ),
            pytest.param(
                True,
                "?deserialize=false",
                "orm deserialized TEST_VALUE",
                id="false-irrelevant",
            ),
            pytest.param(
                False,
                "?deserialize=false",
                "orm deserialized TEST_VALUE",
                id="false",
            ),
            pytest.param(
                True,
                "",
                "orm deserialized TEST_VALUE",
                id="default-irrelevant",
            ),
            pytest.param(
                False,
                "",
                "orm deserialized TEST_VALUE",
                id="default",
            ),
        ],
    )
    @conf_vars({("core", "xcom_backend"): "tests.api_connexion.endpoints.test_xcom_endpoint.CustomXCom"})
    def test_custom_xcom_deserialize(self, allowed: bool, query: str, expected_status_or_value: int | str):
        XCom = resolve_xcom_backend()
        self._create_xcom_entry("dag", "run", utcnow(), "task", "key", backend=XCom)

        url = f"/api/v1/dags/dag/dagRuns/run/taskInstances/task/xcomEntries/key{query}"
        with mock.patch("airflow.api_connexion.endpoints.xcom_endpoint.XCom", XCom):
            with conf_vars({("api", "enable_xcom_deserialize_support"): str(allowed)}):
                response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})

        if isinstance(expected_status_or_value, int):
            assert response.status_code == expected_status_or_value
        else:
            assert response.status_code == 200
            assert response.json["value"] == expected_status_or_value


class TestGetXComEntries(TestXComEndpoint):
    def test_should_respond_200(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)

        self._create_xcom_entries(dag_id, run_id, execution_date_parsed, task_id)
        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert 200 == response.status_code
        response_data = response.json
        for xcom_entry in response_data["xcom_entries"]:
            xcom_entry["timestamp"] = "TIMESTAMP"
        _compare_xcom_collections(
            response_data,
            {
                "xcom_entries": [
                    {
                        "dag_id": dag_id,
                        "execution_date": execution_date,
                        "key": "test-xcom-key-1",
                        "task_id": task_id,
                        "timestamp": "TIMESTAMP",
                        "map_index": -1,
                    },
                    {
                        "dag_id": dag_id,
                        "execution_date": execution_date,
                        "key": "test-xcom-key-2",
                        "task_id": task_id,
                        "timestamp": "TIMESTAMP",
                        "map_index": -1,
                    },
                ],
                "total_entries": 2,
            },
        )

    def test_should_respond_200_with_tilde_and_access_to_all_dags(self):
        dag_id_1 = "test-dag-id-1"
        task_id_1 = "test-task-id-1"
        execution_date = "2005-04-02T00:00:00+00:00"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id_1 = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id_1, run_id_1, execution_date_parsed, task_id_1)

        dag_id_2 = "test-dag-id-2"
        task_id_2 = "test-task-id-2"
        run_id_2 = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id_2, run_id_2, execution_date_parsed, task_id_2)

        response = self.client.get(
            "/api/v1/dags/~/dagRuns/~/taskInstances/~/xcomEntries",
            environ_overrides={"REMOTE_USER": "test"},
        )

        assert 200 == response.status_code
        response_data = response.json
        for xcom_entry in response_data["xcom_entries"]:
            xcom_entry["timestamp"] = "TIMESTAMP"
        _compare_xcom_collections(
            response_data,
            {
                "xcom_entries": [
                    {
                        "dag_id": dag_id_1,
                        "execution_date": execution_date,
                        "key": "test-xcom-key-1",
                        "task_id": task_id_1,
                        "timestamp": "TIMESTAMP",
                        "map_index": -1,
                    },
                    {
                        "dag_id": dag_id_1,
                        "execution_date": execution_date,
                        "key": "test-xcom-key-2",
                        "task_id": task_id_1,
                        "timestamp": "TIMESTAMP",
                        "map_index": -1,
                    },
                    {
                        "dag_id": dag_id_2,
                        "execution_date": execution_date,
                        "key": "test-xcom-key-1",
                        "task_id": task_id_2,
                        "timestamp": "TIMESTAMP",
                        "map_index": -1,
                    },
                    {
                        "dag_id": dag_id_2,
                        "execution_date": execution_date,
                        "key": "test-xcom-key-2",
                        "task_id": task_id_2,
                        "timestamp": "TIMESTAMP",
                        "map_index": -1,
                    },
                ],
                "total_entries": 4,
            },
        )

    def test_should_respond_200_with_map_index(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id, dag_run_id, execution_date_parsed, task_id, mapped_ti=True)

        def assert_expected_result(expected_entries, map_index=None):
            response = self.client.get(
                "/api/v1/dags/~/dagRuns/~/taskInstances/~/xcomEntries"
                f"{('?map_index=' + str(map_index)) if map_index is not None else ''}",
                environ_overrides={"REMOTE_USER": "test"},
            )

            assert 200 == response.status_code
            response_data = response.json
            for xcom_entry in response_data["xcom_entries"]:
                xcom_entry["timestamp"] = "TIMESTAMP"
            assert response_data == {
                "xcom_entries": expected_entries,
                "total_entries": len(expected_entries),
            }

        expected_entry1 = {
            "dag_id": dag_id,
            "execution_date": execution_date,
            "key": "test-xcom-key",
            "task_id": task_id,
            "timestamp": "TIMESTAMP",
            "map_index": 0,
        }
        expected_entry2 = {
            "dag_id": dag_id,
            "execution_date": execution_date,
            "key": "test-xcom-key",
            "task_id": task_id,
            "timestamp": "TIMESTAMP",
            "map_index": 1,
        }
        assert_expected_result([expected_entry1], map_index=0)
        assert_expected_result([expected_entry2], map_index=1)
        assert_expected_result([expected_entry1, expected_entry2], map_index=None)

    def test_should_respond_200_with_xcom_key(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        execution_date_parsed = parse_execution_date(execution_date)
        dag_run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id, dag_run_id, execution_date_parsed, task_id, mapped_ti=True)

        def assert_expected_result(expected_entries, key=None):
            response = self.client.get(
                f"/api/v1/dags/~/dagRuns/~/taskInstances/~/xcomEntries?xcom_key={key}",
                environ_overrides={"REMOTE_USER": "test"},
            )

            assert 200 == response.status_code
            response_data = response.json
            for xcom_entry in response_data["xcom_entries"]:
                xcom_entry["timestamp"] = "TIMESTAMP"
            assert response_data == {
                "xcom_entries": expected_entries,
                "total_entries": len(expected_entries),
            }

        expected_entry1 = {
            "dag_id": dag_id,
            "execution_date": execution_date,
            "key": "test-xcom-key",
            "task_id": task_id,
            "timestamp": "TIMESTAMP",
            "map_index": 0,
        }
        expected_entry2 = {
            "dag_id": dag_id,
            "execution_date": execution_date,
            "key": "test-xcom-key",
            "task_id": task_id,
            "timestamp": "TIMESTAMP",
            "map_index": 1,
        }
        assert_expected_result([expected_entry1, expected_entry2], key="test-xcom-key")
        assert_expected_result([], key="test-xcom-key-1")

    def test_should_raises_401_unauthenticated(self):
        dag_id = "test-dag-id"
        task_id = "test-task-id"
        execution_date = "2005-04-02T00:00:00+00:00"
        execution_date_parsed = parse_execution_date(execution_date)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date_parsed)
        self._create_xcom_entries(dag_id, run_id, execution_date_parsed, task_id)

        response = self.client.get(
            f"/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries"
        )

        assert_401(response)

    def _create_xcom_entries(self, dag_id, run_id, execution_date, task_id, mapped_ti=False):
        with create_session() as session:
            dag = DagModel(dag_id=dag_id)
            session.add(dag)
            dagrun = DagRun(
                dag_id=dag_id,
                run_id=run_id,
                execution_date=execution_date,
                start_date=execution_date,
                run_type=DagRunType.MANUAL,
            )
            session.add(dagrun)
            if mapped_ti:
                for i in [0, 1]:
                    ti = TaskInstance(EmptyOperator(task_id=task_id), run_id=run_id, map_index=i)
                    ti.dag_id = dag_id
                    session.add(ti)
            else:
                ti = TaskInstance(EmptyOperator(task_id=task_id), run_id=run_id)
                ti.dag_id = dag_id
                session.add(ti)

        for i in [1, 2]:
            if mapped_ti:
                key = "test-xcom-key"
                map_index = i - 1
            else:
                key = f"test-xcom-key-{i}"
                map_index = -1

            XCom.set(
                key=key, value="TEST", run_id=run_id, task_id=task_id, dag_id=dag_id, map_index=map_index
            )

    def _create_invalid_xcom_entries(self, execution_date):
        """
        Invalid XCom entries to test join query
        """
        with create_session() as session:
            dag = DagModel(dag_id="invalid_dag")
            session.add(dag)
            dagrun = DagRun(
                dag_id="invalid_dag",
                run_id="invalid_run_id",
                execution_date=execution_date + timedelta(days=1),
                start_date=execution_date,
                run_type=DagRunType.MANUAL,
            )
            session.add(dagrun)
            dagrun1 = DagRun(
                dag_id="invalid_dag",
                run_id="not_this_run_id",
                execution_date=execution_date,
                start_date=execution_date,
                run_type=DagRunType.MANUAL,
            )
            session.add(dagrun1)
            ti = TaskInstance(EmptyOperator(task_id="invalid_task"), run_id="not_this_run_id")
            ti.dag_id = "invalid_dag"
            session.add(ti)
        for i in [1, 2]:
            XCom.set(
                key=f"invalid-xcom-key-{i}",
                value="TEST",
                run_id="not_this_run_id",
                task_id="invalid_task",
                dag_id="invalid_dag",
            )


class TestPaginationGetXComEntries(TestXComEndpoint):
    def setup_method(self):
        self.dag_id = "test-dag-id"
        self.task_id = "test-task-id"
        self.execution_date = "2005-04-02T00:00:00+00:00"
        self.execution_date_parsed = parse_execution_date(self.execution_date)
        self.run_id = DagRun.generate_run_id(DagRunType.MANUAL, self.execution_date_parsed)

    @pytest.mark.parametrize(
        "query_params, expected_xcom_ids",
        [
            (
                "limit=1",
                ["TEST_XCOM_KEY1"],
            ),
            (
                "limit=2",
                ["TEST_XCOM_KEY1", "TEST_XCOM_KEY10"],
            ),
            (
                "offset=5",
                [
                    "TEST_XCOM_KEY5",
                    "TEST_XCOM_KEY6",
                    "TEST_XCOM_KEY7",
                    "TEST_XCOM_KEY8",
                    "TEST_XCOM_KEY9",
                ],
            ),
            (
                "offset=0",
                [
                    "TEST_XCOM_KEY1",
                    "TEST_XCOM_KEY10",
                    "TEST_XCOM_KEY2",
                    "TEST_XCOM_KEY3",
                    "TEST_XCOM_KEY4",
                    "TEST_XCOM_KEY5",
                    "TEST_XCOM_KEY6",
                    "TEST_XCOM_KEY7",
                    "TEST_XCOM_KEY8",
                    "TEST_XCOM_KEY9",
                ],
            ),
            (
                "limit=1&offset=5",
                ["TEST_XCOM_KEY5"],
            ),
            (
                "limit=1&offset=1",
                ["TEST_XCOM_KEY10"],
            ),
            (
                "limit=2&offset=2",
                ["TEST_XCOM_KEY2", "TEST_XCOM_KEY3"],
            ),
        ],
    )
    def test_handle_limit_offset(self, query_params, expected_xcom_ids):
        url = (
            f"/api/v1/dags/{self.dag_id}/dagRuns/{self.run_id}/taskInstances/{self.task_id}/xcomEntries"
            f"?{query_params}"
        )
        with create_session() as session:
            dagrun = DagRun(
                dag_id=self.dag_id,
                run_id=self.run_id,
                execution_date=self.execution_date_parsed,
                start_date=self.execution_date_parsed,
                run_type=DagRunType.MANUAL,
            )
            session.add(dagrun)
            ti = TaskInstance(EmptyOperator(task_id=self.task_id), run_id=self.run_id)
            ti.dag_id = self.dag_id
            session.add(ti)

        with create_session() as session:
            for i in range(1, 11):
                xcom = XCom(
                    dag_run_id=dagrun.id,
                    key=f"TEST_XCOM_KEY{i}",
                    value=b"null",
                    run_id=self.run_id,
                    task_id=self.task_id,
                    dag_id=self.dag_id,
                    timestamp=self.execution_date_parsed,
                )
                session.add(xcom)

        response = self.client.get(url, environ_overrides={"REMOTE_USER": "test"})
        assert response.status_code == 200
        assert response.json["total_entries"] == 10
        conn_ids = [conn["key"] for conn in response.json["xcom_entries"] if conn]
        assert conn_ids == expected_xcom_ids
