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

import json
from unittest import mock

import pytest

from airflow._shared.timezones import timezone
from airflow.api_fastapi.core_api.datamodels.xcom import XComCreateBody
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, AssetAlias
from airflow.sdk.bases.xcom import BaseXCom
from airflow.sdk.execution_time.xcom import resolve_xcom_backend
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs, clear_db_xcom
from tests_common.test_utils.logs import check_last_log

pytestmark = pytest.mark.db_test

TEST_XCOM_KEY = "test_xcom_key"
TEST_XCOM_VALUE = {"key": "value"}
TEST_XCOM_VALUE_AS_JSON = json.dumps(TEST_XCOM_VALUE)
TEST_XCOM_KEY_2 = "test_xcom_key_non_existing"

TEST_DAG_ID = "test-dag-id"
TEST_DAG_DISPLAY_NAME = "test-dag-id"
TEST_TASK_ID = "test-task-id"
TEST_TASK_DISPLAY_NAME = "test-task-id"
TEST_EXECUTION_DATE = "2005-04-02T00:00:00+00:00"

TEST_DAG_ID_2 = "test-dag-id-2"
TEST_DAG_DISPLAY_NAME_2 = "test-dag-id-2"
TEST_TASK_ID_2 = "test-task-id-2"
TEST_TASK_DISPLAY_NAME_2 = "test-task-id-2"

logical_date_parsed = timezone.parse(TEST_EXECUTION_DATE)
logical_date_formatted = logical_date_parsed.strftime("%Y-%m-%dT%H:%M:%SZ")
run_after_parsed = timezone.parse(TEST_EXECUTION_DATE)
run_id = DagRun.generate_run_id(
    run_type=DagRunType.MANUAL, logical_date=logical_date_parsed, run_after=run_after_parsed
)


@provide_session
def _create_xcom(key, value, backend, session=None) -> None:
    XComModel.set(
        key=key,
        value=value,
        dag_id=TEST_DAG_ID,
        task_id=TEST_TASK_ID,
        run_id=run_id,
        session=session,
    )


@provide_session
def _create_dag_run(dag_maker, session=None):
    with dag_maker(TEST_DAG_ID, schedule=None, start_date=logical_date_parsed):
        EmptyOperator(task_id=TEST_TASK_ID)
    dag_maker.create_dagrun(
        run_id=run_id,
        run_type=DagRunType.MANUAL,
        logical_date=logical_date_parsed,
    )

    dag_maker.sync_dagbag_to_db()
    session.merge(dag_maker.dag_model)
    session.commit()


class CustomXCom(BaseXCom):
    @classmethod
    def deserialize_value(cls, xcom):
        return f"real deserialized {super().deserialize_value(xcom)}"


class TestXComEndpoint:
    @staticmethod
    def clear_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_dag_bundles()
        clear_db_xcom()

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker) -> None:
        self.clear_db()
        _create_dag_run(dag_maker)

    def teardown_method(self) -> None:
        self.clear_db()

    def _create_xcom(self, key, value, backend=None) -> None:
        _create_xcom(key, value, backend)


class TestGetXComEntry(TestXComEndpoint):
    def test_should_respond_200_native(self, test_client):
        self._create_xcom(TEST_XCOM_KEY, TEST_XCOM_VALUE)
        response = test_client.get(
            f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY}"
        )
        assert response.status_code == 200

        current_data = response.json()
        assert current_data == {
            "dag_id": TEST_DAG_ID,
            "dag_display_name": TEST_DAG_DISPLAY_NAME,
            "logical_date": logical_date_parsed.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "run_id": run_id,
            "key": TEST_XCOM_KEY,
            "task_id": TEST_TASK_ID,
            "task_display_name": TEST_TASK_DISPLAY_NAME,
            "map_index": -1,
            "timestamp": current_data["timestamp"],
            "value": json.dumps(TEST_XCOM_VALUE),
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY}"
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY}"
        )
        assert response.status_code == 403

    def test_should_raise_404_for_non_existent_xcom(self, test_client):
        response = test_client.get(
            f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY_2}"
        )
        assert response.status_code == 404
        assert response.json()["detail"] == f"XCom entry with key: `{TEST_XCOM_KEY_2}` not found"

    @pytest.mark.parametrize(
        ("params", "expected_value"),
        [
            pytest.param(
                {"deserialize": True},
                TEST_XCOM_VALUE,
                id="deserialize=true",
            ),
            pytest.param(
                {"deserialize": False},
                f"{TEST_XCOM_VALUE_AS_JSON}",
                id="deserialize=false",
            ),
            pytest.param(
                {},
                f"{TEST_XCOM_VALUE_AS_JSON}",
                id="default",
            ),
        ],
    )
    @conf_vars({("core", "xcom_backend"): "unit.api_fastapi.core_api.routes.public.test_xcom.CustomXCom"})
    def test_custom_xcom_deserialize(self, params: str, expected_value: int | str, test_client):
        # Even with a CustomXCom defined, we should not be using it during deserialization because API / UI doesn't integrate their
        # deserialization with custom backends
        XCom = resolve_xcom_backend()
        self._create_xcom(TEST_XCOM_KEY, TEST_XCOM_VALUE, backend=XCom)

        url = f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY}"
        with mock.patch("airflow.sdk.execution_time.xcom.XCom", XCom):
            response = test_client.get(url, params=params)

        assert response.status_code == 200
        assert response.json()["value"] == expected_value

    @pytest.mark.parametrize(
        ("xcom_value", "expected_return"),
        [
            pytest.param(42, 42, id="jsonable"),
            pytest.param(AssetAlias("x"), "AssetAlias(name='x', group='asset')", id="nonjsonable"),
            pytest.param([42, AssetAlias("x")], [42, "AssetAlias(name='x', group='asset')"], id="nested"),
        ],
    )
    def test_stringify_false(self, test_client, xcom_value, expected_return):
        XComModel.set(TEST_XCOM_KEY, xcom_value, dag_id=TEST_DAG_ID, task_id=TEST_TASK_ID, run_id=run_id)

        url = f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY}"
        response = test_client.get(url, params={"deserialize": True, "stringify": False})
        assert response.status_code == 200
        assert response.json()["value"] == expected_return


class TestGetXComEntries(TestXComEndpoint):
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker) -> None:
        self.clear_db()

    def test_should_respond_200(self, test_client):
        self._create_xcom_entries(TEST_DAG_ID, run_id, logical_date_parsed, TEST_TASK_ID)
        with assert_queries_count(4):
            response = test_client.get(
                f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries"
            )
        assert response.status_code == 200
        response_data = response.json()
        for xcom_entry in response_data["xcom_entries"]:
            xcom_entry["timestamp"] = "TIMESTAMP"

        expected_response = {
            "xcom_entries": [
                {
                    "dag_id": TEST_DAG_ID,
                    "dag_display_name": TEST_DAG_DISPLAY_NAME,
                    "logical_date": logical_date_formatted,
                    "run_id": run_id,
                    "key": f"{TEST_XCOM_KEY}-0",
                    "task_id": TEST_TASK_ID,
                    "task_display_name": TEST_TASK_DISPLAY_NAME,
                    "timestamp": "TIMESTAMP",
                    "map_index": -1,
                },
                {
                    "dag_id": TEST_DAG_ID,
                    "dag_display_name": TEST_DAG_DISPLAY_NAME,
                    "logical_date": logical_date_formatted,
                    "run_id": run_id,
                    "key": f"{TEST_XCOM_KEY}-1",
                    "task_id": TEST_TASK_ID,
                    "task_display_name": TEST_TASK_DISPLAY_NAME,
                    "timestamp": "TIMESTAMP",
                    "map_index": -1,
                },
            ],
            "total_entries": 2,
        }
        assert response_data == expected_response

    def test_should_respond_200_with_tilde(self, test_client):
        self._create_xcom_entries(TEST_DAG_ID, run_id, logical_date_parsed, TEST_TASK_ID)
        self._create_xcom_entries(TEST_DAG_ID_2, run_id, logical_date_parsed, TEST_TASK_ID_2)

        with assert_queries_count(4):
            response = test_client.get("/dags/~/dagRuns/~/taskInstances/~/xcomEntries")
        assert response.status_code == 200
        response_data = response.json()
        for xcom_entry in response_data["xcom_entries"]:
            xcom_entry["timestamp"] = "TIMESTAMP"

        expected_response = {
            "xcom_entries": [
                {
                    "dag_id": TEST_DAG_ID,
                    "dag_display_name": TEST_DAG_DISPLAY_NAME,
                    "logical_date": logical_date_formatted,
                    "run_id": run_id,
                    "key": f"{TEST_XCOM_KEY}-0",
                    "task_id": TEST_TASK_ID,
                    "task_display_name": TEST_TASK_DISPLAY_NAME,
                    "timestamp": "TIMESTAMP",
                    "map_index": -1,
                },
                {
                    "dag_id": TEST_DAG_ID,
                    "dag_display_name": TEST_DAG_DISPLAY_NAME,
                    "logical_date": logical_date_formatted,
                    "run_id": run_id,
                    "key": f"{TEST_XCOM_KEY}-1",
                    "task_id": TEST_TASK_ID,
                    "task_display_name": TEST_TASK_DISPLAY_NAME,
                    "timestamp": "TIMESTAMP",
                    "map_index": -1,
                },
                {
                    "dag_id": TEST_DAG_ID_2,
                    "dag_display_name": TEST_DAG_DISPLAY_NAME_2,
                    "logical_date": logical_date_formatted,
                    "run_id": run_id,
                    "key": f"{TEST_XCOM_KEY}-0",
                    "task_id": TEST_TASK_ID_2,
                    "task_display_name": TEST_TASK_DISPLAY_NAME_2,
                    "timestamp": "TIMESTAMP",
                    "map_index": -1,
                },
                {
                    "dag_id": TEST_DAG_ID_2,
                    "dag_display_name": TEST_DAG_DISPLAY_NAME_2,
                    "logical_date": logical_date_formatted,
                    "run_id": run_id,
                    "key": f"{TEST_XCOM_KEY}-1",
                    "task_id": TEST_TASK_ID_2,
                    "task_display_name": TEST_TASK_DISPLAY_NAME_2,
                    "timestamp": "TIMESTAMP",
                    "map_index": -1,
                },
            ],
            "total_entries": 4,
        }
        assert response_data == expected_response

    @pytest.mark.parametrize("map_index", (0, 1, None))
    def test_should_respond_200_with_map_index(self, map_index, test_client):
        self._create_xcom_entries(TEST_DAG_ID, run_id, logical_date_parsed, TEST_TASK_ID, mapped_ti=True)

        with assert_queries_count(4):
            response = test_client.get(
                "/dags/~/dagRuns/~/taskInstances/~/xcomEntries",
                params={"map_index": map_index} if map_index is not None else None,
            )
        assert response.status_code == 200
        response_data = response.json()

        if map_index is None:
            expected_entries = [
                {
                    "dag_id": TEST_DAG_ID,
                    "dag_display_name": TEST_DAG_DISPLAY_NAME,
                    "logical_date": logical_date_formatted,
                    "run_id": run_id,
                    "key": TEST_XCOM_KEY,
                    "task_id": TEST_TASK_ID,
                    "task_display_name": TEST_TASK_DISPLAY_NAME,
                    "timestamp": "TIMESTAMP",
                    "map_index": idx,
                }
                for idx in range(2)
            ]
        else:
            expected_entries = [
                {
                    "dag_id": TEST_DAG_ID,
                    "dag_display_name": TEST_DAG_DISPLAY_NAME,
                    "logical_date": logical_date_formatted,
                    "run_id": run_id,
                    "key": TEST_XCOM_KEY,
                    "task_id": TEST_TASK_ID,
                    "task_display_name": TEST_TASK_DISPLAY_NAME,
                    "timestamp": "TIMESTAMP",
                    "map_index": map_index,
                }
            ]
        for xcom_entry in response_data["xcom_entries"]:
            xcom_entry["timestamp"] = "TIMESTAMP"
        assert response_data == {
            "xcom_entries": expected_entries,
            "total_entries": len(expected_entries),
        }

    @pytest.mark.parametrize(
        ("key", "expected_entries"),
        [
            (
                TEST_XCOM_KEY,
                [
                    {
                        "dag_id": TEST_DAG_ID,
                        "dag_display_name": TEST_DAG_DISPLAY_NAME,
                        "logical_date": logical_date_formatted,
                        "run_id": run_id,
                        "key": TEST_XCOM_KEY,
                        "task_id": TEST_TASK_ID,
                        "task_display_name": TEST_TASK_DISPLAY_NAME,
                        "timestamp": "TIMESTAMP",
                        "map_index": 0,
                    },
                    {
                        "dag_id": TEST_DAG_ID,
                        "dag_display_name": TEST_DAG_DISPLAY_NAME,
                        "logical_date": logical_date_formatted,
                        "run_id": run_id,
                        "key": TEST_XCOM_KEY,
                        "task_id": TEST_TASK_ID,
                        "task_display_name": TEST_TASK_DISPLAY_NAME,
                        "timestamp": "TIMESTAMP",
                        "map_index": 1,
                    },
                ],
            ),
            (f"{TEST_XCOM_KEY}-0", []),
        ],
    )
    def test_should_respond_200_with_xcom_key(self, key, expected_entries, test_client):
        self._create_xcom_entries(TEST_DAG_ID, run_id, logical_date_parsed, TEST_TASK_ID, mapped_ti=True)
        with assert_queries_count(4):
            response = test_client.get(
                "/dags/~/dagRuns/~/taskInstances/~/xcomEntries",
                params={"xcom_key_pattern": key} if key is not None else None,
            )

        assert response.status_code == 200
        response_data = response.json()
        for xcom_entry in response_data["xcom_entries"]:
            xcom_entry["timestamp"] = "TIMESTAMP"
        assert response_data == {
            "xcom_entries": expected_entries,
            "total_entries": len(expected_entries),
        }

    @provide_session
    def _create_xcom_entries(self, dag_id, run_id, logical_date, task_id, mapped_ti=False, session=None):
        bundle_name = "testing"
        orm_dag_bundle = DagBundleModel(name=bundle_name)
        session.merge(orm_dag_bundle)
        session.flush()

        dag = DAG(dag_id=dag_id)
        sync_dag_to_db(dag)
        dagrun = DagRun(
            dag_id=dag_id,
            run_id=run_id,
            logical_date=logical_date,
            start_date=logical_date,
            run_type=DagRunType.MANUAL,
        )
        session.add(dagrun)
        dag_version = DagVersion.get_latest_version(dag.dag_id)
        if mapped_ti:
            for i in [0, 1]:
                ti = TaskInstance(
                    EmptyOperator(task_id=task_id), run_id=run_id, map_index=i, dag_version_id=dag_version.id
                )
                ti.dag_id = dag_id
                session.add(ti)
        else:
            ti = TaskInstance(EmptyOperator(task_id=task_id), run_id=run_id, dag_version_id=dag_version.id)
            ti.dag_id = dag_id
            session.add(ti)
        session.commit()

        for i in [0, 1]:
            if mapped_ti:
                key = TEST_XCOM_KEY
                map_index = i
            else:
                key = f"{TEST_XCOM_KEY}-{i}"
                map_index = -1

            XComModel.set(
                key=key,
                value=TEST_XCOM_VALUE,
                run_id=run_id,
                task_id=task_id,
                dag_id=dag_id,
                map_index=map_index,
            )

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/~/dagRuns/~/taskInstances/~/xcomEntries",
            params={},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            "/dags/~/dagRuns/~/taskInstances/~/xcomEntries",
            params={},
        )
        assert response.status_code == 403


class TestPaginationGetXComEntries(TestXComEndpoint):
    @pytest.mark.parametrize(
        ("query_params", "expected_xcom_ids"),
        [
            (
                {"limit": "1"},
                ["TEST_XCOM_KEY0"],
            ),
            (
                {"limit": "2"},
                ["TEST_XCOM_KEY0", "TEST_XCOM_KEY1"],
            ),
            (
                {"offset": "5"},
                [
                    "TEST_XCOM_KEY5",
                    "TEST_XCOM_KEY6",
                    "TEST_XCOM_KEY7",
                    "TEST_XCOM_KEY8",
                    "TEST_XCOM_KEY9",
                ],
            ),
            (
                {"offset": "0"},
                [
                    "TEST_XCOM_KEY0",
                    "TEST_XCOM_KEY1",
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
                {"limit": "1", "offset": "5"},
                ["TEST_XCOM_KEY5"],
            ),
            (
                {"limit": "1", "offset": "1"},
                ["TEST_XCOM_KEY1"],
            ),
            (
                {"limit": "2", "offset": "2"},
                ["TEST_XCOM_KEY2", "TEST_XCOM_KEY3"],
            ),
        ],
    )
    def test_handle_limit_offset(self, query_params, expected_xcom_ids, test_client):
        for i in range(10):
            self._create_xcom(f"TEST_XCOM_KEY{i}", TEST_XCOM_VALUE)
        response = test_client.get("/dags/~/dagRuns/~/taskInstances/~/xcomEntries", params=query_params)
        assert response.status_code == 200
        response_data = response.json()
        assert response_data["total_entries"] == 10
        conn_ids = [conn["key"] for conn in response_data["xcom_entries"] if conn]
        assert conn_ids == expected_xcom_ids


class TestCreateXComEntry(TestXComEndpoint):
    @pytest.mark.parametrize(
        ("dag_id", "task_id", "dag_run_id", "request_body", "expected_status", "expected_detail"),
        [
            # Test case: Valid input, should succeed with 201 CREATED
            pytest.param(
                TEST_DAG_ID,
                TEST_TASK_ID,
                run_id,
                XComCreateBody(key=TEST_XCOM_KEY, value=TEST_XCOM_VALUE),
                201,
                None,
                id="valid-xcom-entry",
            ),
            # Test case: DAG not found
            pytest.param(
                "invalid-dag-id",
                TEST_TASK_ID,
                run_id,
                XComCreateBody(key=TEST_XCOM_KEY, value=TEST_XCOM_VALUE),
                404,
                "The Dag with ID: `invalid-dag-id` was not found",
                id="dag-not-found",
            ),
            # Test case: Task not found in DAG
            pytest.param(
                TEST_DAG_ID,
                "invalid-task-id",
                run_id,
                XComCreateBody(key=TEST_XCOM_KEY, value=TEST_XCOM_VALUE),
                404,
                f"Task with ID: `invalid-task-id` not found in dag: `{TEST_DAG_ID}`",
                id="task-not-found",
            ),
            # Test case: DAG Run not found
            pytest.param(
                TEST_DAG_ID,
                TEST_TASK_ID,
                "invalid-dag-run-id",
                XComCreateBody(key=TEST_XCOM_KEY, value=TEST_XCOM_VALUE),
                404,
                f"Dag Run with ID: `invalid-dag-run-id` not found for dag: `{TEST_DAG_ID}`",
                id="dag-run-not-found",
            ),
            # Test case: XCom entry already exists
            pytest.param(
                TEST_DAG_ID,
                TEST_TASK_ID,
                run_id,
                XComCreateBody(key=TEST_XCOM_KEY, value=TEST_XCOM_VALUE),
                409,
                f"The XCom with key: `{TEST_XCOM_KEY}` with mentioned task instance already exists.",
                id="xcom-already-exists",
            ),
        ],
    )
    def test_create_xcom_entry(
        self,
        dag_id,
        task_id,
        dag_run_id,
        request_body,
        expected_status,
        expected_detail,
        test_client,
        session,
    ):
        # Pre-create an XCom entry to test conflict case
        if expected_status == 409:
            self._create_xcom(TEST_XCOM_KEY, TEST_XCOM_VALUE)

        response = test_client.post(
            f"/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/xcomEntries",
            json=request_body.dict(),
        )

        assert response.status_code == expected_status
        if expected_detail:
            assert response.json()["detail"] == expected_detail
        elif expected_status == 201:
            # Validate the created XCom response
            current_data = response.json()
            assert current_data["key"] == request_body.key
            assert current_data["value"] == XComModel.serialize_value(request_body.value)
            assert current_data["dag_id"] == dag_id
            assert current_data["task_id"] == task_id
            assert current_data["run_id"] == dag_run_id
            assert current_data["map_index"] == request_body.map_index
        check_last_log(session, dag_id=TEST_DAG_ID, event="create_xcom_entry", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(
            "/dags/dag_id/dagRuns/dag_run_id/taskInstances/task_id/xcomEntries",
            json={},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(
            "/dags/dag_id/dagRuns/dag_run_id/taskInstances/task_id/xcomEntries",
            json={},
        )
        assert response.status_code == 403


class TestPatchXComEntry(TestXComEndpoint):
    @pytest.mark.parametrize(
        ("key", "patch_body", "expected_status", "expected_detail"),
        [
            # Test case: Valid update, should return 200 OK
            pytest.param(
                TEST_XCOM_KEY,
                {"value": "new_value"},
                200,
                None,
                id="valid-xcom-update",
            ),
            # Test case: XCom entry does not exist, should return 404
            pytest.param(
                TEST_XCOM_KEY,
                {"value": "new_value", "map_index": -1},
                404,
                f"The XCom with key: `{TEST_XCOM_KEY}` with mentioned task instance doesn't exist.",
                id="xcom-not-found",
            ),
        ],
    )
    def test_patch_xcom_entry(self, key, patch_body, expected_status, expected_detail, test_client, session):
        # Ensure the XCom entry exists before updating
        if expected_status != 404:
            self._create_xcom(TEST_XCOM_KEY, TEST_XCOM_VALUE)
            new_value = XComModel.serialize_value(patch_body["value"])

        response = test_client.patch(
            f"/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{key}",
            json=patch_body,
        )

        assert response.status_code == expected_status

        if expected_status == 200:
            assert response.json()["value"] == XComModel.serialize_value(new_value)
        else:
            assert response.json()["detail"] == expected_detail
        check_last_log(session, dag_id=TEST_DAG_ID, event="update_xcom_entry", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(
            f"/dags/{TEST_DAG_ID}/dagRuns/run_id/taskInstances/TEST_TASK_ID/xcomEntries/key",
            json={},
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(
            f"/dags/{TEST_DAG_ID}/dagRuns/run_id/taskInstances/TEST_TASK_ID/xcomEntries/key",
            json={},
        )
        assert response.status_code == 403
