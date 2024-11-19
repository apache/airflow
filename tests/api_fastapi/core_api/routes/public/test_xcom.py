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

from unittest import mock

import pytest

from airflow.models import XCom
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import BaseXCom, resolve_xcom_backend
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_xcom

pytestmark = pytest.mark.db_test

TEST_XCOM_KEY = "test_xcom_key"
TEST_XCOM_VALUE = {"key": "value"}
TEST_XCOM_KEY3 = "test_xcom_key_non_existing"

TEST_DAG_ID = "test-dag-id"
TEST_TASK_ID = "test-task-id"
TEST_EXECUTION_DATE = "2005-04-02T00:00:00+00:00"

logical_date_parsed = timezone.parse(TEST_EXECUTION_DATE)
run_id = DagRun.generate_run_id(DagRunType.MANUAL, logical_date_parsed)


@provide_session
def _create_xcom(key, value, backend, session=None) -> None:
    backend.set(
        key=key,
        value=value,
        dag_id=TEST_DAG_ID,
        task_id=TEST_TASK_ID,
        run_id=run_id,
        session=session,
    )


@provide_session
def _create_dag_run(session=None) -> None:
    dagrun = DagRun(
        dag_id=TEST_DAG_ID,
        run_id=run_id,
        logical_date=logical_date_parsed,
        start_date=logical_date_parsed,
        run_type=DagRunType.MANUAL,
    )
    session.add(dagrun)
    ti = TaskInstance(EmptyOperator(task_id=TEST_TASK_ID), run_id=run_id)
    ti.dag_id = TEST_DAG_ID
    session.add(ti)


class CustomXCom(BaseXCom):
    @classmethod
    def deserialize_value(cls, xcom: XCom):
        return f"real deserialized {super().deserialize_value(xcom)}"

    def orm_deserialize_value(self):
        return f"orm deserialized {super().orm_deserialize_value()}"


class TestXComEndpoint:
    @staticmethod
    def clear_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_xcom()

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        self.clear_db()

    def teardown_method(self) -> None:
        self.clear_db()

    def create_xcom(self, key, value, backend=XCom) -> None:
        _create_dag_run()
        _create_xcom(key, value, backend)


class TestGetXComEntry(TestXComEndpoint):
    def test_should_respond_200_stringify(self, test_client):
        self.create_xcom(TEST_XCOM_KEY, TEST_XCOM_VALUE)
        response = test_client.get(
            f"/public/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY}"
        )
        assert response.status_code == 200

        current_data = response.json()
        assert current_data == {
            "dag_id": TEST_DAG_ID,
            "logical_date": logical_date_parsed.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "key": TEST_XCOM_KEY,
            "task_id": TEST_TASK_ID,
            "map_index": -1,
            "timestamp": current_data["timestamp"],
            "value": str(TEST_XCOM_VALUE),
        }

    def test_should_respond_200_native(self, test_client):
        self.create_xcom(TEST_XCOM_KEY, TEST_XCOM_VALUE)
        response = test_client.get(
            f"/public/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY}?stringify=false"
        )
        assert response.status_code == 200

        current_data = response.json()
        assert current_data == {
            "dag_id": TEST_DAG_ID,
            "logical_date": logical_date_parsed.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "key": TEST_XCOM_KEY,
            "task_id": TEST_TASK_ID,
            "map_index": -1,
            "timestamp": current_data["timestamp"],
            "value": TEST_XCOM_VALUE,
        }

    def test_should_raise_404_for_non_existent_xcom(self, test_client):
        response = test_client.get(
            f"/public/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY3}"
        )
        assert response.status_code == 404
        assert response.json()["detail"] == f"XCom entry with key: `{TEST_XCOM_KEY3}` not found"

    @pytest.mark.parametrize(
        "support_deserialize, params, expected_status_or_value",
        [
            pytest.param(
                True,
                {"deserialize": True},
                f"real deserialized {TEST_XCOM_VALUE}",
                id="enabled deserialize-true",
            ),
            pytest.param(
                False,
                {"deserialize": True},
                400,
                id="disabled deserialize-true",
            ),
            pytest.param(
                True,
                {"deserialize": False},
                f"orm deserialized {TEST_XCOM_VALUE}",
                id="enabled deserialize-false",
            ),
            pytest.param(
                False,
                {"deserialize": False},
                f"orm deserialized {TEST_XCOM_VALUE}",
                id="disabled deserialize-false",
            ),
            pytest.param(
                True,
                {},
                f"orm deserialized {TEST_XCOM_VALUE}",
                id="enabled default",
            ),
            pytest.param(
                False,
                {},
                f"orm deserialized {TEST_XCOM_VALUE}",
                id="disabled default",
            ),
        ],
    )
    @conf_vars({("core", "xcom_backend"): "tests.api_fastapi.core_api.routes.public.test_xcom.CustomXCom"})
    def test_custom_xcom_deserialize(
        self, support_deserialize: bool, params: str, expected_status_or_value: int | str, test_client
    ):
        XCom = resolve_xcom_backend()
        self.create_xcom(TEST_XCOM_KEY, TEST_XCOM_VALUE, backend=XCom)

        url = f"/public/dags/{TEST_DAG_ID}/dagRuns/{run_id}/taskInstances/{TEST_TASK_ID}/xcomEntries/{TEST_XCOM_KEY}"
        with mock.patch("airflow.api_fastapi.core_api.routes.public.xcom.XCom", XCom):
            with conf_vars({("api", "enable_xcom_deserialize_support"): str(support_deserialize)}):
                response = test_client.get(url, params=params)

        if isinstance(expected_status_or_value, int):
            assert response.status_code == expected_status_or_value
        else:
            assert response.status_code == 200
            assert response.json()["value"] == expected_status_or_value
