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

from datetime import datetime
from unittest import mock

import pytest
from fastapi import status
from sqlalchemy import select, update

from airflow.models.dag import DAG
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.utils.session import create_session

from tests_common.test_utils.db import clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


class TestDagVersionEndpoint:
    @pytest.fixture(autouse=True)
    def setup(request, dag_maker, session):
        clear_db_dags()
        clear_db_serialized_dags()

        with dag_maker(
            "ANOTHER_DAG_ID",
        ) as dag:
            EmptyOperator(task_id="task_1")
            EmptyOperator(task_id="task_2")

        dag.sync_to_db()
        SerializedDagModel.write_dag(
            dag, bundle_name="another_bundle_name", bundle_version="some_commit_hash"
        )


class TestGetDagVersion(TestDagVersionEndpoint):
    @pytest.mark.parametrize(
        "dag_id, dag_version, expected_response",
        [
            [
                "ANOTHER_DAG_ID",
                1,
                {
                    "bundle_name": "another_bundle_name",
                    "bundle_version": "some_commit_hash",
                    "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash/dags",
                    "created_at": mock.ANY,
                    "dag_id": "ANOTHER_DAG_ID",
                    "id": mock.ANY,
                    "version_number": 1,
                },
            ],
            [
                "dag_with_multiple_versions",
                1,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash1",
                    "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash1/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 1,
                },
            ],
            [
                "dag_with_multiple_versions",
                2,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash2",
                    "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash2/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 2,
                },
            ],
            [
                "dag_with_multiple_versions",
                3,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash3",
                    "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash3/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 3,
                },
            ],
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_version(self, test_client, dag_id, dag_version, expected_response):
        response = test_client.get(f"/dags/{dag_id}/dagVersions/{dag_version}")
        assert response.status_code == 200
        assert response.json() == expected_response

    def test_get_dag_version_404(self, test_client):
        response = test_client.get("/dags/dag_with_multiple_versions/dagVersions/99")
        assert response.status_code == 404
        assert response.json() == {
            "detail": "The DagVersion with dag_id: `dag_with_multiple_versions` and version_number: `99` was not found",
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/99", params={}
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/dag_with_multiple_versions/dagVersions/99", params={})
        assert response.status_code == 403


class TestGetDagVersions(TestDagVersionEndpoint):
    @pytest.mark.parametrize(
        "dag_id, expected_response",
        [
            [
                "~",
                {
                    "dag_versions": [
                        {
                            "bundle_name": "another_bundle_name",
                            "bundle_version": "some_commit_hash",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash/dags",
                            "created_at": mock.ANY,
                            "dag_id": "ANOTHER_DAG_ID",
                            "id": mock.ANY,
                            "version_number": 1,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash1",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash1/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 1,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash2",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash2/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 2,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash3",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash3/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 3,
                        },
                    ],
                    "total_entries": 4,
                },
            ],
            [
                "dag_with_multiple_versions",
                {
                    "dag_versions": [
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash1",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash1/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 1,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash2",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash2/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 2,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash3",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash3/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 3,
                        },
                    ],
                    "total_entries": 3,
                },
            ],
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_versions(self, test_client, dag_id, expected_response):
        response = test_client.get(f"/dags/{dag_id}/dagVersions")
        assert response.status_code == 200
        assert response.json() == expected_response

    def test_get_dag_versions_missing_dag_id_in_serialized_data(self, test_client):
        DAG1_ID = "test_dag_missing_dag_id"
        test_dag = DAG(dag_id=DAG1_ID, start_date=datetime(2025, 4, 15), schedule="@once")
        EmptyOperator(task_id="test_task", dag=test_dag)
        test_dag.sync_to_db()
        SerializedDagModel.write_dag(test_dag, bundle_name=DAG1_ID)

        with create_session() as session:
            dag_model = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == DAG1_ID))
            if not dag_model:
                pytest.fail("Failed to find serialized DAG in database")
            data = dag_model.data
            del data["dag"]["dag_id"]
            session.execute(
                update(SerializedDagModel).where(SerializedDagModel.dag_id == DAG1_ID).values(_data=data)
            )
            session.commit()

        response = test_client.get(f"/dags/{DAG1_ID}/dagVersions")
        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "An unexpected error occurred" in response.json()["detail"]

    @pytest.mark.parametrize(
        "params, expected_versions, expected_total_entries",
        [
            [{"limit": 1}, [("ANOTHER_DAG_ID", 1)], 4],
            [{"limit": 2}, [("ANOTHER_DAG_ID", 1), ("dag_with_multiple_versions", 1)], 4],
            [{"version_number": 1}, [("ANOTHER_DAG_ID", 1), ("dag_with_multiple_versions", 1)], 2],
            [{"version_number": 2}, [("dag_with_multiple_versions", 2)], 1],
            [{"bundle_name": "another_bundle_name"}, [("ANOTHER_DAG_ID", 1)], 1],
            [
                {"order_by": "version_number"},
                [
                    ("ANOTHER_DAG_ID", 1),
                    ("dag_with_multiple_versions", 1),
                    ("dag_with_multiple_versions", 2),
                    ("dag_with_multiple_versions", 3),
                ],
                4,
            ],
            [
                {"order_by": "-version_number"},
                [
                    ("dag_with_multiple_versions", 3),
                    ("dag_with_multiple_versions", 2),
                    ("dag_with_multiple_versions", 1),
                    ("ANOTHER_DAG_ID", 1),
                ],
                4,
            ],
            [
                {"order_by": "bundle_name"},
                [
                    ("ANOTHER_DAG_ID", 1),
                    ("dag_with_multiple_versions", 1),
                    ("dag_with_multiple_versions", 2),
                    ("dag_with_multiple_versions", 3),
                ],
                4,
            ],
            [
                {"order_by": "-bundle_name"},
                [
                    ("dag_with_multiple_versions", 3),
                    ("dag_with_multiple_versions", 2),
                    ("dag_with_multiple_versions", 1),
                    ("ANOTHER_DAG_ID", 1),
                ],
                4,
            ],
            [
                {"order_by": "bundle_version"},
                [
                    ("ANOTHER_DAG_ID", 1),
                    ("dag_with_multiple_versions", 1),
                    ("dag_with_multiple_versions", 2),
                    ("dag_with_multiple_versions", 3),
                ],
                4,
            ],
            [
                {"order_by": "-bundle_version"},
                [
                    ("dag_with_multiple_versions", 3),
                    ("dag_with_multiple_versions", 2),
                    ("dag_with_multiple_versions", 1),
                    ("ANOTHER_DAG_ID", 1),
                ],
                4,
            ],
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_versions_parameters(
        self, test_client, params, expected_versions, expected_total_entries
    ):
        response = test_client.get("/dags/~/dagVersions", params=params)
        assert response.status_code == 200
        response_payload = response.json()
        assert response_payload["total_entries"] == expected_total_entries
        assert [
            (dag_version["dag_id"], dag_version["version_number"])
            for dag_version in response_payload["dag_versions"]
        ] == expected_versions

    def test_get_dag_versions_should_return_404_for_missing_dag(self, test_client):
        response = test_client.get("/dags/MISSING_ID/dagVersions")
        assert response.status_code == 404
        assert response.json() == {
            "detail": "The DAG with dag_id: `MISSING_ID` was not found",
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags/~/dagVersions", params={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/~/dagVersions", params={})
        assert response.status_code == 403
