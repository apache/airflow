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

from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator

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
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash",
                            "created_at": mock.ANY,
                            "dag_id": "ANOTHER_DAG_ID",
                            "id": mock.ANY,
                            "version_number": 1,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash1",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash1",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 1,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash2",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash2",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 2,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash3",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash3",
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
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash1",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 1,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash2",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash2",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 2,
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash3",
                            "bundle_url": "fakeprotocol://test_host.github.com/tree/some_commit_hash3",
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
        response = test_client.get(f"/public/dags/{dag_id}/dagVersions")
        assert response.status_code == 200
        assert response.json() == expected_response

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
        response = test_client.get("/public/dags/~/dagVersions", params=params)
        assert response.status_code == 200
        response_payload = response.json()
        assert response_payload["total_entries"] == expected_total_entries
        assert [
            (dag_version["dag_id"], dag_version["version_number"])
            for dag_version in response_payload["dag_versions"]
        ] == expected_versions

    def test_get_dag_versions_should_return_404_for_missing_dag(self, test_client):
        response = test_client.get("/public/dags/MISSING_ID/dagVersions")
        assert response.status_code == 404
        assert response.json() == {
            "detail": "The DAG with dag_id: `MISSING_ID` was not found",
        }
