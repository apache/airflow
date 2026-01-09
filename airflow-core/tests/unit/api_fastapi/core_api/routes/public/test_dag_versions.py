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

from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


class TestDagVersionEndpoint:
    @pytest.fixture(autouse=True)
    def setup(request, dag_maker, session):
        clear_db_dags()
        clear_db_serialized_dags()
        clear_db_dag_bundles()

        with dag_maker(
            dag_id="ANOTHER_DAG_ID", bundle_version="some_commit_hash", bundle_name="another_bundle_name"
        ):
            EmptyOperator(task_id="task_1")
            EmptyOperator(task_id="task_2")


class TestGetDagVersion(TestDagVersionEndpoint):
    @pytest.mark.parametrize(
        ("dag_id", "dag_version", "expected_response"),
        [
            [
                "ANOTHER_DAG_ID",
                1,
                {
                    "bundle_name": "another_bundle_name",
                    "bundle_version": "some_commit_hash",
                    "bundle_url": "http://test_host.github.com/tree/some_commit_hash/dags",
                    "created_at": mock.ANY,
                    "dag_id": "ANOTHER_DAG_ID",
                    "id": mock.ANY,
                    "version_number": 1,
                    "dag_display_name": "ANOTHER_DAG_ID",
                },
            ],
            [
                "dag_with_multiple_versions",
                1,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash1",
                    "bundle_url": "http://test_host.github.com/tree/some_commit_hash1/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 1,
                    "dag_display_name": "dag_with_multiple_versions",
                },
            ],
            [
                "dag_with_multiple_versions",
                2,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash2",
                    "bundle_url": "http://test_host.github.com/tree/some_commit_hash2/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 2,
                    "dag_display_name": "dag_with_multiple_versions",
                },
            ],
            [
                "dag_with_multiple_versions",
                3,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash3",
                    "bundle_url": "http://test_host.github.com/tree/some_commit_hash3/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 3,
                    "dag_display_name": "dag_with_multiple_versions",
                },
            ],
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_version(self, test_client, dag_id, dag_version, expected_response):
        response = test_client.get(f"/dags/{dag_id}/dagVersions/{dag_version}")
        assert response.status_code == 200
        assert response.json() == expected_response

    @pytest.mark.parametrize(
        ("dag_id", "dag_version", "expected_response"),
        [
            [
                "ANOTHER_DAG_ID",
                1,
                {
                    "bundle_name": "another_bundle_name",
                    "bundle_version": "some_commit_hash",
                    "bundle_url": "http://test_host.github.com/tree/some_commit_hash/dags",
                    "created_at": mock.ANY,
                    "dag_id": "ANOTHER_DAG_ID",
                    "id": mock.ANY,
                    "version_number": 1,
                    "dag_display_name": "ANOTHER_DAG_ID",
                },
            ],
            [
                "dag_with_multiple_versions",
                1,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash1",
                    "bundle_url": "http://test_host.github.com/tree/some_commit_hash1/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 1,
                    "dag_display_name": "dag_with_multiple_versions",
                },
            ],
            [
                "dag_with_multiple_versions",
                2,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash2",
                    "bundle_url": "http://test_host.github.com/tree/some_commit_hash2/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 2,
                    "dag_display_name": "dag_with_multiple_versions",
                },
            ],
            [
                "dag_with_multiple_versions",
                3,
                {
                    "bundle_name": "dag_maker",
                    "bundle_version": "some_commit_hash3",
                    "bundle_url": "http://test_host.github.com/tree/some_commit_hash3/dags",
                    "created_at": mock.ANY,
                    "dag_id": "dag_with_multiple_versions",
                    "id": mock.ANY,
                    "version_number": 3,
                    "dag_display_name": "dag_with_multiple_versions",
                },
            ],
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_version_with_url_template(self, test_client, dag_id, dag_version, expected_response):
        response = test_client.get(f"/dags/{dag_id}/dagVersions/{dag_version}")
        assert response.status_code == 200
        assert response.json() == expected_response

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch("airflow.dag_processing.bundles.manager.DagBundlesManager.view_url")
    @mock.patch("airflow.models.dag_version.hasattr")
    def test_get_dag_version_with_unconfigured_bundle(
        self, mock_hasattr, mock_view_url, test_client, dag_maker, session
    ):
        """Test that when a bundle is no longer configured, the bundle_url returns an error message."""
        mock_hasattr.return_value = False
        mock_view_url.side_effect = ValueError("Bundle not configured")

        response = test_client.get("/dags/dag_with_multiple_versions/dagVersions/1")
        assert response.status_code == 200

        response_data = response.json()
        assert not response_data["bundle_url"]

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
        ("dag_id", "expected_response", "expected_query_count"),
        [
            [
                "~",
                {
                    "dag_versions": [
                        {
                            "bundle_name": "another_bundle_name",
                            "bundle_version": "some_commit_hash",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash/dags",
                            "created_at": mock.ANY,
                            "dag_id": "ANOTHER_DAG_ID",
                            "id": mock.ANY,
                            "version_number": 1,
                            "dag_display_name": "ANOTHER_DAG_ID",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash1",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash1/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 1,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash2",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash2/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 2,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash3",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash3/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 3,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                    ],
                    "total_entries": 4,
                },
                2,
            ],
            [
                "dag_with_multiple_versions",
                {
                    "dag_versions": [
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash1",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash1/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 1,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash2",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash2/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 2,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash3",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash3/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 3,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                    ],
                    "total_entries": 3,
                },
                4,
            ],
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch("airflow.api_fastapi.core_api.datamodels.dag_versions.hasattr")
    def test_get_dag_versions(
        self, mock_hasattr, test_client, dag_id, expected_response, expected_query_count
    ):
        mock_hasattr.return_value = False
        with assert_queries_count(expected_query_count):
            response = test_client.get(f"/dags/{dag_id}/dagVersions")
        assert response.status_code == 200
        assert response.json() == expected_response

    @pytest.mark.parametrize(
        ("dag_id", "expected_response", "expected_query_count"),
        [
            [
                "~",
                {
                    "dag_versions": [
                        {
                            "bundle_name": "another_bundle_name",
                            "bundle_version": "some_commit_hash",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash/dags",
                            "created_at": mock.ANY,
                            "dag_id": "ANOTHER_DAG_ID",
                            "id": mock.ANY,
                            "version_number": 1,
                            "dag_display_name": "ANOTHER_DAG_ID",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash1",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash1/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 1,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash2",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash2/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 2,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash3",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash3/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 3,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                    ],
                    "total_entries": 4,
                },
                2,
            ],
            [
                "dag_with_multiple_versions",
                {
                    "dag_versions": [
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash1",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash1/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 1,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash2",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash2/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 2,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                        {
                            "bundle_name": "dag_maker",
                            "bundle_version": "some_commit_hash3",
                            "bundle_url": "http://test_host.github.com/tree/some_commit_hash3/dags",
                            "created_at": mock.ANY,
                            "dag_id": "dag_with_multiple_versions",
                            "id": mock.ANY,
                            "version_number": 3,
                            "dag_display_name": "dag_with_multiple_versions",
                        },
                    ],
                    "total_entries": 3,
                },
                4,
            ],
        ],
    )
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_versions_with_url_template(
        self, test_client, dag_id, expected_response, expected_query_count
    ):
        with assert_queries_count(expected_query_count):
            response = test_client.get(f"/dags/{dag_id}/dagVersions")
        assert response.status_code == 200
        assert response.json() == expected_response

    @pytest.mark.parametrize(
        ("params", "expected_versions", "expected_total_entries"),
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
        with assert_queries_count(2):
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
            "detail": "The Dag with ID: `MISSING_ID` was not found",
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags/~/dagVersions", params={})
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/~/dagVersions", params={})
        assert response.status_code == 403
