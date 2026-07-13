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
from sqlalchemy import event, select, update

from airflow import settings
from airflow.models.dag_version import DagVersion
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.operators.empty import EmptyOperator

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test


class TestDagVersionEndpoint:
    @pytest.fixture(autouse=True)
    def setup(request, dag_maker, session):
        clear_db_dags()
        clear_db_serialized_dags()

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


class TestGetDagVersionDiff(TestDagVersionEndpoint):
    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_version_diff(self, test_client):
        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"include_values": True},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["diff_schema_version"] == 1
        assert response_data["serialized_dag_schema_versions"] == {"base": 3, "target": 3}
        assert response_data["mode"] == "observed_state"
        assert response_data["truncated"] is False
        assert response_data["values"] == {"status": "available"}
        assert response_data["source"] == {"status": "unavailable", "fidelity": "unavailable"}
        assert any(
            change["path"] == "/dag/tasks/task2"
            and change["operation"] == "added"
            and change["category"] == "task"
            for change in response_data["changes"]
        )
        assert any("after_value" in change for change in response_data["changes"])

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.dag_versions.get_auth_manager", autospec=True)
    def test_get_dag_version_diff_redacts_values_without_code_access(
        self, mock_get_auth_manager, test_client
    ):
        mock_get_auth_manager.return_value.is_authorized_dag.return_value = False

        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"include_values": True},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["values"] == {"status": "unavailable"}
        assert all(
            "before_value" not in change and "after_value" not in change
            for change in response_data["changes"]
        )

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch(
        "airflow.api_fastapi.core_api.routes.public.dag_versions.build_dag_version_diff", autospec=True
    )
    def test_get_dag_version_diff_preserves_explicit_null_values(
        self, mock_build_dag_version_diff, test_client
    ):
        mock_build_dag_version_diff.return_value = {
            "diff_schema_version": 1,
            "serialized_dag_schema_versions": {"base": 3, "target": 3},
            "mode": "observed_state",
            "changes": [
                {
                    "path": "/dag/value",
                    "operation": "added",
                    "category": "unknown",
                    "impact": "unknown",
                    "before_digest": None,
                    "after_digest": "sha256:null",
                    "after_value": None,
                }
            ],
            "source": {"status": "unavailable", "fidelity": "unavailable"},
            "values": {"status": "available"},
            "truncated": False,
        }

        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"include_values": True},
        )

        assert response.status_code == 200
        change = response.json()["changes"][0]
        assert "before_value" not in change
        assert "after_value" in change
        assert change["after_value"] is None

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_version_diff_truncates(self, test_client):
        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"max_changes": 1},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["truncated"] is True
        assert "values" not in response_data

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_version_diff_marks_values_unavailable_when_diff_is_unavailable(
        self, test_client, session
    ):
        serialized_dag = session.scalar(
            select(SerializedDagModel)
            .join(DagVersion)
            .where(
                DagVersion.dag_id == "dag_with_multiple_versions",
                DagVersion.version_number == 1,
            )
        )
        assert serialized_dag is not None
        serialized_data = serialized_dag.data
        assert serialized_data is not None
        session.execute(
            update(SerializedDagModel)
            .where(SerializedDagModel.id == serialized_dag.id)
            .values(
                {
                    SerializedDagModel._data: {**serialized_data, "__version": 99},
                    SerializedDagModel._data_compressed: None,
                }
            )
        )
        session.commit()
        session.expunge_all()

        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"include_values": True},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["mode"] == "unavailable"
        assert response_data["values"] == {"status": "unavailable"}

    def test_get_dag_version_diff_rejects_excessive_change_limit(self, test_client):
        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"max_changes": 5001},
        )

        assert response.status_code == 422

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_version_diff_includes_current_source(self, test_client):
        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"include_source": True},
        )

        assert response.status_code == 200
        source = response.json()["source"]
        assert source["status"] == "current_stored_code"
        assert source["fidelity"] == "current_stored_code"
        assert source["changed"] is False
        assert source["base"]["content"]
        assert source["base"]["content"] == source["target"]["content"]
        assert source["base"]["digest"].startswith("sha256:")

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.dag_versions.get_auth_manager", autospec=True)
    def test_get_dag_version_diff_redacts_unreadable_colocated_source(
        self, mock_get_auth_manager, test_client
    ):
        mock_get_auth_manager.return_value.is_authorized_dag.return_value = True
        mock_get_auth_manager.return_value.get_authorized_dag_ids.return_value = set()

        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"include_source": True},
        )

        assert response.status_code == 200
        assert response.json()["source"] == {"status": "redacted", "fidelity": "redacted"}

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch("airflow.api_fastapi.core_api.routes.public.dag_versions.get_auth_manager", autospec=True)
    def test_get_dag_version_diff_redacts_source_without_code_access(
        self, mock_get_auth_manager, test_client
    ):
        mock_get_auth_manager.return_value.is_authorized_dag.return_value = False
        executed_statements = []

        def capture_statement(_conn, _cursor, statement, _parameters, _context, _executemany):
            executed_statements.append(statement.upper())

        event.listen(settings.engine, "before_cursor_execute", capture_statement)
        try:
            response = test_client.get(
                "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
                params={"include_source": True},
            )
        finally:
            event.remove(settings.engine, "before_cursor_execute", capture_statement)

        assert response.status_code == 200
        assert response.json()["source"] == {"status": "redacted", "fidelity": "redacted"}
        assert all("DAG_CODE" not in statement for statement in executed_statements)

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch(
        "airflow.api_fastapi.core_api.routes.public.dag_versions._get_bundle_team_names",
        autospec=True,
    )
    @mock.patch("airflow.api_fastapi.core_api.routes.public.dag_versions.get_auth_manager", autospec=True)
    def test_get_dag_version_diff_authorizes_raw_data_against_historical_bundle(
        self,
        mock_get_auth_manager,
        mock_get_bundle_team_names,
        test_client,
        session,
    ):
        base_version = session.scalar(
            select(DagVersion).where(
                DagVersion.dag_id == "dag_with_multiple_versions",
                DagVersion.version_number == 1,
            )
        )
        base_version.bundle_name = "another_bundle_name"
        session.commit()
        mock_get_bundle_team_names.return_value = {
            "another_bundle_name": "old-team",
            "dag_maker": "new-team",
        }
        auth_manager = mock_get_auth_manager.return_value
        auth_manager.is_authorized_dag.side_effect = lambda **kwargs: (
            kwargs["details"].team_name == "new-team"
        )

        response = test_client.get(
            "/dags/dag_with_multiple_versions/dagVersions/1/diff/3",
            params={"include_values": True, "include_source": True},
        )

        assert response.status_code == 200
        response_data = response.json()
        assert response_data["values"] == {"status": "unavailable"}
        assert response_data["source"] == {"status": "redacted", "fidelity": "redacted"}
        assert all(
            "before_value" not in change and "after_value" not in change
            for change in response_data["changes"]
        )
        assert [
            call.kwargs["details"].team_name for call in auth_manager.is_authorized_dag.call_args_list
        ] == ["old-team"]

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    def test_get_dag_version_diff_404(self, test_client):
        response = test_client.get("/dags/dag_with_multiple_versions/dagVersions/1/diff/99")

        assert response.status_code == 404
        assert response.json() == {
            "detail": "The DagVersion with dag_id: `dag_with_multiple_versions` and version_number: `99` was not found",
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags/dag_with_multiple_versions/dagVersions/1/diff/2")

        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags/dag_with_multiple_versions/dagVersions/1/diff/2")

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
                3,
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
                5,
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

    @pytest.mark.usefixtures("make_dag_with_multiple_versions")
    @mock.patch(
        "airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids",
        return_value={"dag_with_multiple_versions"},
    )
    def test_get_dag_versions_permission_filtering(self, _, test_client):
        """Test that listing all DAG versions with ~ only returns versions for permitted DAGs."""
        with assert_queries_count(4):
            response = test_client.get("/dags/~/dagVersions")

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == 3
        dag_ids = {v["dag_id"] for v in body["dag_versions"]}
        assert dag_ids == {"dag_with_multiple_versions"}

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
                3,
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
                5,
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
        with assert_queries_count(3):
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
