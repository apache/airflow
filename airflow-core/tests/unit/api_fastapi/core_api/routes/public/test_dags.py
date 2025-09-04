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

from datetime import datetime, timedelta, timezone
from unittest import mock

import pendulum
import pytest
from sqlalchemy import insert, select

from airflow.models.asset import AssetModel, DagScheduleAssetReference
from airflow.models.dag import DagModel, DagTag
from airflow.models.dag_favorite import DagFavorite
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_connections,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)
from tests_common.test_utils.logs import check_last_log

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
DAG1_DISPLAY_NAME = "display1"
DAG2_ID = "test_dag2"
DAG1_START_DATE = datetime(2018, 6, 15, 0, 0, tzinfo=timezone.utc)
DAG2_START_DATE = datetime(2021, 6, 15, tzinfo=timezone.utc)
DAG3_ID = "test_dag3"
DAG4_ID = "test_dag4"
DAG4_DISPLAY_NAME = "display4"
DAG5_ID = "test_dag5"
DAG5_DISPLAY_NAME = "display5"
ASSET_SCHEDULED_DAG_ID = "test_asset_scheduled_dag"
ASSET_DEP_DAG_ID = "test_asset_dep_dag"
ASSET_DEP_DAG2_ID = "test_asset_dep_dag2"
TASK_ID = "op1"
UTC_JSON_REPR = "UTC" if pendulum.__version__.startswith("3") else "Timezone('UTC')"
API_PREFIX = "/dags"
DAG3_START_DATE_1 = datetime(2018, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
DAG3_START_DATE_2 = datetime(2019, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


class TestDagEndpoint:
    """Common class for /dags related unit tests."""

    @staticmethod
    def _clear_db():
        clear_db_connections()
        clear_db_runs()
        clear_db_dags()
        clear_db_assets()
        clear_db_serialized_dags()

    def _create_deactivated_paused_dag(self, session=None):
        dag_model = DagModel(
            dag_id=DAG3_ID,
            bundle_name="dag_maker",
            relative_fileloc="dag_del_1.py",
            fileloc="/tmp/dag_del_1.py",
            timetable_summary="2 2 * * *",
            is_stale=True,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

        dagrun_failed = DagRun(
            dag_id=DAG3_ID,
            run_id="run1",
            logical_date=datetime(2018, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            start_date=DAG3_START_DATE_1,
            run_type=DagRunType.SCHEDULED,
            state=DagRunState.FAILED,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        dagrun_success = DagRun(
            dag_id=DAG3_ID,
            run_id="run2",
            logical_date=datetime(2019, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            start_date=DAG3_START_DATE_2,
            run_type=DagRunType.MANUAL,
            state=DagRunState.SUCCESS,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        session.add(dag_model)
        session.add(dagrun_failed)
        session.add(dagrun_success)

    def _create_dag_tags(self, session=None):
        session.add(DagTag(dag_id=DAG1_ID, name="tag_2"))
        session.add(DagTag(dag_id=DAG2_ID, name="tag_1"))
        session.add(DagTag(dag_id=DAG3_ID, name="tag_1"))

    def _create_asset_test_data(self, session=None):
        """Create test assets and asset-scheduled DAGs."""
        # Create assets
        asset1 = AssetModel(uri="test://asset1", name="test_asset_1", group="test-group")
        asset2 = AssetModel(uri="s3://bucket/dataset", name="dataset_asset", group="test-group")
        asset3 = AssetModel(uri="test://scheduled_asset", name="scheduled_asset", group="test-group")
        session.add_all([asset1, asset2, asset3])
        session.commit()

        # Create a DAG with asset-based scheduling
        asset_scheduled_dag = DagModel(
            dag_id=ASSET_SCHEDULED_DAG_ID,
            bundle_name="dag_maker",
            relative_fileloc="asset_scheduled_dag.py",
            fileloc="/tmp/asset_scheduled_dag.py",
            timetable_summary="Asset",
            timetable_description="Triggered by assets",
            is_stale=False,
            is_paused=False,
            owners="airflow",
            asset_expression={"any": [{"uri": "test://scheduled_asset"}]},
            max_active_tasks=16,
            max_active_runs=16,
            max_consecutive_failed_dag_runs=0,
            has_task_concurrency_limits=False,
            has_import_errors=False,
        )

        # Create DAGs with asset dependencies
        asset_dep_dag = DagModel(
            dag_id=ASSET_DEP_DAG_ID,
            bundle_name="dag_maker",
            relative_fileloc="asset_dep_dag.py",
            fileloc="/tmp/asset_dep_dag.py",
            timetable_summary="Asset",
            timetable_description="Triggered by assets",
            is_stale=False,
            is_paused=False,
            owners="airflow",
            asset_expression={"any": [{"uri": "test://asset1"}]},
            max_active_tasks=16,
            max_active_runs=16,
            max_consecutive_failed_dag_runs=0,
            has_task_concurrency_limits=False,
            has_import_errors=False,
        )

        asset_dep_dag2 = DagModel(
            dag_id=ASSET_DEP_DAG2_ID,
            bundle_name="dag_maker",
            relative_fileloc="asset_dep_dag2.py",
            fileloc="/tmp/asset_dep_dag2.py",
            timetable_summary="Asset",
            timetable_description="Triggered by assets",
            is_stale=False,
            is_paused=False,
            owners="airflow",
            asset_expression={"any": [{"uri": "s3://bucket/dataset"}]},
            max_active_tasks=16,
            max_active_runs=16,
            max_consecutive_failed_dag_runs=0,
            has_task_concurrency_limits=False,
            has_import_errors=False,
        )

        session.add_all([asset_scheduled_dag, asset_dep_dag, asset_dep_dag2])
        session.commit()

        # Create asset dependencies
        asset_ref1 = DagScheduleAssetReference(dag_id=ASSET_DEP_DAG_ID, asset_id=asset1.id)
        asset_ref2 = DagScheduleAssetReference(dag_id=ASSET_DEP_DAG2_ID, asset_id=asset2.id)
        asset_ref3 = DagScheduleAssetReference(dag_id=ASSET_SCHEDULED_DAG_ID, asset_id=asset3.id)
        session.add_all([asset_ref1, asset_ref2, asset_ref3])
        session.commit()

    @pytest.fixture(autouse=True)
    def setup(self, dag_maker, session) -> None:
        self._clear_db()

        with dag_maker(
            DAG1_ID,
            dag_display_name=DAG1_DISPLAY_NAME,
            schedule=None,
            start_date=DAG1_START_DATE,
            doc_md="details",
            default_args={
                "depends_on_past": False,
                "retries": 1,
                "retry_delay": timedelta(minutes=5),
            },
            params={"foo": 1},
            tags=["example"],
        ):
            EmptyOperator(task_id=TASK_ID)

        dag_maker.create_dagrun(state=DagRunState.FAILED)

        with dag_maker(
            DAG2_ID,
            schedule=None,
            start_date=DAG2_START_DATE,
            doc_md="details",
            default_args={
                "depends_on_past": False,
                "retries": 1,
                "retry_delay": timedelta(minutes=5),
            },
            params={"foo": 1},
            max_active_tasks=16,
            max_active_runs=16,
        ):
            EmptyOperator(task_id=TASK_ID)

        self._create_deactivated_paused_dag(session)
        self._create_dag_tags(session)
        dag_maker.sync_dagbag_to_db()
        dag_maker.dag_model.has_task_concurrency_limits = True
        session.merge(dag_maker.dag_model)
        session.commit()

    def teardown_method(self) -> None:
        self._clear_db()


class TestGetDags(TestDagEndpoint):
    """Unit tests for Get DAGs."""

    @pytest.mark.parametrize(
        "query_params, expected_total_entries, expected_ids",
        [
            # Filters
            ({}, 2, [DAG1_ID, DAG2_ID]),
            ({"limit": 1}, 2, [DAG1_ID]),
            ({"offset": 1}, 2, [DAG2_ID]),
            ({"tags": ["example"]}, 1, [DAG1_ID]),
            (
                {"exclude_stale": False},
                3,
                [DAG1_ID, DAG2_ID, DAG3_ID],
            ),
            ({"paused": True, "exclude_stale": False}, 1, [DAG3_ID]),
            (
                {"paused": False},
                2,
                [DAG1_ID, DAG2_ID],
            ),
            (
                {"owners": ["airflow"]},
                2,
                [DAG1_ID, DAG2_ID],
            ),
            ({"owners": ["test_owner"], "exclude_stale": False}, 1, [DAG3_ID]),
            ({"last_dag_run_state": "success", "exclude_stale": False}, 1, [DAG3_ID]),
            ({"last_dag_run_state": "failed", "exclude_stale": False}, 1, [DAG1_ID]),
            ({"dag_run_state": "failed"}, 1, [DAG1_ID]),
            ({"dag_run_state": "failed", "exclude_stale": False}, 1, [DAG1_ID]),
            (
                {"dag_run_start_date_gte": DAG3_START_DATE_2.isoformat(), "exclude_stale": False},
                1,
                [DAG3_ID],
            ),
            (
                {"dag_run_start_date_gt": DAG3_START_DATE_1.isoformat(), "exclude_stale": False},
                2,
                [DAG1_ID, DAG3_ID],
            ),
            (
                {
                    "dag_run_start_date_gte": DAG1_START_DATE.isoformat(),
                    "dag_run_start_date_lte": DAG2_START_DATE.isoformat(),
                },
                1,
                [DAG1_ID],
            ),
            (
                {
                    "dag_run_start_date_gt": DAG1_START_DATE.isoformat(),
                    "dag_run_start_date_lt": DAG2_START_DATE.isoformat(),
                },
                0,
                [],
            ),
            (
                {
                    "dag_run_start_date_gte": (DAG1_START_DATE - timedelta(days=1)).isoformat(),
                    "dag_run_start_date_gt": (DAG1_START_DATE - timedelta(days=1)).isoformat(),
                },
                1,
                [DAG1_ID],
            ),
            (
                {
                    "dag_run_start_date_gt": DAG1_START_DATE.isoformat(),
                    "dag_run_start_date_lte": DAG2_START_DATE.isoformat(),
                },
                0,
                [],
            ),
            (
                {
                    "dag_run_start_date_lt": DAG2_START_DATE.isoformat(),
                },
                1,
                [DAG1_ID],
            ),
            (
                {
                    "dag_run_start_date_lt": DAG2_START_DATE.isoformat(),
                    "dag_run_start_date_lte": DAG3_START_DATE_2.isoformat(),
                },
                1,
                [DAG1_ID],
            ),
            (
                {
                    "dag_run_end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                    "exclude_stale": False,
                },
                2,
                [DAG1_ID, DAG3_ID],
            ),
            (
                {
                    "dag_run_end_date_gte": DAG3_START_DATE_2.isoformat(),
                    "dag_run_end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                    "exclude_stale": False,
                    "last_dag_run_state": "success",
                },
                1,
                [DAG3_ID],
            ),
            (
                {
                    "dag_run_start_date_gte": DAG2_START_DATE.isoformat(),
                    "dag_run_end_date_lte": (datetime.now(tz=timezone.utc) + timedelta(days=1)).isoformat(),
                },
                0,
                [],
            ),
            (
                {
                    "dag_run_start_date_gte": (DAG3_START_DATE_1 - timedelta(days=1)).isoformat(),
                    "dag_run_start_date_lte": (DAG3_START_DATE_1 + timedelta(days=1)).isoformat(),
                    "last_dag_run_state": "success",
                    "dag_run_state": "failed",
                    "exclude_stale": False,
                },
                0,
                [],
            ),
            # Sort
            (
                {"order_by": "-dag_id"},
                2,
                [DAG2_ID, DAG1_ID],
            ),
            (
                {"order_by": "-dag_display_name"},
                2,
                [DAG2_ID, DAG1_ID],
            ),
            (
                {"order_by": "dag_display_name"},
                2,
                [DAG1_ID, DAG2_ID],
            ),
            (
                {"order_by": "next_dagrun", "exclude_stale": False},
                3,
                [DAG3_ID, DAG1_ID, DAG2_ID],
            ),
            (
                {"order_by": "last_run_state", "exclude_stale": False},
                3,
                [DAG1_ID, DAG3_ID, DAG2_ID],
            ),
            (
                {"order_by": "-last_run_state", "exclude_stale": False},
                3,
                [DAG3_ID, DAG1_ID, DAG2_ID],
            ),
            (
                {"order_by": "last_run_start_date", "exclude_stale": False},
                3,
                [DAG1_ID, DAG3_ID, DAG2_ID],
            ),
            (
                {"order_by": "-last_run_start_date", "exclude_stale": False},
                3,
                [DAG3_ID, DAG1_ID, DAG2_ID],
            ),
            (
                {"order_by": ["next_dagrun", "-dag_display_name"], "exclude_stale": False},
                3,
                [DAG3_ID, DAG2_ID, DAG1_ID],
            ),
            # Search
            ({"dag_id_pattern": "1"}, 1, [DAG1_ID]),
            ({"dag_display_name_pattern": "test_dag2"}, 1, [DAG2_ID]),
            # Bundle filters
            (
                {"bundle_name": "dag_maker"},
                2,
                [DAG1_ID, DAG2_ID],
            ),
            ({"bundle_name": "wrong_bundle"}, 0, []),
            ({"bundle_version": "1.0.0"}, 0, []),
            # Asset filters
            ({"has_asset_schedule": True}, 3, [ASSET_DEP_DAG_ID, ASSET_DEP_DAG2_ID, ASSET_SCHEDULED_DAG_ID]),
            ({"has_asset_schedule": False}, 2, [DAG1_ID, DAG2_ID]),
            ({"asset_dependency": "test_asset"}, 1, [ASSET_DEP_DAG_ID]),
            ({"asset_dependency": "dataset"}, 1, [ASSET_DEP_DAG2_ID]),
            ({"asset_dependency": "bucket"}, 1, [ASSET_DEP_DAG2_ID]),
            ({"asset_dependency": "s3://"}, 1, [ASSET_DEP_DAG2_ID]),
            ({"asset_dependency": "nonexistent"}, 0, []),
            ({"has_asset_schedule": True, "asset_dependency": "test_asset"}, 1, [ASSET_DEP_DAG_ID]),
            ({"has_asset_schedule": False, "asset_dependency": "test_asset"}, 0, []),
        ],
    )
    def test_get_dags(self, test_client, query_params, expected_total_entries, expected_ids, session):
        # Only create asset test data for asset-related tests to avoid affecting other tests
        if any(param in query_params for param in ["has_asset_schedule", "asset_dependency"]):
            self._create_asset_test_data(session)

        response = test_client.get("/dags", params=query_params)
        assert response.status_code == 200
        body = response.json()

        assert body["total_entries"] == expected_total_entries
        actual_ids = [dag["dag_id"] for dag in body["dags"]]

        assert actual_ids == expected_ids

    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_get_dags_should_call_authorized_dag_ids(self, mock_get_authorized_dag_ids, test_client):
        mock_get_authorized_dag_ids.return_value = {DAG1_ID, DAG2_ID}
        response = test_client.get("/dags")
        mock_get_authorized_dag_ids.assert_called_once_with(user=mock.ANY, method="GET")
        assert response.status_code == 200
        body = response.json()

        assert body["total_entries"] == 2
        assert [dag["dag_id"] for dag in body["dags"]] == [DAG1_ID, DAG2_ID]

    @pytest.mark.parametrize(
        "setup_favorites, expected_total_entries, expected_ids",
        [
            ([], 0, []),
            ([DAG1_ID], 1, [DAG1_ID]),
            ([DAG1_ID, DAG2_ID], 2, [DAG1_ID, DAG2_ID]),
        ],
    )
    def test_get_dags_filter_favorites(
        self, session, test_client, setup_favorites, expected_total_entries, expected_ids
    ):
        """Test filtering DAGs by is_favorite=true."""
        for dag_id in setup_favorites:
            session.add(DagFavorite(user_id="test", dag_id=dag_id))
        session.commit()

        response = test_client.get("/dags", params={"is_favorite": True})

        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == expected_total_entries
        assert sorted([dag["dag_id"] for dag in body["dags"]]) == sorted(expected_ids)

    def test_get_dags_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags")
        assert response.status_code == 401

    def test_get_dags_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags")
        assert response.status_code == 403


class TestPatchDag(TestDagEndpoint):
    """Unit tests for Patch DAG."""

    @pytest.mark.parametrize(
        "query_params, dag_id, body, expected_status_code, expected_is_paused, expected_tags, expected_display_name",
        [
            ({}, "fake_dag_id", {"is_paused": True}, 404, None, [], "fake_dag_display_name"),
            (
                {"update_mask": ["field_1", "is_paused"]},
                DAG1_ID,
                {"is_paused": True},
                400,
                None,
                [],
                DAG1_DISPLAY_NAME,
            ),
            ({}, DAG1_ID, {"is_paused": True}, 200, True, ["example", "tag_2"], DAG1_DISPLAY_NAME),
            ({}, DAG1_ID, {"is_paused": False}, 200, False, ["example", "tag_2"], DAG1_DISPLAY_NAME),
            (
                {"update_mask": ["is_paused"]},
                DAG1_ID,
                {"is_paused": True},
                200,
                True,
                ["example", "tag_2"],
                DAG1_DISPLAY_NAME,
            ),
            (
                {"update_mask": ["is_paused"]},
                DAG1_ID,
                {"is_paused": False},
                200,
                False,
                ["example", "tag_2"],
                DAG1_DISPLAY_NAME,
            ),
        ],
    )
    def test_patch_dag(
        self,
        test_client,
        query_params,
        dag_id,
        body,
        expected_status_code,
        expected_is_paused,
        expected_tags,
        expected_display_name,
        session,
    ):
        response = test_client.patch(f"/dags/{dag_id}", json=body, params=query_params)

        assert response.status_code == expected_status_code
        if expected_status_code == 200:
            body = response.json()
            assert body["is_paused"] == expected_is_paused
            check_last_log(session, dag_id=dag_id, event="patch_dag", logical_date=None)

            tags = body.get("tags", [])
            assert len(tags) == len(expected_tags)

            for tag in tags:
                assert tag["name"] in expected_tags
                assert tag["dag_id"] == dag_id
                assert tag["dag_display_name"] == expected_display_name

    def test_patch_dag_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch(f"/dags/{DAG1_ID}", json={"is_paused": True})
        assert response.status_code == 401

    def test_patch_dag_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch(f"/dags/{DAG1_ID}", json={"is_paused": True})
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "is_paused_value",
        [True, False],
    )
    def test_patch_dag_audit_log_payload(self, test_client, is_paused_value, session):
        """Test that audit log payload correctly reflects the is_paused value."""
        response = test_client.patch(f"/dags/{DAG1_ID}", json={"is_paused": is_paused_value})
        assert response.status_code == 200

        # Check that the audit log has the correct is_paused value
        expected_extra = {"is_paused": is_paused_value, "method": "PATCH"}
        check_last_log(
            session, dag_id=DAG1_ID, event="patch_dag", logical_date=None, expected_extra=expected_extra
        )


class TestPatchDags(TestDagEndpoint):
    """Unit tests for Patch DAGs."""

    @pytest.mark.parametrize(
        "query_params, body, expected_status_code, expected_ids, expected_paused_ids",
        [
            ({"update_mask": ["field_1", "is_paused"]}, {"is_paused": True}, 400, None, None),
            (
                {"exclude_stale": False},
                {"is_paused": True},
                200,
                [],
                [],
            ),  # no-op because the dag_id_pattern is not provided
            (
                {"exclude_stale": False, "dag_id_pattern": "~"},
                {"is_paused": True},
                200,
                [DAG1_ID, DAG2_ID, DAG3_ID],
                [DAG1_ID, DAG2_ID, DAG3_ID],
            ),
            (
                {"exclude_stale": False, "dag_id_pattern": "~"},
                {"is_paused": False},
                200,
                [DAG1_ID, DAG2_ID, DAG3_ID],
                [],
            ),
            (
                {"dag_id_pattern": "~"},
                {"is_paused": True},
                200,
                [DAG1_ID, DAG2_ID],
                [DAG1_ID, DAG2_ID],
            ),
            (
                {"dag_id_pattern": "dag1"},
                {"is_paused": True},
                200,
                [DAG1_ID],
                [DAG1_ID],
            ),
        ],
    )
    def test_patch_dags(
        self,
        test_client,
        query_params,
        body,
        expected_status_code,
        expected_ids,
        expected_paused_ids,
        session,
    ):
        response = test_client.patch("/dags", json=body, params=query_params)

        assert response.status_code == expected_status_code
        if expected_status_code == 200:
            body = response.json()
            assert {dag["dag_id"] for dag in body["dags"]} == set(expected_ids)
            paused_dag_ids = {dag["dag_id"] for dag in body["dags"] if dag["is_paused"]}
            assert paused_dag_ids == set(expected_paused_ids)
            check_last_log(session, dag_id=DAG1_ID, event="patch_dag", logical_date=None)

    @mock.patch("airflow.api_fastapi.auth.managers.base_auth_manager.BaseAuthManager.get_authorized_dag_ids")
    def test_patch_dags_should_call_authorized_dag_ids(self, mock_get_authorized_dag_ids, test_client):
        mock_get_authorized_dag_ids.return_value = {DAG1_ID, DAG2_ID}
        response = test_client.patch(
            "/dags", json={"is_paused": False}, params={"exclude_stale": False, "dag_id_pattern": "~"}
        )
        mock_get_authorized_dag_ids.assert_called_once_with(user=mock.ANY, method="PUT")
        assert response.status_code == 200
        body = response.json()

        assert {dag["dag_id"] for dag in body["dags"]} == {DAG1_ID, DAG2_ID}

    def test_patch_dags_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.patch("/dags", json={"is_paused": True})
        assert response.status_code == 401

    def test_patch_dags_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.patch("/dags", json={"is_paused": True})
        assert response.status_code == 403


class TestFavoriteDag(TestDagEndpoint):
    """Unit tests for favoriting a DAG."""

    @pytest.mark.parametrize(
        "dag_id, expected_status_code, expected_exist_in_favorites",
        [
            ("fake_dag_id", 404, None),
            (DAG1_ID, 204, True),
        ],
    )
    def test_favorite_dag(
        self, test_client, dag_id, expected_status_code, expected_exist_in_favorites, session
    ):
        response = test_client.post(f"/dags/{dag_id}/favorite")
        assert response.status_code == expected_status_code

        if expected_status_code == 204:
            result = session.execute(
                select(DagFavorite).where(DagFavorite.dag_id == dag_id, DagFavorite.user_id == "test")
            ).first()
            assert result is not None if expected_exist_in_favorites else result is None
            check_last_log(session, dag_id=dag_id, event="favorite_dag", logical_date=None)

    def test_favorite_dag_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(f"/dags/{DAG1_ID}/favorite")
        assert response.status_code == 401

    def test_favorite_dag_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(f"/dags/{DAG1_ID}/favorite")
        assert response.status_code == 403

    def test_favoriting_already_favorited_dag_returns_409(self, test_client):
        response = test_client.post(f"/dags/{DAG1_ID}/favorite")
        assert response.status_code == 204

        response = test_client.post(f"/dags/{DAG1_ID}/favorite")
        assert response.status_code == 409


class TestUnfavoriteDag(TestDagEndpoint):
    """Unit tests for unfavoriting a DAG."""

    @pytest.mark.parametrize(
        "dag_id, expected_status_code, expected_exist_in_favorites",
        [
            ("fake_dag_id", 404, None),
            (DAG1_ID, 204, False),
        ],
    )
    def test_unfavorite_dag(
        self, test_client, dag_id, expected_status_code, expected_exist_in_favorites, session
    ):
        if dag_id != "fake_dag_id":
            session.execute(insert(DagFavorite).values(dag_id=dag_id, user_id="test"))
            session.commit()

        response = test_client.post(f"/dags/{dag_id}/unfavorite")
        assert response.status_code == expected_status_code

        if expected_status_code == 204:
            result = session.execute(
                select(DagFavorite).where(DagFavorite.dag_id == dag_id, DagFavorite.user_id == "test")
            ).first()
            assert result is not None if expected_exist_in_favorites else result is None
            check_last_log(session, dag_id=dag_id, event="unfavorite_dag", logical_date=None)

    def test_unfavorite_dag_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.post(f"/dags/{DAG1_ID}/unfavorite")
        assert response.status_code == 401

    def test_unfavorite_dag_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.post(f"/dags/{DAG1_ID}/unfavorite")
        assert response.status_code == 403

    def test_unfavoriting_dag_that_is_not_favorite_returns_409(self, test_client):
        response = test_client.post(f"/dags/{DAG1_ID}/unfavorite")
        assert response.status_code == 409


class TestDagDetails(TestDagEndpoint):
    """Unit tests for DAG Details."""

    @pytest.mark.parametrize(
        "query_params, dag_id, expected_status_code, dag_display_name, start_date, owner_links",
        [
            ({}, "fake_dag_id", 404, "fake_dag", "2023-12-31T00:00:00Z", {}),
            ({}, DAG2_ID, 200, DAG2_ID, "2021-06-15T00:00:00Z", {}),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    @mock.patch("airflow.api_fastapi.core_api.datamodels.dag_versions.hasattr")
    def test_dag_details(
        self,
        mock_hasattr,
        test_client,
        query_params,
        dag_id,
        expected_status_code,
        dag_display_name,
        start_date,
        owner_links,
    ):
        mock_hasattr.return_value = False
        response = test_client.get(f"/dags/{dag_id}/details", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        # Match expected and actual responses below.
        res_json = response.json()
        last_parsed = res_json["last_parsed"]
        last_parsed_time = res_json["last_parsed_time"]
        file_token = res_json["file_token"]
        expected = {
            "bundle_name": "dag_maker",
            "bundle_version": None,
            "asset_expression": None,
            "catchup": False,
            "concurrency": 16,
            "dag_id": dag_id,
            "dag_display_name": dag_display_name,
            "dag_run_timeout": None,
            "default_args": {
                "depends_on_past": False,
                "retries": 1,
                "retry_delay": "PT5M",
            },
            "description": None,
            "doc_md": "details",
            "end_date": None,
            "fileloc": __file__,
            "file_token": file_token,
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_stale": False,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "latest_dag_version": {
                "bundle_name": "dag_maker",
                "bundle_url": None,
                "bundle_version": None,
                "created_at": mock.ANY,
                "dag_id": "test_dag2",
                "id": mock.ANY,
                "version_number": 1,
                "dag_display_name": dag_display_name,
            },
            "last_expired": None,
            "last_parsed": last_parsed,
            "last_parsed_time": last_parsed_time,
            "max_active_runs": 16,
            "max_active_tasks": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "next_dagrun_logical_date": None,
            "next_dagrun_run_after": None,
            "owners": ["airflow"],
            "owner_links": {},
            "params": {
                "foo": {
                    "__class": "airflow.sdk.definitions.param.Param",
                    "description": None,
                    "schema": {},
                    "value": 1,
                }
            },
            "relative_fileloc": "test_dags.py",
            "render_template_as_native_obj": False,
            "timetable_summary": None,
            "start_date": start_date,
            "tags": [],
            "template_search_path": None,
            "timetable_description": "Never, external triggers only",
            "timezone": UTC_JSON_REPR,
        }
        assert res_json == expected

    @pytest.mark.parametrize(
        "query_params, dag_id, expected_status_code, dag_display_name, start_date, owner_links",
        [
            ({}, "fake_dag_id", 404, "fake_dag", "2023-12-31T00:00:00Z", {}),
            ({}, DAG2_ID, 200, DAG2_ID, "2021-06-15T00:00:00Z", {}),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_dag_details_with_view_url_template(
        self,
        test_client,
        query_params,
        dag_id,
        expected_status_code,
        dag_display_name,
        start_date,
        owner_links,
    ):
        response = test_client.get(f"/dags/{dag_id}/details", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        # Match expected and actual responses below.
        res_json = response.json()
        last_parsed = res_json["last_parsed"]
        last_parsed_time = res_json["last_parsed_time"]
        file_token = res_json["file_token"]
        expected = {
            "bundle_name": "dag_maker",
            "bundle_version": None,
            "asset_expression": None,
            "catchup": False,
            "concurrency": 16,
            "dag_id": dag_id,
            "dag_display_name": dag_display_name,
            "dag_run_timeout": None,
            "default_args": {
                "depends_on_past": False,
                "retries": 1,
                "retry_delay": "PT5M",
            },
            "description": None,
            "doc_md": "details",
            "end_date": None,
            "fileloc": __file__,
            "file_token": file_token,
            "has_import_errors": False,
            "has_task_concurrency_limits": True,
            "is_stale": False,
            "is_paused": False,
            "is_paused_upon_creation": None,
            "latest_dag_version": {
                "bundle_name": "dag_maker",
                "bundle_url": "http://test_host.github.com/tree/None/dags",
                "bundle_version": None,
                "created_at": mock.ANY,
                "dag_id": "test_dag2",
                "id": mock.ANY,
                "version_number": 1,
                "dag_display_name": dag_display_name,
            },
            "last_expired": None,
            "last_parsed": last_parsed,
            "last_parsed_time": last_parsed_time,
            "max_active_runs": 16,
            "max_active_tasks": 16,
            "max_consecutive_failed_dag_runs": 0,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_data_interval_start": None,
            "next_dagrun_logical_date": None,
            "next_dagrun_run_after": None,
            "owners": ["airflow"],
            "owner_links": {},
            "params": {
                "foo": {
                    "__class": "airflow.sdk.definitions.param.Param",
                    "description": None,
                    "schema": {},
                    "value": 1,
                }
            },
            "relative_fileloc": "test_dags.py",
            "render_template_as_native_obj": False,
            "timetable_summary": None,
            "start_date": start_date,
            "tags": [],
            "template_search_path": None,
            "timetable_description": "Never, external triggers only",
            "timezone": UTC_JSON_REPR,
        }
        assert res_json == expected

    def test_dag_details_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/dags/{DAG1_ID}/details")
        assert response.status_code == 401

    def test_dag_details_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/dags/{DAG1_ID}/details")
        assert response.status_code == 403


class TestGetDag(TestDagEndpoint):
    """Unit tests for Get DAG."""

    @pytest.mark.parametrize(
        "query_params, dag_id, expected_status_code, dag_display_name, expected_tags",
        [
            ({}, "fake_dag_id", 404, "fake_dag", []),
            ({}, DAG2_ID, 200, DAG2_ID, []),
        ],
    )
    def test_get_dag(
        self, test_client, query_params, dag_id, expected_status_code, dag_display_name, expected_tags
    ):
        response = test_client.get(f"/dags/{dag_id}", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        # Match expected and actual responses below.
        res_json = response.json()
        last_parsed_time = res_json["last_parsed_time"]
        file_token = res_json["file_token"]
        tags = res_json.get("tags", [])

        assert len(tags) == len(expected_tags)

        for tag in tags:
            assert tag["name"] in expected_tags
            assert tag["dag_id"] == dag_id
            assert tag["dag_display_name"] == dag_display_name

        expected = {
            "dag_id": dag_id,
            "dag_display_name": dag_display_name,
            "description": None,
            "fileloc": __file__,
            "file_token": file_token,
            "is_paused": False,
            "is_stale": False,
            "owners": ["airflow"],
            "timetable_summary": None,
            "tags": tags,
            "has_task_concurrency_limits": True,
            "next_dagrun_data_interval_start": None,
            "next_dagrun_data_interval_end": None,
            "next_dagrun_logical_date": None,
            "next_dagrun_run_after": None,
            "max_active_runs": 16,
            "max_consecutive_failed_dag_runs": 0,
            "last_expired": None,
            "max_active_tasks": 16,
            "last_parsed_time": last_parsed_time,
            "timetable_description": "Never, external triggers only",
            "has_import_errors": False,
            "bundle_name": "dag_maker",
            "bundle_version": None,
            "relative_fileloc": "test_dags.py",
        }
        assert res_json == expected

    def test_get_dag_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/dags/{DAG1_ID}")
        assert response.status_code == 401

    def test_get_dag_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/dags/{DAG1_ID}")
        assert response.status_code == 403


class TestDeleteDAG(TestDagEndpoint):
    """Unit tests for Delete DAG."""

    def _create_dag_for_deletion(
        self,
        dag_maker,
        dag_id=None,
        dag_display_name=None,
        has_running_dagruns=False,
    ):
        with dag_maker(
            dag_id,
            dag_display_name=dag_display_name,
            start_date=datetime(2024, 10, 10, tzinfo=timezone.utc),
        ):
            EmptyOperator(task_id="dummy")

        if has_running_dagruns:
            dr = dag_maker.create_dagrun()
            ti = dr.get_task_instances()[0]
            ti.set_state(TaskInstanceState.RUNNING)

        dag_maker.sync_dagbag_to_db()

    @pytest.mark.parametrize(
        "dag_id, dag_display_name, status_code_delete, status_code_details, has_running_dagruns, is_create_dag",
        [
            ("test_nonexistent_dag_id", "nonexistent_display_name", 404, 404, False, False),
            (DAG4_ID, DAG4_DISPLAY_NAME, 204, 404, False, True),
            (DAG5_ID, DAG5_DISPLAY_NAME, 409, 200, True, True),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_delete_dag(
        self,
        dag_maker,
        test_client,
        dag_id,
        dag_display_name,
        status_code_delete,
        status_code_details,
        has_running_dagruns,
        is_create_dag,
        session,
    ):
        if is_create_dag:
            self._create_dag_for_deletion(
                dag_maker=dag_maker,
                dag_id=dag_id,
                dag_display_name=dag_display_name,
                has_running_dagruns=has_running_dagruns,
            )

        delete_response = test_client.delete(f"{API_PREFIX}/{dag_id}")
        assert delete_response.status_code == status_code_delete

        details_response = test_client.get(f"{API_PREFIX}/{dag_id}/details")
        assert details_response.status_code == status_code_details

        if details_response.status_code == 204:
            check_last_log(session, dag_id=dag_id, event="delete_dag", logical_date=None)

    def test_delete_dag_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.delete(f"{API_PREFIX}/{DAG1_ID}")
        assert response.status_code == 401

    def test_delete_dag_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.delete(f"{API_PREFIX}/{DAG1_ID}")
        assert response.status_code == 403
