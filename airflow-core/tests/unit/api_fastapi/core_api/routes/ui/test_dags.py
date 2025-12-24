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

from typing import TYPE_CHECKING
from unittest import mock

import pendulum
import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from airflow.models import DagRun
from airflow.models.dag import DagModel, DagTag
from airflow.models.dag_favorite import DagFavorite
from airflow.models.hitl import HITLDetail
from airflow.sdk.timezone import utcnow
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.asserts import count_queries
from unit.api_fastapi.core_api.routes.public.test_dags import (
    DAG1_ID,
    DAG2_ID,
    DAG3_ID,
    DAG4_ID,
    DAG5_ID,
    TestDagEndpoint as TestPublicDagEndpoint,
)

if TYPE_CHECKING:
    from tests_common.pytest_plugin import TaskInstance

pytestmark = pytest.mark.db_test


class TestGetDagRuns(TestPublicDagEndpoint):
    @pytest.fixture(autouse=True)
    @provide_session
    def setup_dag_runs(self, session=None) -> None:
        # Create DAG Runs
        for dag_id in [DAG1_ID, DAG2_ID, DAG3_ID, DAG4_ID, DAG5_ID]:
            dag_runs_count = 5 if dag_id in [DAG1_ID, DAG2_ID] else 2
            for i in range(dag_runs_count):
                start_date = pendulum.datetime(2021 + i, 1, 1, 0, 0, 0, tz="UTC")
                dag_run = DagRun(
                    dag_id=dag_id,
                    run_id=f"run_id_{i + 1}",
                    run_type=DagRunType.MANUAL,
                    start_date=start_date,
                    logical_date=start_date,
                    run_after=start_date,
                    state=(DagRunState.FAILED if i % 2 == 0 else DagRunState.SUCCESS),
                    triggered_by=DagRunTriggeredByType.TEST,
                )
                if dag_run.start_date is not None:
                    dag_run.end_date = dag_run.start_date + pendulum.duration(hours=1)
                session.add(dag_run)
        session.commit()

    @pytest.mark.parametrize(
        ("query_params", "expected_ids", "expected_total_dag_runs"),
        [
            # Filters
            ({}, [DAG1_ID, DAG2_ID], 11),
            ({"limit": 1}, [DAG1_ID], 2),
            ({"offset": 1}, [DAG1_ID, DAG2_ID], 11),
            ({"tags": ["example"]}, [DAG1_ID], 6),
            ({"exclude_stale": False}, [DAG1_ID, DAG2_ID, DAG3_ID], 15),
            ({"paused": True, "exclude_stale": False}, [DAG3_ID], 4),
            ({"paused": False}, [DAG1_ID, DAG2_ID], 11),
            ({"owners": ["airflow"]}, [DAG1_ID, DAG2_ID], 11),
            ({"owners": ["test_owner"], "exclude_stale": False}, [DAG3_ID], 4),
            ({"dag_ids": [DAG1_ID]}, [DAG1_ID], 6),
            ({"dag_ids": [DAG1_ID, DAG2_ID]}, [DAG1_ID, DAG2_ID], 11),
            ({"last_dag_run_state": "success", "exclude_stale": False}, [DAG1_ID, DAG2_ID, DAG3_ID], 6),
            ({"last_dag_run_state": "failed", "exclude_stale": False}, [DAG1_ID, DAG2_ID, DAG3_ID], 9),
            # Search
            ({"dag_id_pattern": "1"}, [DAG1_ID], 6),
            ({"dag_display_name_pattern": "test_dag2"}, [DAG2_ID], 5),
            # Bundle filters
            ({"bundle_name": "dag_maker"}, [DAG1_ID, DAG2_ID], 11),
            ({"bundle_name": "wrong_bundle"}, [], 0),
            ({"bundle_version": "some_commit_hash"}, [DAG1_ID, DAG2_ID], 11),
            ({"bundle_version": "wrong_version"}, [], 0),
            ({"bundle_name": "dag_maker", "bundle_version": "some_commit_hash"}, [DAG1_ID, DAG2_ID], 11),
            ({"bundle_name": "dag_maker", "bundle_version": "wrong_version"}, [], 0),
            ({"bundle_name": "wrong_bundle", "bundle_version": "some_commit_hash"}, [], 0),
        ],
    )
    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_should_return_200(self, test_client, query_params, expected_ids, expected_total_dag_runs):
        response = test_client.get("/dags", params=query_params)
        assert response.status_code == 200
        body = response.json()
        required_dag_run_key = [
            "dag_id",
            "run_id",
            "state",
            "run_after",
            "start_date",
            "end_date",
            "logical_date",
        ]
        for recent_dag_runs in body["dags"]:
            dag_runs = recent_dag_runs["latest_dag_runs"]
            # check date ordering
            previous_run_after = None
            for dag_run in dag_runs:
                # validate the response
                for key in required_dag_run_key:
                    assert key in dag_run
                if previous_run_after:
                    assert previous_run_after > dag_run["run_after"]
                previous_run_after = dag_run["run_after"]

    @pytest.fixture
    def setup_hitl_data(self, create_task_instance: TaskInstance, session: Session):
        """Setup HITL test data for parametrized tests."""
        # 3 Dags (test_dag0 created here and test_dag1, test_dag2 created in setup_dag_runs)
        # 5 task instances in test_dag0
        TI_COUNT = 5
        tis = [
            create_task_instance(
                dag_id="test_dag0",
                run_id=f"hitl_run_{ti_i}",
                task_id=f"test_task_{ti_i}",
                session=session,
                state=TaskInstanceState.DEFERRED,
            )
            for ti_i in range(TI_COUNT)
        ]
        session.add_all(tis)
        session.commit()

        # test_dag_0 has 3 HITL details not responded, 2 already responded
        # test_dag_0 has 3 HITL details that have not been responded to, while 2 have already received responses.
        hitl_detail_models = [
            HITLDetail(
                ti_id=tis[i].id,
                options=["Approve", "Reject"],
                subject=f"This is subject {i}",
                defaults=["Approve"],
            )
            for i in range(3)
        ] + [
            HITLDetail(
                ti_id=tis[i].id,
                options=["Approve", "Reject"],
                subject=f"This is subject {i}",
                defaults=["Approve"],
                responded_at=utcnow(),
                chosen_options=["Approve"],
                responded_by={"id": "test", "name": "test"},
            )
            for i in range(3, 5)
        ]
        session.add_all(hitl_detail_models)
        session.commit()

    @pytest.mark.parametrize(
        ("has_pending_actions", "expected_total_entries", "expected_pending_actions"),
        [
            # Without has_pending_actions param, should query all DAGs
            (None, 3, None),
            # With has_pending_actions=True, should only query DAGs with pending actions
            (
                True,
                1,
                [
                    {
                        "task_instance": mock.ANY,
                        "options": ["Approve", "Reject"],
                        "subject": f"This is subject {i}",
                        "defaults": ["Approve"],
                        "multiple": False,
                        "params": {},
                        "params_input": {},
                        "response_received": False,
                        "assigned_users": [],
                        "created_at": mock.ANY,
                    }
                    for i in range(3)
                ],
            ),
        ],
    )
    def test_should_return_200_with_hitl(
        self,
        test_client: TestClient,
        setup_hitl_data,
        has_pending_actions,
        expected_total_entries,
        expected_pending_actions,
    ):
        # Build query params
        params = {}
        if has_pending_actions is not None:
            params["has_pending_actions"] = has_pending_actions

        # Make request
        response = test_client.get("/dags", params=params)
        assert response.status_code == 200

        body = response.json()
        assert body["total_entries"] == expected_total_entries

        # Check pending_actions structure when specified
        if expected_pending_actions is not None:
            for dag_json in body["dags"]:
                pending_actions = dag_json["pending_actions"]
                pending_actions.sort(key=lambda x: x["subject"])
                assert pending_actions == expected_pending_actions

    def test_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dags", params={})
        assert response.status_code == 401

    def test_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dags", params={})
        assert response.status_code == 403

    def test_get_dags_no_n_plus_one_queries(self, session, test_client):
        """Test that fetching DAGs with tags doesn't trigger n+1 queries."""
        num_dags = 5
        for i in range(num_dags):
            dag_id = f"test_dag_queries_ui_{i}"
            dag_model = DagModel(
                dag_id=dag_id,
                bundle_name="dag_maker",
                fileloc=f"/tmp/{dag_id}.py",
                is_stale=False,
            )
            session.add(dag_model)
            session.flush()

            for j in range(3):
                tag = DagTag(name=f"tag_ui_{i}_{j}", dag_id=dag_id)
                session.add(tag)

        session.commit()
        session.expire_all()

        with count_queries() as result:
            response = test_client.get("/dags", params={"limit": 10})

        assert response.status_code == 200
        body = response.json()
        dags_with_our_prefix = [d for d in body["dags"] if d["dag_id"].startswith("test_dag_queries_ui_")]
        assert len(dags_with_our_prefix) == num_dags
        for dag in dags_with_our_prefix:
            assert len(dag["tags"]) == 3

        first_query_count = sum(result.values())

        # Add more DAGs and verify query count doesn't scale linearly
        for i in range(num_dags, num_dags + 3):
            dag_id = f"test_dag_queries_ui_{i}"
            dag_model = DagModel(
                dag_id=dag_id,
                bundle_name="dag_maker",
                fileloc=f"/tmp/{dag_id}.py",
                is_stale=False,
            )
            session.add(dag_model)
            session.flush()

            for j in range(3):
                tag = DagTag(name=f"tag_ui_{i}_{j}", dag_id=dag_id)
                session.add(tag)

        session.commit()
        session.expire_all()

        with count_queries() as result2:
            response = test_client.get("/dags", params={"limit": 15})

        assert response.status_code == 200
        second_query_count = sum(result2.values())

        # With n+1, adding 3 DAGs would add ~3 tag queries
        # Without n+1, query count should remain nearly identical
        assert second_query_count - first_query_count < 3, (
            f"Added 3 DAGs but query count increased by {second_query_count - first_query_count} "
            f"({first_query_count} → {second_query_count}), suggesting n+1 queries for tags"
        )

    @pytest.mark.usefixtures("configure_git_connection_for_dag_bundle")
    def test_latest_run_should_return_200(self, test_client):
        response = test_client.get(f"/dags/{DAG1_ID}/latest_run")
        assert response.status_code == 200
        body = response.json()
        assert body == {
            "id": mock.ANY,
            "dag_id": "test_dag1",
            "run_id": "run_id_5",
            "logical_date": "2025-01-01T00:00:00Z",
            "run_after": "2025-01-01T00:00:00Z",
            "start_date": "2025-01-01T00:00:00Z",
            "end_date": "2025-01-01T01:00:00Z",
            "state": "failed",
            "duration": 3600.0,
        }

    def test_latest_run_should_response_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"/dags/{DAG1_ID}/latest_run")
        assert response.status_code == 401

    def test_latest_run_should_response_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"/dags/{DAG1_ID}/latest_run")
        assert response.status_code == 403

    @pytest.mark.parametrize(
        ("query_params", "expected_dag_count"),
        [
            ({"has_asset_schedule": True}, 3),
            ({"has_asset_schedule": False}, 2),
            ({"asset_dependency": "test_asset"}, 1),
            ({"asset_dependency": "dataset"}, 1),
            ({"asset_dependency": "bucket"}, 1),
            ({"asset_dependency": "s3://"}, 1),
            ({"asset_dependency": "nonexistent"}, 0),
            ({"has_asset_schedule": True, "asset_dependency": "test_asset"}, 1),  # Combined filters
            ({"has_asset_schedule": False, "asset_dependency": "test_asset"}, 0),  # No match
        ],
    )
    def test_asset_filtering(self, test_client, query_params, expected_dag_count, session):
        """Test asset-based filtering on the UI DAGs endpoint."""

        self._create_asset_test_data(session)

        response = test_client.get("/dags", params=query_params)
        assert response.status_code == 200
        body = response.json()
        assert body["total_entries"] == expected_dag_count
        assert len(body["dags"]) == expected_dag_count

    def test_is_favorite_field_with_multiple_favorites(self, test_client, session):
        """Test is_favorite field with multiple DAGs marked as favorites."""
        # Mark both DAG1 and DAG2 as favorites
        session.add(DagFavorite(user_id="test", dag_id=DAG1_ID))
        session.add(DagFavorite(user_id="test", dag_id=DAG2_ID))
        session.commit()

        response = test_client.get("/dags")
        assert response.status_code == 200
        body = response.json()

        # Count favorites in response
        favorite_count = sum(1 for dag in body["dags"] if dag["is_favorite"])
        assert favorite_count == 2

        # Verify specific DAGs are marked as favorites
        dag_favorites = {dag["dag_id"]: dag["is_favorite"] for dag in body["dags"]}
        assert dag_favorites[DAG1_ID] is True
        assert dag_favorites[DAG2_ID] is True

    def test_is_favorite_field_user_specific(self, test_client, session):
        """Test that is_favorite field is user-specific."""
        # Mark DAG1 as favorite for a different user
        session.add(DagFavorite(user_id="other_user", dag_id=DAG1_ID))
        session.commit()

        # Request as the test user (not other_user)
        response = test_client.get("/dags")
        assert response.status_code == 200
        body = response.json()

        # Verify that DAG1 is not marked as favorite for the test user
        dag1_data = next(dag for dag in body["dags"] if dag["dag_id"] == DAG1_ID)
        assert dag1_data["is_favorite"] is False
